package indexreader

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

const (
	AcquiredBundleType        = "gonimbus.index.acquired_bundle.v1"
	AcquiredBundleSchema      = "gonimbus/v1.0.0/index-acquired-bundle.v1"
	AcquiredBundleTypeV2      = "gonimbus.index.acquired_bundle.v2"
	AcquiredBundleSchemaV2    = "gonimbus/v1.0.0/index-acquired-bundle.v2"
	HubMarkerSchemaV2         = "gonimbus.index.hub_marker.v2"
	maxAcquiredSegments       = 200_000
	maxAcquiredLineageNodes   = 64
	acquiredHubFormat         = "durable-v2"
	acquiredIdentityRole      = "source_identity"
	acquiredHubCompleteRole   = "hub_complete"
	acquiredManifestRole      = "manifest"
	acquiredSegmentRole       = "segment"
	acquiredLocalCompleteRole = "local_complete"
)

var (
	fullAcquiredIndexSetRE = regexp.MustCompile(`^idx_[0-9a-f]{64}$`)
	acquiredRunIDRE        = regexp.MustCompile(`^run_([0-9]{1,32}|[0-9A-HJKMNP-TV-Z]{26})$`)
	acquiredSegmentPathRE  = regexp.MustCompile(`^segments/[A-Za-z0-9._-]+$`)

	// ErrHubExactRead deliberately omits provider error text. A provider may put
	// account coordinates, request IDs, or credential-adjacent details in errors.
	ErrHubExactRead = errors.New("hub exact-object read failed")
	// ErrAcquiredBundleConflict means the final destination already names a
	// different or unverifiable acquisition. It is never overlaid or repaired.
	ErrAcquiredBundleConflict = errors.New("acquired bundle destination conflict")
	// ErrNotAcquiredBundle is the stable refusal for hydrate directories,
	// canonical index directories, and other unmarked paths.
	ErrNotAcquiredBundle = errors.New("not a final acquired bundle")

	acquiredBeforeDestinationBind func(parentPath string) error
	acquiredBeforeOpenRootBind    func(rootPath string) error
	acquiredBeforeStageCleanup    func(stagePath string) error
	acquiredAfterMetadataWrite    func(stagePath, relativePath string) error
	acquiredSyncParentDirectory   = syncAcquiredDirectory
)

// HubExactObjectReader is the entire authority surface exposed to acquisition.
// Implementations open only the exact key supplied by the library. Listing,
// latest selection, write, and delete are deliberately absent.
type HubExactObjectReader interface {
	OpenExact(ctx context.Context, key string) (body io.ReadCloser, sizeBytes int64, err error)
}

// AcquireBundleOptions identifies one immutable acquisition. Destination is the
// final bundle directory. ProofThroughRunID, when set, acquires and verifies the
// exact continuous ancestry from RunID through that run without ancestor rows.
type AcquireBundleOptions struct {
	IndexSetID        string
	RunID             string
	ProofThroughRunID string
	Destination       string
	Limits            AcquiredBundleLimits
	// Warning receives post-commit durability/cleanup warnings. A successful
	// no-replace rename remains authoritative and is never reported as failure.
	Warning func(message string)
}

// AcquiredBundleLimits bounds every remotely supplied allocation/read.
type AcquiredBundleLimits struct {
	MaxMarkerBytes    int64
	MaxManifestBytes  int64
	MaxIdentityBytes  int64
	MaxSegmentBytes   int64
	MaxSegments       int
	MaxLineageNodes   int
	MaxAggregateBytes int64
	MaxMetadataBytes  int64
}

func (l AcquiredBundleLimits) normalize() AcquiredBundleLimits {
	l.MaxMarkerBytes = normalizeAcquiredByteLimit(l.MaxMarkerBytes, 1<<20)
	l.MaxManifestBytes = normalizeAcquiredByteLimit(l.MaxManifestBytes, 64<<20)
	l.MaxIdentityBytes = normalizeAcquiredByteLimit(l.MaxIdentityBytes, 1<<20)
	l.MaxSegmentBytes = normalizeAcquiredByteLimit(l.MaxSegmentBytes, 1<<40)
	l.MaxAggregateBytes = normalizeAcquiredByteLimit(l.MaxAggregateBytes, 100<<40)
	l.MaxMetadataBytes = normalizeAcquiredByteLimit(
		l.MaxMetadataBytes,
		indexsubstrate.DefaultAncestryMaxAggregateBytes,
	)
	if l.MaxSegments <= 0 || l.MaxSegments > maxAcquiredSegments {
		l.MaxSegments = maxAcquiredSegments
	}
	if l.MaxLineageNodes <= 0 || l.MaxLineageNodes > maxAcquiredLineageNodes {
		l.MaxLineageNodes = maxAcquiredLineageNodes
	}
	return l
}

func normalizeAcquiredByteLimit(value, defaultValue int64) int64 {
	if value <= 0 {
		value = defaultValue
	}
	if value > maxBridgeJSONCount {
		return maxBridgeJSONCount
	}
	return value
}

// AcquiredArtifact binds one bundle-internal relative path.
type AcquiredArtifact struct {
	Path      string `json:"path"`
	Role      string `json:"role"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// AcquiredLineageNode binds the hub and manifest bytes for one verified node.
type AcquiredLineageNode struct {
	IndexSetID               string `json:"index_set_id"`
	RunID                    string `json:"run_id"`
	MarkerSchemaVersion      string `json:"marker_schema_version,omitempty"`
	HubCompleteSHA256        string `json:"hub_complete_sha256"`
	ManifestSHA256           string `json:"manifest_sha256"`
	ConversionIdentitySHA256 string `json:"conversion_identity_sha256,omitempty"`
}

// AcquiredBundleMarker is the final acquisition receipt. It intentionally
// contains no source coordinates, provider configuration, or credential handle.
type AcquiredBundleMarker struct {
	Type                        string                `json:"type"`
	Schema                      string                `json:"schema"`
	IndexSetID                  string                `json:"index_set_id"`
	RunID                       string                `json:"run_id"`
	ProofThroughRunID           string                `json:"proof_through_run_id,omitempty"`
	SourceIdentitySHA256        string                `json:"source_identity_sha256"`
	SourceIdentitySchema        string                `json:"source_identity_schema"`
	SourceIdentityProfile       string                `json:"source_identity_profile"`
	HubMarkerSchemaVersion      string                `json:"hub_marker_schema_version,omitempty"`
	RunStart                    *BridgeRunStart       `json:"run_start,omitempty"`
	SnapshotTime                *BridgeSnapshotTime   `json:"snapshot_time,omitempty"`
	ConversionIdentitySHA256    string                `json:"conversion_identity_sha256,omitempty"`
	SnapshotCompletedAt         string                `json:"snapshot_completed_at,omitempty"`
	SnapshotCompletionSemantics string                `json:"snapshot_completion_semantics,omitempty"`
	HubCommittedAt              string                `json:"hub_committed_at"`
	HubCompleteSHA256           string                `json:"hub_complete_sha256"`
	ManifestSHA256              string                `json:"manifest_sha256"`
	AcquiredAt                  string                `json:"acquired_at"`
	Lineage                     []AcquiredLineageNode `json:"lineage"`
	Artifacts                   []AcquiredArtifact    `json:"artifacts"`
}

type acquiredHubArtifact struct {
	Path      string `json:"path"`
	Role      string `json:"role"`
	Required  bool   `json:"required"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type acquiredHubComplete struct {
	Version                     string             `json:"version"`
	MarkerSchemaVersion         string             `json:"marker_schema_version"`
	Format                      string             `json:"format"`
	FormatVersion               string             `json:"format_version"`
	IndexSetID                  string             `json:"index_set_id"`
	RunID                       string             `json:"run_id"`
	LegacyMarkerSchemaVersion   string             `json:"legacy_marker_schema_version,omitempty"`
	LegacyMarkerSHA256          string             `json:"legacy_marker_sha256,omitempty"`
	IdentitySchema              string             `json:"identity_schema,omitempty"`
	IdentityProfile             string             `json:"identity_profile,omitempty"`
	RunStart                    BridgeRunStart     `json:"run_start,omitempty"`
	SnapshotTime                BridgeSnapshotTime `json:"snapshot_time,omitempty"`
	ConversionIdentitySHA256    string             `json:"conversion_identity_sha256,omitempty"`
	CompletedAt                 string             `json:"completed_at"`
	SnapshotCompletedAt         string             `json:"snapshot_completed_at"`
	SnapshotCompletionSemantics string             `json:"snapshot_completion_semantics,omitempty"`
	HubCommittedAt              string             `json:"hub_committed_at"`
	ExportedBy                  string             `json:"exported_by"`
	Artifacts                   struct {
		Identity acquiredHubArtifact   `json:"identity_json"`
		Manifest acquiredHubArtifact   `json:"manifest"`
		Segments []acquiredHubArtifact `json:"segments"`
	} `json:"artifacts"`
	Durable struct {
		ManifestType       string `json:"manifest_type"`
		ManifestRender     string `json:"manifest_render"`
		IndexSchemaVersion int    `json:"index_schema_version"`
		SegmentNamespace   string `json:"segment_namespace"`
		Segments           int    `json:"segments"`
		Rows               int    `json:"rows"`
	} `json:"durable"`
}

func (hub acquiredHubComplete) bridgeV3() bridgeHubCompleteV3 {
	return bridgeHubCompleteV3{
		Version:                   hub.Version,
		MarkerSchemaVersion:       hub.MarkerSchemaVersion,
		Format:                    hub.Format,
		FormatVersion:             hub.FormatVersion,
		IndexSetID:                hub.IndexSetID,
		RunID:                     hub.RunID,
		LegacyMarkerSchemaVersion: hub.LegacyMarkerSchemaVersion,
		LegacyMarkerSHA256:        hub.LegacyMarkerSHA256,
		IdentitySchema:            hub.IdentitySchema,
		IdentityProfile:           hub.IdentityProfile,
		RunStart:                  hub.RunStart,
		SnapshotTime:              hub.SnapshotTime,
		ConversionIdentitySHA256:  hub.ConversionIdentitySHA256,
		HubCommittedAt:            hub.HubCommittedAt,
		ExportedBy:                hub.ExportedBy,
		Artifacts: bridgeV3Artifacts{
			Identity: acquiredBridgeArtifactRef(hub.Artifacts.Identity),
			Manifest: acquiredBridgeArtifactRef(hub.Artifacts.Manifest),
			Segments: acquiredBridgeArtifactRefs(hub.Artifacts.Segments),
		},
		Durable: bridgeDurableSummary{
			ManifestType:       hub.Durable.ManifestType,
			ManifestRender:     hub.Durable.ManifestRender,
			IndexSchemaVersion: hub.Durable.IndexSchemaVersion,
			SegmentNamespace:   hub.Durable.SegmentNamespace,
			Segments:           hub.Durable.Segments,
			Rows:               hub.Durable.Rows,
		},
	}
}

func acquiredBridgeArtifactRef(ref acquiredHubArtifact) bridgeArtifactRef {
	return bridgeArtifactRef{
		Path: ref.Path, Role: ref.Role, Required: ref.Required,
		SizeBytes: ref.SizeBytes, SHA256: ref.SHA256,
	}
}

func acquiredBridgeArtifactRefs(refs []acquiredHubArtifact) []bridgeArtifactRef {
	out := make([]bridgeArtifactRef, len(refs))
	for i := range refs {
		out[i] = acquiredBridgeArtifactRef(refs[i])
	}
	return out
}

type acquiredLocalComplete struct {
	Type                        string `json:"type"`
	IndexSetID                  string `json:"index_set_id"`
	RunID                       string `json:"run_id"`
	SnapshotCompletedAt         string `json:"snapshot_completed_at,omitempty"`
	SnapshotCompletionSemantics string `json:"snapshot_completion_semantics,omitempty"`
	ManifestPath                string `json:"manifest_path"`
	ManifestSHA256              string `json:"manifest_sha256"`
	SegmentDir                  string `json:"segment_dir"`
	Segments                    int    `json:"segments"`
}

type acquiredRunMaterial struct {
	hub         acquiredHubComplete
	hubSHA      string
	manifest    indexsubstrate.InternalManifest
	manifestSHA string
	runDirRel   string
}

type bundleStage struct {
	rootPath string
	name     string
	parent   *boundAcquiredDirectory
	root     *os.File
	rootCap  *os.Root
	rootInfo os.FileInfo
	dirs     map[string]*os.File
}

type boundAcquiredDirectory struct {
	path string
	root *os.Root
	file *os.File
	info os.FileInfo
}

func newBundleStage(parent *boundAcquiredDirectory, finalBase string) (*bundleStage, error) {
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("create acquisition nonce: %w", err)
	}
	name := "." + finalBase + ".acquire-" + hex.EncodeToString(nonce[:])
	stagePath := filepath.Join(parent.path, name)
	root, err := createDirectoryAt(parent.file, name, stagePath, true)
	if err != nil {
		return nil, fmt.Errorf("create acquisition staging directory: %w", err)
	}
	rootInfo, err := root.Stat()
	if err != nil {
		_ = root.Close()
		_ = parent.root.Remove(name)
		return nil, fmt.Errorf("inspect acquisition staging directory: %w", err)
	}
	rootCap, err := parent.root.OpenRoot(name)
	if err != nil {
		_ = root.Close()
		_ = parent.root.Remove(name)
		return nil, fmt.Errorf("bind acquisition staging directory: %w", err)
	}
	bound, err := rootCap.Stat(".")
	if err != nil || !os.SameFile(rootInfo, bound) {
		_ = rootCap.Close()
		_ = root.Close()
		_ = parent.root.Remove(name)
		return nil, errors.Join(fmt.Errorf("acquisition staging directory binding changed"), err)
	}
	return &bundleStage{
		rootPath: stagePath, name: name, parent: parent,
		root: root, rootCap: rootCap, rootInfo: rootInfo,
		dirs: map[string]*os.File{"": root},
	}, nil
}

func (s *bundleStage) ensureDir(rel string) (*os.File, error) {
	rel, err := normalizeBundleRelativePath(rel, true)
	if err != nil {
		return nil, err
	}
	if rel == "" {
		return s.root, nil
	}
	current := ""
	parent := s.root
	for _, component := range strings.Split(rel, "/") {
		next := component
		if current != "" {
			next = current + "/" + component
		}
		if handle := s.dirs[next]; handle != nil {
			parent = handle
			current = next
			continue
		}
		handle, createErr := createDirectoryAt(parent, component, filepath.Join(s.rootPath, filepath.FromSlash(next)), false)
		if createErr != nil {
			return nil, fmt.Errorf("create acquisition directory: %w", createErr)
		}
		s.dirs[next] = handle
		parent = handle
		current = next
	}
	return parent, nil
}

func (s *bundleStage) createFile(rel string) (*os.File, error) {
	rel, err := normalizeBundleRelativePath(rel, false)
	if err != nil {
		return nil, err
	}
	dir, base := path.Split(rel)
	parent, err := s.ensureDir(strings.TrimSuffix(dir, "/"))
	if err != nil {
		return nil, err
	}
	f, err := createFileExclusiveAt(parent, base, filepath.Join(s.rootPath, filepath.FromSlash(rel)))
	if err != nil {
		return nil, fmt.Errorf("create acquisition artifact: %w", err)
	}
	return f, nil
}

func (s *bundleStage) writeBytes(rel string, data []byte) (AcquiredArtifact, error) {
	f, err := s.createFile(rel)
	if err != nil {
		return AcquiredArtifact{}, err
	}
	if err := writeAllAndSync(f, bytes.NewReader(data)); err != nil {
		return AcquiredArtifact{}, err
	}
	sum := sha256.Sum256(data)
	return AcquiredArtifact{Path: rel, SizeBytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}, nil
}

func writeAllAndSync(f *os.File, src io.Reader) (err error) {
	if f == nil {
		return fmt.Errorf("acquisition artifact handle is nil")
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if _, err = io.Copy(f, src); err != nil {
		return fmt.Errorf("write acquisition artifact: %w", err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("sync acquisition artifact: %w", err)
	}
	return nil
}

func (s *bundleStage) closeArtifactHandles() error {
	var err error
	keys := make([]string, 0, len(s.dirs))
	for key := range s.dirs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, key := range keys {
		handle := s.dirs[key]
		if handle == nil {
			continue
		}
		if syncErr := syncAcquiredDirectory(handle); syncErr != nil {
			err = errors.Join(err, syncErr)
		}
		err = errors.Join(err, handle.Close())
		s.dirs[key] = nil
	}
	s.root = nil
	return err
}

func (s *bundleStage) close() error {
	if s == nil {
		return nil
	}
	err := s.closeArtifactHandles()
	if s.rootCap != nil {
		err = errors.Join(err, s.rootCap.Close())
		s.rootCap = nil
	}
	return err
}

func (s *bundleStage) prepareRename() error {
	if s == nil || s.root == nil {
		return fmt.Errorf("acquisition stage is closed")
	}
	var err error
	for key, handle := range s.dirs {
		if key == "" || handle == nil {
			continue
		}
		err = errors.Join(err, syncAcquiredDirectory(handle), handle.Close())
		s.dirs[key] = nil
	}
	err = errors.Join(err, syncAcquiredDirectory(s.root))
	if s.rootCap != nil {
		err = errors.Join(err, s.rootCap.Close())
		s.rootCap = nil
	}
	return err
}

func (s *bundleStage) cleanup() {
	if s == nil {
		return
	}
	if acquiredBeforeStageCleanup != nil {
		if err := acquiredBeforeStageCleanup(s.rootPath); err != nil {
			_ = s.close()
			return
		}
	}
	_ = s.closeArtifactHandles()
	if s.rootCap == nil {
		rootCap, err := s.parent.root.OpenRoot(s.name)
		if err != nil {
			return
		}
		bound, err := rootCap.Stat(".")
		if err != nil || !os.SameFile(s.rootInfo, bound) {
			_ = rootCap.Close()
			return
		}
		s.rootCap = rootCap
	}
	dir, err := s.rootCap.Open(".")
	if err == nil {
		entries, readErr := dir.ReadDir(-1)
		_ = dir.Close()
		if readErr == nil {
			for _, entry := range entries {
				_ = s.rootCap.RemoveAll(entry.Name())
			}
		}
	}
	_ = s.rootCap.Close()
	s.rootCap = nil

	// Never recursively delete by the staging pathname. Only unlink the now
	// empty directory when the parent's retained capability still names the
	// exact inode created for this stage.
	named, err := s.parent.root.Lstat(s.name)
	if err != nil || !named.IsDir() || named.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(s.rootInfo, named) {
		return
	}
	_ = s.parent.root.Remove(s.name)
	_ = acquiredSyncParentDirectory(s.parent.file)
}

func exactHubKey(indexSetID, runID, name string) string {
	return path.Join("index-sets", indexSetID, "runs", runID, name)
}

func downloadExactBytes(ctx context.Context, source HubExactObjectReader, key string, maxBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, fmt.Errorf("hub exact-object reader is required")
	}
	body, declared, err := source.OpenExact(ctx, key)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, ErrHubExactRead
	}
	if body == nil {
		return nil, ErrHubExactRead
	}
	defer func() { _ = body.Close() }()
	if declared < 0 || declared > maxBytes {
		return nil, fmt.Errorf("hub object declared size exceeds acquisition limit")
	}
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, ErrHubExactRead
	}
	if int64(len(data)) > maxBytes || int64(len(data)) != declared {
		return nil, fmt.Errorf("hub object size mismatch")
	}
	return data, nil
}

func downloadExactArtifact(ctx context.Context, source HubExactObjectReader, stage *bundleStage, key, rel string, ref acquiredHubArtifact, maxBytes int64) (AcquiredArtifact, error) {
	if err := ctx.Err(); err != nil {
		return AcquiredArtifact{}, err
	}
	if ref.SizeBytes < 0 || ref.SizeBytes > maxBytes {
		return AcquiredArtifact{}, fmt.Errorf("hub artifact declared size exceeds acquisition limit")
	}
	body, declared, err := source.OpenExact(ctx, key)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return AcquiredArtifact{}, ctxErr
		}
		return AcquiredArtifact{}, ErrHubExactRead
	}
	if body == nil {
		return AcquiredArtifact{}, ErrHubExactRead
	}
	defer func() { _ = body.Close() }()
	if declared != ref.SizeBytes {
		return AcquiredArtifact{}, fmt.Errorf("hub artifact declared size mismatch")
	}
	f, err := stage.createFile(rel)
	if err != nil {
		return AcquiredArtifact{}, err
	}
	h := sha256.New()
	limited := io.LimitReader(body, ref.SizeBytes+1)
	var writeErr error
	n, copyErr := io.Copy(io.MultiWriter(f, h), limited)
	if copyErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			writeErr = ctxErr
		} else {
			writeErr = ErrHubExactRead
		}
	} else if n != ref.SizeBytes {
		writeErr = fmt.Errorf("hub artifact size mismatch")
	} else if got := hex.EncodeToString(h.Sum(nil)); got != ref.SHA256 {
		writeErr = fmt.Errorf("hub artifact digest mismatch")
	} else if syncErr := f.Sync(); syncErr != nil {
		writeErr = fmt.Errorf("sync acquisition artifact: %w", syncErr)
	}
	writeErr = errors.Join(writeErr, f.Close())
	if writeErr != nil {
		return AcquiredArtifact{}, writeErr
	}
	return AcquiredArtifact{Path: rel, SizeBytes: n, SHA256: ref.SHA256}, nil
}

func downloadExactMetadataArtifact(
	ctx context.Context,
	source HubExactObjectReader,
	stage *bundleStage,
	key, rel string,
	ref acquiredHubArtifact,
	maxBytes int64,
) (AcquiredArtifact, []byte, error) {
	if ref.SizeBytes < 0 || ref.SizeBytes > maxBytes {
		return AcquiredArtifact{}, nil, fmt.Errorf("hub metadata declared size exceeds acquisition limit")
	}
	data, err := downloadExactBytes(ctx, source, key, maxBytes)
	if err != nil {
		return AcquiredArtifact{}, nil, err
	}
	if int64(len(data)) != ref.SizeBytes || sha256Hex(data) != ref.SHA256 {
		return AcquiredArtifact{}, nil, fmt.Errorf("hub metadata digest or size mismatch")
	}
	artifact, err := stage.writeBytes(rel, data)
	if err != nil {
		return AcquiredArtifact{}, nil, err
	}
	if artifact.SizeBytes != ref.SizeBytes || artifact.SHA256 != ref.SHA256 {
		return AcquiredArtifact{}, nil, fmt.Errorf("staged hub metadata digest or size mismatch")
	}
	if acquiredAfterMetadataWrite != nil {
		if err := acquiredAfterMetadataWrite(stage.rootPath, rel); err != nil {
			return AcquiredArtifact{}, nil, err
		}
	}
	return artifact, data, nil
}

// AcquireBundle downloads and verifies exactly one durable hub run and optional
// proof-through ancestry, then publishes acquired.json last via atomic
// no-replace directory rename.
func AcquireBundle(ctx context.Context, source HubExactObjectReader, opts AcquireBundleOptions) (AcquiredBundleMarker, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return AcquiredBundleMarker{}, err
	}
	if !fullAcquiredIndexSetRE.MatchString(opts.IndexSetID) {
		return AcquiredBundleMarker{}, fmt.Errorf("acquire requires a full lowercase index_set_id")
	}
	if !acquiredRunIDRE.MatchString(opts.RunID) {
		return AcquiredBundleMarker{}, fmt.Errorf("acquire requires an exact run_id")
	}
	if opts.ProofThroughRunID != "" && !acquiredRunIDRE.MatchString(opts.ProofThroughRunID) {
		return AcquiredBundleMarker{}, fmt.Errorf("acquire proof-through requires an exact run_id")
	}
	limits := opts.Limits.normalize()
	finalPath, _, finalBase, parent, err := bindAcquiredDestination(opts.Destination)
	if err != nil {
		return AcquiredBundleMarker{}, err
	}
	defer func() { _ = parent.close() }()
	stage, err := newBundleStage(parent, finalBase)
	if err != nil {
		return AcquiredBundleMarker{}, err
	}
	keepStage := false
	defer func() {
		if !keepStage {
			stage.cleanup()
		}
	}()

	var aggregate int64
	var metadata int64
	addAggregate := func(n int64) error {
		if n < 0 || aggregate > limits.MaxAggregateBytes-n {
			return fmt.Errorf("acquisition aggregate byte limit exceeded")
		}
		aggregate += n
		return nil
	}
	addMetadata := func(n int64) error {
		if n < 0 || metadata > limits.MaxMetadataBytes-n {
			return fmt.Errorf("acquisition metadata byte limit exceeded")
		}
		metadata += n
		return nil
	}

	loadRun := func(runID string, current bool) (acquiredRunMaterial, []AcquiredArtifact, error) {
		runRel := "runs/" + runID
		hubData, readErr := downloadExactBytes(ctx, source, exactHubKey(opts.IndexSetID, runID, "complete.json"), limits.MaxMarkerBytes)
		if readErr != nil {
			return acquiredRunMaterial{}, nil, readErr
		}
		if err := addAggregate(int64(len(hubData))); err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		if err := addMetadata(int64(len(hubData))); err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		var hub acquiredHubComplete
		if err := decodeStrictJSON(hubData, &hub); err != nil {
			return acquiredRunMaterial{}, nil, fmt.Errorf("invalid durable hub complete marker")
		}
		if err := validateAcquiredHubComplete(opts.IndexSetID, runID, hub, limits); err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		hubArtifact, err := stage.writeBytes(runRel+"/hub-complete.json", hubData)
		if err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		hubArtifact.Role = acquiredHubCompleteRole

		manifestArtifact, manifestData, err := downloadExactMetadataArtifact(ctx, source, stage,
			exactHubKey(opts.IndexSetID, runID, "manifest.json"),
			runRel+"/manifest.json", hub.Artifacts.Manifest, limits.MaxManifestBytes)
		if err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		if err := addAggregate(manifestArtifact.SizeBytes); err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		if err := addMetadata(manifestArtifact.SizeBytes); err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		manifestArtifact.Role = acquiredManifestRole
		var manifest indexsubstrate.InternalManifest
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			return acquiredRunMaterial{}, nil, fmt.Errorf("parse acquired manifest: %w", err)
		}
		if err := validateAcquiredManifest(opts.IndexSetID, runID, hub, manifest); err != nil {
			return acquiredRunMaterial{}, nil, err
		}
		if err := validateAcquiredHubTime(hub, manifest); err != nil {
			return acquiredRunMaterial{}, nil, err
		}

		artifacts := []AcquiredArtifact{hubArtifact, manifestArtifact}
		if current {
			for i := range manifest.Segments {
				if err := ctx.Err(); err != nil {
					return acquiredRunMaterial{}, nil, err
				}
				ref := hub.Artifacts.Segments[i]
				segmentArtifact, err := downloadExactArtifact(ctx, source, stage,
					exactHubKey(opts.IndexSetID, runID, ref.Path),
					runRel+"/"+ref.Path, ref, limits.MaxSegmentBytes)
				if err != nil {
					return acquiredRunMaterial{}, nil, err
				}
				if err := addAggregate(segmentArtifact.SizeBytes); err != nil {
					return acquiredRunMaterial{}, nil, err
				}
				segmentArtifact.Role = acquiredSegmentRole
				artifacts = append(artifacts, segmentArtifact)
			}
		}
		return acquiredRunMaterial{
			hub: hub, hubSHA: sha256Hex(hubData), manifest: manifest,
			manifestSHA: manifestArtifact.SHA256, runDirRel: runRel,
		}, artifacts, nil
	}

	current, artifacts, err := loadRun(opts.RunID, true)
	if err != nil {
		return AcquiredBundleMarker{}, err
	}
	identityKey := exactHubKey(opts.IndexSetID, opts.RunID, "identity.json")
	if current.hub.MarkerSchemaVersion == HubMarkerSchemaV3 {
		identityKey = path.Join("index-sets", opts.IndexSetID, "identity.json")
	}
	identityArtifact, identityData, err := downloadExactMetadataArtifact(ctx, source, stage,
		identityKey,
		"identity.json", current.hub.Artifacts.Identity, limits.MaxIdentityBytes)
	if err != nil {
		return AcquiredBundleMarker{}, err
	}
	if err := addAggregate(identityArtifact.SizeBytes); err != nil {
		return AcquiredBundleMarker{}, err
	}
	if err := addMetadata(identityArtifact.SizeBytes); err != nil {
		return AcquiredBundleMarker{}, err
	}
	identityArtifact.Role = acquiredIdentityRole
	artifacts = append([]AcquiredArtifact{identityArtifact}, artifacts...)
	identity, err := readVerifiedLocalIdentityBytes(identityData, opts.IndexSetID)
	if err != nil {
		return AcquiredBundleMarker{}, fmt.Errorf("canonical source identity refused: %w", err)
	}
	if identity.CompleteFileSHA256 != current.hub.Artifacts.Identity.SHA256 {
		return AcquiredBundleMarker{}, fmt.Errorf("source identity digest mismatch")
	}

	runs := []acquiredRunMaterial{current}
	for opts.ProofThroughRunID != "" && runs[len(runs)-1].manifest.RunID != opts.ProofThroughRunID {
		if len(runs) >= limits.MaxLineageNodes {
			return AcquiredBundleMarker{}, fmt.Errorf("acquisition lineage node limit exceeded")
		}
		child := runs[len(runs)-1]
		if child.manifest.StateParent == nil {
			return AcquiredBundleMarker{}, &indexsubstrate.LineageError{
				Code: indexsubstrate.LineageCodeBaselineNotAncestor, Message: "proof-through run is not an ancestor",
			}
		}
		parentID := strings.TrimSpace(child.manifest.StateParent.RunID)
		if !acquiredRunIDRE.MatchString(parentID) || child.manifest.StateParent.IndexSetID != opts.IndexSetID {
			return AcquiredBundleMarker{}, fmt.Errorf("acquired lineage parent identity is invalid")
		}
		parentRun, parentArtifacts, err := loadRun(parentID, false)
		if err != nil {
			return AcquiredBundleMarker{}, err
		}
		if parentRun.manifestSHA != child.manifest.StateParent.ManifestSHA256 {
			return AcquiredBundleMarker{}, fmt.Errorf("acquired lineage manifest digest mismatch")
		}
		if parentRun.hub.Artifacts.Identity.SHA256 != identity.CompleteFileSHA256 {
			return AcquiredBundleMarker{}, fmt.Errorf("acquired lineage source identity digest mismatch")
		}
		artifacts = append(artifacts, parentArtifacts...)
		runs = append(runs, parentRun)
	}

	manifests := make([]indexsubstrate.InternalManifest, 0, len(runs))
	for _, run := range runs {
		manifests = append(manifests, run.manifest)
	}
	if err := validateAcquiredManifestChain(opts.IndexSetID, opts.ProofThroughRunID, manifests); err != nil {
		return AcquiredBundleMarker{}, fmt.Errorf("verify acquired lineage proof: %w", err)
	}

	acquiredV2 := current.hub.MarkerSchemaVersion == HubMarkerSchemaV3

	// Relative adapter markers keep v1 bundles movable. Acquired v2 binds its
	// classified time directly and deliberately has no synthetic complete file.
	localCompleteArtifacts := make([]AcquiredArtifact, 0, len(runs))
	if !acquiredV2 {
		for _, run := range runs {
			completeRel := run.runDirRel + "/complete.json"
			completeData, err := acquiredLocalCompleteJSON(
				filepath.Join(stage.rootPath, filepath.FromSlash(run.runDirRel)),
				run.hub, run.manifestSHA, len(run.manifest.Segments))
			if err != nil {
				return AcquiredBundleMarker{}, err
			}
			artifact, err := stage.writeBytes(completeRel, completeData)
			if err != nil {
				return AcquiredBundleMarker{}, err
			}
			artifact.Role = acquiredLocalCompleteRole
			localCompleteArtifacts = append(localCompleteArtifacts, artifact)
		}
	}

	artifacts = append(artifacts, localCompleteArtifacts...)
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	lineage := make([]AcquiredLineageNode, 0, len(runs))
	for _, run := range runs {
		node := AcquiredLineageNode{
			IndexSetID: opts.IndexSetID, RunID: run.manifest.RunID,
			HubCompleteSHA256: run.hubSHA, ManifestSHA256: run.manifestSHA,
		}
		if acquiredV2 {
			node.MarkerSchemaVersion = run.hub.MarkerSchemaVersion
			if run.hub.MarkerSchemaVersion == HubMarkerSchemaV3 {
				node.ConversionIdentitySHA256 = run.hub.ConversionIdentitySHA256
			}
		}
		lineage = append(lineage, node)
	}
	marker := AcquiredBundleMarker{
		Type: AcquiredBundleType, Schema: AcquiredBundleSchema,
		IndexSetID: opts.IndexSetID, RunID: opts.RunID, ProofThroughRunID: opts.ProofThroughRunID,
		SourceIdentitySHA256: identity.CompleteFileSHA256,
		SourceIdentitySchema: SourceIdentitySchemaV1, SourceIdentityProfile: SourceIdentityProfileV1,
		SnapshotCompletedAt:         current.hub.SnapshotCompletedAt,
		SnapshotCompletionSemantics: current.hub.SnapshotCompletionSemantics,
		HubCommittedAt:              current.hub.HubCommittedAt,
		HubCompleteSHA256:           current.hubSHA, ManifestSHA256: current.manifestSHA,
		AcquiredAt: time.Now().UTC().Format(time.RFC3339Nano),
		Lineage:    lineage, Artifacts: artifacts,
	}
	if acquiredV2 {
		runStart := current.hub.RunStart
		snapshotTime := current.hub.SnapshotTime
		marker.Type = AcquiredBundleTypeV2
		marker.Schema = AcquiredBundleSchemaV2
		marker.SourceIdentitySchema = BridgeIdentitySchema
		marker.SourceIdentityProfile = BridgeIdentityProfile
		marker.HubMarkerSchemaVersion = HubMarkerSchemaV3
		marker.RunStart = &runStart
		marker.SnapshotTime = &snapshotTime
		marker.ConversionIdentitySHA256 = current.hub.ConversionIdentitySHA256
		marker.SnapshotCompletedAt = ""
		marker.SnapshotCompletionSemantics = ""
	}
	markerData, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return AcquiredBundleMarker{}, fmt.Errorf("encode acquired bundle marker: %w", err)
	}
	markerData = append(markerData, '\n')
	if _, err := stage.writeBytes("acquired.json", markerData); err != nil {
		return AcquiredBundleMarker{}, err
	}
	if err := ctx.Err(); err != nil {
		return AcquiredBundleMarker{}, err
	}
	verificationRoot, err := boundAcquiredRootFromParent(stage.parent, stage.name, stage.rootInfo)
	if err != nil {
		return AcquiredBundleMarker{}, fmt.Errorf("bind staged acquired bundle: %w", err)
	}
	reader, err := openAcquiredBundleBound(verificationRoot, limits)
	if err != nil {
		_ = verificationRoot.close()
		return AcquiredBundleMarker{}, fmt.Errorf("verify staged acquired bundle: %w", err)
	}
	if err := reader.Close(); err != nil {
		return AcquiredBundleMarker{}, fmt.Errorf("close staged acquired bundle verification: %w", err)
	}
	if err := stage.prepareRename(); err != nil {
		return AcquiredBundleMarker{}, fmt.Errorf("sync acquisition staging directory: %w", err)
	}
	if err := renameDirectoryNoReplace(parent.file, stage.root, stage.name, finalBase); err != nil {
		_ = stage.closeArtifactHandles()
		if existing, matchErr := existingEquivalentAcquiredBundle(finalPath, marker, limits); matchErr == nil {
			stage.cleanup()
			return existing, nil
		}
		return AcquiredBundleMarker{}, fmt.Errorf("%w", ErrAcquiredBundleConflict)
	}
	if closeErr := stage.closeArtifactHandles(); closeErr != nil && opts.Warning != nil {
		opts.Warning(fmt.Sprintf("committed acquired bundle handle close failed: %v", closeErr))
	}
	keepStage = true
	if err := acquiredSyncParentDirectory(parent.file); err != nil && opts.Warning != nil {
		opts.Warning(fmt.Sprintf("committed acquired bundle parent sync failed: %v", err))
	}
	return marker, nil
}

func bindAcquiredDestination(raw string) (finalPath, parentPath, finalBase string, parent *boundAcquiredDirectory, err error) {
	if strings.TrimSpace(raw) == "" {
		return "", "", "", nil, fmt.Errorf("acquired bundle destination is required")
	}
	finalPath, err = filepath.Abs(filepath.Clean(raw))
	if err != nil {
		return "", "", "", nil, fmt.Errorf("resolve acquired bundle destination: %w", err)
	}
	parentPath, finalBase = filepath.Dir(finalPath), filepath.Base(finalPath)
	if finalBase == "." || finalBase == string(filepath.Separator) || finalBase == "" {
		return "", "", "", nil, fmt.Errorf("acquired bundle destination must name one child directory")
	}
	parent, err = openBoundAcquiredDirectory(parentPath, acquiredBeforeDestinationBind)
	if err != nil {
		return "", "", "", nil, fmt.Errorf("bind acquired bundle parent: %w", err)
	}
	return finalPath, parentPath, finalBase, parent, nil
}

func acquiredLocalCompleteJSON(_ string, hub acquiredHubComplete, manifestSHA string, segments int) ([]byte, error) {
	doc := acquiredLocalComplete{
		Type: indexsubstrate.CompleteMarkerTypeV2, IndexSetID: hub.IndexSetID, RunID: hub.RunID,
		SnapshotCompletedAt:         hub.SnapshotCompletedAt,
		SnapshotCompletionSemantics: hub.SnapshotCompletionSemantics,
		ManifestPath:                "manifest.json", ManifestSHA256: manifestSHA,
		SegmentDir: "segments", Segments: segments,
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode local acquired adapter marker: %w", err)
	}
	return append(data, '\n'), nil
}

func acquiredClassifiedCompleteJSON(
	marker AcquiredBundleMarker,
	manifestSHA string,
	segments int,
) ([]byte, error) {
	if marker.RunStart == nil || marker.SnapshotTime == nil {
		return nil, fmt.Errorf("classified acquired time is required")
	}
	doc := acquiredLocalComplete{
		Type:           indexsubstrate.CompleteMarkerTypeV1,
		IndexSetID:     marker.IndexSetID,
		RunID:          marker.RunID,
		ManifestPath:   "manifest.json",
		ManifestSHA256: manifestSHA,
		SegmentDir:     "segments",
		Segments:       segments,
	}
	if marker.SnapshotTime.Basis == BridgeSnapshotTimeExactCommit {
		doc.Type = indexsubstrate.CompleteMarkerTypeV2
		doc.SnapshotCompletedAt = marker.SnapshotTime.CompletedAt
		doc.SnapshotCompletionSemantics = indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit
	} else if marker.SnapshotTime.Basis != BridgeSnapshotTimeLegacyUnavailable {
		return nil, fmt.Errorf("classified acquired snapshot time is invalid")
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode classified acquired adapter marker: %w", err)
	}
	return append(data, '\n'), nil
}

func validateAcquiredHubComplete(indexSetID, runID string, hub acquiredHubComplete, limits AcquiredBundleLimits) error {
	if hub.Version != "1.0" ||
		hub.Format != acquiredHubFormat || hub.FormatVersion != "2" ||
		hub.IndexSetID != indexSetID || hub.RunID != runID || strings.TrimSpace(hub.ExportedBy) == "" {
		return fmt.Errorf("durable hub complete marker contract mismatch")
	}
	switch hub.MarkerSchemaVersion {
	case HubMarkerSchemaV2:
		if hub.LegacyMarkerSchemaVersion != "" || hub.LegacyMarkerSHA256 != "" ||
			hub.IdentitySchema != "" || hub.IdentityProfile != "" ||
			hub.RunStart.Basis != "" || hub.SnapshotTime.Basis != "" ||
			hub.ConversionIdentitySHA256 != "" {
			return fmt.Errorf("durable hub v2 marker contains v3 fields")
		}
		snapshotAt, snapshotOK := parseCanonicalAcquiredTime(hub.SnapshotCompletedAt)
		hubAt, hubOK := parseCanonicalAcquiredTime(hub.HubCommittedAt)
		completedAt, completedOK := parseCanonicalAcquiredTime(hub.CompletedAt)
		if !snapshotOK || !hubOK || !completedOK ||
			hubAt.Before(snapshotAt) || !completedAt.Equal(hubAt) {
			return fmt.Errorf("durable hub completion times are invalid")
		}
		if hub.SnapshotCompletionSemantics != "" &&
			hub.SnapshotCompletionSemantics != indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit {
			return fmt.Errorf("durable hub snapshot completion semantics are invalid")
		}
	case HubMarkerSchemaV3:
		if hub.CompletedAt != "" || hub.SnapshotCompletedAt != "" ||
			hub.SnapshotCompletionSemantics != "" ||
			hub.LegacyMarkerSchemaVersion != HubMarkerSchemaV1 ||
			!validSHA256(hub.LegacyMarkerSHA256) ||
			hub.IdentitySchema != BridgeIdentitySchema ||
			hub.IdentityProfile != BridgeIdentityProfile ||
			hub.RunStart.Basis != BridgeRunStartLegacyAsserted ||
			!validSHA256(hub.ConversionIdentitySHA256) {
			return fmt.Errorf("durable hub v3 marker contract mismatch")
		}
		runStartedAt, runStartOK := parseCanonicalAcquiredTime(hub.RunStart.StartedAt)
		hubAt, hubOK := parseCanonicalAcquiredTime(hub.HubCommittedAt)
		if !runStartOK || !hubOK ||
			validateBridgeSnapshotTime(hub.SnapshotTime, runStartedAt, hubAt) != nil {
			return fmt.Errorf("durable hub v3 classified times are invalid")
		}
		conversion, err := json.Marshal(bridgeConversionFromMarker(hub.bridgeV3()))
		if err != nil {
			return fmt.Errorf("durable hub v3 conversion identity is invalid")
		}
		conversion = append(conversion, '\n')
		if sha256Hex(conversion) != hub.ConversionIdentitySHA256 {
			return fmt.Errorf("durable hub v3 conversion identity mismatch")
		}
	default:
		return fmt.Errorf("durable hub complete marker contract mismatch")
	}
	if err := validateHubArtifact(hub.Artifacts.Identity, "identity.json", "identity", limits.MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateHubArtifact(hub.Artifacts.Manifest, "manifest.json", "manifest", limits.MaxManifestBytes); err != nil {
		return err
	}
	if len(hub.Artifacts.Segments) > limits.MaxSegments ||
		hub.Durable.Segments != len(hub.Artifacts.Segments) ||
		hub.Durable.Rows < 0 || int64(hub.Durable.Rows) > maxBridgeJSONCount {
		return fmt.Errorf("durable hub artifact count exceeds acquisition limits")
	}
	seen := make(map[string]struct{}, len(hub.Artifacts.Segments))
	for _, ref := range hub.Artifacts.Segments {
		if err := validateHubArtifact(ref, ref.Path, "segment", limits.MaxSegmentBytes); err != nil {
			return err
		}
		if !acquiredSegmentPathRE.MatchString(ref.Path) {
			return fmt.Errorf("durable hub segment path is invalid")
		}
		normalized, err := normalizeBundleRelativePath(ref.Path, false)
		if err != nil || normalized != ref.Path || strings.Contains(ref.Path, "%") {
			return fmt.Errorf("durable hub segment path is invalid")
		}
		if _, ok := seen[ref.Path]; ok {
			return fmt.Errorf("durable hub segment path is duplicated")
		}
		seen[ref.Path] = struct{}{}
	}
	return nil
}

func validateAcquiredHubTime(hub acquiredHubComplete, manifest indexsubstrate.InternalManifest) error {
	if manifest.RunStartedAt == nil {
		return fmt.Errorf("durable manifest run_started_at is required for acquisition")
	}
	if hub.MarkerSchemaVersion == HubMarkerSchemaV3 {
		startedAt, err := indexsubstrate.ParseCanonicalUTCTime(hub.RunStart.StartedAt)
		if err != nil || !startedAt.Equal(*manifest.RunStartedAt) {
			return fmt.Errorf("durable hub run_start disagrees with manifest")
		}
		hubAt, err := indexsubstrate.ParseCanonicalUTCTime(hub.HubCommittedAt)
		if err != nil {
			return fmt.Errorf("durable hub hub_committed_at is invalid")
		}
		if err := validateBridgeSnapshotTime(hub.SnapshotTime, startedAt, hubAt); err != nil {
			return fmt.Errorf("durable hub classified snapshot time is invalid")
		}
		return nil
	}
	snapshotAt, err := indexsubstrate.ParseCanonicalUTCTime(hub.SnapshotCompletedAt)
	if err != nil {
		return fmt.Errorf("durable hub snapshot_completed_at is invalid")
	}
	hubAt, err := indexsubstrate.ParseCanonicalUTCTime(hub.HubCommittedAt)
	if err != nil {
		return fmt.Errorf("durable hub hub_committed_at is invalid")
	}
	if err := indexsubstrate.ValidateExactSnapshotCompletion(
		hub.SnapshotCompletionSemantics,
		*manifest.RunStartedAt,
		snapshotAt,
		hubAt,
	); err != nil {
		return fmt.Errorf("durable hub snapshot completion is not exact-time eligible: %w", err)
	}
	return nil
}

func validateHubArtifact(ref acquiredHubArtifact, expectedPath, expectedRole string, maxBytes int64) error {
	if ref.Path != expectedPath || ref.Role != expectedRole || !ref.Required ||
		ref.SizeBytes < 0 || ref.SizeBytes > maxBytes || !validSHA256(ref.SHA256) {
		return fmt.Errorf("durable hub artifact contract mismatch")
	}
	return nil
}

func validateAcquiredManifest(indexSetID, runID string, hub acquiredHubComplete, manifest indexsubstrate.InternalManifest) error {
	if manifest.Type != indexsubstrate.ManifestType ||
		manifest.Render != indexsubstrate.ManifestRenderType ||
		manifest.IndexSetID != indexSetID || manifest.RunID != runID ||
		manifest.IndexSchemaVersion != indexsubstrate.IndexSchemaVersion ||
		hub.Durable.ManifestType != manifest.Type ||
		hub.Durable.ManifestRender != manifest.Render ||
		hub.Durable.IndexSchemaVersion != manifest.IndexSchemaVersion ||
		hub.Durable.SegmentNamespace != manifest.Reachability.SegmentNamespace ||
		hub.Durable.Segments != len(manifest.Segments) ||
		hub.Durable.Rows != manifest.Counts.Rows ||
		len(hub.Artifacts.Segments) != len(manifest.Segments) {
		return fmt.Errorf("durable hub manifest contract mismatch")
	}
	if err := indexsubstrate.ValidateManifestLineageStructure(manifest); err != nil {
		return err
	}
	for i, segment := range manifest.Segments {
		ref := hub.Artifacts.Segments[i]
		if ref.Path != "segments/"+segment.Path || ref.SizeBytes != segment.SizeBytes ||
			ref.SHA256 != segment.Digest.Hex || segment.Digest.Algorithm != "sha256" {
			return fmt.Errorf("durable hub segment manifest binding mismatch")
		}
	}
	return nil
}

func validateAcquiredManifestChain(indexSetID, proofThrough string, manifests []indexsubstrate.InternalManifest) error {
	if len(manifests) == 0 || manifests[0].IndexSetID != indexSetID {
		return fmt.Errorf("acquired lineage root is invalid")
	}
	if proofThrough == "" {
		if len(manifests) != 1 {
			return fmt.Errorf("unrequested acquired lineage is invalid")
		}
		return nil
	}
	if manifests[len(manifests)-1].RunID != proofThrough {
		return &indexsubstrate.LineageError{
			Code: indexsubstrate.LineageCodeBaselineNotAncestor, Message: "proof-through run is not an ancestor",
		}
	}
	seen := make(map[string]struct{}, len(manifests))
	for i := range manifests {
		current := manifests[i]
		if current.IndexSetID != indexSetID || current.Lineage == nil || current.RunStartedAt == nil {
			return &indexsubstrate.LineageError{
				Code: indexsubstrate.LineageCodeRequireContinuous, Message: "proof-through requires continuous lineage",
			}
		}
		if _, duplicate := seen[current.RunID]; duplicate {
			return &indexsubstrate.LineageError{
				Code: indexsubstrate.LineageCodeCycle, Message: "lineage cycle detected",
			}
		}
		seen[current.RunID] = struct{}{}
		if i+1 == len(manifests) {
			continue
		}
		parent := manifests[i+1]
		if current.StateParent == nil ||
			current.StateParent.IndexSetID != parent.IndexSetID ||
			current.StateParent.RunID != parent.RunID {
			return &indexsubstrate.LineageError{
				Code: indexsubstrate.LineageCodeSetRunMismatch, Message: "lineage parent identity mismatch",
			}
		}
		if current.Lineage.Generation != parent.Lineage.Generation+1 {
			return &indexsubstrate.LineageError{
				Code: indexsubstrate.LineageCodeGeneration, Message: "lineage generation is discontinuous",
			}
		}
		if !current.RunStartedAt.After(*parent.RunStartedAt) {
			return &indexsubstrate.LineageError{
				Code: indexsubstrate.LineageCodeInvalidTime, Message: "lineage run times are not strictly ordered",
			}
		}
	}
	return nil
}

func validateAcquiredLineageProof(marker AcquiredBundleMarker, manifests []indexsubstrate.InternalManifest) error {
	if len(manifests) != len(marker.Lineage) {
		return fmt.Errorf("acquired lineage artifact count mismatch")
	}
	for i, manifest := range manifests {
		node := marker.Lineage[i]
		if manifest.IndexSetID != node.IndexSetID || manifest.RunID != node.RunID {
			return fmt.Errorf("acquired lineage node binding mismatch")
		}
		if i+1 < len(manifests) &&
			(manifest.StateParent == nil ||
				manifest.StateParent.ManifestSHA256 != marker.Lineage[i+1].ManifestSHA256) {
			return fmt.Errorf("acquired lineage parent digest mismatch")
		}
	}
	return validateAcquiredManifestChain(marker.IndexSetID, marker.ProofThroughRunID, manifests)
}

func decodeStrictJSON(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}

func normalizeBundleRelativePath(raw string, allowEmpty bool) (string, error) {
	if raw == "" && allowEmpty {
		return "", nil
	}
	if raw == "" || strings.Contains(raw, "\\") || strings.ContainsRune(raw, '\x00') ||
		strings.HasPrefix(raw, "/") || path.Clean(raw) != raw || raw == "." ||
		raw == ".." || strings.HasPrefix(raw, "../") || filepath.IsAbs(raw) {
		return "", fmt.Errorf("invalid acquired artifact path")
	}
	return raw, nil
}

func validSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func existingEquivalentAcquiredBundle(finalPath string, want AcquiredBundleMarker, limits AcquiredBundleLimits) (AcquiredBundleMarker, error) {
	reader, err := OpenAcquiredBundle(AcquiredOpenOptions{Directory: finalPath, Limits: limits})
	if err != nil {
		return AcquiredBundleMarker{}, err
	}
	durable, ok := reader.(*durableReader)
	if !ok || durable.acquiredRoot == nil {
		_ = reader.Close()
		return AcquiredBundleMarker{}, ErrAcquiredBundleConflict
	}
	data, err := durable.acquiredRoot.readBoundedRegular("acquired.json", limits.MaxMarkerBytes)
	_ = reader.Close()
	if err != nil {
		return AcquiredBundleMarker{}, err
	}
	var got AcquiredBundleMarker
	if err := decodeStrictJSON(data, &got); err != nil {
		return AcquiredBundleMarker{}, err
	}
	gotTime, wantTime := got.AcquiredAt, want.AcquiredAt
	got.AcquiredAt, want.AcquiredAt = "", ""
	if !equalAcquiredMarker(got, want) {
		return AcquiredBundleMarker{}, ErrAcquiredBundleConflict
	}
	got.AcquiredAt = gotTime
	want.AcquiredAt = wantTime
	return got, nil
}

func equalAcquiredMarker(a, b AcquiredBundleMarker) bool {
	aa, errA := json.Marshal(a)
	bb, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(aa, bb)
}

// AcquiredOpenOptions selects a final acquired bundle.
type AcquiredOpenOptions struct {
	Directory string
	Limits    AcquiredBundleLimits
}

// OpenAcquiredBundle validates acquired.json first, revalidates every bound
// artifact beneath the no-symlink root, and returns the same durable reader
// used by pinned local queries.
func OpenAcquiredBundle(opts AcquiredOpenOptions) (Reader, error) {
	limits := opts.Limits.normalize()
	root, err := bindAcquiredRoot(opts.Directory)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotAcquiredBundle, err)
	}
	reader, err := openAcquiredBundleBound(root, limits)
	if err != nil {
		_ = root.close()
		return nil, err
	}
	return reader, nil
}

func openAcquiredBundleBound(root *boundAcquiredRoot, limits AcquiredBundleLimits) (Reader, error) {
	markerData, err := root.readBoundedRegular("acquired.json", limits.MaxMarkerBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: acquired.json is required", ErrNotAcquiredBundle)
	}
	metadata := int64(len(markerData))
	if metadata > limits.MaxMetadataBytes {
		return nil, fmt.Errorf("%w: metadata byte limit exceeded", ErrNotAcquiredBundle)
	}
	var marker AcquiredBundleMarker
	if err := decodeStrictJSON(markerData, &marker); err != nil {
		return nil, fmt.Errorf("%w: acquired.json is invalid", ErrNotAcquiredBundle)
	}
	if err := validateAcquiredMarker(marker, limits); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotAcquiredBundle, err)
	}
	var aggregate int64
	byPath := make(map[string]AcquiredArtifact, len(marker.Artifacts))
	verifiedData := make(map[string][]byte, 3*len(marker.Lineage)+1)
	for _, artifact := range marker.Artifacts {
		if aggregate > limits.MaxAggregateBytes-artifact.SizeBytes {
			return nil, fmt.Errorf("%w: artifact byte limit exceeded", ErrNotAcquiredBundle)
		}
		aggregate += artifact.SizeBytes
		var gotSHA string
		var gotSize int64
		if artifact.Role == acquiredSegmentRole {
			gotSHA, gotSize, err = root.hashRegular(artifact.Path, artifact.SizeBytes)
		} else {
			if metadata > limits.MaxMetadataBytes-artifact.SizeBytes {
				return nil, fmt.Errorf("%w: metadata byte limit exceeded", ErrNotAcquiredBundle)
			}
			metadata += artifact.SizeBytes
			var data []byte
			data, err = root.readExactRegular(artifact.Path, artifact.SizeBytes)
			gotSize = int64(len(data))
			gotSHA = sha256Hex(data)
			if err == nil {
				verifiedData[artifact.Path] = data
			}
		}
		if err != nil || gotSize != artifact.SizeBytes || gotSHA != artifact.SHA256 {
			return nil, fmt.Errorf("%w: artifact verification failed", ErrNotAcquiredBundle)
		}
		byPath[artifact.Path] = artifact
	}
	identity, err := readVerifiedLocalIdentityBytes(verifiedData["identity.json"], marker.IndexSetID)
	if err != nil || identity.CompleteFileSHA256 != marker.SourceIdentitySHA256 {
		return nil, fmt.Errorf("%w: source identity verification failed", ErrNotAcquiredBundle)
	}
	currentPrefix := "runs/" + marker.RunID + "/"
	hubPath := currentPrefix + "hub-complete.json"
	hubData := verifiedData[hubPath]
	if sha256Hex(hubData) != marker.HubCompleteSHA256 {
		return nil, fmt.Errorf("%w: hub complete verification failed", ErrNotAcquiredBundle)
	}
	var hub acquiredHubComplete
	if err := decodeStrictJSON(hubData, &hub); err != nil {
		return nil, fmt.Errorf("%w: hub complete marker is invalid", ErrNotAcquiredBundle)
	}
	if err := validateAcquiredHubComplete(marker.IndexSetID, marker.RunID, hub, limits); err != nil {
		return nil, fmt.Errorf("%w: hub complete marker refused", ErrNotAcquiredBundle)
	}
	if hub.Artifacts.Identity.SHA256 != marker.SourceIdentitySHA256 ||
		hub.Artifacts.Manifest.SHA256 != marker.ManifestSHA256 ||
		hub.HubCommittedAt != marker.HubCommittedAt {
		return nil, fmt.Errorf("%w: acquired marker binding mismatch", ErrNotAcquiredBundle)
	}
	if marker.Type == AcquiredBundleTypeV2 {
		if marker.RunStart == nil || marker.SnapshotTime == nil ||
			hub.MarkerSchemaVersion != marker.HubMarkerSchemaVersion ||
			hub.RunStart != *marker.RunStart ||
			hub.SnapshotTime != *marker.SnapshotTime ||
			hub.ConversionIdentitySHA256 != marker.ConversionIdentitySHA256 ||
			hub.IdentitySchema != marker.SourceIdentitySchema ||
			hub.IdentityProfile != marker.SourceIdentityProfile {
			return nil, fmt.Errorf("%w: acquired v2 marker binding mismatch", ErrNotAcquiredBundle)
		}
	} else if hub.SnapshotCompletedAt != marker.SnapshotCompletedAt ||
		hub.SnapshotCompletionSemantics != marker.SnapshotCompletionSemantics {
		return nil, fmt.Errorf("%w: acquired marker time binding mismatch", ErrNotAcquiredBundle)
	}
	identityRef := byPath["identity.json"]
	if identityRef.Role != acquiredIdentityRole ||
		identityRef.SHA256 != marker.SourceIdentitySHA256 ||
		identityRef.SizeBytes != hub.Artifacts.Identity.SizeBytes {
		return nil, fmt.Errorf("%w: source identity artifact binding mismatch", ErrNotAcquiredBundle)
	}
	completeRel := currentPrefix + "complete.json"
	manifestRel := currentPrefix + "manifest.json"
	segmentRel := currentPrefix + "segments"
	completeData := verifiedData[completeRel]
	if marker.Type == AcquiredBundleTypeV2 {
		completeData, err = acquiredClassifiedCompleteJSON(
			marker, marker.ManifestSHA256, len(hub.Artifacts.Segments),
		)
		if err != nil {
			return nil, fmt.Errorf("%w: classified snapshot adapter failed", ErrNotAcquiredBundle)
		}
	}
	snap, err := indexsubstrate.OpenPublishedRunSnapshotMaterial(
		completeRel, manifestRel, segmentRel,
		completeData, verifiedData[manifestRel],
		marker.IndexSetID, marker.RunID,
	)
	if err != nil || snap.Complete.ManifestSHA256 != marker.ManifestSHA256 {
		return nil, fmt.Errorf("%w: current snapshot verification failed", ErrNotAcquiredBundle)
	}
	expectedArtifacts := map[string]string{"identity.json": acquiredIdentityRole}
	lineageManifests := make([]indexsubstrate.InternalManifest, 0, len(marker.Lineage))
	for _, node := range marker.Lineage {
		prefix := "runs/" + node.RunID + "/"
		expectedArtifacts[prefix+"hub-complete.json"] = acquiredHubCompleteRole
		expectedArtifacts[prefix+"manifest.json"] = acquiredManifestRole
		if marker.Type != AcquiredBundleTypeV2 {
			expectedArtifacts[prefix+"complete.json"] = acquiredLocalCompleteRole
		}
		nodeHubData := verifiedData[prefix+"hub-complete.json"]
		if sha256Hex(nodeHubData) != node.HubCompleteSHA256 {
			return nil, fmt.Errorf("%w: lineage hub complete verification failed", ErrNotAcquiredBundle)
		}
		var nodeHub acquiredHubComplete
		if err := decodeStrictJSON(nodeHubData, &nodeHub); err != nil {
			return nil, fmt.Errorf("%w: lineage hub complete marker is invalid", ErrNotAcquiredBundle)
		}
		if err := validateAcquiredHubComplete(marker.IndexSetID, node.RunID, nodeHub, limits); err != nil ||
			nodeHub.Artifacts.Identity.SHA256 != marker.SourceIdentitySHA256 ||
			nodeHub.Artifacts.Manifest.SHA256 != node.ManifestSHA256 {
			return nil, fmt.Errorf("%w: lineage hub complete binding mismatch", ErrNotAcquiredBundle)
		}
		if marker.Type == AcquiredBundleTypeV2 &&
			(nodeHub.MarkerSchemaVersion != node.MarkerSchemaVersion ||
				nodeHub.ConversionIdentitySHA256 != node.ConversionIdentitySHA256) {
			return nil, fmt.Errorf("%w: classified lineage marker binding mismatch", ErrNotAcquiredBundle)
		}
		nodeManifestData := verifiedData[prefix+"manifest.json"]
		if sha256Hex(nodeManifestData) != node.ManifestSHA256 {
			return nil, fmt.Errorf("%w: lineage manifest verification failed", ErrNotAcquiredBundle)
		}
		var nodeManifest indexsubstrate.InternalManifest
		if err := json.Unmarshal(nodeManifestData, &nodeManifest); err != nil {
			return nil, fmt.Errorf("%w: lineage manifest is invalid", ErrNotAcquiredBundle)
		}
		if err := validateAcquiredManifest(marker.IndexSetID, node.RunID, nodeHub, nodeManifest); err != nil {
			return nil, fmt.Errorf("%w: lineage manifest binding mismatch", ErrNotAcquiredBundle)
		}
		lineageManifests = append(lineageManifests, nodeManifest)
		requiredArtifacts := []struct {
			path string
			role string
			sha  string
		}{
			{prefix + "hub-complete.json", acquiredHubCompleteRole, node.HubCompleteSHA256},
			{prefix + "manifest.json", acquiredManifestRole, node.ManifestSHA256},
		}
		if marker.Type != AcquiredBundleTypeV2 {
			requiredArtifacts = append(requiredArtifacts, struct {
				path string
				role string
				sha  string
			}{prefix + "complete.json", acquiredLocalCompleteRole, ""})
		}
		for _, required := range requiredArtifacts {
			ref := byPath[required.path]
			if ref.Role != required.role || (required.sha != "" && ref.SHA256 != required.sha) {
				return nil, fmt.Errorf("%w: lineage artifact role binding mismatch", ErrNotAcquiredBundle)
			}
		}
	}
	for _, segment := range snap.Manifest.Segments {
		rel := "runs/" + marker.RunID + "/segments/" + segment.Path
		expectedArtifacts[rel] = acquiredSegmentRole
		ref := byPath[rel]
		if ref.Role != acquiredSegmentRole || ref.SHA256 != segment.Digest.Hex || ref.SizeBytes != segment.SizeBytes {
			return nil, fmt.Errorf("%w: current segment artifact binding mismatch", ErrNotAcquiredBundle)
		}
	}
	if len(expectedArtifacts) != len(byPath) {
		return nil, fmt.Errorf("%w: acquired artifact set is not exact", ErrNotAcquiredBundle)
	}
	for artifactPath, role := range expectedArtifacts {
		if byPath[artifactPath].Role != role {
			return nil, fmt.Errorf("%w: acquired artifact role mismatch", ErrNotAcquiredBundle)
		}
	}
	if err := validateAcquiredLineageProof(marker, lineageManifests); err != nil {
		return nil, fmt.Errorf("%w: lineage proof verification failed", ErrNotAcquiredBundle)
	}
	acquiredFilters := make(map[string]indexstore.SinceRunFilter, len(lineageManifests))
	for i, boundary := range lineageManifests {
		if boundary.Lineage == nil || boundary.RunStartedAt == nil {
			continue
		}
		descendants := make([]string, 0, i)
		for _, manifest := range lineageManifests[:i] {
			descendants = append(descendants, manifest.RunID)
		}
		acquiredFilters[boundary.RunID] = indexstore.SinceRunFilter{
			RunID: boundary.RunID, StartedAt: *boundary.RunStartedAt,
			VerifiedDescendantRunIDs: descendants, SameRun: i == 0,
		}
	}
	snapshotAt, _ := time.Parse(time.RFC3339Nano, marker.SnapshotCompletedAt)
	hubAt, _ := time.Parse(time.RFC3339Nano, marker.HubCommittedAt)
	reader := &durableReader{
		meta: Meta{
			Format: FormatDurableV2, IndexSetID: marker.IndexSetID,
			BaseURI: identity.Payload.BaseURI, Provider: identity.Payload.Provider,
			IdentityDir: root.path, SourcePath: filepath.Join(root.path, filepath.FromSlash(completeRel)), RunID: marker.RunID,
		},
		opts: ResolveOptions{
			MaxMarkerBytes: limits.MaxMarkerBytes, MaxManifestBytes: limits.MaxManifestBytes,
			SegmentCacheRoot: root.path,
		},
		snap: snap, sourceIdentity: identity, segmentCacheRoot: root.path, pinned: true,
		sourceKind: SnapshotSourceAcquiredHub, snapshotCompletedAt: snapshotAt,
		snapshotCompletionSemantics: marker.SnapshotCompletionSemantics,
		hubCommittedAt:              hubAt, hubCompleteSHA256: marker.HubCompleteSHA256,
		acquiredRoot: root, acquiredMarkerType: marker.Type,
		acquiredMarkerSchema: marker.Schema, acquiredHubMarkerSchema: marker.HubMarkerSchemaVersion,
		acquiredIdentityPayload: identity.Payload, acquiredManifestRaw: verifiedData[manifestRel],
		acquiredSinceFilters: acquiredFilters,
	}
	if marker.Type == AcquiredBundleTypeV2 {
		reader.runStart = *marker.RunStart
		reader.snapshotTime = *marker.SnapshotTime
		reader.sourceIdentitySchema = marker.SourceIdentitySchema
		reader.sourceIdentityProfile = marker.SourceIdentityProfile
	}
	return reader, nil
}

func validateAcquiredMarker(marker AcquiredBundleMarker, limits AcquiredBundleLimits) error {
	v2 := marker.Type == AcquiredBundleTypeV2 && marker.Schema == AcquiredBundleSchemaV2
	v1 := marker.Type == AcquiredBundleType && marker.Schema == AcquiredBundleSchema
	if (!v1 && !v2) || !fullAcquiredIndexSetRE.MatchString(marker.IndexSetID) ||
		!acquiredRunIDRE.MatchString(marker.RunID) ||
		(marker.ProofThroughRunID != "" && !acquiredRunIDRE.MatchString(marker.ProofThroughRunID)) ||
		!validSHA256(marker.SourceIdentitySHA256) ||
		!validSHA256(marker.HubCompleteSHA256) || !validSHA256(marker.ManifestSHA256) {
		return fmt.Errorf("acquired marker contract mismatch")
	}
	if _, ok := parseCanonicalAcquiredTime(marker.AcquiredAt); !ok {
		return fmt.Errorf("acquired_at is invalid")
	}
	hubAt, hubOK := parseCanonicalAcquiredTime(marker.HubCommittedAt)
	if !hubOK {
		return fmt.Errorf("acquired hub commit time is invalid")
	}
	if v2 {
		if marker.SourceIdentitySchema != BridgeIdentitySchema ||
			marker.SourceIdentityProfile != BridgeIdentityProfile ||
			marker.HubMarkerSchemaVersion != HubMarkerSchemaV3 ||
			marker.RunStart == nil || marker.SnapshotTime == nil ||
			!validSHA256(marker.ConversionIdentitySHA256) ||
			marker.SnapshotCompletedAt != "" ||
			marker.SnapshotCompletionSemantics != "" {
			return fmt.Errorf("acquired v2 marker contract mismatch")
		}
		startedAt, ok := parseCanonicalAcquiredTime(marker.RunStart.StartedAt)
		if !ok || marker.RunStart.Basis != BridgeRunStartLegacyAsserted ||
			validateBridgeSnapshotTime(*marker.SnapshotTime, startedAt, hubAt) != nil {
			return fmt.Errorf("acquired v2 classified times are invalid")
		}
	} else {
		if marker.SourceIdentitySchema != SourceIdentitySchemaV1 ||
			marker.SourceIdentityProfile != SourceIdentityProfileV1 ||
			marker.HubMarkerSchemaVersion != "" || marker.RunStart != nil ||
			marker.SnapshotTime != nil || marker.ConversionIdentitySHA256 != "" {
			return fmt.Errorf("acquired v1 marker contract mismatch")
		}
		if marker.SnapshotCompletionSemantics != "" &&
			marker.SnapshotCompletionSemantics != indexsubstrate.SnapshotCompletionSemanticsCompleteMarkerCommit {
			return fmt.Errorf("acquired snapshot completion semantics are invalid")
		}
		snapshotAt, snapshotOK := parseCanonicalAcquiredTime(marker.SnapshotCompletedAt)
		if !snapshotOK || hubAt.Before(snapshotAt) {
			return fmt.Errorf("acquired source times are invalid")
		}
	}
	if len(marker.Lineage) == 0 || len(marker.Lineage) > limits.MaxLineageNodes {
		return fmt.Errorf("acquired lineage node limit exceeded")
	}
	if marker.Lineage[0].IndexSetID != marker.IndexSetID ||
		marker.Lineage[0].RunID != marker.RunID ||
		marker.Lineage[0].HubCompleteSHA256 != marker.HubCompleteSHA256 ||
		marker.Lineage[0].ManifestSHA256 != marker.ManifestSHA256 {
		return fmt.Errorf("acquired lineage root mismatch")
	}
	if marker.ProofThroughRunID == "" && len(marker.Lineage) != 1 {
		return fmt.Errorf("unrequested lineage artifacts are refused")
	}
	if marker.ProofThroughRunID != "" && marker.Lineage[len(marker.Lineage)-1].RunID != marker.ProofThroughRunID {
		return fmt.Errorf("proof-through lineage boundary mismatch")
	}
	seenRuns := make(map[string]struct{}, len(marker.Lineage))
	for i, node := range marker.Lineage {
		if node.IndexSetID != marker.IndexSetID || !acquiredRunIDRE.MatchString(node.RunID) ||
			!validSHA256(node.HubCompleteSHA256) || !validSHA256(node.ManifestSHA256) {
			return fmt.Errorf("acquired lineage node is invalid")
		}
		if v2 {
			switch node.MarkerSchemaVersion {
			case HubMarkerSchemaV2:
				if node.ConversionIdentitySHA256 != "" {
					return fmt.Errorf("acquired v2 lineage conversion identity is invalid")
				}
			case HubMarkerSchemaV3:
				if !validSHA256(node.ConversionIdentitySHA256) {
					return fmt.Errorf("acquired v3 lineage conversion identity is invalid")
				}
			default:
				return fmt.Errorf("acquired lineage marker schema is invalid")
			}
			if i == 0 &&
				(node.MarkerSchemaVersion != marker.HubMarkerSchemaVersion ||
					node.ConversionIdentitySHA256 != marker.ConversionIdentitySHA256) {
				return fmt.Errorf("acquired lineage root classification mismatch")
			}
		} else if node.MarkerSchemaVersion != "" || node.ConversionIdentitySHA256 != "" {
			return fmt.Errorf("acquired v1 lineage classification is invalid")
		}
		if _, exists := seenRuns[node.RunID]; exists {
			return fmt.Errorf("acquired lineage node is duplicated")
		}
		seenRuns[node.RunID] = struct{}{}
	}
	maxArtifacts := limits.MaxSegments + 3*limits.MaxLineageNodes + 1
	if v2 && maxArtifacts > 200128 {
		maxArtifacts = 200128
	}
	if len(marker.Artifacts) == 0 || len(marker.Artifacts) > maxArtifacts {
		return fmt.Errorf("acquired artifacts are required")
	}
	seenPaths := make(map[string]struct{}, len(marker.Artifacts))
	for _, artifact := range marker.Artifacts {
		normalized, err := normalizeBundleRelativePath(artifact.Path, false)
		if err != nil || normalized != artifact.Path || strings.Contains(artifact.Path, "%") ||
			len(artifact.Path) > 1024 ||
			artifact.SizeBytes < 0 || artifact.SizeBytes > maxBridgeJSONCount ||
			!validSHA256(artifact.SHA256) {
			return fmt.Errorf("acquired artifact is invalid")
		}
		switch artifact.Role {
		case acquiredIdentityRole:
			if artifact.Path != "identity.json" || artifact.SizeBytes > limits.MaxIdentityBytes {
				return fmt.Errorf("acquired identity artifact is invalid")
			}
		case acquiredHubCompleteRole:
			if artifact.SizeBytes > limits.MaxMarkerBytes {
				return fmt.Errorf("acquired marker artifact exceeds limit")
			}
		case acquiredLocalCompleteRole:
			if v2 || artifact.SizeBytes > limits.MaxMarkerBytes {
				return fmt.Errorf("acquired local marker artifact is invalid")
			}
		case acquiredManifestRole:
			if artifact.SizeBytes > limits.MaxManifestBytes {
				return fmt.Errorf("acquired manifest artifact exceeds limit")
			}
		case acquiredSegmentRole:
			if artifact.SizeBytes > limits.MaxSegmentBytes {
				return fmt.Errorf("acquired segment artifact exceeds limit")
			}
		default:
			return fmt.Errorf("acquired artifact role is invalid")
		}
		if _, exists := seenPaths[artifact.Path]; exists {
			return fmt.Errorf("acquired artifact path is duplicated")
		}
		seenPaths[artifact.Path] = struct{}{}
	}
	required := []string{
		"identity.json",
		"runs/" + marker.RunID + "/hub-complete.json",
		"runs/" + marker.RunID + "/manifest.json",
	}
	if !v2 {
		required = append(required, "runs/"+marker.RunID+"/complete.json")
	}
	for _, requiredPath := range required {
		if _, ok := seenPaths[requiredPath]; !ok {
			return fmt.Errorf("acquired required artifact is missing")
		}
	}
	return nil
}

func parseCanonicalAcquiredTime(raw string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.IsZero() || raw != parsed.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, false
	}
	return parsed, true
}

type boundAcquiredRoot struct {
	path      string
	root      *os.Root
	info      os.FileInfo
	closeOnce sync.Once
	closeErr  error
}

func bindAcquiredRoot(raw string) (*boundAcquiredRoot, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("bundle directory is required")
	}
	rootPath, err := filepath.Abs(filepath.Clean(raw))
	if err != nil {
		return nil, err
	}
	return openBoundAcquiredRootPath(rootPath, acquiredBeforeOpenRootBind)
}

func openBoundAcquiredDirectory(path string, beforeBind func(string) error) (*boundAcquiredDirectory, error) {
	root, err := openBoundAcquiredRootPath(path, beforeBind)
	if err != nil {
		return nil, err
	}
	file, err := root.root.Open(".")
	if err != nil {
		_ = root.close()
		return nil, err
	}
	bound, err := file.Stat()
	if err != nil || !os.SameFile(root.info, bound) {
		_ = file.Close()
		_ = root.close()
		return nil, errors.Join(fmt.Errorf("bound directory file identity changed"), err)
	}
	return &boundAcquiredDirectory{path: root.path, root: root.root, file: file, info: root.info}, nil
}

func (d *boundAcquiredDirectory) close() error {
	if d == nil {
		return nil
	}
	var err error
	if d.file != nil {
		err = d.file.Close()
		d.file = nil
	}
	if d.root != nil {
		err = errors.Join(err, d.root.Close())
		d.root = nil
	}
	return err
}

func openBoundAcquiredRootPath(path string, beforeBind func(string) error) (*boundAcquiredRoot, error) {
	path, preInfo, err := validateAcquiredDirectoryPath(path)
	if err != nil {
		return nil, err
	}
	if beforeBind != nil {
		if err := beforeBind(path); err != nil {
			return nil, err
		}
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("open retained directory root: %w", err)
	}
	_, postInfo, err := validateAcquiredDirectoryPath(path)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	bound, err := root.Stat(".")
	if err != nil || !bound.IsDir() ||
		!os.SameFile(preInfo, bound) || !os.SameFile(postInfo, bound) {
		_ = root.Close()
		return nil, errors.Join(fmt.Errorf("directory root binding changed"), err)
	}
	return &boundAcquiredRoot{path: path, root: root, info: bound}, nil
}

func boundAcquiredRootFromParent(parent *boundAcquiredDirectory, name string, expected os.FileInfo) (*boundAcquiredRoot, error) {
	if parent == nil || parent.root == nil || expected == nil {
		return nil, fmt.Errorf("bound acquisition parent is required")
	}
	named, err := parent.root.Lstat(name)
	if err != nil || !named.IsDir() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(expected, named) {
		return nil, errors.Join(fmt.Errorf("acquired child binding changed"), err)
	}
	root, err := parent.root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	bound, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	named, namedErr := parent.root.Lstat(name)
	if namedErr != nil || !os.SameFile(expected, named) || !os.SameFile(bound, named) {
		_ = root.Close()
		return nil, errors.Join(fmt.Errorf("acquired child binding changed after open"), namedErr)
	}
	return &boundAcquiredRoot{
		path: filepath.Join(parent.path, name), root: root, info: bound,
	}, nil
}

func validateAcquiredDirectoryPath(raw string) (string, os.FileInfo, error) {
	absolute, err := filepath.Abs(filepath.Clean(raw))
	if err != nil {
		return "", nil, err
	}
	components := []string{absolute}
	for current := absolute; ; {
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		components = append(components, parent)
		current = parent
	}
	var final os.FileInfo
	for i := len(components) - 1; i >= 0; i-- {
		info, statErr := os.Lstat(components[i])
		if statErr != nil {
			return "", nil, statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", nil, fmt.Errorf("directory path component is not a no-follow directory")
		}
		if i == 0 {
			final = info
		}
	}
	return absolute, final, nil
}

func (r *boundAcquiredRoot) verifyRelativeRegular(rel string) (os.FileInfo, error) {
	if r == nil || r.root == nil {
		return nil, fmt.Errorf("acquired root is closed")
	}
	rel, err := normalizeBundleRelativePath(rel, false)
	if err != nil {
		return nil, err
	}
	current := ""
	parts := strings.Split(rel, "/")
	for i, component := range parts {
		current = path.Join(current, component)
		info, err := r.root.Lstat(filepath.FromSlash(current))
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.Join(fmt.Errorf("artifact component is missing or linked"), err)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return nil, fmt.Errorf("artifact parent component is not a directory")
		}
		if i == len(parts)-1 && !info.Mode().IsRegular() {
			return nil, fmt.Errorf("artifact is not a regular file")
		}
		if i == len(parts)-1 {
			return info, nil
		}
	}
	return nil, fmt.Errorf("artifact path is empty")
}

func (r *boundAcquiredRoot) openRegular(rel string) (*os.File, error) {
	preInfo, err := r.verifyRelativeRegular(rel)
	if err != nil {
		return nil, err
	}
	f, err := r.root.Open(filepath.FromSlash(rel))
	if err != nil {
		return nil, err
	}
	bound, err := f.Stat()
	if err != nil || !bound.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.Join(fmt.Errorf("artifact is not a bound regular file"), err)
	}
	postInfo, err := r.verifyRelativeRegular(rel)
	if err != nil || !os.SameFile(preInfo, bound) || !os.SameFile(postInfo, bound) {
		_ = f.Close()
		return nil, errors.Join(fmt.Errorf("artifact binding changed"), err)
	}
	return f, nil
}

func (r *boundAcquiredRoot) readBoundedRegular(rel string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	f, err := r.openRegular(rel)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("acquired artifact size exceeds limit")
	}
	return data, nil
}

func (r *boundAcquiredRoot) readExactRegular(rel string, expectedSize int64) ([]byte, error) {
	if expectedSize < 0 {
		return nil, fmt.Errorf("negative expected artifact size")
	}
	data, err := r.readBoundedRegular(rel, expectedSize)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != expectedSize {
		return nil, fmt.Errorf("acquired artifact size mismatch")
	}
	return data, nil
}

func (r *boundAcquiredRoot) hashRegular(rel string, expectedSize int64) (string, int64, error) {
	if expectedSize < 0 {
		return "", 0, fmt.Errorf("negative expected artifact size")
	}
	f, err := r.openRegular(rel)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()
	bound, err := f.Stat()
	if err != nil || bound.Size() != expectedSize {
		return "", 0, errors.Join(fmt.Errorf("artifact size mismatch"), err)
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, expectedSize+1))
	if err != nil || n != expectedSize {
		return "", n, errors.Join(fmt.Errorf("artifact bounded read mismatch"), err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func (r *boundAcquiredRoot) close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.root != nil {
			r.closeErr = r.root.Close()
			r.root = nil
		}
	})
	return r.closeErr
}
