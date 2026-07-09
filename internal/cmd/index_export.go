package cmd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/spf13/cobra"
)

var indexExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export an index run to a hub",
	Long: `Export an index run from the local index store to a hub (S3 or file).

Default --format is auto: prefer a local durable-v2 snapshot when present,
otherwise export sqlite-v1. Explicit --format durable and --format sqlite remain
available. Durable export resolves from the local durable complete marker and
does not require index.db.

SQLite export layout:

  <hub>/index-sets/<index_set_id>/runs/<run_id>/index.db
  <hub>/index-sets/<index_set_id>/runs/<run_id>/identity.json
  <hub>/index-sets/<index_set_id>/runs/<run_id>/complete.json  (commit marker)

Durable export layout:

  <hub>/index-sets/<index_set_id>/runs/<run_id>/manifest.json
  <hub>/index-sets/<index_set_id>/runs/<run_id>/segments/...
  <hub>/index-sets/<index_set_id>/runs/<run_id>/complete.json

After a successful upload, the command attempts to advance the latest pointer:

  <hub>/index-sets/<index_set_id>/latest.json

The complete.json marker is written last and serves as the commit signal.
Consumers should only trust runs where complete.json is present.

Examples:
  # Export latest local artifact (auto: durable if present, else sqlite)
  gonimbus index export --hub file:///data/index-hub/ --index-set idx_da038d8171b4a9ba

  # Export to S3 hub with explicit profile
  gonimbus index export --hub s3://my-bucket/index-hub/ \
    --index-set idx_da038d8171b4a9ba --hub-profile my-profile

  # Export a specific run
  gonimbus index export --hub s3://my-bucket/index-hub/ \
    --index-set idx_da038d8171b4a9ba --run-id run_1709654400000000000

  # Force SQLite compatibility export
  gonimbus index export --hub file:///data/index-hub/ \
    --index-set idx_da038d8171b4a9ba --format sqlite`,
	RunE: runIndexExport,
}

func init() {
	indexCmd.AddCommand(indexExportCmd)

	indexExportCmd.Flags().String("hub", "", "Hub root URI (s3://bucket/prefix/, gs://bucket/prefix/, or file:///path/)")
	indexExportCmd.Flags().String("index-set", "", "Index set ID to export (required)")
	indexExportCmd.Flags().String("run-id", "", "Specific run ID to export (default: latest successful)")
	indexExportCmd.Flags().String("db", "", "Explicit local DB path (overrides index-set lookup)")
	indexExportCmd.Flags().String("format", "auto", "Index format to export (auto, sqlite, sqlite-v1, durable, durable-v2)")

	// Hub provider auth
	indexExportCmd.Flags().String("hub-profile", "", "AWS profile for hub destination")
	indexExportCmd.Flags().String("hub-region", "", "AWS region for hub destination")
	indexExportCmd.Flags().String("hub-endpoint", "", "Custom endpoint for hub destination")
	indexExportCmd.Flags().String("hub-gcp-project", "", "GCP project hint for GCS hub destination")
	addLatestPointerFlags(indexExportCmd)

	_ = indexExportCmd.MarkFlagRequired("hub")
	_ = indexExportCmd.MarkFlagRequired("index-set")
}

// hubDestSpec describes a hub root for export operations.
type hubDestSpec struct {
	Provider       string
	Bucket         string
	Prefix         string // key prefix within the bucket (includes trailing /)
	Region         string
	Profile        string
	Endpoint       string
	ForcePathStyle bool
	GCPProject     string
	BaseDir        string // for file:// hubs
}

// parseHubURI parses a hub root URI into a destination spec.
// Unlike parseOutputDest, this expects a prefix (directory), not a single file.
func parseHubURI(uri string) (*hubDestSpec, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil, fmt.Errorf("hub URI is required")
	}

	// Ensure trailing slash for prefix consistency
	if !strings.HasSuffix(uri, "/") {
		uri += "/"
	}

	lower := strings.ToLower(uri)

	if strings.HasPrefix(lower, "file://") {
		path := uri[len("file://"):]
		path = filepath.Clean(path)
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("file hub path must be absolute: %s", path)
		}
		return &hubDestSpec{
			Provider: "file",
			BaseDir:  path,
		}, nil
	}

	if strings.HasPrefix(lower, "s3://") || strings.HasPrefix(lower, "gs://") {
		scheme := "s3"
		providerName := string(provider.ProviderS3)
		prefixLen := len("s3://")
		if strings.HasPrefix(lower, "gs://") {
			scheme = "gs"
			providerName = string(provider.ProviderGCS)
			prefixLen = len("gs://")
		}
		remainder := uri[prefixLen:]
		slashIdx := strings.Index(remainder, "/")
		if slashIdx == -1 {
			return nil, fmt.Errorf("%s hub URI must include a prefix path: %s", scheme, uri)
		}
		bucket := remainder[:slashIdx]
		prefix := remainder[slashIdx+1:]
		if bucket == "" {
			return nil, fmt.Errorf("%s hub URI missing bucket: %s", scheme, uri)
		}
		return &hubDestSpec{
			Provider: providerName,
			Bucket:   bucket,
			Prefix:   prefix,
		}, nil
	}

	return nil, fmt.Errorf("unsupported hub scheme (supported: s3, gs, file): %s", uri)
}

// hubArtifactKey builds the full key for an artifact within the hub.
func hubArtifactKey(hub *hubDestSpec, parts ...string) string {
	return hub.Prefix + strings.Join(parts, "/")
}

// newHubProvider creates a provider for the hub destination.
func newHubProvider(ctx context.Context, hub *hubDestSpec) (provider.ObjectPutter, error) {
	spec := &outputDestSpec{
		Provider:       hub.Provider,
		Bucket:         hub.Bucket,
		Region:         hub.Region,
		Profile:        hub.Profile,
		Endpoint:       hub.Endpoint,
		ForcePathStyle: hub.ForcePathStyle,
		GCPProject:     hub.GCPProject,
		BaseDir:        hub.BaseDir,
	}
	return newOutputProvider(ctx, spec)
}

func runIndexExport(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	hubURI, _ := cmd.Flags().GetString("hub")
	indexSetFlag, _ := cmd.Flags().GetString("index-set")
	runIDFlag, _ := cmd.Flags().GetString("run-id")
	dbFlag, _ := cmd.Flags().GetString("db")
	formatFlag, _ := cmd.Flags().GetString("format")
	hubProfile, _ := cmd.Flags().GetString("hub-profile")
	hubRegion, _ := cmd.Flags().GetString("hub-region")
	hubEndpoint, _ := cmd.Flags().GetString("hub-endpoint")
	hubGCPProject, _ := cmd.Flags().GetString("hub-gcp-project")

	// Parse hub destination
	hub, err := parseHubURI(hubURI)
	if err != nil {
		return err
	}
	hub.Profile = hubProfile
	hub.Region = hubRegion
	hub.Endpoint = hubEndpoint
	hub.GCPProject = strings.TrimSpace(hubGCPProject)
	if hub.Endpoint != "" {
		hub.ForcePathStyle = true
	}

	// Validate index-set ID early (shared validation with index query)
	cleanSetID := strings.TrimPrefix(indexSetFlag, "idx_")
	if cleanSetID == "" || !validHexPattern.MatchString(cleanSetID) {
		return fmt.Errorf("invalid index set ID: %s (must be hex characters, max 64)", indexSetFlag)
	}

	formatMode, err := normalizeIndexExportFormat(formatFlag)
	if err != nil {
		return err
	}

	// Create hub provider once for either path.
	putter, err := newHubProvider(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := putter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	// Durable-first auto selection (and explicit durable) without requiring index.db.
	// Explicit --db remains a SQLite-centric operator path: resolve the SQLite run first.
	if dbFlag == "" && (formatMode == "auto" || formatMode == indexHubFormatDurableV2) {
		if durableIndexSetID, durableRunID, durableErr := resolveLocalDurableExportTarget(indexSetFlag, runIDFlag); durableErr == nil {
			if _, loadErr := loadLocalDurableSnapshotForExport(durableIndexSetID, durableRunID); loadErr == nil {
				if formatMode == "auto" {
					_, _ = fmt.Fprintf(os.Stderr, "export format auto selected: %s\n", indexHubFormatDurableV2)
				}
				indexSet := &indexstore.IndexSet{IndexSetID: durableIndexSetID}
				run := &indexstore.IndexRun{RunID: durableRunID}
				_, _ = fmt.Fprintf(os.Stderr, "Exporting index_set=%s run=%s format=%s to %s\n", durableIndexSetID, durableRunID, indexHubFormatDurableV2, hubURI)
				return runIndexExportDurable(ctx, cmd, hub, getter, putter, indexSet, run)
			} else if formatMode == indexHubFormatDurableV2 {
				return loadErr
			}
		} else if formatMode == indexHubFormatDurableV2 {
			return durableErr
		}
	}

	// Open local index DB and resolve artifact paths (sqlite path / auto fallback)
	var db *sql.DB
	var indexSet *indexstore.IndexSet
	var localDBPath string
	if dbFlag != "" {
		// --db mode: derive all paths from the explicit DB location
		localDBPath = dbFlag
		db, err = openIndexDB(ctx, dbFlag)
		if err != nil {
			return fmt.Errorf("open index database: %w", err)
		}
		indexSet, err = matchIndexSetInDB(ctx, db, cleanSetID)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("index set %s not found in database %s: %w", indexSetFlag, dbFlag, err)
		}
	} else {
		db, indexSet, err = openIndexDBByID(ctx, indexSetFlag)
		if err != nil {
			if formatMode == "auto" {
				return fmt.Errorf("auto export found neither a local durable snapshot nor a local sqlite index for %s: %w", indexSetFlag, err)
			}
			return err
		}
		// Resolve artifact path via shared resolver
		localDBPath, err = resolveLocalDBPath(indexSetFlag)
		if err != nil {
			_ = db.Close()
			return err
		}
	}
	defer func() { _ = db.Close() }()

	// Resolve run from SQLite
	run, err := resolveExportRun(ctx, db, indexSet.IndexSetID, runIDFlag)
	if err != nil {
		return err
	}

	// With --db (or auto after durable miss), prefer durable when the selected run has a durable complete marker.
	if formatMode == "auto" || formatMode == indexHubFormatDurableV2 {
		if _, loadErr := loadLocalDurableSnapshotForExport(indexSet.IndexSetID, run.RunID); loadErr == nil {
			if formatMode == "auto" {
				_, _ = fmt.Fprintf(os.Stderr, "export format auto selected: %s\n", indexHubFormatDurableV2)
			}
			_, _ = fmt.Fprintf(os.Stderr, "Exporting index_set=%s run=%s format=%s to %s\n", indexSet.IndexSetID, run.RunID, indexHubFormatDurableV2, hubURI)
			return runIndexExportDurable(ctx, cmd, hub, getter, putter, indexSet, run)
		} else if formatMode == indexHubFormatDurableV2 {
			return loadErr
		}
	}

	if formatMode == "auto" {
		_, _ = fmt.Fprintf(os.Stderr, "export format auto selected: %s\n", indexHubFormatSQLiteV1)
	}
	_, _ = fmt.Fprintf(os.Stderr, "Exporting index_set=%s run=%s format=%s to %s\n", indexSet.IndexSetID, run.RunID, indexHubFormatSQLiteV1, hubURI)

	localDir := filepath.Dir(localDBPath)
	localIdentityPath := filepath.Join(localDir, "identity.json")

	// Compute checksums
	dbChecksum, dbSize, err := hashFile(localDBPath)
	if err != nil {
		return fmt.Errorf("hash index.db: %w", err)
	}

	var identityChecksum string
	var identitySize int64
	identityBytes, err := os.ReadFile(localIdentityPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("read identity.json: %w", err)
		}
		// identity.json is optional for older indexes
		_, _ = fmt.Fprintln(os.Stderr, "warning: identity.json not found; complete.json will omit identity artifact entry")
	} else {
		h := sha256.Sum256(identityBytes)
		identityChecksum = hex.EncodeToString(h[:])
		identitySize = int64(len(identityBytes))
	}

	// Get summary stats for provenance
	summary, err := indexstore.GetIndexSetSummary(ctx, db, indexSet.IndexSetID)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: could not get index summary: %v\n", err)
	}

	// Publish sequence (brief contract): index.db + identity.json first, complete.json last, then latest.json
	runPrefix := []string{"index-sets", indexSet.IndexSetID, "runs", run.RunID}

	// 1. Upload index.db
	indexDBKey := hubArtifactKey(hub, append(runPrefix, "index.db")...)
	_, _ = fmt.Fprintf(os.Stderr, "  uploading index.db (%d bytes)...\n", dbSize)
	if err := uploadToOutputDest(ctx, putter, indexDBKey, localDBPath); err != nil {
		return fmt.Errorf("upload index.db: %w", err)
	}

	// 2. Upload identity.json (if present)
	if len(identityBytes) > 0 {
		identityKey := hubArtifactKey(hub, append(runPrefix, "identity.json")...)
		_, _ = fmt.Fprintf(os.Stderr, "  uploading identity.json (%d bytes)...\n", identitySize)
		if err := uploadToOutputDest(ctx, putter, identityKey, localIdentityPath); err != nil {
			return fmt.Errorf("upload identity.json: %w", err)
		}
	}

	// 3. Write complete.json (commit marker — written last)
	completeJSON, err := buildCompleteJSON(indexSet, run, summary, dbChecksum, dbSize, identityChecksum, identitySize)
	if err != nil {
		return fmt.Errorf("build complete.json: %w", err)
	}
	completeKey := hubArtifactKey(hub, append(runPrefix, "complete.json")...)
	_, _ = fmt.Fprintln(os.Stderr, "  writing complete.json (commit marker)...")
	if err := uploadBytes(ctx, putter, completeKey, completeJSON); err != nil {
		return fmt.Errorf("upload complete.json: %w", err)
	}

	// 4. Update latest.json (CAS pointer advance by default)
	_, _ = fmt.Fprintln(os.Stderr, "  updating latest.json...")
	latestOpts, err := latestPointerOptionsFromCommand(cmd)
	if err != nil {
		return err
	}
	outcome, err := advanceLatestPointer(ctx, hub, getter, putter, indexSet.IndexSetID, run.RunID, latestOpts)
	if err != nil {
		return fmt.Errorf("update latest.json: %w", err)
	}
	printLatestPointerOutcome(os.Stderr, outcome, indexSet.IndexSetID, run.RunID)

	_, _ = fmt.Fprintf(os.Stderr, "Export complete: %s/runs/%s/\n", indexSet.IndexSetID, run.RunID)
	return nil
}

// normalizeIndexExportFormat accepts auto plus the hub format aliases.
// Returns "auto", sqlite-v1, or durable-v2.
func normalizeIndexExportFormat(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "auto":
		return "auto", nil
	default:
		format, err := normalizeIndexHubFormat(raw)
		if err != nil {
			return "", fmt.Errorf("--format must be one of: auto, sqlite, sqlite-v1, durable, durable-v2")
		}
		return format, nil
	}
}

// resolveLocalDurableExportTarget finds a local durable index set + run without opening SQLite.
func resolveLocalDurableExportTarget(indexSetFlag, runIDFlag string) (string, string, error) {
	indexSetID, err := resolveLocalDurableIndexSetID(indexSetFlag)
	if err != nil {
		return "", "", err
	}
	runID := strings.TrimSpace(runIDFlag)
	if runID == "" {
		runID, err = resolveLocalDurableLatestRunID(indexSetID)
		if err != nil {
			return "", "", err
		}
	} else if err := validateRunID(runID); err != nil {
		return "", "", err
	}
	return indexSetID, runID, nil
}

func resolveLocalDurableIndexSetID(indexSetFlag string) (string, error) {
	cleanID := strings.TrimPrefix(strings.TrimSpace(indexSetFlag), "idx_")
	if cleanID == "" || !validHexPattern.MatchString(cleanID) {
		return "", fmt.Errorf("invalid index set ID: %s (must be hex characters, max 64)", indexSetFlag)
	}
	// Full IDs can be used directly for the segment-cache path.
	if len(cleanID) == 64 {
		fullID := "idx_" + cleanID
		if _, err := indexSubstrateSegmentCacheDir(fullID); err != nil {
			return "", err
		}
		return fullID, nil
	}

	segmentRoot, err := appDataPath(appDataClassSegmentCache)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(segmentRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no local durable snapshots found")
		}
		return "", fmt.Errorf("read durable segment cache: %w", err)
	}
	var matches []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		dirHex := strings.TrimPrefix(name, "idx_")
		if strings.HasPrefix(dirHex, cleanID) || (len(cleanID) == 64 && strings.HasPrefix(cleanID, dirHex)) {
			// Prefer directories that actually look like durable snapshots.
			if _, statErr := os.Stat(filepath.Join(segmentRoot, name, "latest.json")); statErr == nil {
				matches = append(matches, name)
				continue
			}
			if runs, readErr := os.ReadDir(filepath.Join(segmentRoot, name, "runs")); readErr == nil && len(runs) > 0 {
				matches = append(matches, name)
			}
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no local durable snapshot matching index set %s", indexSetFlag)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("ambiguous durable index set ID %s matches %d snapshots: %s", indexSetFlag, len(matches), strings.Join(matches, ", "))
	}
	return matches[0], nil
}

func resolveLocalDurableLatestRunID(indexSetID string) (string, error) {
	segmentRoot, err := indexSubstrateSegmentCacheDir(indexSetID)
	if err != nil {
		return "", err
	}
	latestPath := filepath.Join(segmentRoot, "latest.json")
	data, err := os.ReadFile(latestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no local durable latest.json for index set %s; pass --run-id", indexSetID)
		}
		return "", fmt.Errorf("read local durable latest.json: %w", err)
	}
	var latest struct {
		IndexSetID string `json:"index_set_id"`
		RunID      string `json:"run_id"`
	}
	if err := json.Unmarshal(data, &latest); err != nil {
		return "", fmt.Errorf("parse local durable latest.json: %w", err)
	}
	if strings.TrimSpace(latest.RunID) == "" {
		return "", fmt.Errorf("local durable latest.json missing run_id")
	}
	if strings.TrimSpace(latest.IndexSetID) != "" && latest.IndexSetID != indexSetID {
		return "", fmt.Errorf("local durable latest.json index_set_id mismatch")
	}
	if err := validateRunID(latest.RunID); err != nil {
		return "", fmt.Errorf("local durable latest.json: %w", err)
	}
	return latest.RunID, nil
}

func runIndexExportDurable(ctx context.Context, cmd *cobra.Command, hub *hubDestSpec, getter provider.ObjectGetter, putter provider.ObjectPutter, indexSet *indexstore.IndexSet, run *indexstore.IndexRun) error {
	local, err := loadLocalDurableSnapshotForExport(indexSet.IndexSetID, run.RunID)
	if err != nil {
		return err
	}

	runPrefix := []string{"index-sets", indexSet.IndexSetID, "runs", run.RunID}
	for _, segment := range local.Manifest.Segments {
		localPath, err := safeLocalArtifactPath(local.SegmentDir, segment.Path)
		if err != nil {
			return fmt.Errorf("segment %s: %w", segment.SegmentID, err)
		}
		key := hubArtifactKey(hub, append(runPrefix, "segments", segment.Path)...)
		_, _ = fmt.Fprintf(os.Stderr, "  uploading segment %s (%d bytes)...\n", segment.Path, segment.SizeBytes)
		if err := uploadToOutputDest(ctx, putter, key, localPath); err != nil {
			return fmt.Errorf("upload segment %s: %w", segment.Path, err)
		}
	}

	manifestKey := hubArtifactKey(hub, append(runPrefix, "manifest.json")...)
	_, _ = fmt.Fprintf(os.Stderr, "  uploading manifest.json (%d bytes)...\n", local.ManifestSize)
	if err := uploadToOutputDest(ctx, putter, manifestKey, local.ManifestPath); err != nil {
		return fmt.Errorf("upload durable manifest: %w", err)
	}

	completeJSON, err := buildDurableCompleteJSON(indexSet, run, local)
	if err != nil {
		return fmt.Errorf("build durable complete.json: %w", err)
	}
	completeKey := hubArtifactKey(hub, append(runPrefix, "complete.json")...)
	_, _ = fmt.Fprintln(os.Stderr, "  writing complete.json (commit marker)...")
	if err := uploadBytes(ctx, putter, completeKey, completeJSON); err != nil {
		return fmt.Errorf("upload complete.json: %w", err)
	}

	_, _ = fmt.Fprintln(os.Stderr, "  updating latest.json...")
	latestOpts, err := latestPointerOptionsFromCommand(cmd)
	if err != nil {
		return err
	}
	outcome, err := advanceLatestPointer(ctx, hub, getter, putter, indexSet.IndexSetID, run.RunID, latestOpts)
	if err != nil {
		return fmt.Errorf("update latest.json: %w", err)
	}
	printLatestPointerOutcome(os.Stderr, outcome, indexSet.IndexSetID, run.RunID)

	_, _ = fmt.Fprintf(os.Stderr, "Export complete: %s/runs/%s/ (%s)\n", indexSet.IndexSetID, run.RunID, indexHubFormatDurableV2)
	return nil
}

// resolveExportRun finds the run to export: explicit run ID or latest successful.
func resolveExportRun(ctx context.Context, db *sql.DB, indexSetID, runID string) (*indexstore.IndexRun, error) {
	if runID != "" {
		run, err := indexstore.GetIndexRun(ctx, db, runID)
		if err != nil {
			return nil, fmt.Errorf("run %s not found: %w", runID, err)
		}
		if run.IndexSetID != indexSetID {
			return nil, fmt.Errorf("run %s belongs to index set %s, not %s", runID, run.IndexSetID, indexSetID)
		}
		return run, nil
	}

	runs, err := indexstore.ListIndexRuns(ctx, db, indexSetID)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}

	// Find latest successful run
	for i := range runs {
		if runs[i].Status == indexstore.RunStatusSuccess {
			return &runs[i], nil
		}
	}

	// Fallback: any completed run
	for i := range runs {
		if runs[i].Status == indexstore.RunStatusPartial {
			_, _ = fmt.Fprintf(os.Stderr, "warning: no successful run found; exporting partial run %s\n", runs[i].RunID)
			return &runs[i], nil
		}
	}

	return nil, fmt.Errorf("no exportable run found for index set %s", indexSetID)
}

// resolveLocalDBPath finds the local index.db path for an index set
// using the shared directory resolver (same validation/ambiguity as index query).
func resolveLocalDBPath(indexSetFlag string) (string, error) {
	rootDir, err := indexRootDir()
	if err != nil {
		return "", err
	}

	match, err := resolveIndexDirInRoot(rootDir, indexSetFlag)
	if err != nil {
		return "", err
	}
	return match.DBPath, nil
}

// matchIndexSetInDB finds an index set in an already-opened DB by prefix match.
// Uses the same hex validation as resolveIndexDirInRoot to prevent loose matching.
func matchIndexSetInDB(ctx context.Context, db *sql.DB, cleanHexID string) (*indexstore.IndexSet, error) {
	sets, err := indexstore.ListIndexSets(ctx, db, "")
	if err != nil {
		return nil, fmt.Errorf("list index sets: %w", err)
	}

	var matches []*indexstore.IndexSet
	for i := range sets {
		setHex := strings.TrimPrefix(sets[i].IndexSetID, "idx_")
		if strings.HasPrefix(setHex, cleanHexID) {
			matches = append(matches, &sets[i])
		}
	}

	if len(matches) == 0 {
		return nil, fmt.Errorf("no matching index set")
	}
	if len(matches) > 1 {
		ids := make([]string, len(matches))
		for i, m := range matches {
			ids[i] = m.IndexSetID
		}
		return nil, fmt.Errorf("ambiguous index set ID, matches %d sets: %s", len(matches), strings.Join(ids, ", "))
	}
	return matches[0], nil
}

// hashFile computes the SHA-256 of a file and returns the hex digest and size.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// uploadBytes uploads raw bytes to the hub via a temp file.
func uploadBytes(ctx context.Context, putter provider.ObjectPutter, key string, data []byte) error {
	tmp, err := os.CreateTemp("", "gonimbus-export-*.json")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	return uploadToOutputDest(ctx, putter, key, tmpPath)
}

func exportedByString() string {
	v := versionInfo.Version
	if v == "" {
		v = "dev"
	}
	return fmt.Sprintf("gonimbus/%s", v)
}

// exportableRunStatus maps RunStatus to the schema-valid run_status enum values.
// This guards against drift if new statuses are added to indexstore.RunStatus.
func exportableRunStatus(status indexstore.RunStatus) (string, error) {
	switch status {
	case indexstore.RunStatusSuccess:
		return "success", nil
	case indexstore.RunStatusPartial:
		return "partial", nil
	case indexstore.RunStatusFailed:
		return "failed", nil
	case indexstore.RunStatusFailedResumable:
		return "failed-resumable", nil
	default:
		return "", fmt.Errorf("unsupported run status for export: %q", status)
	}
}

// buildCompleteJSON constructs the complete.json commit marker.
func buildCompleteJSON(
	indexSet *indexstore.IndexSet,
	run *indexstore.IndexRun,
	summary *indexstore.IndexSetSummary,
	dbSHA256 string, dbSize int64,
	identitySHA256 string, identitySize int64,
) ([]byte, error) {
	type artifactEntry struct {
		SizeBytes int64  `json:"size_bytes"`
		SHA256    string `json:"sha256"`
	}

	type sourceInfo struct {
		BaseURI        string  `json:"base_uri"`
		Provider       string  `json:"provider"`
		RunStatus      string  `json:"run_status"`
		RunStartedAt   string  `json:"run_started_at"`
		RunEndedAt     *string `json:"run_ended_at,omitempty"`
		ObjectCount    *int64  `json:"object_count,omitempty"`
		TotalSizeBytes *int64  `json:"total_size_bytes,omitempty"`
	}

	type artifacts struct {
		IndexDB      artifactEntry  `json:"index_db"`
		IdentityJSON *artifactEntry `json:"identity_json,omitempty"`
	}

	type completeDoc struct {
		Version             string     `json:"version"`
		MarkerSchemaVersion string     `json:"marker_schema_version"`
		Format              string     `json:"format"`
		FormatVersion       string     `json:"format_version"`
		IndexSetID          string     `json:"index_set_id"`
		RunID               string     `json:"run_id"`
		CompletedAt         string     `json:"completed_at"`
		ExportedBy          string     `json:"exported_by"`
		Artifacts           artifacts  `json:"artifacts"`
		Source              sourceInfo `json:"source"`
	}

	runStatus, err := exportableRunStatus(run.Status)
	if err != nil {
		return nil, err
	}

	doc := completeDoc{
		Version:             "1.0",
		MarkerSchemaVersion: indexHubMarkerSchemaV1,
		Format:              indexHubFormatSQLiteV1,
		FormatVersion:       "1",
		IndexSetID:          indexSet.IndexSetID,
		RunID:               run.RunID,
		CompletedAt:         time.Now().UTC().Format(time.RFC3339),
		ExportedBy:          exportedByString(),
		Artifacts: artifacts{
			IndexDB: artifactEntry{SizeBytes: dbSize, SHA256: dbSHA256},
		},
		Source: sourceInfo{
			BaseURI:      indexSet.BaseURI,
			Provider:     indexSet.Provider,
			RunStatus:    runStatus,
			RunStartedAt: run.StartedAt.Format(time.RFC3339),
		},
	}

	if identitySHA256 != "" {
		doc.Artifacts.IdentityJSON = &artifactEntry{SizeBytes: identitySize, SHA256: identitySHA256}
	}

	if run.EndedAt != nil {
		ts := run.EndedAt.Format(time.RFC3339)
		doc.Source.RunEndedAt = &ts
	}

	if summary != nil {
		doc.Source.ObjectCount = &summary.ActiveObjects
		doc.Source.TotalSizeBytes = &summary.TotalSizeBytes
	}

	return json.MarshalIndent(doc, "", "  ")
}

// buildLatestJSON constructs the latest.json pointer.
func buildLatestJSON(indexSet *indexstore.IndexSet, run *indexstore.IndexRun) ([]byte, error) {
	return buildLatestJSONForRun(indexSet.IndexSetID, run.RunID)
}
