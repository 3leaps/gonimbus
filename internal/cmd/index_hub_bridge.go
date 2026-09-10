package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	custodyBridgeOutputFormat = "custody-bridge-receipt-jsonl-v1"
	custodyBridgeReceiptType  = "gonimbus.index.custody_bridge_receipt.v1"
	custodyBridgeReceiptV1    = "1.0.0"
	custodyBridgeInputLimit   = int64(1 << 20)
	custodyBridgeMaxJSONCount = int64(1<<53 - 1)
)

var (
	custodyBridgeRunIDRE     = regexp.MustCompile(`^run_([0-9]{1,32}|[0-9A-HJKMNP-TV-Z]{26})$`)
	custodyBridgeDigestRE    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	custodyBridgeHandleRefRE = regexp.MustCompile(`^hnd_[0-9a-f]{32}$`)
	custodyBridgeBucketRE    = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$`)

	configuredBridgeHubReadHandleResolver           = resolveConfiguredBridgeHubReadHandle
	configuredHubPublishHandleResolver              = resolveConfiguredHubPublishHandle
	runLegacyDurableBridge                          = indexreader.BridgeLegacyDurableRun
	custodyBridgeClock                              = func() time.Time { return time.Now().UTC() }
	custodyBridgeRandom                   io.Reader = rand.Reader
)

var indexHubBridgeDurableCmd = &cobra.Command{
	Use:   "bridge-durable",
	Short: "Bridge one exact legacy durable run into a new custody envelope",
	Long: `Verify one exact legacy durable run through a configured read handle
and publish its immutable bytes through a separate atomic create-only handle.
The command never lists, selects latest, overwrites, deletes, or walks a live
source. Source and target authority ranges must be provably disjoint.`,
	Args: cobra.NoArgs,
	RunE: runIndexHubBridgeDurable,
}

func init() {
	indexHubCmd.AddCommand(indexHubBridgeDurableCmd)
	indexHubBridgeDurableCmd.Flags().String("source-hub-read-handle", "", "Configured source hub_read_handle logical name (required)")
	indexHubBridgeDurableCmd.Flags().String("target-hub-publish-handle", "", "Configured target hub_publish_handle logical name (required)")
	indexHubBridgeDurableCmd.Flags().String("index-set", "", "Full lowercase index set ID (required)")
	indexHubBridgeDurableCmd.Flags().String("run-id", "", "Exact durable run ID (required)")
	indexHubBridgeDurableCmd.Flags().String("identity-file", "", "Bounded no-follow canonical identity file (exclusive with --identity-declaration)")
	indexHubBridgeDurableCmd.Flags().String("identity-declaration", "", "Bounded no-follow typed identity declaration (exclusive with --identity-file)")
	indexHubBridgeDurableCmd.Flags().String("snapshot-completion-evidence", "", "Optional bounded no-follow complete.v2 evidence file")
	indexHubBridgeDurableCmd.Flags().String("output-format", "", "Output framing (required: custody-bridge-receipt-jsonl-v1)")
}

type hubAuthorityRange struct {
	provider     string
	endpoint     string
	container    string
	prefix       string
	physicalPath string
	fileRootInfo os.FileInfo
}

type hubPublishHandleResolution struct {
	publisher indexreader.HubConditionalCreatePublisher
	closer    io.Closer
	rangeID   hubAuthorityRange
}

type bridgeHubReadHandleResolution struct {
	reader  indexreader.HubExactObjectReader
	closer  io.Closer
	rangeID hubAuthorityRange
}

type bridgeHubExactProviderAdapter struct {
	getter       provider.ObjectGetter
	prefix       string
	fileRoot     string
	fileRootInfo os.FileInfo
}

func (a *bridgeHubExactProviderAdapter) OpenExact(
	ctx context.Context,
	key string,
) (io.ReadCloser, int64, error) {
	if a == nil || a.getter == nil || !validExactHubRelativeKey(key) || !a.fileRootBindingValid() {
		return nil, 0, indexreader.ErrHubExactRead
	}
	return a.getter.GetObject(ctx, a.prefix+key)
}

func (a *bridgeHubExactProviderAdapter) fileRootBindingValid() bool {
	if a.fileRoot == "" {
		return true
	}
	return sameNoFollowDirectory(a.fileRoot, a.fileRootInfo)
}

type custodyBridgeReceipt struct {
	Type                      string                         `json:"type"`
	SchemaVersion             string                         `json:"schema_version"`
	Outcome                   string                         `json:"outcome"`
	SourceHandleRef           string                         `json:"source_handle_ref"`
	TargetHandleRef           string                         `json:"target_handle_ref"`
	IndexSetID                string                         `json:"index_set_id"`
	RunID                     string                         `json:"run_id"`
	LegacyMarkerSchemaVersion string                         `json:"legacy_marker_schema_version"`
	ResultMarkerSchemaVersion string                         `json:"result_marker_schema_version"`
	LegacyMarkerSHA256        string                         `json:"legacy_marker_sha256"`
	SourceIdentitySHA256      string                         `json:"source_identity_sha256"`
	SourceIdentitySchema      string                         `json:"source_identity_schema"`
	SourceIdentityProfile     string                         `json:"source_identity_profile"`
	ManifestSHA256            string                         `json:"manifest_sha256"`
	Declared                  custodyBridgeDeclared          `json:"declared"`
	Verification              custodyBridgeVerification      `json:"verification"`
	RunStart                  indexreader.BridgeRunStart     `json:"run_start"`
	SnapshotTime              indexreader.BridgeSnapshotTime `json:"snapshot_time"`
	ConversionIdentitySHA256  string                         `json:"conversion_identity_sha256"`
	ResultCompleteSHA256      string                         `json:"result_complete_sha256"`
	OperationStartedAt        string                         `json:"operation_started_at"`
	OperationCompletedAt      string                         `json:"operation_completed_at"`
	Disposition               string                         `json:"disposition"`
}

type custodyBridgeDeclared struct {
	Rows     int `json:"rows"`
	Segments int `json:"segments"`
}

type custodyBridgeVerification struct {
	SegmentsVerified          int   `json:"segments_verified"`
	BytesRead                 int64 `json:"bytes_read"`
	BytesConditionallyCreated int64 `json:"bytes_conditionally_created"`
}

func runIndexHubBridgeDurable(cmd *cobra.Command, _ []string) error {
	if IsReadOnly() {
		return errors.New("readonly mode enabled: custody bridge publication is disabled")
	}
	sourceName, _ := cmd.Flags().GetString("source-hub-read-handle")
	targetName, _ := cmd.Flags().GetString("target-hub-publish-handle")
	indexSetID, _ := cmd.Flags().GetString("index-set")
	runID, _ := cmd.Flags().GetString("run-id")
	identityPath, _ := cmd.Flags().GetString("identity-file")
	declarationPath, _ := cmd.Flags().GetString("identity-declaration")
	evidencePath, _ := cmd.Flags().GetString("snapshot-completion-evidence")
	outputFormat, _ := cmd.Flags().GetString("output-format")

	sourceName = strings.TrimSpace(sourceName)
	targetName = strings.TrimSpace(targetName)
	indexSetID = strings.TrimSpace(indexSetID)
	runID = strings.TrimSpace(runID)
	outputFormat = strings.TrimSpace(outputFormat)
	if !hubReadHandleNameRE.MatchString(sourceName) {
		return errors.New("source hub read handle is missing or invalid")
	}
	if !hubReadHandleNameRE.MatchString(targetName) {
		return errors.New("target hub publish handle is missing or invalid")
	}
	if sourceName == targetName {
		return errors.New("source and target logical handle names must differ")
	}
	if !validFullIndexSetID(indexSetID) || indexSetID != strings.ToLower(indexSetID) {
		return errors.New("index set selector must be one full lowercase ID")
	}
	if !custodyBridgeRunIDRE.MatchString(runID) {
		return errors.New("run selector must be one exact run ID")
	}
	if outputFormat != custodyBridgeOutputFormat {
		return fmt.Errorf("--output-format must be %s", custodyBridgeOutputFormat)
	}
	hasIdentityFile := cmd.Flags().Changed("identity-file")
	hasDeclaration := cmd.Flags().Changed("identity-declaration")
	if hasIdentityFile == hasDeclaration {
		return errors.New("exactly one identity input is required")
	}

	var authority indexreader.CanonicalIdentityAuthority
	if hasIdentityFile {
		body, err := readBoundedNoFollowLocalFile(strings.TrimSpace(identityPath), custodyBridgeInputLimit)
		if err != nil {
			return errors.New("identity input is invalid")
		}
		authority.CanonicalJSONLF = body
	} else {
		body, err := readBoundedNoFollowLocalFile(strings.TrimSpace(declarationPath), custodyBridgeInputLimit)
		if err != nil {
			return errors.New("identity input is invalid")
		}
		authority.DeclarationJSON = body
	}
	var evidence []byte
	if strings.TrimSpace(evidencePath) != "" || cmd.Flags().Changed("snapshot-completion-evidence") {
		body, err := readBoundedNoFollowLocalFile(strings.TrimSpace(evidencePath), custodyBridgeInputLimit)
		if err != nil {
			return errors.New("snapshot completion evidence is invalid")
		}
		evidence = body
	}

	source, err := configuredBridgeHubReadHandleResolver(cmd.Context(), sourceName)
	if err != nil {
		return err
	}
	if source.closer != nil {
		defer func() { _ = source.closer.Close() }()
	}
	target, err := configuredHubPublishHandleResolver(cmd.Context(), targetName)
	if err != nil {
		return err
	}
	if target.closer != nil {
		defer func() { _ = target.closer.Close() }()
	}
	disjoint, err := proveHubAuthorityRangesDisjoint(source.rangeID, target.rangeID)
	if err != nil || !disjoint {
		return errors.New("source and target authority ranges are not provably disjoint")
	}

	sourceRef, targetRef, err := newCustodyBridgeHandleRefs()
	if err != nil {
		return errors.New("operation handle references are unavailable")
	}
	startedAt := custodyBridgeClock().UTC()
	if startedAt.IsZero() {
		return errors.New("operation clock is unavailable")
	}
	result, err := runLegacyDurableBridge(
		cmd.Context(),
		source.reader,
		target.publisher,
		authority,
		evidence,
		indexreader.BridgeOptions{
			IndexSetID:     indexSetID,
			RunID:          runID,
			ExportedBy:     exportedByString(),
			HubCommitClock: custodyBridgeClock,
		},
	)
	if err != nil {
		return sanitizedCustodyBridgeError(err)
	}
	completedAt := custodyBridgeClock().UTC()
	if completedAt.IsZero() || completedAt.Before(startedAt) {
		completedAt = startedAt
	}
	receipt := custodyBridgeReceiptFromResult(result, sourceRef, targetRef, startedAt, completedAt)
	if err := validateCustodyBridgeReceipt(receipt); err != nil {
		return errors.New("custody bridge produced an invalid terminal result")
	}
	if err := json.NewEncoder(cmd.OutOrStdout()).Encode(receipt); err != nil {
		return errors.New("write terminal custody bridge receipt")
	}
	return nil
}

func sanitizedCustodyBridgeError(err error) error {
	var bridgeErr *indexreader.BridgeError
	switch {
	case errors.As(err, &bridgeErr):
		return fmt.Errorf("custody bridge failed: %s", bridgeErr.Code)
	case errors.Is(err, context.Canceled):
		return errors.New("custody bridge interrupted")
	case errors.Is(err, context.DeadlineExceeded):
		return errors.New("custody bridge timed out")
	default:
		return errors.New("custody bridge failed")
	}
}

func custodyBridgeReceiptFromResult(
	result indexreader.BridgeResult,
	sourceRef, targetRef string,
	startedAt, completedAt time.Time,
) custodyBridgeReceipt {
	return custodyBridgeReceipt{
		Type: custodyBridgeReceiptType, SchemaVersion: custodyBridgeReceiptV1, Outcome: "success",
		SourceHandleRef: sourceRef, TargetHandleRef: targetRef,
		IndexSetID: result.IndexSetID, RunID: result.RunID,
		LegacyMarkerSchemaVersion: indexreader.HubMarkerSchemaV1,
		ResultMarkerSchemaVersion: indexreader.HubMarkerSchemaV3,
		LegacyMarkerSHA256:        result.LegacyMarkerSHA256,
		SourceIdentitySHA256:      result.IdentitySHA256,
		SourceIdentitySchema:      result.IdentitySchema,
		SourceIdentityProfile:     result.IdentityProfile,
		ManifestSHA256:            result.ManifestSHA256,
		Declared:                  custodyBridgeDeclared{Rows: result.DeclaredRows, Segments: result.DeclaredSegments},
		Verification: custodyBridgeVerification{
			SegmentsVerified:          result.SegmentsVerified,
			BytesRead:                 result.BytesRead,
			BytesConditionallyCreated: result.BytesConditionallyCreated,
		},
		RunStart: result.RunStart, SnapshotTime: result.SnapshotTime,
		ConversionIdentitySHA256: result.ConversionIdentitySHA256,
		ResultCompleteSHA256:     result.CompleteSHA256,
		OperationStartedAt:       startedAt.Format(time.RFC3339Nano),
		OperationCompletedAt:     completedAt.Format(time.RFC3339Nano),
		Disposition:              result.Disposition,
	}
}

func validateCustodyBridgeReceipt(receipt custodyBridgeReceipt) error {
	if receipt.Type != custodyBridgeReceiptType ||
		receipt.SchemaVersion != custodyBridgeReceiptV1 ||
		receipt.Outcome != "success" ||
		!custodyBridgeHandleRefRE.MatchString(receipt.SourceHandleRef) ||
		!custodyBridgeHandleRefRE.MatchString(receipt.TargetHandleRef) ||
		receipt.SourceHandleRef == receipt.TargetHandleRef ||
		!validFullIndexSetID(receipt.IndexSetID) ||
		!custodyBridgeRunIDRE.MatchString(receipt.RunID) ||
		receipt.LegacyMarkerSchemaVersion != indexreader.HubMarkerSchemaV1 ||
		receipt.ResultMarkerSchemaVersion != indexreader.HubMarkerSchemaV3 ||
		!custodyBridgeDigestRE.MatchString(receipt.LegacyMarkerSHA256) ||
		!custodyBridgeDigestRE.MatchString(receipt.SourceIdentitySHA256) ||
		receipt.SourceIdentitySchema != indexreader.BridgeIdentitySchema ||
		receipt.SourceIdentityProfile != indexreader.BridgeIdentityProfile ||
		!custodyBridgeDigestRE.MatchString(receipt.ManifestSHA256) ||
		!custodyBridgeDigestRE.MatchString(receipt.ConversionIdentitySHA256) ||
		!custodyBridgeDigestRE.MatchString(receipt.ResultCompleteSHA256) ||
		!validCustodyBridgeCounts(receipt) ||
		receipt.RunStart.Basis != indexreader.BridgeRunStartLegacyAsserted ||
		!validCanonicalUTCString(receipt.RunStart.StartedAt) ||
		!validCustodyBridgeSnapshotTime(receipt.SnapshotTime) ||
		(receipt.Disposition != indexreader.BridgeDispositionCreated &&
			receipt.Disposition != indexreader.BridgeDispositionAlreadyIdentical) {
		return errors.New("receipt contract")
	}
	started, err := time.Parse(time.RFC3339Nano, receipt.OperationStartedAt)
	if err != nil || started.Location() != time.UTC {
		return errors.New("receipt start")
	}
	completed, err := time.Parse(time.RFC3339Nano, receipt.OperationCompletedAt)
	if err != nil || completed.Location() != time.UTC || completed.Before(started) {
		return errors.New("receipt completion")
	}
	return nil
}

func validCustodyBridgeCounts(receipt custodyBridgeReceipt) bool {
	values := []int64{
		int64(receipt.Declared.Rows),
		int64(receipt.Declared.Segments),
		int64(receipt.Verification.SegmentsVerified),
		receipt.Verification.BytesRead,
		receipt.Verification.BytesConditionallyCreated,
	}
	for _, value := range values {
		if value < 0 || value > custodyBridgeMaxJSONCount {
			return false
		}
	}
	return true
}

func validCustodyBridgeSnapshotTime(snapshot indexreader.BridgeSnapshotTime) bool {
	switch snapshot.Basis {
	case indexreader.BridgeSnapshotTimeLegacyUnavailable:
		return snapshot.CompletedAt == "" && snapshot.EvidenceType == "" && snapshot.EvidenceSHA256 == ""
	case indexreader.BridgeSnapshotTimeExactCommit:
		return validCanonicalUTCString(snapshot.CompletedAt) &&
			snapshot.EvidenceType == "gonimbus.index.complete.v2" &&
			custodyBridgeDigestRE.MatchString(snapshot.EvidenceSHA256)
	default:
		return false
	}
}

func validCanonicalUTCString(value string) bool {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && parsed.Location() == time.UTC &&
		parsed.UTC().Format(time.RFC3339Nano) == value
}

func newCustodyBridgeHandleRefs() (string, string, error) {
	first, err := newCustodyBridgeHandleRef()
	if err != nil {
		return "", "", err
	}
	for attempts := 0; attempts < 4; attempts++ {
		second, err := newCustodyBridgeHandleRef()
		if err != nil {
			return "", "", err
		}
		if second != first {
			return first, second, nil
		}
	}
	return "", "", errors.New("handle reference collision")
}

func newCustodyBridgeHandleRef() (string, error) {
	var nonce [16]byte
	if _, err := io.ReadFull(custodyBridgeRandom, nonce[:]); err != nil {
		return "", err
	}
	return "hnd_" + hex.EncodeToString(nonce[:]), nil
}

func resolveConfiguredBridgeHubReadHandle(
	ctx context.Context,
	name string,
) (bridgeHubReadHandleResolution, error) {
	return resolveConfiguredBridgeHubReadHandleFrom(ctx, viper.GetViper(), name)
}

func resolveConfiguredBridgeHubReadHandleFrom(
	ctx context.Context,
	config *viper.Viper,
	name string,
) (bridgeHubReadHandleResolution, error) {
	if !hubReadHandleNameRE.MatchString(name) {
		return bridgeHubReadHandleResolution{}, errors.New("hub_read_handle name is invalid")
	}
	if config == nil {
		return bridgeHubReadHandleResolution{}, errors.New("hub_read_handle configuration is invalid")
	}
	cfg := config.Sub("hub_read_handles." + name)
	if cfg == nil {
		return bridgeHubReadHandleResolution{}, errors.New("hub_read_handle is not configured")
	}
	hub, err := parseHubURI(strings.TrimSpace(cfg.GetString("uri")))
	if err != nil {
		return bridgeHubReadHandleResolution{}, errors.New("hub_read_handle configuration is invalid")
	}
	hub.Profile = strings.TrimSpace(cfg.GetString("profile"))
	hub.Region = strings.TrimSpace(cfg.GetString("region"))
	hub.Endpoint = strings.TrimSpace(cfg.GetString("endpoint"))
	hub.GCPProject = strings.TrimSpace(cfg.GetString("gcp_project"))
	if hub.Endpoint != "" {
		hub.ForcePathStyle = true
	}
	rangeID, err := canonicalHubAuthorityRange(hub)
	if err != nil {
		return bridgeHubReadHandleResolution{}, errors.New("hub_read_handle configuration is invalid")
	}
	src := &uri.ObjectURI{Provider: hub.Provider, Bucket: hub.Bucket, Key: hub.Prefix}
	if hub.Provider == string(provider.ProviderFile) {
		src.Bucket = "local"
		src.Key = filepath.ToSlash(hub.BaseDir)
	}
	p, err := providerdispatch.NewSource(ctx, src, providerdispatch.SourceOptions{
		Command:     "index hub bridge-durable",
		FileBaseDir: hub.BaseDir,
		S3: providerdispatch.S3Options{
			Region: hub.Region, Profile: hub.Profile, Endpoint: hub.Endpoint,
			ForcePathStyle: hub.ForcePathStyle,
		},
		GCS: providerdispatch.GCSOptions{Project: hub.GCPProject},
	})
	if err != nil {
		return bridgeHubReadHandleResolution{}, errors.New("open hub_read_handle: provider unavailable")
	}
	getter, err := providerdispatch.RequireCapability[provider.ObjectGetter](
		p, "index hub bridge-durable", hub.Provider, "ObjectGetter",
	)
	if err != nil {
		_ = p.Close()
		return bridgeHubReadHandleResolution{}, errors.New("open hub_read_handle: exact object reads unavailable")
	}
	return bridgeHubReadHandleResolution{
		reader: &bridgeHubExactProviderAdapter{
			getter: getter, prefix: hub.Prefix,
			fileRoot: rangeID.physicalPath, fileRootInfo: rangeID.fileRootInfo,
		},
		closer:  p,
		rangeID: rangeID,
	}, nil
}

func resolveConfiguredHubPublishHandle(ctx context.Context, name string) (hubPublishHandleResolution, error) {
	return resolveConfiguredHubPublishHandleFrom(ctx, viper.GetViper(), name)
}

func resolveConfiguredHubPublishHandleFrom(
	ctx context.Context,
	config *viper.Viper,
	name string,
) (hubPublishHandleResolution, error) {
	if !hubReadHandleNameRE.MatchString(name) {
		return hubPublishHandleResolution{}, errors.New("hub_publish_handle name is invalid")
	}
	if config == nil {
		return hubPublishHandleResolution{}, errors.New("hub_publish_handle configuration is invalid")
	}
	cfg := config.Sub("hub_publish_handles." + name)
	if cfg == nil {
		return hubPublishHandleResolution{}, errors.New("hub_publish_handle is not configured")
	}
	hub, err := parseHubURI(strings.TrimSpace(cfg.GetString("uri")))
	if err != nil {
		return hubPublishHandleResolution{}, errors.New("hub_publish_handle configuration is invalid")
	}
	hub.Profile = strings.TrimSpace(cfg.GetString("profile"))
	hub.Region = strings.TrimSpace(cfg.GetString("region"))
	hub.Endpoint = strings.TrimSpace(cfg.GetString("endpoint"))
	hub.GCPProject = strings.TrimSpace(cfg.GetString("gcp_project"))
	if hub.Endpoint != "" {
		hub.ForcePathStyle = true
	}
	rangeID, err := canonicalHubAuthorityRange(hub)
	if err != nil {
		return hubPublishHandleResolution{}, errors.New("hub_publish_handle configuration is invalid")
	}
	src := &uri.ObjectURI{Provider: hub.Provider, Bucket: hub.Bucket, Key: hub.Prefix}
	if hub.Provider == string(provider.ProviderFile) {
		src.Bucket = "local"
		src.Key = filepath.ToSlash(hub.BaseDir)
	}
	p, err := providerdispatch.NewSource(ctx, src, providerdispatch.SourceOptions{
		Command:     "index hub bridge-durable",
		FileBaseDir: hub.BaseDir,
		S3: providerdispatch.S3Options{
			Region: hub.Region, Profile: hub.Profile, Endpoint: hub.Endpoint,
			ForcePathStyle: hub.ForcePathStyle,
		},
		GCS: providerdispatch.GCSOptions{Project: hub.GCPProject},
	})
	if err != nil {
		return hubPublishHandleResolution{}, errors.New("open hub_publish_handle: provider unavailable")
	}
	getter, err := providerdispatch.RequireCapability[provider.ObjectGetter](
		p, "index hub bridge-durable", hub.Provider, "ObjectGetter",
	)
	if err != nil {
		_ = p.Close()
		return hubPublishHandleResolution{}, errors.New("open hub_publish_handle: exact object reads unavailable")
	}
	putter, err := providerdispatch.RequireCapability[provider.ConditionalPutter](
		p, "index hub bridge-durable", hub.Provider, "ConditionalPutter",
	)
	if err != nil {
		_ = p.Close()
		return hubPublishHandleResolution{}, errors.New("open hub_publish_handle: conditional create unavailable")
	}
	reporter, ok := p.(provider.ConditionalCapabilityReporter)
	if !ok || !reporter.ConditionalWriteCapabilities().IfAbsent {
		_ = p.Close()
		return hubPublishHandleResolution{}, errors.New("open hub_publish_handle: conditional create unproven")
	}
	return hubPublishHandleResolution{
		publisher: &hubConditionalProviderAdapter{
			bridgeHubExactProviderAdapter: bridgeHubExactProviderAdapter{
				getter: getter, prefix: hub.Prefix,
				fileRoot: rangeID.physicalPath, fileRootInfo: rangeID.fileRootInfo,
			},
			putter: putter,
		},
		closer:  p,
		rangeID: rangeID,
	}, nil
}

type hubConditionalProviderAdapter struct {
	bridgeHubExactProviderAdapter
	putter provider.ConditionalPutter
}

func (a *hubConditionalProviderAdapter) CreateExact(
	ctx context.Context,
	key string,
	body io.Reader,
	sizeBytes int64,
) (bool, error) {
	if a == nil || a.putter == nil || body == nil || sizeBytes < 0 ||
		!validExactHubRelativeKey(key) || !a.fileRootBindingValid() {
		return false, errors.New("hub conditional create failed")
	}
	if a.fileRoot != "" && !fileTargetAncestorsSafe(a.fileRoot, key) {
		return false, errors.New("hub conditional create failed")
	}
	_, err := a.putter.PutObjectConditional(
		ctx,
		a.prefix+key,
		body,
		sizeBytes,
		provider.PutPrecondition{IfAbsent: true},
	)
	if err == nil {
		return true, nil
	}
	if provider.IsAlreadyExists(err) || provider.IsPreconditionFailed(err) {
		return false, nil
	}
	return false, errors.New("hub conditional create failed")
}

func validExactHubRelativeKey(key string) bool {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") ||
		strings.Contains(strings.ToLower(key), "%2f") ||
		strings.Contains(strings.ToLower(key), "%5c") {
		return false
	}
	return path.Clean(key) == key && key != "." && key != ".." && !strings.HasPrefix(key, "../")
}

func canonicalHubAuthorityRange(hub *hubDestSpec) (hubAuthorityRange, error) {
	if hub == nil {
		return hubAuthorityRange{}, errors.New("missing hub")
	}
	switch hub.Provider {
	case string(provider.ProviderFile):
		physical, info, err := resolveNoFollowPhysicalDirectory(hub.BaseDir)
		if err != nil {
			return hubAuthorityRange{}, err
		}
		hub.BaseDir = physical
		return hubAuthorityRange{
			provider: string(provider.ProviderFile), endpoint: "physical",
			physicalPath: physical, fileRootInfo: info,
		}, nil
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		container := strings.TrimSpace(hub.Bucket)
		if container != strings.ToLower(container) || !custodyBridgeBucketRE.MatchString(container) {
			return hubAuthorityRange{}, errors.New("uncanonical container")
		}
		if hub.Provider == string(provider.ProviderS3) && s3ContainerMayBeAlias(container) {
			return hubAuthorityRange{}, errors.New("unprovable S3 container alias")
		}
		prefix, err := canonicalHubPrefix(hub.Prefix)
		if err != nil {
			return hubAuthorityRange{}, err
		}
		hub.Prefix = prefix
		if hub.Prefix != "" {
			hub.Prefix += "/"
		}
		endpoint := "default"
		if hub.Provider == string(provider.ProviderGCS) {
			if strings.TrimSpace(hub.Endpoint) != "" {
				return hubAuthorityRange{}, errors.New("unprovable GCS endpoint")
			}
		} else if strings.TrimSpace(hub.Endpoint) != "" {
			endpoint, err = canonicalEndpoint(hub.Endpoint)
			if err != nil {
				return hubAuthorityRange{}, err
			}
		}
		return hubAuthorityRange{
			provider: hub.Provider, endpoint: endpoint,
			container: container, prefix: prefix,
		}, nil
	default:
		return hubAuthorityRange{}, errors.New("unsupported provider")
	}
}

func s3ContainerMayBeAlias(container string) bool {
	return strings.HasSuffix(container, "-s3alias") ||
		strings.Contains(container, ".s3-accesspoint.") ||
		strings.Contains(container, ".s3-object-lambda.") ||
		strings.Contains(container, ".accesspoint.s3-global.")
}

func canonicalHubPrefix(raw string) (string, error) {
	if strings.Contains(raw, "\\") || strings.HasPrefix(raw, "/") ||
		strings.Contains(strings.ToLower(raw), "%2f") ||
		strings.Contains(strings.ToLower(raw), "%5c") {
		return "", errors.New("ambiguous prefix")
	}
	trimmed := strings.TrimSuffix(raw, "/")
	if trimmed == "" {
		return "", nil
	}
	if strings.HasSuffix(trimmed, "/") || path.Clean(trimmed) != trimmed ||
		trimmed == ".." || strings.HasPrefix(trimmed, "../") {
		return "", errors.New("ambiguous prefix")
	}
	return trimmed, nil
}

func canonicalEndpoint(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.User != nil || parsed.Host == "" ||
		(parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("uncanonical endpoint")
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "" || !isASCII(host) {
		return "", errors.New("uncanonical endpoint")
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	port := parsed.Port()
	if (parsed.Scheme == "https" && port == "443") || (parsed.Scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return parsed.Scheme + "://" + host, nil
}

func isASCII(value string) bool {
	for _, r := range value {
		if r > 127 {
			return false
		}
	}
	return true
}

func proveHubAuthorityRangesDisjoint(a, b hubAuthorityRange) (bool, error) {
	if a.provider == "" || b.provider == "" {
		return false, errors.New("missing range identity")
	}
	if a.provider != b.provider {
		return false, errors.New("cross-provider alias is unprovable")
	}
	if a.provider == string(provider.ProviderFile) {
		if a.physicalPath == "" || b.physicalPath == "" ||
			!sameNoFollowDirectory(a.physicalPath, a.fileRootInfo) ||
			!sameNoFollowDirectory(b.physicalPath, b.fileRootInfo) {
			return false, errors.New("unresolved physical range")
		}
		overlap, err := physicalDirectoriesOverlap(a.physicalPath, b.physicalPath)
		return !overlap, err
	}
	if a.endpoint == "" || b.endpoint == "" || a.endpoint != b.endpoint {
		return false, errors.New("endpoint alias is unprovable")
	}
	if a.container != b.container {
		return true, nil
	}
	return !hubPrefixesOverlap(a.prefix, b.prefix), nil
}

func hubPrefixesOverlap(a, b string) bool {
	if a == "" || b == "" || a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func resolveNoFollowPhysicalDirectory(input string) (string, os.FileInfo, error) {
	if strings.TrimSpace(input) == "" {
		return "", nil, errors.New("missing physical path")
	}
	abs, err := filepath.Abs(filepath.Clean(input))
	if err != nil {
		return "", nil, err
	}
	if !pathComponentsAreNoFollowDirectories(abs) {
		return "", nil, errors.New("physical path is unresolved")
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Lstat(resolved)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", nil, errors.New("physical path is not a directory")
	}
	return filepath.Clean(resolved), info, nil
}

func pathComponentsAreNoFollowDirectories(abs string) bool {
	volume := filepath.VolumeName(abs)
	remainder := strings.TrimPrefix(abs, volume)
	current := volume + string(filepath.Separator)
	for _, component := range strings.FieldsFunc(remainder, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return false
		}
	}
	return true
}

func sameNoFollowDirectory(path string, expected os.FileInfo) bool {
	if path == "" || expected == nil {
		return false
	}
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink == 0 && info.IsDir() && os.SameFile(info, expected)
}

func physicalDirectoriesOverlap(a, b string) (bool, error) {
	aInfo, err := os.Stat(a)
	if err != nil {
		return false, err
	}
	bInfo, err := os.Stat(b)
	if err != nil {
		return false, err
	}
	if os.SameFile(aInfo, bInfo) {
		return true, nil
	}
	aAncestor, err := physicalDirectoryIsAncestor(aInfo, b)
	if err != nil || aAncestor {
		return aAncestor, err
	}
	return physicalDirectoryIsAncestor(bInfo, a)
}

func physicalDirectoryIsAncestor(ancestor os.FileInfo, child string) (bool, error) {
	current := filepath.Clean(child)
	for {
		info, err := os.Stat(current)
		if err != nil {
			return false, err
		}
		if os.SameFile(ancestor, info) {
			return true, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false, nil
		}
		current = parent
	}
}

func fileTargetAncestorsSafe(root, key string) bool {
	if root == "" || !validExactHubRelativeKey(key) {
		return false
	}
	current := root
	parts := strings.Split(path.Dir(key), "/")
	for _, component := range parts {
		if component == "." || component == "" {
			continue
		}
		current = filepath.Join(current, filepath.FromSlash(component))
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			return true
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return false
		}
	}
	return true
}

func readBoundedNoFollowLocalFile(input string, maxBytes int64) ([]byte, error) {
	if strings.TrimSpace(input) == "" || maxBytes <= 0 {
		return nil, errors.New("invalid local input")
	}
	abs, err := filepath.Abs(filepath.Clean(input))
	if err != nil {
		return nil, err
	}
	physicalParent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return nil, errors.New("local input parent is unavailable")
	}
	abs = filepath.Join(physicalParent, filepath.Base(abs))
	before, err := os.Lstat(abs)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() ||
		before.Size() < 0 || before.Size() > maxBytes {
		return nil, errors.New("local input is not a bounded regular file")
	}
	file, err := os.Open(abs) // #nosec G304 -- every component and leaf was checked no-follow and the opened identity is re-attested.
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, errors.New("local input binding changed")
	}
	body, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(body)) > maxBytes || int64(len(body)) != opened.Size() {
		return nil, errors.New("local input read failed")
	}
	openedAfter, err := file.Stat()
	if err != nil || !os.SameFile(opened, openedAfter) ||
		openedAfter.Size() != opened.Size() ||
		!openedAfter.ModTime().Equal(opened.ModTime()) {
		return nil, errors.New("local input changed during read")
	}
	after, err := os.Lstat(abs)
	if err != nil || after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() ||
		!os.SameFile(openedAfter, after) || after.Size() != openedAfter.Size() ||
		!after.ModTime().Equal(openedAfter.ModTime()) {
		return nil, errors.New("local input binding changed")
	}
	return body, nil
}
