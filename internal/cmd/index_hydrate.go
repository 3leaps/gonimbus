package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/spf13/cobra"
)

var indexHydrateCmd = &cobra.Command{
	Use:   "hydrate",
	Short: "Download an index run from a hub",
	Long: `Download an index run from a hub (S3 or file) to a local directory.

The hydrate command resolves a run from the hub and downloads its artifacts
(index.db, identity.json) to the specified destination directory. The run
must have a valid complete.json commit marker.

Resolution order:
  1. --run-id: explicit run
  2. --latest (default): reads latest.json pointer

After download, the integrity of index.db is verified against the SHA-256
recorded in complete.json.

Examples:
  # Hydrate latest run to a local directory
  gonimbus index hydrate --hub file:///data/index-hub/ \
    --index-set idx_da038d8171b4a9ba --dest /tmp/hydrated/

  # Hydrate from S3 hub with explicit profile
  gonimbus index hydrate --hub s3://my-bucket/index-hub/ \
    --index-set idx_da038d8171b4a9ba --dest /tmp/hydrated/ \
    --hub-profile my-profile

  # Hydrate a specific run
  gonimbus index hydrate --hub s3://my-bucket/index-hub/ \
    --index-set idx_da038d8171b4a9ba --run-id run_1709654400000000000 \
    --dest /tmp/hydrated/`,
	RunE: runIndexHydrate,
}

func init() {
	indexCmd.AddCommand(indexHydrateCmd)

	indexHydrateCmd.Flags().String("hub", "", "Hub root URI (s3://bucket/prefix/, gs://bucket/prefix/, or file:///path/)")
	indexHydrateCmd.Flags().String("index-set", "", "Index set ID to hydrate (required)")
	indexHydrateCmd.Flags().String("run-id", "", "Specific run ID (default: resolve from latest.json)")
	indexHydrateCmd.Flags().String("dest", "", "Local destination directory (required)")

	// Hub provider auth
	indexHydrateCmd.Flags().String("hub-profile", "", "AWS profile for hub source")
	indexHydrateCmd.Flags().String("hub-region", "", "AWS region for hub source")
	indexHydrateCmd.Flags().String("hub-endpoint", "", "Custom endpoint for hub source")
	indexHydrateCmd.Flags().String("hub-gcp-project", "", "GCP project hint for GCS hub source")

	_ = indexHydrateCmd.MarkFlagRequired("hub")
	_ = indexHydrateCmd.MarkFlagRequired("index-set")
	_ = indexHydrateCmd.MarkFlagRequired("dest")
}

// newHubGetter creates a provider.ObjectGetter for reading from a hub.
func newHubGetter(ctx context.Context, hub *hubDestSpec) (provider.ObjectGetter, error) {
	if hub.Provider == string(provider.ProviderFile) {
		if err := os.MkdirAll(hub.BaseDir, 0o755); err != nil {
			return nil, fmt.Errorf("access hub directory: %w", err)
		}
	}
	p, err := providerdispatch.NewDestination(ctx, providerdispatch.DestinationOptions{
		Command:     "index hydrate",
		Provider:    hub.Provider,
		S3Bucket:    hub.Bucket,
		S3Prefix:    hub.Prefix,
		GCSBucket:   hub.Bucket,
		GCSPrefix:   hub.Prefix,
		FileBaseDir: hub.BaseDir,
		S3: providerdispatch.S3Options{
			Region:         hub.Region,
			Endpoint:       hub.Endpoint,
			Profile:        hub.Profile,
			ForcePathStyle: hub.ForcePathStyle,
		},
		GCS: providerdispatch.GCSOptions{
			Project: strings.TrimSpace(hub.GCPProject),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create hub provider: %w", err)
	}
	getter, err := providerdispatch.RequireCapability[provider.ObjectGetter](p, "index hydrate", hub.Provider, "ObjectGetter")
	if err != nil {
		return nil, err
	}
	return getter, nil
}

func runIndexHydrate(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	hubURI, _ := cmd.Flags().GetString("hub")
	indexSetFlag, _ := cmd.Flags().GetString("index-set")
	runIDFlag, _ := cmd.Flags().GetString("run-id")
	destDir, _ := cmd.Flags().GetString("dest")
	hubProfile, _ := cmd.Flags().GetString("hub-profile")
	hubRegion, _ := cmd.Flags().GetString("hub-region")
	hubEndpoint, _ := cmd.Flags().GetString("hub-endpoint")
	hubGCPProject, _ := cmd.Flags().GetString("hub-gcp-project")

	// Validate index-set ID: hydrate requires full idx_<64hex> since hub paths are exact
	if err := validateFullIndexSetID(indexSetFlag); err != nil {
		return err
	}

	// Validate --run-id if provided
	if runIDFlag != "" {
		if err := validateRunID(runIDFlag); err != nil {
			return err
		}
	}

	// Parse hub
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

	// Create hub reader
	getter, err := newHubGetter(ctx, hub)
	if err != nil {
		return fmt.Errorf("hub provider: %w", err)
	}
	if closer, ok := getter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	// Resolve run ID
	runID := runIDFlag
	if runID == "" {
		resolved, resolveErr := resolveLatestRunID(ctx, getter, hub, indexSetFlag)
		if resolveErr != nil {
			return resolveErr
		}
		runID = resolved
	}

	_, _ = fmt.Fprintf(os.Stderr, "Hydrating index_set=%s run=%s from %s\n", indexSetFlag, runID, hubURI)

	// Read and verify complete.json
	runPrefix := hubArtifactKey(hub, "index-sets", indexSetFlag, "runs", runID)
	completeKey := runPrefix + "/complete.json"

	completeData, err := downloadBytesBounded(ctx, getter, completeKey, maxHubCompleteMarkerBytes, "complete.json")
	if err != nil {
		if provider.IsNotFound(err) {
			return fmt.Errorf("run %s is not committed (complete.json not found); cannot hydrate", runID)
		}
		return fmt.Errorf("read complete.json: %w", err)
	}

	var complete completeMarker
	if err := json.Unmarshal(completeData, &complete); err != nil {
		return fmt.Errorf("parse complete.json: %w", err)
	}
	format := completeMarkerFormat(complete)

	// Prepare destination
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	switch format {
	case indexHubFormatSQLiteV1:
		return hydrateSQLiteRun(ctx, getter, runPrefix, complete, completeData, destDir)
	case indexHubFormatDurableV2:
		return hydrateDurableRun(ctx, getter, runPrefix, indexSetFlag, runID, complete, completeData, destDir)
	case "":
		return fmt.Errorf("complete.json has no format and no sqlite index_db artifact")
	default:
		return fmt.Errorf("unsupported index hub format %q", format)
	}
}

func hydrateSQLiteRun(ctx context.Context, getter provider.ObjectGetter, runPrefix string, complete completeMarker, completeData []byte, destDir string) error {
	if complete.Artifacts.IndexDB == nil {
		return fmt.Errorf("sqlite hub run is missing index_db artifact")
	}
	// Download index.db
	indexDBKey := runPrefix + "/index.db"
	indexDBDest := filepath.Join(destDir, "index.db")
	_, _ = fmt.Fprintf(os.Stderr, "  downloading index.db (%d bytes)...\n", complete.Artifacts.IndexDB.SizeBytes)
	if err := downloadFile(ctx, getter, indexDBKey, indexDBDest); err != nil {
		return fmt.Errorf("download index.db: %w", err)
	}

	// Verify index.db integrity
	actualHash, actualSize, err := hashFile(indexDBDest)
	if err != nil {
		return fmt.Errorf("verify index.db: %w", err)
	}
	if actualHash != complete.Artifacts.IndexDB.SHA256 {
		return fmt.Errorf("index.db integrity check failed: expected sha256=%s, got %s", complete.Artifacts.IndexDB.SHA256, actualHash)
	}
	if actualSize != complete.Artifacts.IndexDB.SizeBytes {
		return fmt.Errorf("index.db size mismatch: expected %d, got %d", complete.Artifacts.IndexDB.SizeBytes, actualSize)
	}

	// Download identity.json (optional)
	identityKey := runPrefix + "/identity.json"
	identityDest := filepath.Join(destDir, "identity.json")
	if err := downloadFile(ctx, getter, identityKey, identityDest); err != nil {
		if provider.IsNotFound(err) {
			_, _ = fmt.Fprintln(os.Stderr, "  identity.json not present in hub (older index)")
		} else {
			return fmt.Errorf("download identity.json: %w", err)
		}
	} else {
		_, _ = fmt.Fprintln(os.Stderr, "  downloaded identity.json")
		// Verify identity.json if checksum is in complete.json
		if complete.Artifacts.IdentityJSON != nil && complete.Artifacts.IdentityJSON.SHA256 != "" {
			idData, readErr := os.ReadFile(identityDest)
			if readErr != nil {
				return fmt.Errorf("verify identity.json: %w", readErr)
			}
			h := sha256.Sum256(idData)
			if hex.EncodeToString(h[:]) != complete.Artifacts.IdentityJSON.SHA256 {
				return fmt.Errorf("identity.json integrity check failed: expected sha256=%s", complete.Artifacts.IdentityJSON.SHA256)
			}
			if complete.Artifacts.IdentityJSON.SizeBytes > 0 && int64(len(idData)) != complete.Artifacts.IdentityJSON.SizeBytes {
				return fmt.Errorf("identity.json size mismatch: expected %d, got %d", complete.Artifacts.IdentityJSON.SizeBytes, len(idData))
			}
		}
	}

	// Write complete.json to dest for provenance
	completeDest := filepath.Join(destDir, "complete.json")
	if err := os.WriteFile(completeDest, completeData, 0o644); err != nil {
		return fmt.Errorf("write complete.json: %w", err)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Hydrate complete: %s\n", destDir)
	return nil
}

func hydrateDurableRun(ctx context.Context, getter provider.ObjectGetter, runPrefix, indexSetID, runID string, complete completeMarker, completeData []byte, destDir string) error {
	if err := validateDurableCompleteMarker(indexSetID, runID, complete); err != nil {
		return err
	}
	manifestRef := *complete.Artifacts.Manifest
	if manifestRef.Path != "manifest.json" || manifestRef.Role != "manifest" || !manifestRef.Required {
		return fmt.Errorf("durable manifest artifact must be required manifest role at manifest.json")
	}
	if manifestRef.SizeBytes > maxDurableManifestBytes {
		return fmt.Errorf("durable manifest size %d exceeds limit %d", manifestRef.SizeBytes, maxDurableManifestBytes)
	}
	manifestKey := runPrefix + "/manifest.json"
	_, _ = fmt.Fprintf(os.Stderr, "  downloading manifest.json (%d bytes)...\n", manifestRef.SizeBytes)
	manifestData, err := downloadBytesBounded(ctx, getter, manifestKey, maxDurableManifestBytes, "manifest.json")
	if err != nil {
		return fmt.Errorf("download durable manifest: %w", err)
	}
	if int64(len(manifestData)) != manifestRef.SizeBytes {
		return fmt.Errorf("durable manifest size mismatch: expected %d, got %d", manifestRef.SizeBytes, len(manifestData))
	}
	if sha256HexBytes(manifestData) != manifestRef.SHA256 {
		return fmt.Errorf("durable manifest integrity check failed: expected sha256=%s", manifestRef.SHA256)
	}
	var manifest indexsubstrate.InternalManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return fmt.Errorf("parse durable manifest: %w", err)
	}
	if err := validateDurableHubManifest(indexSetID, runID, manifest); err != nil {
		return err
	}
	if err := validateDurableHubManifestBounds(manifest); err != nil {
		return err
	}
	segmentRefs, err := durableSegmentArtifactMap(complete, manifest)
	if err != nil {
		return err
	}

	for _, segment := range manifest.Segments {
		ref := segmentRefs["segments/"+segment.Path]
		destPath, err := safeLocalArtifactPath(destDir, ref.Path)
		if err != nil {
			return fmt.Errorf("segment %s: %w", segment.SegmentID, err)
		}
		key := runPrefix + "/" + ref.Path
		_, _ = fmt.Fprintf(os.Stderr, "  downloading segment %s (%d bytes)...\n", segment.Path, ref.SizeBytes)
		if err := downloadFile(ctx, getter, key, destPath); err != nil {
			return fmt.Errorf("download segment %s: %w", segment.Path, err)
		}
		gotSHA, gotSize, err := hashFile(destPath)
		if err != nil {
			return fmt.Errorf("verify segment %s: %w", segment.Path, err)
		}
		if gotSHA != ref.SHA256 {
			return fmt.Errorf("segment %s integrity check failed: expected sha256=%s, got %s", segment.Path, ref.SHA256, gotSHA)
		}
		if gotSize != ref.SizeBytes {
			return fmt.Errorf("segment %s size mismatch: expected %d, got %d", segment.Path, ref.SizeBytes, gotSize)
		}
	}
	if err := os.WriteFile(filepath.Join(destDir, "manifest.json"), manifestData, 0o644); err != nil {
		return fmt.Errorf("write manifest.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(destDir, "complete.json"), completeData, 0o644); err != nil {
		return fmt.Errorf("write complete.json: %w", err)
	}
	_, _ = fmt.Fprintf(os.Stderr, "Hydrate complete: %s\n", destDir)
	return nil
}

func durableSegmentArtifactMap(complete completeMarker, manifest indexsubstrate.InternalManifest) (map[string]artifactRef, error) {
	if len(complete.Artifacts.Segments) != len(manifest.Segments) {
		return nil, fmt.Errorf("durable segment artifact count mismatch")
	}
	var totalBytes int64
	refs := make(map[string]artifactRef, len(complete.Artifacts.Segments))
	for _, ref := range complete.Artifacts.Segments {
		if ref.Path == "" || ref.Role != "segment" || !ref.Required {
			return nil, fmt.Errorf("durable segment artifact entries must be required segment roles")
		}
		if filepath.Clean(ref.Path) != ref.Path || !strings.HasPrefix(ref.Path, "segments/") {
			return nil, fmt.Errorf("durable segment artifact path must be canonical segments/<name>")
		}
		if _, err := safeLocalArtifactPath(".", ref.Path); err != nil {
			return nil, fmt.Errorf("durable segment artifact %q: %w", ref.Path, err)
		}
		if ref.SizeBytes < 0 {
			return nil, fmt.Errorf("durable segment artifact size must be non-negative")
		}
		if totalBytes > maxDurableDeclaredArtifactSum-ref.SizeBytes {
			return nil, fmt.Errorf("durable declared artifact bytes exceed limit")
		}
		totalBytes += ref.SizeBytes
		if _, exists := refs[ref.Path]; exists {
			return nil, fmt.Errorf("duplicate durable segment artifact %s", ref.Path)
		}
		refs[ref.Path] = ref
	}
	for _, segment := range manifest.Segments {
		path := "segments/" + segment.Path
		ref, ok := refs[path]
		if !ok {
			return nil, fmt.Errorf("durable complete marker missing segment artifact %s", path)
		}
		if ref.SHA256 != segment.Digest.Hex || ref.SizeBytes != segment.SizeBytes {
			return nil, fmt.Errorf("durable segment artifact mismatch for %s", path)
		}
	}
	return refs, nil
}

// validFullIndexSetPattern matches idx_<exactly 64 hex chars>.
var validFullIndexSetPattern = regexp.MustCompile(`^idx_[0-9a-f]{64}$`)

// validRunIDPattern matches run_<digits> or run_<ULID> per the hub schema contract.
var validRunIDPattern = regexp.MustCompile(`^run_([0-9]{1,32}|[0-9A-HJKMNP-TV-Z]{26})$`)

// validateFullIndexSetID ensures the index set ID is the full idx_<64hex> form.
// Hydrate uses exact hub paths (no prefix resolution), so short IDs are not supported.
func validateFullIndexSetID(id string) error {
	if !validFullIndexSetPattern.MatchString(id) {
		cleanID := strings.TrimPrefix(id, "idx_")
		if validHexPattern.MatchString(cleanID) && len(cleanID) < 64 {
			return fmt.Errorf("hydrate requires full index set ID (idx_<64hex>), got short prefix: %s", id)
		}
		return fmt.Errorf("invalid index set ID: %s (must be idx_<64 hex chars>)", id)
	}
	return nil
}

// validateRunID checks that a run ID matches the schema contract.
func validateRunID(id string) error {
	if !validRunIDPattern.MatchString(id) {
		return fmt.Errorf("invalid run ID: %s (must match run_<digits> or run_<ULID>)", id)
	}
	return nil
}

// resolveLatestRunID reads latest.json from the hub to determine the run ID.
func resolveLatestRunID(ctx context.Context, getter provider.ObjectGetter, hub *hubDestSpec, indexSetID string) (string, error) {
	latestKey := hubArtifactKey(hub, "index-sets", indexSetID, "latest.json")
	data, err := downloadBytesBounded(ctx, getter, latestKey, maxHubMarkerBytes, "latest.json")
	if err != nil {
		if provider.IsNotFound(err) {
			return "", fmt.Errorf("no latest.json found for index set %s; use --run-id to specify explicitly", indexSetID)
		}
		return "", fmt.Errorf("read latest.json: %w", err)
	}

	var latest struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(data, &latest); err != nil {
		return "", fmt.Errorf("parse latest.json: %w", err)
	}
	if latest.RunID == "" {
		return "", fmt.Errorf("latest.json has empty run_id")
	}
	if err := validateRunID(latest.RunID); err != nil {
		return "", fmt.Errorf("latest.json contains %w", err)
	}

	_, _ = fmt.Fprintf(os.Stderr, "  resolved latest run: %s\n", latest.RunID)
	return latest.RunID, nil
}

// completeMarker is the subset of complete.json needed for hydration verification.
type completeMarker struct {
	MarkerSchemaVersion string `json:"marker_schema_version,omitempty"`
	Format              string `json:"format,omitempty"`
	FormatVersion       string `json:"format_version,omitempty"`
	IndexSetID          string `json:"index_set_id,omitempty"`
	RunID               string `json:"run_id,omitempty"`
	ExportedBy          string `json:"exported_by,omitempty"`
	Artifacts           struct {
		IndexDB      *artifactRef  `json:"index_db,omitempty"`
		IdentityJSON *artifactRef  `json:"identity_json,omitempty"`
		Manifest     *artifactRef  `json:"manifest,omitempty"`
		Segments     []artifactRef `json:"segments,omitempty"`
	} `json:"artifacts"`
}

type artifactRef struct {
	Path      string `json:"path,omitempty"`
	Role      string `json:"role,omitempty"`
	Required  bool   `json:"required,omitempty"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// downloadBytes reads a small object entirely into memory.
func downloadBytes(ctx context.Context, getter provider.ObjectGetter, key string) ([]byte, error) {
	body, _, err := getter.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	return io.ReadAll(body)
}

func downloadBytesBounded(ctx context.Context, getter provider.ObjectGetter, key string, maxBytes int64, label string) ([]byte, error) {
	body, declaredSize, err := getter.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	return readAllBounded(body, declaredSize, maxBytes, label)
}

func readAllBounded(r io.Reader, declaredSize, maxBytes int64, label string) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("%s size limit must be positive", label)
	}
	if declaredSize > maxBytes {
		return nil, fmt.Errorf("%s size %d exceeds limit %d", label, declaredSize, maxBytes)
	}
	limited := io.LimitReader(r, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%s size exceeds limit %d", label, maxBytes)
	}
	return data, nil
}

// downloadFile streams an object from the hub to a local file.
func downloadFile(ctx context.Context, getter provider.ObjectGetter, key, destPath string) error {
	body, _, err := getter.GetObject(ctx, key)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(destPath), err)
	}

	if _, err := io.Copy(f, body); err != nil {
		_ = f.Close()
		_ = os.Remove(destPath)
		return fmt.Errorf("write %s: %w", filepath.Base(destPath), err)
	}

	return f.Close()
}
