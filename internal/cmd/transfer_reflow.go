package cmd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
	reflowRecordType        = "gonimbus.reflow.v1"
	reflowRunRecordType     = "gonimbus.reflow.run.v1"
	reflowSourceRecordType  = "gonimbus.reflow.source.v1"
	reflowWarningRecord     = "gonimbus.warning.v1"
	provenanceSchema        = "gonimbus.provenance.v1"
	provenanceSchemaVer     = "1.0.0"
	provenanceModeNone      = "none"
	provenanceModeSidecar   = "sidecar"
	provenancePlaceSibling  = "sibling"
	provenancePlaceMirror   = "mirrored-root"
	provenanceErrorWarn     = "warn"
	provenanceErrorFail     = "fail"
	provenanceSuffix        = ".gnb.json"
	reflowCollisionSkip     = "skip-if-duplicate"
	reflowCollisionLog      = "log"
	reflowCollisionFail     = "fail"
	reflowCollisionOver     = "overwrite"
	reflowCollisionQuar     = "quarantine"
	reflowCollisionSrcNew   = "overwrite-if-source-newer"
	collisionDuplicate      = "duplicate"
	collisionConflict       = "conflict"
	collisionQuarantined    = "conflict_quarantined"
	collisionOverwritten    = "overwritten"
	collisionSrcOlder       = "skipped_src_older"
	collisionConcurrentMut  = "skipped_concurrent_mutation"
	decisionIfAbsentHead    = "ifabsent_then_head"
	decisionOverwrite       = "unconditional_overwrite"
	decisionQuarantine      = "quarantine_routed"
	decisionHeadCompare     = "head_compare_then_conditional_overwrite"
	reasonSrcNewer          = "src_newer"
	reasonSrcOlder          = "src_older"
	reasonEqualSizeDiffers  = "equal_time_size_differs"
	reasonConcurrentMut     = "concurrent_mutation"
	metadataPolicyClear     = "clear"
	metadataPolicyPreserve  = "preserve"
	metadataPolicyMerge     = "merge"
	metadataMissingSkip     = "skip"
	metadataMissingFail     = "fail"
	metadataMissingEmpty    = "empty"
	metadataMaxPairBytes    = 2 * 1024
	metadataMaxTotalBytes   = 8 * 1024
	storageClassPropagate   = "propagate"
	reflowSourceBucketFile  = "local"
	reflowSymlinkSkip       = "skip"
	reflowSymlinkFollow     = "follow"
	reflowHiddenSkip        = "skip"
	reflowHiddenInclude     = "include"
	reflowSourceFailSkip    = "skip"
	reflowSourceFailFail    = "fail"
	operationTransferReflow = "transfer-reflow"
)

var transferReflowCmd = &cobra.Command{
	Use:   "reflow [source-uri]",
	Short: "Copy objects to a new key layout (JSONL)",
	Long: `Copy objects from a source location to a destination prefix while rewriting keys.

Input can be provided via --stdin (one item per line):
- Plain URIs: s3://bucket/key, s3://bucket/prefix/, s3://bucket/prefix/**/*.xml
- JSONL index objects: {"type":"gonimbus.index.object.v1", ...}

Notes:
- v0.1.7 supports a single source bucket per reflow run.
- Metadata values supplied via --metadata-set, --metadata-set-from-source-key, --metadata-set-from-source-derived, or carried by --metadata-policy=preserve|merge, --preserve-content-type, or --destination-storage-class are durable destination metadata and are not redacted at destination. For file destinations, metadata is written in cleartext JSON sidecars using --metadata-sidecar-suffix.
- Per-object metadata derivation is an explicit allow-list: each destination key must be named. Audit derivation expressions against the source metadata inventory before running against buckets that may contain sensitive source subfields.
- Local-tree reflow skips hidden files and dot-directories by default. Use --hidden=include only after reviewing --dry-run output. Hidden filtering is not gitignore-aware; use --exclude for non-hidden generated paths such as node_modules/*, dist/*, target/*, and *.log.

Output is JSONL on stdout.
Errors are emitted on stdout as gonimbus.error.v1 records.
`,
	Args: validateTransferReflowArgs,
	RunE: runTransferReflow,
}

var (
	reflowStdin       bool
	reflowDest        string
	reflowRewriteFrom string
	reflowRewriteTo   string
	reflowParallel    int
	reflowDryRun      bool
	reflowResume      bool
	reflowResumeRun   string
	reflowCheckpoint  string
	reflowOverwrite   bool
	reflowOnCollision string
	reflowCollQuar    string
	reflowProvenance  string
	reflowProvRoot    string
	reflowProvSuffix  string
	reflowProvOnError string
	reflowProvUnsafe  bool
	reflowMetaPolicy  string
	reflowMetaSets    []string
	reflowMetaSrcKeys []string
	reflowMetaDerived []string
	reflowMetaMissing string
	reflowMetaContent bool
	reflowMetaStorage string
	reflowMetaSuffix  string
	reflowSymlinks    string
	reflowHidden      string
	reflowExcludes    []string
	reflowPreserve    bool
	reflowSrcFailure  string

	reflowSrcRegion   string
	reflowSrcProfile  string
	reflowSrcEndpoint string
	reflowDstRegion   string
	reflowDstProfile  string
	reflowDstEndpoint string
)

var (
	newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
		return reflowstate.Open(ctx, cfg)
	}
)

type reflowStateStore interface {
	Close() error
	SetSourceMetadata(ctx context.Context, provider, bucket, root, sourceURI string) error
	SetOperationCheckpointIdentity(ctx context.Context, operation, fingerprint string) error
	OperationCheckpointFingerprint(ctx context.Context, operation string) (string, error)
	ItemDone(ctx context.Context, sourceURI, destURI string) (bool, string, error)
	UpsertItem(ctx context.Context, p reflowstate.UpsertItemParams) error
	NoteDestKeySource(ctx context.Context, destKey, sourceURI, sourceETag string, sourceSize int64) error
	NoteCollision(ctx context.Context, destKey string, kind reflowstate.CollisionKind, sourceURI, sourceETag string, sourceSize int64, destETag string, destSize int64) error
	DestKeyObserved(ctx context.Context, destKey string) (bool, error)
	MarkDestKeyObserved(ctx context.Context, destKey string) error
}

func init() {
	transferCmd.AddCommand(transferReflowCmd)

	transferReflowCmd.Flags().BoolVar(&reflowStdin, "stdin", false, "Read selection from stdin")
	transferReflowCmd.Flags().StringVar(&reflowDest, "dest", "", "Destination base URI (prefix), e.g. s3://bucket/base/ or file:///tmp/out/")
	transferReflowCmd.Flags().StringVar(&reflowRewriteFrom, "rewrite-from", "", "Rewrite source template (segment captures)")
	transferReflowCmd.Flags().StringVar(&reflowRewriteTo, "rewrite-to", "", "Rewrite destination template (segment renders)")
	transferReflowCmd.Flags().IntVar(&reflowParallel, "parallel", 16, "Concurrent copy workers")
	transferReflowCmd.Flags().BoolVar(&reflowDryRun, "dry-run", false, "Emit planned mappings without writing")
	transferReflowCmd.Flags().BoolVar(&reflowResume, "resume", false, "Resume from checkpoint (requires --checkpoint)")
	transferReflowCmd.Flags().StringVar(&reflowResumeRun, "resume-run", "", "Resume a failed-resumable transfer reflow run by run id")
	transferReflowCmd.Flags().StringVar(&reflowCheckpoint, "checkpoint", "", "Checkpoint DB path (sqlite)")
	transferReflowCmd.Flags().BoolVar(&reflowOverwrite, "overwrite", false, "Allow overwriting destination objects")
	transferReflowCmd.Flags().StringVar(&reflowOnCollision, "on-collision", reflowCollisionSkip, "Collision policy: skip-if-duplicate|fail|overwrite|quarantine|overwrite-if-source-newer (log is a deprecated alias)")
	transferReflowCmd.Flags().StringVar(&reflowCollQuar, "collision-quarantine-prefix", "", "Relative destination prefix for --on-collision=quarantine")
	transferReflowCmd.Flags().StringVar(&reflowProvenance, "provenance", provenanceModeNone, "Provenance mode: none|sidecar")
	transferReflowCmd.Flags().StringVar(&reflowProvRoot, "provenance-sidecar-root", "", "Sidecar root URI for mirrored-root provenance placement (must end in /)")
	transferReflowCmd.Flags().StringVar(&reflowProvSuffix, "provenance-suffix", provenanceSuffix, "Sidecar key suffix (default .gnb.json)")
	transferReflowCmd.Flags().StringVar(&reflowProvOnError, "provenance-on-write-error", provenanceErrorWarn, "Sidecar write failure policy: warn|fail")
	transferReflowCmd.Flags().BoolVar(&reflowProvUnsafe, "allow-unsafe-suffix", false, "Allow a provenance suffix that collides with common data extensions")
	transferReflowCmd.Flags().StringVar(&reflowMetaPolicy, "metadata-policy", metadataPolicyClear, "Destination user metadata policy: clear|preserve|merge")
	transferReflowCmd.Flags().StringArrayVar(&reflowMetaSets, "metadata-set", nil, "Destination user metadata key=value override; repeatable; keys are normalized to lower case")
	transferReflowCmd.Flags().StringArrayVar(&reflowMetaSrcKeys, "metadata-set-from-source-key", nil, "Destination user metadata dest=source-key projection from each source object's metadata; repeatable")
	transferReflowCmd.Flags().StringArrayVar(&reflowMetaDerived, "metadata-set-from-source-derived", nil, "Destination user metadata dest=expression projection from each source object; repeatable")
	transferReflowCmd.Flags().StringVar(&reflowMetaMissing, "metadata-on-missing-source", metadataMissingSkip, "Per-object metadata missing/unsupported value policy: skip|fail|empty")
	transferReflowCmd.Flags().BoolVar(&reflowMetaContent, "preserve-content-type", false, "Preserve the source Content-Type on destination objects")
	transferReflowCmd.Flags().StringVar(&reflowMetaStorage, "destination-storage-class", "", "Destination storage class, or propagate to copy source storage class")
	transferReflowCmd.Flags().StringVar(&reflowMetaSuffix, "metadata-sidecar-suffix", providerfile.DefaultMetadataSidecarSuffix, "File destination metadata sidecar suffix")
	transferReflowCmd.Flags().StringVar(&reflowSymlinks, "symlinks", reflowSymlinkSkip, "File source symlink policy: skip|follow")
	transferReflowCmd.Flags().StringVar(&reflowHidden, "hidden", reflowHiddenSkip, "File source hidden path policy: skip|include")
	transferReflowCmd.Flags().StringArrayVar(&reflowExcludes, "exclude", nil, "File source exclusion glob relative to source root; repeatable.")
	transferReflowCmd.Flags().BoolVar(&reflowPreserve, "preserve-mode", false, "Preserve Unix mode bits for file:// source to file:// destination; warns and no-ops for other cells")
	transferReflowCmd.Flags().StringVar(&reflowSrcFailure, "on-source-failure", reflowSourceFailSkip, "Source failure policy: skip|fail")

	transferReflowCmd.Flags().StringVar(&reflowSrcRegion, "src-region", "", "Source AWS region")
	transferReflowCmd.Flags().StringVar(&reflowSrcProfile, "src-profile", "", "Source AWS profile")
	transferReflowCmd.Flags().StringVar(&reflowSrcEndpoint, "src-endpoint", "", "Source custom S3 endpoint")
	transferReflowCmd.Flags().StringVar(&reflowDstRegion, "dest-region", "", "Destination AWS region")
	transferReflowCmd.Flags().StringVar(&reflowDstProfile, "dest-profile", "", "Destination AWS profile")
	transferReflowCmd.Flags().StringVar(&reflowDstEndpoint, "dest-endpoint", "", "Destination custom S3 endpoint")

	_ = viper.BindPFlag("on_collision", transferReflowCmd.Flags().Lookup("on-collision"))
	_ = viper.BindPFlag("collision_quarantine_prefix", transferReflowCmd.Flags().Lookup("collision-quarantine-prefix"))
	_ = viper.BindPFlag("provenance.mode", transferReflowCmd.Flags().Lookup("provenance"))
	_ = viper.BindPFlag("provenance.sidecar_root", transferReflowCmd.Flags().Lookup("provenance-sidecar-root"))
	_ = viper.BindPFlag("provenance.suffix", transferReflowCmd.Flags().Lookup("provenance-suffix"))
	_ = viper.BindPFlag("provenance.on_write_error", transferReflowCmd.Flags().Lookup("provenance-on-write-error"))
	_ = viper.BindPFlag("provenance.allow_unsafe_suffix", transferReflowCmd.Flags().Lookup("allow-unsafe-suffix"))
	_ = viper.BindPFlag("metadata.policy", transferReflowCmd.Flags().Lookup("metadata-policy"))
	_ = viper.BindPFlag("metadata.set", transferReflowCmd.Flags().Lookup("metadata-set"))
	_ = viper.BindPFlag("metadata.set_from_source_key", transferReflowCmd.Flags().Lookup("metadata-set-from-source-key"))
	_ = viper.BindPFlag("metadata.set_from_source_derived", transferReflowCmd.Flags().Lookup("metadata-set-from-source-derived"))
	_ = viper.BindPFlag("metadata.on_missing_source", transferReflowCmd.Flags().Lookup("metadata-on-missing-source"))
	_ = viper.BindPFlag("metadata.preserve_content_type", transferReflowCmd.Flags().Lookup("preserve-content-type"))
	_ = viper.BindPFlag("metadata.destination_storage_class", transferReflowCmd.Flags().Lookup("destination-storage-class"))
	_ = viper.BindPFlag("metadata.sidecar_suffix", transferReflowCmd.Flags().Lookup("metadata-sidecar-suffix"))
	_ = viper.BindPFlag("source.symlinks", transferReflowCmd.Flags().Lookup("symlinks"))
	_ = viper.BindPFlag("source.hidden", transferReflowCmd.Flags().Lookup("hidden"))
	_ = viper.BindPFlag("source.exclude", transferReflowCmd.Flags().Lookup("exclude"))
	_ = viper.BindPFlag("source.preserve_mode", transferReflowCmd.Flags().Lookup("preserve-mode"))
	_ = viper.BindPFlag("source.on_failure", transferReflowCmd.Flags().Lookup("on-source-failure"))

}

func validateTransferReflowArgs(cmd *cobra.Command, args []string) error {
	resumeRun, _ := cmd.Flags().GetString("resume-run")
	if strings.TrimSpace(resumeRun) != "" {
		if len(args) > 0 {
			return fmt.Errorf("do not provide source-uri arguments with --resume-run")
		}
		return nil
	}
	stdin, _ := cmd.Flags().GetBool("stdin")
	if stdin {
		if len(args) > 0 {
			return fmt.Errorf("when using --stdin, do not provide source-uri arguments")
		}
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("requires exactly 1 argument: [source-uri] (or use --stdin)")
	}
	return nil
}

type reflowTask struct {
	SourceProvider    string
	SourceBucket      string
	SourceRoot        string
	SourceURI         string
	SourceCheckpoint  string
	SourceKey         string
	SourceETag        string
	SourceSize        int64
	SourceLastMod     time.Time
	SourceMode        fs.FileMode
	SourceFailure     string
	Vars              map[string]string
	Probe             *probe.ProbeAudit
	DestRelKey        string
	RoutingClass      string
	QuarantinePrefix  string
	RejectSymlinkPath bool
}

type reflowRecord struct {
	SourceURI    string         `json:"source_uri"`
	SourceBucket string         `json:"source_bucket,omitempty"`
	SourceRoot   string         `json:"source_root,omitempty"`
	SourceKey    string         `json:"source_key"`
	SourceETag   string         `json:"source_etag,omitempty"`
	SourceSize   int64          `json:"source_size_bytes,omitempty"`
	DestURI      string         `json:"dest_uri"`
	DestKey      string         `json:"dest_key"`
	Bytes        int64          `json:"bytes,omitempty"`
	Status       string         `json:"status"`
	Reason       string         `json:"reason,omitempty"`
	RoutingClass string         `json:"routing_class,omitempty"`
	Collision    *collisionInfo `json:"collision,omitempty"`
	Provenance   *provenanceRef `json:"provenance,omitempty"`
	Details      map[string]any `json:"details,omitempty"`
}

func (r reflowRecord) MarshalJSON() ([]byte, error) {
	type alias reflowRecord
	out := alias(r)
	if out.SourceBucket == "" {
		switch {
		case strings.HasPrefix(out.SourceURI, "file://local/"):
			out.SourceBucket = reflowSourceBucketFile
		default:
			if parsed, err := uri.ParseURI(out.SourceURI); err == nil {
				out.SourceBucket = parsed.Bucket
			}
		}
	}
	return json.Marshal(out)
}

type reflowRunRecord struct {
	DestURI        string               `json:"dest_uri"`
	CheckpointPath string               `json:"checkpoint_path"`
	DryRun         bool                 `json:"dry_run"`
	Resume         bool                 `json:"resume"`
	Parallel       int                  `json:"parallel"`
	Provenance     *provenanceRunConfig `json:"provenance,omitempty"`
	Metadata       *metadataRunConfig   `json:"metadata,omitempty"`
}

type reflowSourceRunRecord struct {
	Provider   string `json:"provider"`
	Bucket     string `json:"source_bucket,omitempty"`
	Root       string `json:"source_root,omitempty"`
	URI        string `json:"source_uri"`
	OutputOnly bool   `json:"source_uri_output_only,omitempty"`
}

type reflowFilePreflightSummary struct {
	SourceRoot string
	FileCount  int64
	TotalBytes int64
}

type collisionConfig struct {
	Mode             string
	QuarantinePrefix string
	DeprecatedLog    bool
}

type collisionInfo struct {
	Kind                     string     `json:"kind"`
	DestETagObserved         string     `json:"dest_etag_observed,omitempty"`
	DestSizeObserved         *int64     `json:"dest_size_observed,omitempty"`
	SrcLastModified          *time.Time `json:"src_last_modified,omitempty"`
	DestLastModifiedObserved *time.Time `json:"dest_last_modified_observed,omitempty"`
	DecisionReason           string     `json:"decision_reason,omitempty"`
	DecisionPath             string     `json:"decision_path"`
}

type reflowMetadataConfig struct {
	Policy                  string
	Set                     map[string]string
	SourceKeyRules          []metadataSourceKeyRule
	DerivedRules            []metadataDerivedRule
	OnMissingSource         string
	PreserveContentType     bool
	DestinationStorageClass string
	MetadataSidecarSuffix   string
}

type reflowSourceConfig struct {
	Symlinks        string
	Hidden          string
	Excludes        []string
	PreserveMode    bool
	OnSourceFailure string
}

type metadataRunConfig struct {
	Policy                  string            `json:"policy"`
	SetKeys                 []string          `json:"set_keys,omitempty"`
	SourceKeyRuleKeys       []string          `json:"source_key_rule_keys,omitempty"`
	DerivedRuleKeys         []string          `json:"derived_rule_keys,omitempty"`
	OnMissingSource         string            `json:"on_missing_source,omitempty"`
	PreserveContentType     bool              `json:"preserve_content_type,omitempty"`
	DestinationStorageClass string            `json:"destination_storage_class,omitempty"`
	MetadataSidecarSuffix   string            `json:"metadata_sidecar_suffix,omitempty"`
	Set                     map[string]string `json:"set,omitempty"`
}

type metadataBudgetError struct {
	OverLimitKeys []string
	PairLimit     int
	TotalBytes    int
	TotalLimit    int
	Count         int
}

func (e *metadataBudgetError) Error() string {
	return fmt.Sprintf("user metadata exceeds S3 metadata budget: keys=%v count=%d total_bytes=%d total_limit=%d pair_limit=%d", e.OverLimitKeys, e.Count, e.TotalBytes, e.TotalLimit, e.PairLimit)
}

func (e *metadataBudgetError) details() map[string]any {
	return map[string]any{
		"metadata_keys":        append([]string(nil), e.OverLimitKeys...),
		"metadata_count":       e.Count,
		"metadata_total_bytes": e.TotalBytes,
		"metadata_total_limit": e.TotalLimit,
		"metadata_pair_limit":  e.PairLimit,
	}
}

func (c reflowMetadataConfig) needsSourceHead() bool {
	return c.Policy == metadataPolicyPreserve || c.Policy == metadataPolicyMerge || c.PreserveContentType || c.DestinationStorageClass == storageClassPropagate || c.hasPerObjectRules()
}

func (c reflowMetadataConfig) requiresCapability() bool {
	return c.Policy == metadataPolicyPreserve || c.Policy == metadataPolicyMerge || len(c.Set) > 0 || c.hasPerObjectRules() || c.PreserveContentType || c.DestinationStorageClass != ""
}

func (c reflowMetadataConfig) capabilityFlags() []string {
	var out []string
	if c.Policy == metadataPolicyPreserve || c.Policy == metadataPolicyMerge {
		out = append(out, "--metadata-policy")
	}
	if len(c.Set) > 0 {
		out = append(out, "--metadata-set")
	}
	if len(c.SourceKeyRules) > 0 {
		out = append(out, "--metadata-set-from-source-key")
	}
	if len(c.DerivedRules) > 0 {
		out = append(out, "--metadata-set-from-source-derived")
	}
	if c.OnMissingSource != "" && c.OnMissingSource != metadataMissingSkip {
		out = append(out, "--metadata-on-missing-source")
	}
	if c.PreserveContentType {
		out = append(out, "--preserve-content-type")
	}
	if c.DestinationStorageClass != "" {
		out = append(out, "--destination-storage-class")
	}
	return out
}

func (c reflowMetadataConfig) runConfig() *metadataRunConfig {
	if !c.requiresCapability() && c.MetadataSidecarSuffix == providerfile.DefaultMetadataSidecarSuffix && c.OnMissingSource == metadataMissingSkip {
		return nil
	}
	keys := make([]string, 0, len(c.Set))
	for key := range c.Set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sourceKeys := metadataSourceRuleDestKeys(c.SourceKeyRules)
	derivedKeys := metadataDerivedRuleDestKeys(c.DerivedRules)
	onMissing := ""
	if c.hasPerObjectRules() || c.OnMissingSource != metadataMissingSkip {
		onMissing = c.OnMissingSource
	}
	return &metadataRunConfig{
		Policy:                  c.Policy,
		SetKeys:                 keys,
		SourceKeyRuleKeys:       sourceKeys,
		DerivedRuleKeys:         derivedKeys,
		OnMissingSource:         onMissing,
		PreserveContentType:     c.PreserveContentType,
		DestinationStorageClass: c.DestinationStorageClass,
		MetadataSidecarSuffix:   c.MetadataSidecarSuffix,
	}
}

type reflowDestKeyArbiter struct {
	mu    sync.Mutex
	gates map[string]*reflowDestKeyGate
}

type reflowDestKeyGate struct {
	mu       sync.Mutex
	refs     int
	observed bool
}

func newReflowDestKeyArbiter() *reflowDestKeyArbiter {
	return &reflowDestKeyArbiter{gates: map[string]*reflowDestKeyGate{}}
}

func (a *reflowDestKeyArbiter) acquire(key string) (*reflowDestKeyGate, func()) {
	a.mu.Lock()
	g, ok := a.gates[key]
	if !ok {
		g = &reflowDestKeyGate{}
		a.gates[key] = g
	}
	g.refs++
	a.mu.Unlock()

	g.mu.Lock()
	return g, func() {
		g.mu.Unlock()
		a.mu.Lock()
		defer a.mu.Unlock()
		g.refs--
		if g.refs == 0 && a.gates[key] == g {
			delete(a.gates, key)
		}
	}
}

func (a *reflowDestKeyArbiter) activeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.gates)
}

type provenanceConfig struct {
	Mode              string
	Suffix            string
	OnWriteError      string
	AllowUnsafeSuffix bool
	PlacementMode     string
	SidecarRootRaw    string
	SidecarRoot       *reflowDestSpec
}

type provenanceRunConfig struct {
	Mode         string                     `json:"mode"`
	Suffix       string                     `json:"suffix,omitempty"`
	OnWriteError string                     `json:"on_write_error,omitempty"`
	Placement    provenancePlacementContext `json:"placement"`
}

type provenancePlacementContext struct {
	Mode        string `json:"mode"`
	SidecarRoot string `json:"sidecar_root,omitempty"`
}

type provenanceRef struct {
	Written bool   `json:"written"`
	Key     string `json:"key"`
	URI     string `json:"uri,omitempty"`
}

func (t reflowTask) withSourceMeta(etag string, size int64) reflowTask {
	t.SourceETag = etag
	t.SourceSize = size
	return t
}

func (t reflowTask) auditSourceURI() string {
	if t.SourceProvider == string(provider.ProviderFile) && t.SourceRoot != "" {
		return fileAuditSourceURI(t.SourceRoot, t.SourceKey)
	}
	return t.SourceURI
}

func (t reflowTask) checkpointSourceURI() string {
	if t.SourceCheckpoint != "" {
		return t.SourceCheckpoint
	}
	if t.SourceProvider == string(provider.ProviderFile) && t.SourceRoot != "" {
		return fileCheckpointSourceURI(t.SourceKey)
	}
	return t.SourceURI
}

func (t reflowTask) sourceProviderURI() *uri.ObjectURI {
	switch t.SourceProvider {
	case string(provider.ProviderFile):
		return &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: reflowSourceBucketFile, Key: t.SourceRoot}
	default:
		return &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: t.SourceBucket}
	}
}

func (t reflowTask) reflowRecord(destURI, destKey, status string) reflowRecord {
	rec := reflowRecord{
		SourceURI:  t.auditSourceURI(),
		SourceKey:  t.SourceKey,
		SourceETag: t.SourceETag,
		SourceSize: t.SourceSize,
		DestURI:    destURI,
		DestKey:    destKey,
		Status:     status,
	}
	switch t.SourceProvider {
	case string(provider.ProviderFile):
		rec.SourceBucket = reflowSourceBucketFile
		if verbose {
			rec.SourceRoot = t.SourceRoot
		}
	case string(provider.ProviderS3), "":
		rec.SourceBucket = t.SourceBucket
	}
	return rec
}

func reflowActionForTask(task reflowTask) string {
	if task.RoutingClass == "quarantine" {
		return "quarantined"
	}
	return "landed"
}

func newCollisionInfo(kind string, destMeta *provider.ObjectMeta, decisionPath string) *collisionInfo {
	info := &collisionInfo{Kind: kind, DecisionPath: decisionPath}
	if destMeta != nil {
		size := destMeta.Size
		info.DestETagObserved = destMeta.ETag
		info.DestSizeObserved = &size
	}
	return info
}

func newSourceNewerCollisionInfo(kind string, destMeta *provider.ObjectMeta, srcLastModified time.Time, decisionReason string) *collisionInfo {
	info := newCollisionInfo(kind, destMeta, decisionHeadCompare)
	if !srcLastModified.IsZero() {
		t := srcLastModified.UTC()
		info.SrcLastModified = &t
	}
	if destMeta != nil && !destMeta.LastModified.IsZero() {
		t := destMeta.LastModified.UTC()
		info.DestLastModifiedObserved = &t
	}
	info.DecisionReason = decisionReason
	return info
}

func recordWithCollision(rec reflowRecord, collision *collisionInfo) reflowRecord {
	if collision == nil {
		return rec
	}
	rec.Collision = collision
	return rec
}

func isConditionalExists(err error) bool {
	return provider.IsAlreadyExists(err) || provider.IsPreconditionFailed(err)
}

func isDuplicateCollision(srcETag string, srcSize int64, dstMeta *provider.ObjectMeta) bool {
	if dstMeta == nil || srcETag == "" || dstMeta.ETag == "" || srcETag != dstMeta.ETag {
		return false
	}
	return srcSize <= 0 || dstMeta.Size <= 0 || srcSize == dstMeta.Size
}

func isDuplicateCollisionForReflow(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey string, dstKey string, destProvider string, srcETag string, srcSize int64, dstMeta *provider.ObjectMeta) (bool, error) {
	if isDuplicateCollision(srcETag, srcSize, dstMeta) {
		return true, nil
	}
	if destProvider != string(provider.ProviderFile) || dstMeta == nil {
		return false, nil
	}
	if srcSize != dstMeta.Size {
		return false, nil
	}
	return objectBodiesEqual(ctx, src, dst, srcKey, dstKey)
}

func objectBodiesEqual(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey string, dstKey string) (bool, error) {
	srcGetter, ok := src.(provider.ObjectGetter)
	if !ok {
		return false, fmt.Errorf("source provider does not support GetObject")
	}
	dstGetter, ok := dst.(provider.ObjectGetter)
	if !ok {
		return false, fmt.Errorf("destination provider does not support GetObject")
	}

	srcBody, _, err := srcGetter.GetObject(ctx, srcKey)
	if err != nil {
		return false, err
	}
	defer func() { _ = srcBody.Close() }()

	dstBody, _, err := dstGetter.GetObject(ctx, dstKey)
	if err != nil {
		return false, err
	}
	defer func() { _ = dstBody.Close() }()

	srcHash := sha256.New()
	if _, err := io.Copy(srcHash, srcBody); err != nil {
		return false, err
	}

	dstHash := sha256.New()
	if _, err := io.Copy(dstHash, dstBody); err != nil {
		return false, err
	}

	return bytes.Equal(srcHash.Sum(nil), dstHash.Sum(nil)), nil
}

type reflowDestSpec struct {
	Provider string
	BaseURI  string

	// S3 destination
	Bucket         string
	Prefix         string
	Region         string
	Profile        string
	Endpoint       string
	ForcePathStyle bool

	// File destination
	BaseDir string
}

func parseReflowDest(raw string) (*reflowDestSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("destination is required")
	}

	parsed, err := uri.ParseURI(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Provider == string(provider.ProviderFile) {
		baseDir := filepath.Clean(filepath.FromSlash(parsed.Key))
		baseURI := fileURI(baseDir)
		if !strings.HasSuffix(baseURI, "/") {
			baseURI += "/"
		}
		return &reflowDestSpec{Provider: string(provider.ProviderFile), BaseURI: baseURI, BaseDir: baseDir}, nil
	}

	if parsed.Provider != string(provider.ProviderS3) {
		return nil, fmt.Errorf("provider %q is not supported", parsed.Provider)
	}
	if parsed.IsPattern() {
		return nil, fmt.Errorf("destination must be a prefix URI")
	}
	if !parsed.IsPrefix() {
		parsed.Key = strings.TrimSuffix(parsed.Key, "/") + "/"
	}

	baseURI := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, parsed.Key)
	return &reflowDestSpec{Provider: parsed.Provider, BaseURI: baseURI, Bucket: parsed.Bucket, Prefix: parsed.Key}, nil
}

func fileURI(path string) string {
	path = filepath.ToSlash(path)
	// For unix absolute paths, this produces file:///...
	return "file://" + path
}

func buildReflowDestKey(dest *reflowDestSpec, destRel string) string {
	destRel = strings.Trim(destRel, "/")
	if dest == nil {
		return destRel
	}
	switch dest.Provider {
	case string(provider.ProviderS3):
		key := strings.TrimPrefix(dest.Prefix+destRel, "/")
		key = strings.ReplaceAll(key, "//", "/")
		return key
	case string(provider.ProviderFile):
		return destRel
	default:
		return destRel
	}
}

func buildReflowDestURI(dest *reflowDestSpec, destKey string) string {
	if dest == nil {
		return ""
	}
	switch dest.Provider {
	case string(provider.ProviderS3):
		return fmt.Sprintf("%s://%s/%s", dest.Provider, dest.Bucket, destKey)
	case string(provider.ProviderFile):
		full := filepath.Join(dest.BaseDir, filepath.FromSlash(destKey))
		return fileURI(full)
	default:
		return ""
	}
}

func resolveCollisionConfig(cmd *cobra.Command) (collisionConfig, error) {
	cfg := collisionConfig{Mode: reflowCollisionSkip}
	if cmd != nil && cmd.Flags().Changed("on-collision") {
		cfg.Mode = reflowOnCollision
	} else if viper.IsSet("on_collision") {
		cfg.Mode = viper.GetString("on_collision")
	}
	if cmd != nil && cmd.Flags().Changed("collision-quarantine-prefix") {
		cfg.QuarantinePrefix = reflowCollQuar
	} else if viper.IsSet("collision_quarantine_prefix") {
		cfg.QuarantinePrefix = viper.GetString("collision_quarantine_prefix")
	}

	cfg.Mode = strings.TrimSpace(strings.ToLower(cfg.Mode))
	cfg.QuarantinePrefix = strings.TrimSpace(cfg.QuarantinePrefix)
	if cfg.Mode == reflowCollisionLog {
		cfg.Mode = reflowCollisionSkip
		cfg.DeprecatedLog = true
	}
	if err := validateCollisionConfig(cfg); err != nil {
		return cfg, err
	}
	cfg.QuarantinePrefix = strings.Trim(cfg.QuarantinePrefix, "/")
	return cfg, nil
}

func validateCollisionConfig(cfg collisionConfig) error {
	switch cfg.Mode {
	case reflowCollisionSkip, reflowCollisionFail, reflowCollisionOver, reflowCollisionQuar, reflowCollisionSrcNew:
		// ok
	default:
		return fmt.Errorf("on-collision must be one of: skip-if-duplicate, fail, overwrite, quarantine, overwrite-if-source-newer")
	}
	if cfg.Mode == reflowCollisionQuar {
		if cfg.QuarantinePrefix == "" {
			return fmt.Errorf("collision_quarantine_prefix is required when on_collision=quarantine")
		}
		if !isRelativeQuarantinePrefix(cfg.QuarantinePrefix) {
			return fmt.Errorf("collision_quarantine_prefix must be a relative destination prefix")
		}
	}
	return nil
}

func resolveMetadataConfig(cmd *cobra.Command) (reflowMetadataConfig, error) {
	cfg := reflowMetadataConfig{
		Policy:                metadataPolicyClear,
		MetadataSidecarSuffix: providerfile.DefaultMetadataSidecarSuffix,
		OnMissingSource:       metadataMissingSkip,
	}
	if cmd != nil && cmd.Flags().Changed("metadata-policy") {
		cfg.Policy = reflowMetaPolicy
	} else if viper.IsSet("metadata.policy") {
		cfg.Policy = viper.GetString("metadata.policy")
	}
	if cmd != nil && cmd.Flags().Changed("metadata-set") {
		cfg.Set = parseMetadataSetRaw(reflowMetaSets)
	} else if viper.IsSet("metadata.set") {
		cfg.Set = parseMetadataSetRaw(viper.GetStringSlice("metadata.set"))
	}
	var err error
	if cmd != nil && cmd.Flags().Changed("metadata-set-from-source-key") {
		cfg.SourceKeyRules, err = parseMetadataSourceKeyRules(reflowMetaSrcKeys)
	} else if viper.IsSet("metadata.set_from_source_key") {
		cfg.SourceKeyRules, err = parseMetadataSourceKeyRules(viper.GetStringSlice("metadata.set_from_source_key"))
	}
	if err != nil {
		return cfg, err
	}
	if cmd != nil && cmd.Flags().Changed("metadata-set-from-source-derived") {
		cfg.DerivedRules, err = parseMetadataDerivedRules(reflowMetaDerived)
	} else if viper.IsSet("metadata.set_from_source_derived") {
		cfg.DerivedRules, err = parseMetadataDerivedRules(viper.GetStringSlice("metadata.set_from_source_derived"))
	}
	if err != nil {
		return cfg, err
	}
	if cmd != nil && cmd.Flags().Changed("preserve-content-type") {
		cfg.PreserveContentType = reflowMetaContent
	} else if viper.IsSet("metadata.preserve_content_type") {
		cfg.PreserveContentType = viper.GetBool("metadata.preserve_content_type")
	}
	if cmd != nil && cmd.Flags().Changed("destination-storage-class") {
		cfg.DestinationStorageClass = reflowMetaStorage
	} else if viper.IsSet("metadata.destination_storage_class") {
		cfg.DestinationStorageClass = viper.GetString("metadata.destination_storage_class")
	}
	if cmd != nil && cmd.Flags().Changed("metadata-on-missing-source") {
		cfg.OnMissingSource = reflowMetaMissing
	} else if viper.IsSet("metadata.on_missing_source") {
		cfg.OnMissingSource = viper.GetString("metadata.on_missing_source")
	}
	if cmd != nil && cmd.Flags().Changed("metadata-sidecar-suffix") {
		cfg.MetadataSidecarSuffix = reflowMetaSuffix
	} else if viper.IsSet("metadata.sidecar_suffix") {
		cfg.MetadataSidecarSuffix = viper.GetString("metadata.sidecar_suffix")
	}

	cfg.Policy = strings.TrimSpace(strings.ToLower(cfg.Policy))
	cfg.DestinationStorageClass = strings.TrimSpace(cfg.DestinationStorageClass)
	cfg.OnMissingSource = strings.TrimSpace(strings.ToLower(cfg.OnMissingSource))
	cfg.MetadataSidecarSuffix = strings.TrimSpace(cfg.MetadataSidecarSuffix)
	if cfg.MetadataSidecarSuffix == "" {
		cfg.MetadataSidecarSuffix = providerfile.DefaultMetadataSidecarSuffix
	}
	if err := validateMetadataConfig(cfg); err != nil {
		return cfg, err
	}
	if strings.EqualFold(cfg.DestinationStorageClass, storageClassPropagate) {
		cfg.DestinationStorageClass = storageClassPropagate
	} else {
		cfg.DestinationStorageClass = strings.ToUpper(cfg.DestinationStorageClass)
	}
	return cfg, nil
}

func parseMetadataSetRaw(raw []string) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for _, entry := range raw {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			out[""] = ""
			continue
		}
		out[strings.ToLower(strings.TrimSpace(key))] = value
	}
	return out
}

func validateMetadataConfig(cfg reflowMetadataConfig) error {
	switch cfg.Policy {
	case metadataPolicyClear, metadataPolicyPreserve, metadataPolicyMerge:
		// ok
	default:
		return fmt.Errorf("metadata-policy must be one of: clear, preserve, merge")
	}
	if _, bad := cfg.Set[""]; bad {
		return fmt.Errorf("metadata-set entries must use non-empty key=value syntax")
	}
	for key := range cfg.Set {
		if strings.ContainsAny(key, " \t\r\n=") {
			return fmt.Errorf("metadata-set keys must be non-empty tokens without whitespace or '='")
		}
	}
	switch cfg.OnMissingSource {
	case "", metadataMissingSkip, metadataMissingFail, metadataMissingEmpty:
	default:
		return fmt.Errorf("metadata-on-missing-source must be one of: skip, fail, empty")
	}
	if err := validatePerObjectMetadataRules(cfg.SourceKeyRules, cfg.DerivedRules); err != nil {
		return err
	}
	if !strings.HasPrefix(cfg.MetadataSidecarSuffix, ".") {
		return fmt.Errorf("metadata-sidecar-suffix must start with a leading dot")
	}
	if strings.Contains(cfg.MetadataSidecarSuffix, "/") {
		return fmt.Errorf("metadata-sidecar-suffix must not contain '/'")
	}
	if cfg.DestinationStorageClass == "" {
		return nil
	}
	if strings.EqualFold(cfg.DestinationStorageClass, storageClassPropagate) {
		return nil
	}
	if !isValidPutStorageClass(strings.ToUpper(cfg.DestinationStorageClass)) {
		return fmt.Errorf("destination-storage-class is not a valid PUT target")
	}
	return nil
}

func isValidPutStorageClass(storageClass string) bool {
	switch strings.ToUpper(strings.TrimSpace(storageClass)) {
	case "STANDARD", "INTELLIGENT_TIERING", "STANDARD_IA", "ONEZONE_IA", "GLACIER_IR", "REDUCED_REDUNDANCY":
		return true
	default:
		return false
	}
}

func validateMetadataBudget(metadata map[string]string) error {
	if len(metadata) == 0 {
		return nil
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	total := 0
	overLimitKeys := make([]string, 0)
	for _, key := range keys {
		pairBytes := len([]byte(key)) + len([]byte(metadata[key]))
		total += pairBytes
		if pairBytes > metadataMaxPairBytes {
			overLimitKeys = append(overLimitKeys, key)
		}
	}
	if total > metadataMaxTotalBytes {
		overLimitKeys = append(overLimitKeys, keys...)
	}
	if len(overLimitKeys) == 0 {
		return nil
	}
	overLimitKeys = uniqueSortedStrings(overLimitKeys)
	return &metadataBudgetError{
		OverLimitKeys: overLimitKeys,
		PairLimit:     metadataMaxPairBytes,
		TotalBytes:    total,
		TotalLimit:    metadataMaxTotalBytes,
		Count:         len(metadata),
	}
}

func resolveSourceConfig(cmd *cobra.Command) (reflowSourceConfig, error) {
	cfg := reflowSourceConfig{
		Symlinks:        reflowSymlinkSkip,
		Hidden:          reflowHiddenSkip,
		OnSourceFailure: reflowSourceFailSkip,
	}
	if cmd != nil && cmd.Flags().Changed("symlinks") {
		cfg.Symlinks = reflowSymlinks
	} else if viper.IsSet("source.symlinks") {
		cfg.Symlinks = viper.GetString("source.symlinks")
	}
	if cmd != nil && cmd.Flags().Changed("hidden") {
		cfg.Hidden = reflowHidden
	} else if viper.IsSet("source.hidden") {
		cfg.Hidden = viper.GetString("source.hidden")
	}
	if cmd != nil && cmd.Flags().Changed("exclude") {
		cfg.Excludes = append([]string(nil), reflowExcludes...)
	} else if viper.IsSet("source.exclude") {
		cfg.Excludes = viper.GetStringSlice("source.exclude")
	}
	if cmd != nil && cmd.Flags().Changed("preserve-mode") {
		cfg.PreserveMode = reflowPreserve
	} else if viper.IsSet("source.preserve_mode") {
		cfg.PreserveMode = viper.GetBool("source.preserve_mode")
	}
	if cmd != nil && cmd.Flags().Changed("on-source-failure") {
		cfg.OnSourceFailure = reflowSrcFailure
	} else if viper.IsSet("source.on_failure") {
		cfg.OnSourceFailure = viper.GetString("source.on_failure")
	}
	cfg.Symlinks = strings.TrimSpace(strings.ToLower(cfg.Symlinks))
	cfg.Hidden = strings.TrimSpace(strings.ToLower(cfg.Hidden))
	cfg.OnSourceFailure = strings.TrimSpace(strings.ToLower(cfg.OnSourceFailure))
	for i := range cfg.Excludes {
		cfg.Excludes[i] = filepath.ToSlash(strings.TrimSpace(cfg.Excludes[i]))
	}
	return cfg, validateSourceConfig(cfg)
}

func validateSourceConfig(cfg reflowSourceConfig) error {
	switch cfg.Symlinks {
	case "", reflowSymlinkSkip, reflowSymlinkFollow:
	default:
		if cfg.Symlinks == "preserve" {
			return fmt.Errorf("--symlinks=preserve is not supported in v1; deferred to follow-up brief covering symlink-aware provider capability + preserve-mode escape policy. Use --symlinks=skip or --symlinks=follow")
		}
		return fmt.Errorf("symlinks must be one of: skip, follow")
	}
	switch cfg.Hidden {
	case "", reflowHiddenSkip, reflowHiddenInclude:
	default:
		return fmt.Errorf("hidden must be one of: skip, include")
	}
	switch cfg.OnSourceFailure {
	case "", reflowSourceFailSkip, reflowSourceFailFail:
	default:
		if cfg.OnSourceFailure == "quarantine" {
			return fmt.Errorf("--on-source-failure=quarantine is not supported in v1; source failures have no readable body to quarantine. Use --on-source-failure=skip|fail")
		}
		return fmt.Errorf("on-source-failure must be one of: skip, fail")
	}
	for _, pattern := range cfg.Excludes {
		if pattern == "" {
			continue
		}
		if _, err := pathMatch(pattern, "x"); err != nil {
			return fmt.Errorf("invalid exclude glob %q: %w", pattern, err)
		}
	}
	return nil
}

func pathMatch(pattern, name string) (bool, error) {
	return filepath.Match(pattern, name)
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func ensureMetadataCapability(dst provider.Provider, destProvider string, cfg reflowMetadataConfig) error {
	if !cfg.requiresCapability() {
		return nil
	}
	_, err := providerdispatch.RequireCapability[provider.MetadataAwarePutter](dst, operationTransferReflow, destProvider, "metadata-aware PUT (MetadataAwarePutter)")
	if err != nil {
		return fmt.Errorf("%w required by %s", err, strings.Join(cfg.capabilityFlags(), ", "))
	}
	return nil
}

func ensureCollisionCapability(dst provider.Provider, destProvider string, cfg collisionConfig) error {
	if cfg.Mode != reflowCollisionSrcNew {
		return nil
	}
	if destProvider == "" {
		destProvider = "destination"
	}
	_, err := providerdispatch.RequireCapability[provider.ConditionalPutter](dst, operationTransferReflow, destProvider, "ConditionalPutter.IfMatchETag")
	if err != nil {
		return fmt.Errorf("%w required by --on-collision=%s", err, reflowCollisionSrcNew)
	}
	return nil
}

func emitReflowConfigError(ctx context.Context, w output.Writer, msg string, err error, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	details["mode"] = "transfer_reflow"
	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: output.ErrCodeInvalidInput, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Details: details}); werr != nil {
		observability.CLILogger.Debug("Failed to emit reflow config error record", zap.Error(werr))
	}
	return exitError(foundry.ExitInvalidArgument, msg, err)
}

func (c reflowMetadataConfig) putOptions(source *provider.ObjectMeta) (provider.PutOptions, error) {
	var opts provider.PutOptions
	switch c.Policy {
	case metadataPolicyPreserve:
		userMeta, err := canonicalizeSourceMetadata(source)
		if err != nil {
			return opts, err
		}
		opts.UserMetadata = userMeta
	case metadataPolicyMerge:
		userMeta, err := canonicalizeSourceMetadata(source)
		if err != nil {
			return opts, err
		}
		opts.UserMetadata = userMeta
	case metadataPolicyClear:
	}
	if err := c.applyPerObjectMetadata(&opts, source); err != nil {
		return opts, err
	}
	if len(c.Set) > 0 {
		if opts.UserMetadata == nil {
			opts.UserMetadata = map[string]string{}
		}
		for key, value := range c.Set {
			opts.UserMetadata[key] = value
		}
	}
	if c.PreserveContentType {
		if source == nil {
			return opts, fmt.Errorf("source metadata is required to preserve content type")
		}
		opts.ContentType = source.ContentType
	}
	if c.DestinationStorageClass != "" {
		if c.DestinationStorageClass == storageClassPropagate {
			if source == nil {
				return opts, fmt.Errorf("source metadata is required to propagate storage class")
			}
			storageClass := source.StorageClass
			if storageClass == "" {
				storageClass = "STANDARD"
			}
			storageClass = strings.ToUpper(storageClass)
			if !isValidPutStorageClass(storageClass) {
				return opts, fmt.Errorf("source storage class is not a valid PUT target")
			}
			opts.StorageClass = storageClass
		} else {
			opts.StorageClass = strings.ToUpper(c.DestinationStorageClass)
		}
	}
	return opts, nil
}

func canonicalizeSourceMetadata(source *provider.ObjectMeta) (map[string]string, error) {
	if source == nil {
		return nil, fmt.Errorf("source metadata is required for metadata-policy")
	}
	out := make(map[string]string, len(source.Metadata))
	seenOriginal := make(map[string]string, len(source.Metadata))
	for key, value := range source.Metadata {
		canon := strings.ToLower(strings.TrimSpace(key))
		if canon == "" {
			continue
		}
		if first, ok := seenOriginal[canon]; ok && first != key {
			keys := []string{first, key}
			sort.Strings(keys)
			return nil, &sourceMetadataCollisionError{Keys: keys}
		}
		seenOriginal[canon] = key
		out[canon] = value
	}
	return out, nil
}

func cloneMetadataMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func resolveProvenanceConfig(cmd *cobra.Command, dest *reflowDestSpec) (provenanceConfig, error) {
	cfg := provenanceConfig{
		Mode:              provenanceModeNone,
		Suffix:            provenanceSuffix,
		OnWriteError:      provenanceErrorWarn,
		AllowUnsafeSuffix: false,
		PlacementMode:     provenancePlaceSibling,
	}

	if cmd != nil && cmd.Flags().Changed("provenance") {
		cfg.Mode = reflowProvenance
	} else if viper.IsSet("provenance.mode") {
		cfg.Mode = viper.GetString("provenance.mode")
	}
	if cmd != nil && cmd.Flags().Changed("provenance-sidecar-root") {
		cfg.SidecarRootRaw = reflowProvRoot
	} else if viper.IsSet("provenance.sidecar_root") {
		cfg.SidecarRootRaw = viper.GetString("provenance.sidecar_root")
	}
	if cmd != nil && cmd.Flags().Changed("provenance-suffix") {
		cfg.Suffix = reflowProvSuffix
	} else if viper.IsSet("provenance.suffix") {
		cfg.Suffix = viper.GetString("provenance.suffix")
	}
	if cmd != nil && cmd.Flags().Changed("provenance-on-write-error") {
		cfg.OnWriteError = reflowProvOnError
	} else if viper.IsSet("provenance.on_write_error") {
		cfg.OnWriteError = viper.GetString("provenance.on_write_error")
	}
	if cmd != nil && cmd.Flags().Changed("allow-unsafe-suffix") {
		cfg.AllowUnsafeSuffix = reflowProvUnsafe
	} else if viper.IsSet("provenance.allow_unsafe_suffix") {
		cfg.AllowUnsafeSuffix = viper.GetBool("provenance.allow_unsafe_suffix")
	}

	cfg.Mode = strings.TrimSpace(strings.ToLower(cfg.Mode))
	cfg.SidecarRootRaw = strings.TrimSpace(cfg.SidecarRootRaw)
	cfg.Suffix = strings.TrimSpace(cfg.Suffix)
	cfg.OnWriteError = strings.TrimSpace(strings.ToLower(cfg.OnWriteError))
	if cfg.SidecarRootRaw != "" {
		cfg.PlacementMode = provenancePlaceMirror
	}
	if err := validateProvenanceConfig(cfg); err != nil {
		return cfg, err
	}
	if cfg.enabled() && cfg.SidecarRootRaw != "" {
		root, err := parseProvenanceSidecarRoot(cfg.SidecarRootRaw, dest)
		if err != nil {
			return cfg, err
		}
		cfg.SidecarRoot = root
	}
	return cfg, nil
}

func validateProvenanceConfig(cfg provenanceConfig) error {
	switch cfg.Mode {
	case "", provenanceModeNone:
		if cfg.SidecarRootRaw != "" {
			return fmt.Errorf("provenance-sidecar-root requires --provenance sidecar")
		}
		return nil
	case provenanceModeSidecar:
		// Continue below.
	default:
		return fmt.Errorf("provenance must be one of: none, sidecar")
	}

	switch cfg.OnWriteError {
	case "", provenanceErrorWarn, provenanceErrorFail:
		// ok
	default:
		return fmt.Errorf("provenance-on-write-error must be one of: warn, fail")
	}
	if !strings.HasPrefix(cfg.Suffix, ".") {
		return fmt.Errorf("provenance suffix must start with a leading dot")
	}
	if strings.Contains(cfg.Suffix, "/") {
		return fmt.Errorf("provenance suffix must not contain '/'")
	}
	if strings.ContainsAny(cfg.Suffix, "*?[") {
		return fmt.Errorf("provenance suffix must not look like a glob pattern")
	}
	if !cfg.AllowUnsafeSuffix && isUnsafeProvenanceSuffix(cfg.Suffix) {
		return fmt.Errorf("provenance suffix %q collides with common data extensions; pass --allow-unsafe-suffix to confirm", cfg.Suffix)
	}
	return nil
}

func parseProvenanceSidecarRoot(raw string, dest *reflowDestSpec) (*reflowDestSpec, error) {
	if dest == nil {
		return nil, fmt.Errorf("destination is required before provenance-sidecar-root validation")
	}
	if !strings.HasSuffix(raw, "/") {
		return nil, fmt.Errorf("provenance-sidecar-root %q must end in '/'", raw)
	}
	root, err := parseReflowDest(raw)
	if err != nil {
		return nil, fmt.Errorf("provenance-sidecar-root %q invalid: %w", raw, err)
	}
	if root.Provider != dest.Provider {
		return nil, fmt.Errorf("different-provider-scheme sidecar placement not supported -- file an issue if needed")
	}
	if root.Provider == string(provider.ProviderS3) && root.Bucket != dest.Bucket {
		return nil, fmt.Errorf("different-bucket sidecar placement requires the --provenance-sidecar-* flag family -- future enhancement; file an issue if needed")
	}
	return root, nil
}

func emitProvenancePlacementWarnings(w io.Writer, dest *reflowDestSpec, cfg provenanceConfig) {
	if w == nil || dest == nil || !cfg.enabled() || cfg.PlacementMode != provenancePlaceMirror || cfg.SidecarRoot == nil {
		return
	}
	nesting := provenanceRootNesting(dest, cfg.SidecarRoot)
	if nesting == "" {
		return
	}
	_, _ = fmt.Fprintf(w, "warning: provenance sidecar root nesting detected: %s; sidecars may land inside the data tree this feature is designed to keep clean\n", nesting)
}

func provenanceRootNesting(dest, sidecar *reflowDestSpec) string {
	if dest == nil || sidecar == nil || dest.Provider != sidecar.Provider {
		return ""
	}
	destRoot := comparableRootURI(dest)
	sidecarRoot := comparableRootURI(sidecar)
	if destRoot == "" || sidecarRoot == "" {
		return ""
	}
	switch {
	case destRoot == sidecarRoot:
		return "sidecar root equals dest root"
	case strings.HasPrefix(sidecarRoot, destRoot):
		return "sidecar root is a descendant of dest root"
	case strings.HasPrefix(destRoot, sidecarRoot):
		return "dest root is a descendant of sidecar root"
	default:
		return ""
	}
}

func comparableRootURI(spec *reflowDestSpec) string {
	if spec == nil {
		return ""
	}
	switch spec.Provider {
	case string(provider.ProviderS3):
		return fmt.Sprintf("%s://%s/%s", spec.Provider, spec.Bucket, ensureTrailingSlash(strings.TrimPrefix(spec.Prefix, "/")))
	case string(provider.ProviderFile):
		return ensureTrailingSlash(fileURI(spec.BaseDir))
	default:
		return ensureTrailingSlash(spec.BaseURI)
	}
}

func ensureTrailingSlash(s string) string {
	if s == "" || strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

func isUnsafeProvenanceSuffix(suffix string) bool {
	switch strings.ToLower(suffix) {
	case ".xml", ".json", ".jsonl", ".csv", ".parquet", ".avro", ".txt", ".gz", ".zst", ".zip", ".tar", ".html", ".pdf":
		return true
	default:
		return false
	}
}

func (cfg provenanceConfig) enabled() bool {
	return cfg.Mode == provenanceModeSidecar
}

func (cfg provenanceConfig) runConfig() *provenanceRunConfig {
	if !cfg.enabled() {
		return nil
	}
	placement := provenancePlacementContext{Mode: cfg.PlacementMode}
	if cfg.PlacementMode == provenancePlaceMirror {
		placement.SidecarRoot = cfg.SidecarRootRaw
	}
	return &provenanceRunConfig{Mode: cfg.Mode, Suffix: cfg.Suffix, OnWriteError: cfg.OnWriteError, Placement: placement}
}

type provenanceSidecar struct {
	Schema        string                `json:"schema"`
	SchemaVersion string                `json:"schema_version"`
	Source        provenanceSource      `json:"source"`
	Destination   provenanceDestination `json:"destination"`
	Run           provenanceRun         `json:"run"`
	Routing       provenanceRouting     `json:"routing"`
	Collision     *collisionInfo        `json:"collision,omitempty"`
	Vars          map[string]string     `json:"vars,omitempty"`
	Probe         *probe.ProbeAudit     `json:"probe,omitempty"`
	Action        string                `json:"action"`
}

type provenanceSource struct {
	URI          string     `json:"uri"`
	ETag         string     `json:"etag,omitempty"`
	Size         int64      `json:"size,omitempty"`
	LastModified *time.Time `json:"last_modified,omitempty"`
}

type provenanceDestination struct {
	URI  string `json:"uri"`
	ETag string `json:"etag,omitempty"`
	Size int64  `json:"size,omitempty"`
}

type provenanceRun struct {
	RunID       string `json:"run_id"`
	TS          string `json:"ts"`
	ToolVersion string `json:"tool_version"`
}

type provenanceRouting struct {
	RoutingClass     string  `json:"routing_class"`
	RewriteTemplate  string  `json:"rewrite_template,omitempty"`
	QuarantinePrefix *string `json:"quarantine_prefix"`
}

type reflowWarning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Key     string         `json:"key,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func writeProvenanceSidecar(ctx context.Context, w *output.JSONLWriter, sidecarDst provider.Provider, cfg provenanceConfig, destSpec *reflowDestSpec, task reflowTask, destRel string, destKey string, destURI string, destMeta *provider.ObjectMeta, rewriteTemplate string, action string, jobID string, collision *collisionInfo) (*provenanceRef, bool) {
	if !cfg.enabled() {
		return nil, false
	}

	sidecarKey := buildProvenanceSidecarKey(cfg, destSpec, destRel, destKey)
	sidecarURI := buildProvenanceSidecarURI(cfg, destSpec, sidecarKey)
	ref := &provenanceRef{Written: false, Key: sidecarKey, URI: sidecarURI}
	putter, ok := sidecarDst.(provider.ObjectPutter)
	if !ok {
		err := fmt.Errorf("destination provider does not support PutObject")
		return ref, handleProvenanceWriteError(ctx, w, cfg, sidecarKey, sidecarURI, destURI, err)
	}

	payload, err := json.Marshal(buildProvenanceSidecar(task, destURI, destMeta, rewriteTemplate, action, jobID, collision))
	if err != nil {
		return ref, handleProvenanceWriteError(ctx, w, cfg, sidecarKey, sidecarURI, destURI, err)
	}
	payload = append(payload, '\n')
	if err := putter.PutObject(ctx, sidecarKey, bytes.NewReader(payload), int64(len(payload))); err != nil {
		return ref, handleProvenanceWriteError(ctx, w, cfg, sidecarKey, sidecarURI, destURI, err)
	}
	ref.Written = true
	return ref, false
}

func buildProvenanceSidecarKey(cfg provenanceConfig, destSpec *reflowDestSpec, destRel string, destKey string) string {
	if cfg.PlacementMode != provenancePlaceMirror || cfg.SidecarRoot == nil {
		return destKey + cfg.Suffix
	}
	rel := strings.Trim(destRel, "/")
	switch cfg.SidecarRoot.Provider {
	case string(provider.ProviderS3):
		key := strings.TrimPrefix(cfg.SidecarRoot.Prefix+rel, "/")
		key = strings.ReplaceAll(key, "//", "/")
		return key + cfg.Suffix
	case string(provider.ProviderFile):
		return rel + cfg.Suffix
	default:
		if destSpec != nil {
			return destKey + cfg.Suffix
		}
		return rel + cfg.Suffix
	}
}

func buildProvenanceSidecarURI(cfg provenanceConfig, destSpec *reflowDestSpec, sidecarKey string) string {
	root := destSpec
	if cfg.PlacementMode == provenancePlaceMirror && cfg.SidecarRoot != nil {
		root = cfg.SidecarRoot
	}
	return buildReflowDestURI(root, sidecarKey)
}

func buildProvenanceSidecar(task reflowTask, destURI string, destMeta *provider.ObjectMeta, rewriteTemplate string, action string, jobID string, collision *collisionInfo) provenanceSidecar {
	routingClass := task.RoutingClass
	if routingClass == "" {
		routingClass = "normal"
	}
	var lastModified *time.Time
	if !task.SourceLastMod.IsZero() {
		t := task.SourceLastMod.UTC()
		lastModified = &t
	}
	var quarantinePrefix *string
	if routingClass == "quarantine" {
		prefix := task.QuarantinePrefix
		quarantinePrefix = &prefix
	}

	dest := provenanceDestination{URI: destURI}
	if destMeta != nil {
		dest.ETag = destMeta.ETag
		dest.Size = destMeta.Size
	}

	return provenanceSidecar{
		Schema:        provenanceSchema,
		SchemaVersion: provenanceSchemaVer,
		Source: provenanceSource{
			URI:          task.auditSourceURI(),
			ETag:         task.SourceETag,
			Size:         task.SourceSize,
			LastModified: lastModified,
		},
		Destination: dest,
		Run: provenanceRun{
			RunID:       jobID,
			TS:          time.Now().UTC().Format(time.RFC3339Nano),
			ToolVersion: reflowToolVersion(),
		},
		Routing: provenanceRouting{
			RoutingClass:     routingClass,
			RewriteTemplate:  rewriteTemplate,
			QuarantinePrefix: quarantinePrefix,
		},
		Collision: collision,
		Vars:      task.Vars,
		Probe:     task.Probe,
		Action:    action,
	}
}

func handleProvenanceWriteError(ctx context.Context, w *output.JSONLWriter, cfg provenanceConfig, sidecarKey string, sidecarURI string, destURI string, err error) bool {
	details := map[string]any{"sidecar_key": sidecarKey, "sidecar_uri": sidecarURI, "dest_uri": destURI, "mode": "transfer_reflow"}
	if cfg.OnWriteError == provenanceErrorFail {
		_ = emitReflowError(ctx, w, sidecarKey, "provenance sidecar write failed", err, details)
		return true
	}
	_ = w.WriteAny(ctx, reflowWarningRecord, reflowWarning{
		Code:    "PROVENANCE_WRITE_FAILED",
		Message: fmt.Sprintf("provenance sidecar write failed: %s", err.Error()),
		Key:     sidecarKey,
		Details: details,
	})
	return false
}

func reflowToolVersion() string {
	version := strings.TrimSpace(versionInfo.Version)
	if version == "" {
		version = "dev"
	}
	return "gonimbus " + version
}

func newDestProvider(ctx context.Context, dest *reflowDestSpec, metaCfg reflowMetadataConfig) (provider.Provider, error) {
	if dest == nil {
		return nil, fmt.Errorf("destination is nil")
	}
	return providerdispatch.NewDestination(ctx, providerdispatch.DestinationOptions{
		Command:             operationTransferReflow,
		Provider:            dest.Provider,
		S3Bucket:            dest.Bucket,
		S3Prefix:            dest.Prefix,
		FileBaseDir:         dest.BaseDir,
		FileMetadataSidecar: metaCfg.MetadataSidecarSuffix,
		S3: providerdispatch.S3Options{
			Region:         dest.Region,
			Endpoint:       dest.Endpoint,
			Profile:        dest.Profile,
			ForcePathStyle: dest.ForcePathStyle,
		},
	})
}

func newSourceProvider(ctx context.Context, src *uri.ObjectURI) (provider.Provider, error) {
	if src == nil {
		return nil, fmt.Errorf("source URI is nil")
	}
	return providerdispatch.NewSource(ctx, src, providerdispatch.SourceOptions{
		Command:             operationTransferReflow,
		FileMetadataSidecar: reflowMetaSuffix,
		S3: providerdispatch.S3Options{
			Region:         reflowSrcRegion,
			Endpoint:       reflowSrcEndpoint,
			Profile:        reflowSrcProfile,
			ForcePathStyle: reflowSrcEndpoint != "",
		},
	})
}

func reflowSourceIdentity(src *uri.ObjectURI) string {
	if src == nil {
		return ""
	}
	switch src.Provider {
	case string(provider.ProviderFile):
		return "file:" + filepath.Clean(src.Key)
	case string(provider.ProviderS3):
		return "s3:" + src.Bucket
	default:
		return src.Provider + ":" + src.Bucket + ":" + src.Key
	}
}

func fileReflowInputRootAndKey(sourcePath string, sourceKey string) (string, string, error) {
	cleanSourcePath := filepath.Clean(sourcePath)
	key := strings.TrimSpace(sourceKey)
	if key == "" {
		return filepath.Dir(cleanSourcePath), filepath.ToSlash(filepath.Base(cleanSourcePath)), nil
	}
	key = strings.TrimPrefix(filepath.ToSlash(key), "/")
	key = pathpkg.Clean(key)
	if key == "." || key == ".." || strings.HasPrefix(key, "../") {
		return "", "", fmt.Errorf("file reflow input source_key must be relative")
	}

	sourceSlash := filepath.ToSlash(cleanSourcePath)
	suffix := "/" + key
	if !strings.HasSuffix(sourceSlash, suffix) {
		return "", "", fmt.Errorf("file reflow input source_key must match source_uri path suffix")
	}
	rootSlash := strings.TrimSuffix(sourceSlash, suffix)
	if rootSlash == "" {
		rootSlash = "/"
	}
	return filepath.Clean(filepath.FromSlash(rootSlash)), key, nil
}

func fileAuditSourceURI(root string, rel string) string {
	rel = strings.TrimPrefix(filepath.ToSlash(rel), "/")
	if verbose {
		return fileURI(filepath.Join(root, filepath.FromSlash(rel)))
	}
	if rel == "" {
		return "file://local/"
	}
	return "file://local/" + rel
}

func fileCheckpointSourceURI(rel string) string {
	rel = strings.TrimPrefix(filepath.ToSlash(rel), "/")
	if rel == "" {
		return "file-checkpoint://local/"
	}
	return "file-checkpoint://local/" + rel
}

func filePathContainsSymlink(path string) (bool, error) {
	cleanPath := filepath.Clean(path)
	volume := filepath.VolumeName(cleanPath)
	rest := strings.TrimPrefix(cleanPath, volume)
	if filepath.IsAbs(cleanPath) {
		rest = strings.TrimPrefix(rest, string(filepath.Separator))
	}
	parts := strings.Split(rest, string(filepath.Separator))

	cur := volume
	if filepath.IsAbs(cleanPath) {
		cur += string(filepath.Separator)
	}
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if cur == "" || cur == string(filepath.Separator) || strings.HasSuffix(cur, string(filepath.Separator)) {
			cur += part
		} else {
			cur = filepath.Join(cur, part)
		}
		info, err := os.Lstat(cur)
		if err != nil {
			return false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func redactedPathError(defaultMessage string, verboseMessage string) error {
	if verbose && verboseMessage != "" {
		return errors.New(verboseMessage)
	}
	return errors.New(defaultMessage)
}

func runFileReflowPreflight(ctx context.Context, w output.Writer, parsed *uri.ObjectURI, dest *reflowDestSpec, srcCfg reflowSourceConfig) (reflowFilePreflightSummary, error) {
	summary := reflowFilePreflightSummary{SourceRoot: filepath.Clean(parsed.Key)}
	rec := &output.PreflightRecord{Mode: "reflow-file-source"}
	add := func(capability string, allowed bool, method string, err error, detail string) {
		result := output.PreflightCheckResult{Capability: capability, Allowed: allowed, Method: method, Detail: detail}
		if err != nil {
			result.ErrorCode = preflightErrorCode(err)
			if result.Detail == "" {
				result.Detail = err.Error()
			}
		}
		rec.Results = append(rec.Results, result)
	}

	st, err := os.Stat(summary.SourceRoot)
	if err != nil {
		reportErr := redactedPathError("source root is not accessible", err.Error())
		add("source.file.stat", false, "Stat(source_root)", reportErr, "")
		_ = w.WritePreflight(ctx, rec)
		return summary, reportErr
	}
	if !st.IsDir() {
		reportErr := redactedPathError("file source root must be a directory", fmt.Sprintf("file source root must be a directory: %s", summary.SourceRoot))
		add("source.file.stat", false, "Stat(source_root)", reportErr, "")
		_ = w.WritePreflight(ctx, rec)
		return summary, reportErr
	}
	add("source.file.stat", true, "Stat(source_root)", nil, "")

	if err := summarizeFileSource(ctx, summary.SourceRoot, srcCfg, &summary); err != nil {
		reportErr := redactedPathError("source root could not be enumerated", err.Error())
		add("source.file.enumerate", false, "Walk(source_root)", reportErr, "")
		_ = w.WritePreflight(ctx, rec)
		return summary, reportErr
	}
	add("source.file.enumerate", true, "Walk(source_root)", nil, fmt.Sprintf("files=%d bytes=%d", summary.FileCount, summary.TotalBytes))

	if dest.Provider == string(provider.ProviderFile) {
		destRoot := filepath.Clean(dest.BaseDir)
		if pathWithinRoot(summary.SourceRoot, destRoot) || pathWithinRoot(destRoot, summary.SourceRoot) {
			err := redactedPathError("file source and destination paths overlap", fmt.Sprintf("file source and destination paths overlap: source=%s dest=%s", summary.SourceRoot, destRoot))
			add("destination.file.self_copy", false, "Compare(source_root,dest_root)", err, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, err
		}
		add("destination.file.self_copy", true, "Compare(source_root,dest_root)", nil, "")

		if err := ensureDirWritable(destRoot); err != nil {
			reportErr := redactedPathError("file destination is not writable", err.Error())
			add("destination.file.write", false, "CreateTemp(dest_root)", reportErr, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, reportErr
		}
		add("destination.file.write", true, "CreateTemp(dest_root)", nil, "")

		free, err := availableBytes(destRoot)
		if err != nil {
			reportErr := redactedPathError("file destination space could not be checked", err.Error())
			add("destination.file.space", false, "Statfs(dest_root)", reportErr, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, reportErr
		}
		if summary.TotalBytes > free {
			err := fmt.Errorf("insufficient destination free space: source_bytes=%d free_bytes=%d", summary.TotalBytes, free)
			add("destination.file.space", false, "Statfs(dest_root)", err, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, err
		}
		add("destination.file.space", true, "Statfs(dest_root)", nil, fmt.Sprintf("source_bytes=%d free_bytes=%d", summary.TotalBytes, free))
	}

	_ = w.WritePreflight(ctx, rec)
	return summary, nil
}

func summarizeFileSource(ctx context.Context, root string, srcCfg reflowSourceConfig, summary *reflowFilePreflightSummary) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if fileSourceSkipped(key, srcCfg) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			if srcCfg.Symlinks == reflowSymlinkSkip || srcCfg.Symlinks == "" {
				return nil
			}
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Mode().IsRegular() {
			summary.FileCount++
			summary.TotalBytes += info.Size()
		}
		return nil
	})
}

func ensureDirWritable(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".gonimbus-preflight-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if rerr := os.Remove(name); rerr != nil && err == nil {
		err = rerr
	}
	return err
}

func emitReflowSourceRunRecord(ctx context.Context, w interface {
	WriteAny(context.Context, string, any) error
}, state reflowStateStore, parsed *uri.ObjectURI) {
	if parsed == nil {
		return
	}
	rec := reflowSourceRunRecord{Provider: parsed.Provider, Bucket: parsed.Bucket, URI: parsed.String()}
	if parsed.Provider == string(provider.ProviderFile) {
		rec.Bucket = reflowSourceBucketFile
		rec.Root = filepath.Clean(parsed.Key)
		rec.URI = "file://local/"
		rec.OutputOnly = true
	}
	_ = w.WriteAny(ctx, reflowSourceRecordType, rec)
	if err := state.SetSourceMetadata(ctx, rec.Provider, rec.Bucket, rec.Root, parsed.String()); err != nil {
		observability.CLILogger.Debug("Checkpoint source metadata write failed", zap.Error(err))
	}
}

func emitPreserveModeWarning(w io.Writer, srcProvider string, destProvider string) {
	if srcProvider == string(provider.ProviderFile) && destProvider == string(provider.ProviderFile) {
		return
	}
	switch {
	case srcProvider != string(provider.ProviderFile) && destProvider != string(provider.ProviderFile):
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect unless both source and destination are file:// (S3 has no Unix mode bits to read or preserve).")
	case srcProvider != string(provider.ProviderFile):
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect unless the source is file:// (S3 has no Unix mode bits to preserve).")
	case destProvider != string(provider.ProviderFile):
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect unless the destination is file:// (S3 has no Unix mode-bits concept).")
	default:
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect for this provider combination.")
	}
}

type transferReflowCheckpointPayload struct {
	Config transferReflowCheckpointConfig `json:"config"`
}

type transferReflowCheckpointConfig struct {
	SourceURI                 string   `json:"source_uri"`
	Stdin                     bool     `json:"stdin"`
	Dest                      string   `json:"dest"`
	RewriteFrom               string   `json:"rewrite_from"`
	RewriteTo                 string   `json:"rewrite_to"`
	Parallel                  int      `json:"parallel"`
	DryRun                    bool     `json:"dry_run"`
	CheckpointPath            string   `json:"checkpoint_path"`
	Overwrite                 bool     `json:"overwrite"`
	OnCollision               string   `json:"on_collision"`
	CollisionQuarantinePrefix string   `json:"collision_quarantine_prefix,omitempty"`
	Provenance                string   `json:"provenance"`
	ProvenanceSidecarRoot     string   `json:"provenance_sidecar_root,omitempty"`
	ProvenanceSuffix          string   `json:"provenance_suffix,omitempty"`
	ProvenanceOnWriteError    string   `json:"provenance_on_write_error,omitempty"`
	AllowUnsafeSuffix         bool     `json:"allow_unsafe_suffix"`
	MetadataPolicy            string   `json:"metadata_policy"`
	MetadataSet               []string `json:"metadata_set,omitempty"`
	MetadataSetFromSourceKey  []string `json:"metadata_set_from_source_key,omitempty"`
	MetadataSetDerived        []string `json:"metadata_set_from_source_derived,omitempty"`
	MetadataOnMissingSource   string   `json:"metadata_on_missing_source,omitempty"`
	PreserveContentType       bool     `json:"preserve_content_type"`
	DestinationStorageClass   string   `json:"destination_storage_class,omitempty"`
	MetadataSidecarSuffix     string   `json:"metadata_sidecar_suffix,omitempty"`
	Symlinks                  string   `json:"symlinks"`
	Hidden                    string   `json:"hidden"`
	Excludes                  []string `json:"excludes,omitempty"`
	PreserveMode              bool     `json:"preserve_mode"`
	OnSourceFailure           string   `json:"on_source_failure"`
	SrcRegion                 string   `json:"src_region,omitempty"`
	SrcProfile                string   `json:"src_profile,omitempty"`
	SrcEndpoint               string   `json:"src_endpoint,omitempty"`
	DstRegion                 string   `json:"dest_region,omitempty"`
	DstProfile                string   `json:"dest_profile,omitempty"`
	DstEndpoint               string   `json:"dest_endpoint,omitempty"`
}

func transferReflowCheckpointConfigFromEffective(
	args []string,
	checkpointPath string,
	collCfg collisionConfig,
	metaCfg reflowMetadataConfig,
	srcCfg reflowSourceConfig,
	provCfg provenanceConfig,
) transferReflowCheckpointConfig {
	sourceURI := ""
	if len(args) == 1 {
		sourceURI = strings.TrimSpace(args[0])
	}
	return transferReflowCheckpointConfig{
		SourceURI:                 sourceURI,
		Stdin:                     reflowStdin,
		Dest:                      reflowDest,
		RewriteFrom:               reflowRewriteFrom,
		RewriteTo:                 reflowRewriteTo,
		Parallel:                  reflowParallel,
		DryRun:                    reflowDryRun,
		CheckpointPath:            checkpointPath,
		Overwrite:                 reflowOverwrite,
		OnCollision:               collCfg.Mode,
		CollisionQuarantinePrefix: collCfg.QuarantinePrefix,
		Provenance:                provCfg.Mode,
		ProvenanceSidecarRoot:     provCfg.SidecarRootRaw,
		ProvenanceSuffix:          provCfg.Suffix,
		ProvenanceOnWriteError:    provCfg.OnWriteError,
		AllowUnsafeSuffix:         provCfg.AllowUnsafeSuffix,
		MetadataPolicy:            metaCfg.Policy,
		MetadataSet:               metadataSetRawFromMap(metaCfg.Set),
		MetadataSetFromSourceKey:  metadataSourceRuleRaw(metaCfg.SourceKeyRules),
		MetadataSetDerived:        metadataDerivedRuleRaw(metaCfg.DerivedRules),
		MetadataOnMissingSource:   metaCfg.OnMissingSource,
		PreserveContentType:       metaCfg.PreserveContentType,
		DestinationStorageClass:   metaCfg.DestinationStorageClass,
		MetadataSidecarSuffix:     metaCfg.MetadataSidecarSuffix,
		Symlinks:                  srcCfg.Symlinks,
		Hidden:                    srcCfg.Hidden,
		Excludes:                  append([]string(nil), srcCfg.Excludes...),
		PreserveMode:              srcCfg.PreserveMode,
		OnSourceFailure:           srcCfg.OnSourceFailure,
		SrcRegion:                 reflowSrcRegion,
		SrcProfile:                reflowSrcProfile,
		SrcEndpoint:               reflowSrcEndpoint,
		DstRegion:                 reflowDstRegion,
		DstProfile:                reflowDstProfile,
		DstEndpoint:               reflowDstEndpoint,
	}
}

func metadataSetRawFromMap(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}
	return out
}

func metadataSourceRuleRaw(rules []metadataSourceKeyRule) []string {
	if len(rules) == 0 {
		return nil
	}
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.Raw)
	}
	sort.Strings(out)
	return out
}

func metadataDerivedRuleRaw(rules []metadataDerivedRule) []string {
	if len(rules) == 0 {
		return nil
	}
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.Raw)
	}
	sort.Strings(out)
	return out
}

func transferReflowCheckpointEligible(cfg transferReflowCheckpointConfig) bool {
	return !cfg.Stdin && !cfg.DryRun && strings.TrimSpace(cfg.SourceURI) != "" && strings.TrimSpace(cfg.CheckpointPath) != ""
}

func runTransferReflowResume(ctx context.Context, cmd *cobra.Command, runID string) error {
	if err := rejectTransferReflowResumeRunFlagConflicts(cmd); err != nil {
		return err
	}
	opStore, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return err
	}
	env, err := opStore.ReadCheckpoint(ctx, operationTransferReflow, runID)
	if err != nil {
		return fmt.Errorf("read operation checkpoint: %w", err)
	}
	var payload transferReflowCheckpointPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("parse transfer reflow checkpoint payload: %w", err)
	}
	if payload.Config.Stdin || strings.TrimSpace(payload.Config.SourceURI) == "" {
		return fmt.Errorf("--resume-run %s is not supported for stdin-backed transfer reflow checkpoints in this slice", runID)
	}
	state, err := newReflowStateStore(ctx, reflowstate.Config{Path: payload.Config.CheckpointPath})
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to open checkpoint", err)
	}
	defer func() {
		if state != nil {
			_ = state.Close()
		}
	}()
	fingerprint, err := validateTransferReflowCheckpointIdentity(ctx, opStore, state, env, payload.Config)
	if err != nil {
		return err
	}
	if err := state.Close(); err != nil {
		return err
	}
	state = nil
	lease, err := opStore.ClaimLease(ctx, operationTransferReflow, runID, "gonimbus-"+uuid.NewString(), resumeLeaseTTL)
	if err != nil {
		return err
	}
	heartbeat, leaseCtx, err := startResumeLeaseHeartbeat(ctx, opStore, operationTransferReflow, lease)
	if err != nil {
		return err
	}
	leaseCtx = contextWithResumeLeaseHeartbeat(leaseCtx, heartbeat)
	previousCtx := cmd.Context()
	cmd.SetContext(leaseCtx)
	defer func() {
		cmd.SetContext(previousCtx)
		_ = heartbeat.Stop()
		_ = opStore.ReleaseLease(operationTransferReflow, *lease)
	}()

	if err := applyTransferReflowCheckpointConfig(cmd, payload.Config); err != nil {
		return err
	}
	if err := opStore.ValidateIdentity(env, opcheckpoint.Identity{Operation: operationTransferReflow, RunID: runID, ConfigFingerprint: fingerprint}); err != nil {
		return err
	}
	err = runTransferReflowWithRunID(cmd, []string{payload.Config.SourceURI}, runID)
	if err != nil {
		return err
	}
	if err := stopResumeLeaseHeartbeat(heartbeat); err != nil {
		return err
	}
	env.Status = opcheckpoint.StatusSuccess
	env.Progress = nil
	env.Events = append(env.Events, opcheckpoint.CheckpointEvent{Type: "resume_completed", At: time.Now().UTC()})
	if err := opStore.WriteCheckpoint(context.Background(), *env); err != nil {
		return fmt.Errorf("write completed checkpoint: %w", err)
	}
	return nil
}

func validateTransferReflowCheckpointIdentity(ctx context.Context, opStore *opcheckpoint.Store, state reflowStateStore, env *opcheckpoint.Envelope, cfg transferReflowCheckpointConfig) (string, error) {
	if env == nil {
		return "", fmt.Errorf("checkpoint envelope is nil")
	}
	if env.Status != opcheckpoint.StatusFailedResumable {
		return "", opcheckpoint.ErrIdentityMismatch
	}
	expected, err := state.OperationCheckpointFingerprint(ctx, operationTransferReflow)
	if err != nil {
		return "", err
	}
	if env.Operation != operationTransferReflow || env.ConfigFingerprint != expected {
		return "", opcheckpoint.ErrIdentityMismatch
	}
	actual, err := checkpointFingerprint(cfg)
	if err != nil {
		return "", err
	}
	if actual != expected {
		return "", opcheckpoint.ErrIdentityMismatch
	}
	if err := opStore.ValidateIdentity(env, opcheckpoint.Identity{Operation: operationTransferReflow, RunID: env.RunID, ConfigFingerprint: expected}); err != nil {
		return "", err
	}
	return expected, nil
}

func rejectTransferReflowResumeRunFlagConflicts(cmd *cobra.Command) error {
	if cmd == nil {
		return nil
	}
	var conflicts []string
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		if flag.Name != "resume-run" {
			conflicts = append(conflicts, "--"+flag.Name)
		}
	})
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return fmt.Errorf("%s are not accepted with --resume-run; resume uses checkpointed reflow config", strings.Join(conflicts, ", "))
	}
	return nil
}

func applyTransferReflowCheckpointConfig(cmd *cobra.Command, cfg transferReflowCheckpointConfig) error {
	set := func(name, value string) error {
		if err := cmd.Flags().Set(name, value); err != nil {
			return fmt.Errorf("restore --%s: %w", name, err)
		}
		return nil
	}
	setBool := func(name string, value bool) error {
		if value {
			return set(name, "true")
		}
		return set(name, "false")
	}
	setInt := func(name string, value int) error {
		return set(name, fmt.Sprintf("%d", value))
	}
	setArray := func(name string, values []string) error {
		for _, value := range values {
			if err := set(name, value); err != nil {
				return err
			}
		}
		return nil
	}
	if err := setBool("stdin", false); err != nil {
		return err
	}
	for _, pair := range []struct{ name, value string }{
		{"dest", cfg.Dest},
		{"rewrite-from", cfg.RewriteFrom},
		{"rewrite-to", cfg.RewriteTo},
		{"checkpoint", cfg.CheckpointPath},
		{"on-collision", cfg.OnCollision},
		{"collision-quarantine-prefix", cfg.CollisionQuarantinePrefix},
		{"provenance", cfg.Provenance},
		{"provenance-sidecar-root", cfg.ProvenanceSidecarRoot},
		{"provenance-suffix", cfg.ProvenanceSuffix},
		{"provenance-on-write-error", cfg.ProvenanceOnWriteError},
		{"metadata-policy", cfg.MetadataPolicy},
		{"metadata-on-missing-source", cfg.MetadataOnMissingSource},
		{"destination-storage-class", cfg.DestinationStorageClass},
		{"metadata-sidecar-suffix", cfg.MetadataSidecarSuffix},
		{"symlinks", cfg.Symlinks},
		{"hidden", cfg.Hidden},
		{"on-source-failure", cfg.OnSourceFailure},
		{"src-region", cfg.SrcRegion},
		{"src-profile", cfg.SrcProfile},
		{"src-endpoint", cfg.SrcEndpoint},
		{"dest-region", cfg.DstRegion},
		{"dest-profile", cfg.DstProfile},
		{"dest-endpoint", cfg.DstEndpoint},
	} {
		if err := set(pair.name, pair.value); err != nil {
			return err
		}
	}
	for _, pair := range []struct {
		name  string
		value bool
	}{
		{"dry-run", false},
		{"resume", true},
		{"overwrite", cfg.Overwrite},
		{"allow-unsafe-suffix", cfg.AllowUnsafeSuffix},
		{"preserve-content-type", cfg.PreserveContentType},
		{"preserve-mode", cfg.PreserveMode},
	} {
		if err := setBool(pair.name, pair.value); err != nil {
			return err
		}
	}
	if err := setInt("parallel", cfg.Parallel); err != nil {
		return err
	}
	for _, pair := range []struct {
		name   string
		values []string
	}{
		{"metadata-set", cfg.MetadataSet},
		{"metadata-set-from-source-key", cfg.MetadataSetFromSourceKey},
		{"metadata-set-from-source-derived", cfg.MetadataSetDerived},
		{"exclude", cfg.Excludes},
	} {
		if err := setArray(pair.name, pair.values); err != nil {
			return err
		}
	}
	reflowResumeRun = ""
	return nil
}

func writeFailedResumableTransferReflowCheckpoint(ctx context.Context, state reflowStateStore, runID string, cfg transferReflowCheckpointConfig, class opcheckpoint.ErrorClass, progress map[string]int64) error {
	opStore, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return fmt.Errorf("open operation checkpoint store: %w", err)
	}
	fingerprint, err := checkpointFingerprint(cfg)
	if err != nil {
		return err
	}
	if err := state.SetOperationCheckpointIdentity(ctx, operationTransferReflow, fingerprint); err != nil {
		return fmt.Errorf("record operation checkpoint identity: %w", err)
	}
	rawPayload, err := json.Marshal(transferReflowCheckpointPayload{Config: cfg})
	if err != nil {
		return fmt.Errorf("marshal checkpoint payload: %w", err)
	}
	now := time.Now().UTC()
	env := opcheckpoint.Envelope{
		SchemaVersion:     opcheckpoint.SchemaVersion,
		Operation:         operationTransferReflow,
		RunID:             runID,
		ConfigFingerprint: fingerprint,
		Status:            opcheckpoint.StatusFailedResumable,
		ErrorClass:        class,
		CreatedAt:         now,
		Progress:          progress,
		Payload:           rawPayload,
		Events: []opcheckpoint.CheckpointEvent{{
			Type:       "failed_resumable",
			At:         now,
			ErrorClass: class,
		}},
	}
	return opStore.WriteCheckpoint(ctx, env)
}

func transferReflowProgress(invalidCount, errorCount int64) map[string]int64 {
	progress := map[string]int64{}
	if invalidCount > 0 {
		progress["invalid_inputs"] = invalidCount
	}
	if errorCount > 0 {
		progress["errors"] = errorCount
	}
	if len(progress) == 0 {
		return nil
	}
	return progress
}

func classifyTransferReflowRunErrorWithConfig(err error, _ transferReflowCheckpointConfig) opcheckpoint.Classification {
	if err == nil {
		return opcheckpoint.Classification{Class: opcheckpoint.ErrorClassRuntimeFailure, Resumable: false}
	}
	return opcheckpoint.ClassifyFatalError(err, opcheckpoint.ClassifierInput{
		Interrupted: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
	})
}

func transferReflowFatalExitCode(classification opcheckpoint.Classification) int {
	if classification.Class == opcheckpoint.ErrorClassInterrupted {
		return foundry.ExitSignalInt
	}
	return foundry.ExitExternalServiceUnavailable
}

func transferReflowFatalExitMessage(classification opcheckpoint.Classification, checkpointWritten bool) string {
	if classification.Class == opcheckpoint.ErrorClassInterrupted {
		return "reflow cancelled"
	}
	if !checkpointWritten {
		return "reflow failed"
	}
	return "reflow failed resumable"
}

func runTransferReflow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	resumeRun := strings.TrimSpace(reflowResumeRun)
	if resumeRun != "" {
		return runTransferReflowResume(ctx, cmd, resumeRun)
	}
	return runTransferReflowWithRunID(cmd, args, "")
}

func runTransferReflowWithRunID(cmd *cobra.Command, args []string, runID string) error {
	ctx, cancelWork := context.WithCancel(cmd.Context())
	defer cancelWork()
	if reflowParallel < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --parallel value", fmt.Errorf("parallel must be >= 1"))
	}
	if reflowResume && strings.TrimSpace(reflowCheckpoint) == "" {
		return exitError(foundry.ExitInvalidArgument, "Invalid --resume usage", fmt.Errorf("--resume requires --checkpoint"))
	}
	if strings.TrimSpace(reflowDest) == "" {
		return exitError(foundry.ExitInvalidArgument, "Missing --dest", fmt.Errorf("--dest is required"))
	}
	if strings.TrimSpace(reflowRewriteFrom) == "" {
		return exitError(foundry.ExitInvalidArgument, "Missing --rewrite-from", fmt.Errorf("--rewrite-from is required"))
	}
	if strings.TrimSpace(reflowRewriteTo) == "" {
		return exitError(foundry.ExitInvalidArgument, "Missing --rewrite-to", fmt.Errorf("--rewrite-to is required"))
	}
	collCfg, err := resolveCollisionConfig(cmd)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid collision configuration", err)
	}
	if collCfg.DeprecatedLog {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "warning: --on-collision=log is deprecated; use --on-collision=skip-if-duplicate")
	}
	if collCfg.Mode == reflowCollisionOver && !reflowOverwrite {
		return exitError(foundry.ExitInvalidArgument, "Overwrite not enabled", fmt.Errorf("--on-collision=overwrite requires --overwrite"))
	}
	destSpec, err := parseReflowDest(reflowDest)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --dest URI", err)
	}
	if destSpec.Provider == string(provider.ProviderS3) {
		destSpec.Region = reflowDstRegion
		destSpec.Endpoint = reflowDstEndpoint
		destSpec.Profile = reflowDstProfile
		destSpec.ForcePathStyle = reflowDstEndpoint != ""
	}
	destURI := destSpec.BaseURI

	metaCfg, err := resolveMetadataConfig(cmd)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid metadata configuration", err)
	}
	srcCfg, err := resolveSourceConfig(cmd)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid source configuration", err)
	}

	provCfg, err := resolveProvenanceConfig(cmd, destSpec)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid provenance configuration", err)
	}
	emitProvenancePlacementWarnings(cmd.ErrOrStderr(), destSpec, provCfg)

	rewrite, err := transfer.CompileReflowRewrite(reflowRewriteFrom, reflowRewriteTo)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid rewrite templates", err)
	}
	cmd.SilenceUsage = true

	jobID := strings.TrimSpace(runID)
	if jobID == "" {
		jobID = uuid.New().String()
	}
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, destSpec.Provider)
	defer func() { _ = w.Close() }()

	checkpointPath, err := resolveReflowCheckpointPath(jobID)
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to resolve checkpoint path", err)
	}
	if strings.TrimSpace(reflowCheckpoint) != "" {
		checkpointPath = reflowCheckpoint
	}
	checkpointCfg := transferReflowCheckpointConfigFromEffective(args, checkpointPath, collCfg, metaCfg, srcCfg, provCfg)

	state, err := newReflowStateStore(ctx, reflowstate.Config{Path: checkpointPath})
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to open checkpoint", err)
	}
	defer func() { _ = state.Close() }()

	var (
		srcProv             provider.Provider
		dstProv             provider.Provider
		sidecarProv         provider.Provider
		srcProviderIdentity string
		preserveWarned      bool
		provMu              sync.Mutex
	)
	dstProv, err = newDestProvider(ctx, destSpec, metaCfg)
	if err != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to destination provider", err)
	}
	if err := ensureMetadataCapability(dstProv, destSpec.Provider, metaCfg); err != nil {
		return emitReflowConfigError(ctx, w, "Invalid metadata configuration", err, map[string]any{
			"missing_capability": "MetadataAwarePutter",
			"flags":              metaCfg.capabilityFlags(),
			"provider":           destSpec.Provider,
		})
	}
	if err := ensureCollisionCapability(dstProv, destSpec.Provider, collCfg); err != nil {
		return emitReflowConfigError(ctx, w, "Invalid collision configuration", err, map[string]any{
			"missing_capability": "ConditionalPutter.IfMatchETag",
			"on_collision":       collCfg.Mode,
			"provider":           destSpec.Provider,
		})
	}

	_ = w.WriteAny(ctx, reflowRunRecordType, reflowRunRecord{
		DestURI:        destURI,
		CheckpointPath: checkpointPath,
		DryRun:         reflowDryRun,
		Resume:         reflowResume,
		Parallel:       reflowParallel,
		Provenance:     provCfg.runConfig(),
		Metadata:       metaCfg.runConfig(),
	})

	if !reflowStdin {
		if parsed, parseErr := uri.ParseURI(args[0]); parseErr == nil {
			emitReflowSourceRunRecord(ctx, w, state, parsed)
			if parsed.Provider == string(provider.ProviderFile) {
				if _, pfErr := runFileReflowPreflight(ctx, w, parsed, destSpec, srcCfg); pfErr != nil {
					return exitError(foundry.ExitInvalidArgument, "File source preflight failed", pfErr)
				}
			}
		}
	}

	// Source providers are created after we discover the source URI shape.
	getProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, provider.Provider, error) {
		provMu.Lock()
		defer provMu.Unlock()

		if dstProv == nil {
			pNew, err := newDestProvider(ctx, destSpec, metaCfg)
			if err != nil {
				return nil, nil, nil, err
			}
			if err := ensureMetadataCapability(pNew, destSpec.Provider, metaCfg); err != nil {
				return nil, nil, nil, err
			}
			if err := ensureCollisionCapability(pNew, destSpec.Provider, collCfg); err != nil {
				return nil, nil, nil, err
			}
			dstProv = pNew
		}
		if sidecarProv == nil {
			if provCfg.PlacementMode == provenancePlaceMirror && provCfg.SidecarRoot != nil && provCfg.SidecarRoot.Provider == string(provider.ProviderFile) && provCfg.SidecarRoot.BaseDir != destSpec.BaseDir {
				pNew, err := newDestProvider(ctx, provCfg.SidecarRoot, metaCfg)
				if err != nil {
					return nil, nil, nil, err
				}
				sidecarProv = pNew
			} else {
				sidecarProv = dstProv
			}
		}
		identity := reflowSourceIdentity(srcURI)
		if srcCfg.PreserveMode && !preserveWarned {
			emitPreserveModeWarning(cmd.ErrOrStderr(), srcURI.Provider, destSpec.Provider)
			preserveWarned = true
		}
		if srcProv == nil {
			pNew, err := newSourceProvider(ctx, srcURI)
			if err != nil {
				return nil, nil, nil, err
			}
			srcProv = pNew
			srcProviderIdentity = identity
		} else if srcProviderIdentity != "" && identity != "" && srcProviderIdentity != identity {
			return nil, nil, nil, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcProviderIdentity)
		}
		return srcProv, dstProv, sidecarProv, nil
	}
	getInputProviders := func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		src, dst, _, err := getProviders(srcURI)
		return src, dst, err
	}
	defer func() {
		provMu.Lock()
		toCloseSrc := srcProv
		toCloseDst := dstProv
		toCloseSidecar := sidecarProv
		provMu.Unlock()
		if toCloseSrc != nil {
			_ = toCloseSrc.Close()
		}
		if toCloseDst != nil {
			_ = toCloseDst.Close()
		}
		if toCloseSidecar != nil && toCloseSidecar != toCloseDst {
			_ = toCloseSidecar.Close()
		}
	}()

	var (
		invalidCount atomic.Int64
		errorCount   atomic.Int64
		fatalMu      sync.Mutex
		fatalErr     error
	)
	recordFatalReflowError := func(err error) bool {
		classification := classifyTransferReflowRunErrorWithConfig(err, checkpointCfg)
		if !classification.Resumable {
			return false
		}
		fatalMu.Lock()
		defer fatalMu.Unlock()
		if fatalErr == nil {
			fatalErr = err
			cancelWork()
		}
		return true
	}
	currentFatalReflowError := func() error {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		return fatalErr
	}

	tasks := make(chan reflowTask, reflowParallel*2)
	destArbiter := newReflowDestKeyArbiter()
	var wg sync.WaitGroup
	for i := 0; i < reflowParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}
				srcAuditURI := task.auditSourceURI()
				srcCheckpointURI := task.checkpointSourceURI()
				if task.SourceFailure != "" {
					rec := task.reflowRecord("", "", "skipped")
					rec.Reason = task.SourceFailure
					if srcCfg.OnSourceFailure == reflowSourceFailFail {
						errorCount.Add(1)
						err := errors.New(task.SourceFailure)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "source failure", err, map[string]any{"source_uri": srcAuditURI})
						rec.Status = "failed"
					}
					_ = w.WriteAny(ctx, reflowRecordType, rec)
					continue
				}

				src, dst, sidecarDst, err := getProviders(task.sourceProviderURI())
				if err != nil {
					if recordFatalReflowError(err) {
						continue
					}
					errorCount.Add(1)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "failed to connect to provider", err, map[string]any{"source_uri": srcAuditURI})
					continue
				}

				var destRel string
				if task.RoutingClass == "quarantine" {
					destRel = buildQuarantineDestRel(task.QuarantinePrefix, task.SourceKey)
				} else if task.DestRelKey != "" {
					destRel = task.DestRelKey
				} else {
					mapped, _, err := rewrite.ApplyWithVars(task.SourceKey, task.Vars)
					if err != nil {
						invalidCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "rewrite failed", err, map[string]any{"source_uri": srcAuditURI})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: "", SourceKey: task.SourceKey, DestKey: "", SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
					destRel = mapped
				}

				dstKey := buildReflowDestKey(destSpec, destRel)
				dstURI := buildReflowDestURI(destSpec, dstKey)

				if reflowResume {
					done, status, err := state.ItemDone(ctx, srcCheckpointURI, dstURI)
					if err != nil {
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "checkpoint read failed", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
						continue
					}
					if done {
						rec := task.reflowRecord(dstURI, dstKey, "skipped")
						rec.Reason = "resume." + status
						_ = w.WriteAny(ctx, reflowRecordType, rec)
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "skipped", Reason: "resume." + status}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
				}

				if reflowDryRun {
					_ = w.WriteAny(ctx, reflowRecordType, task.reflowRecord(dstURI, dstKey, "planned"))
					continue
				}

				if task.RejectSymlinkPath {
					sourcePath := filepath.Join(task.SourceRoot, filepath.FromSlash(task.SourceKey))
					hasSymlink, err := filePathContainsSymlink(sourcePath)
					if err != nil {
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "source path validation failed", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "failed", ErrorCode: output.ErrCodeInvalidInput, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						rec := task.reflowRecord(dstURI, dstKey, "failed")
						rec.Reason = "source.path.validation_failed"
						_ = w.WriteAny(ctx, reflowRecordType, rec)
						continue
					}
					if hasSymlink {
						errorCount.Add(1)
						err := errors.New("file source path uses a symlink")
						_ = emitReflowError(context.Background(), w, task.SourceKey, "source symlink refused", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "failed", ErrorCode: output.ErrCodeInvalidInput, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						rec := task.reflowRecord(dstURI, dstKey, "failed")
						rec.Reason = "source.symlink.refused"
						_ = w.WriteAny(ctx, reflowRecordType, rec)
						continue
					}
				}

				_ = w.WriteAny(ctx, reflowRecordType, task.reflowRecord(dstURI, dstKey, "in_progress"))

				srcETag := task.SourceETag
				srcSize := task.SourceSize
				var sourceMeta *provider.ObjectMeta
				needsSourceHeadForCollision := collCfg.Mode == reflowCollisionSrcNew && task.SourceLastMod.IsZero()
				if metaCfg.needsSourceHead() || srcETag == "" || srcSize == 0 || needsSourceHeadForCollision {
					meta, err := src.Head(ctx, task.SourceKey)
					if err == nil {
						sourceMeta = meta
						srcETag = meta.ETag
						srcSize = meta.Size
						if !meta.LastModified.IsZero() {
							task.SourceLastMod = meta.LastModified
						}
					} else if metaCfg.needsSourceHead() || needsSourceHeadForCollision {
						if recordFatalReflowError(err) {
							continue
						}
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "source metadata read failed", err, map[string]any{"source_uri": srcAuditURI})
						continue
					}
				}
				if collCfg.Mode == reflowCollisionSrcNew && task.SourceLastMod.IsZero() {
					errorCount.Add(1)
					err := fmt.Errorf("source LastModified is required for --on-collision=%s", reflowCollisionSrcNew)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "source metadata unavailable", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
					continue
				}
				putOptions, err := metaCfg.putOptions(sourceMeta)
				if err == nil && destSpec.Provider == string(provider.ProviderS3) {
					err = validateMetadataBudget(putOptions.UserMetadata)
				}
				if err != nil {
					details := map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI}
					var budgetErr *metadataBudgetError
					if errors.As(err, &budgetErr) {
						for key, value := range budgetErr.details() {
							details[key] = value
						}
					}
					var derivErr *metadataDerivationError
					if errors.As(err, &derivErr) {
						for key, value := range derivErr.details() {
							details[key] = value
						}
					}
					_ = emitReflowError(context.Background(), w, task.SourceKey, "destination metadata options failed", err, details)
					if derivErr != nil && collCfg.Mode == reflowCollisionQuar {
						quarantineDestRel := buildQuarantineDestRel(collCfg.QuarantinePrefix, task.SourceKey)
						quarantineDstKey := buildReflowDestKey(destSpec, quarantineDestRel)
						quarantineDstURI := buildReflowDestURI(destSpec, quarantineDstKey)
						bytes, qerr := transfer.CopyObjectWithOptions(ctx, src, dst, task.SourceKey, quarantineDstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes, provider.PutOptions{})
						if qerr != nil {
							if recordFatalReflowError(qerr) {
								continue
							}
							errorCount.Add(1)
							code := reflowErrCode(qerr)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "metadata quarantine copy failed", qerr, map[string]any{"source_uri": srcAuditURI, "dest_uri": quarantineDstURI, "original_dest_uri": dstURI})
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: qerr.Error()}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(quarantineDstURI, quarantineDstKey, "failed")
							rec.Reason = "metadata.quarantine_copy_failed"
							rec.RoutingClass = "quarantine"
							_ = w.WriteAny(ctx, reflowRecordType, rec)
							continue
						}
						if werr := state.NoteDestKeySource(context.Background(), quarantineDstKey, srcCheckpointURI, srcETag, srcSize); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						destMeta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: quarantineDstKey, Size: bytes}}
						sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), quarantineDestRel, quarantineDstKey, quarantineDstURI, destMeta, reflowRewriteTo, "quarantined", jobID, nil)
						if sidecarFatal {
							errorCount.Add(1)
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(quarantineDstURI, quarantineDstKey, "failed")
							rec.Reason = "provenance.write_failed"
							rec.Bytes = bytes
							rec.RoutingClass = "quarantine"
							rec.Provenance = sidecarRef
							_ = w.WriteAny(ctx, reflowRecordType, rec)
							continue
						}
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "quarantined", Reason: "metadata.derivation.quarantined", Bytes: bytes}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(quarantineDstURI, quarantineDstKey, "quarantined")
						rec.Reason = "metadata.derivation.quarantined"
						rec.Bytes = bytes
						rec.RoutingClass = "quarantine"
						rec.Provenance = sidecarRef
						_ = w.WriteAny(ctx, reflowRecordType, rec)
						continue
					}
					errorCount.Add(1)
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInvalidInput, ErrorMessage: err.Error()}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					continue
				}

				var collision *collisionInfo
				var bytes int64
				var putResult provider.PutResult
				if collCfg.Mode == reflowCollisionOver {
					dstMeta, headErr := dst.Head(ctx, dstKey)
					if headErr == nil {
						kind := collisionConflict
						if isDuplicateCollision(srcETag, srcSize, dstMeta) {
							kind = collisionDuplicate
						}
						collision = newCollisionInfo(kind, dstMeta, decisionOverwrite)
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionOverwrite, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
					} else if !provider.IsNotFound(headErr) {
						if recordFatalReflowError(headErr) {
							continue
						}
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed", headErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
						continue
					}
					bytes, err = transfer.CopyObjectWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes, putOptions)
				} else {
					gate, releaseGate := destArbiter.acquire(dstKey)
					// Keep active mutexes bounded to in-flight keys; durable per-run
					// destination observations live in the checkpoint DB.
					if gate.observed {
						err = &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderType(destSpec.Provider), Key: dstKey, Err: provider.ErrAlreadyExists}
					} else {
						observed, observedErr := state.DestKeyObserved(ctx, dstKey)
						if observedErr != nil {
							releaseGate()
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination arbitration state lookup failed", observedErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
							continue
						}
						if observed {
							gate.observed = true
							err = &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderType(destSpec.Provider), Key: dstKey, Err: provider.ErrAlreadyExists}
						} else {
							bytes, putResult, err = transfer.CopyObjectConditionalWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes, provider.PutPrecondition{IfAbsent: true}, putOptions)
							if err == nil || isConditionalExists(err) {
								gate.observed = true
								if markErr := state.MarkDestKeyObserved(ctx, dstKey); markErr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(markErr))
									_ = w.WriteAny(ctx, reflowWarningRecord, reflowWarning{
										Code:    "REFLOW_ARBITRATION_STATE_WRITE_FAILED",
										Message: fmt.Sprintf("destination arbitration state write failed: %s", markErr.Error()),
										Key:     dstKey,
										Details: map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI},
									})
								}
							}
						}
					}
					releaseGate()
					if err != nil && isConditionalExists(err) {
						dstMeta, headErr := dst.Head(ctx, dstKey)
						if headErr != nil {
							if recordFatalReflowError(headErr) {
								continue
							}
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed after collision", headErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
							continue
						}
						dup, dupErr := isDuplicateCollisionForReflow(ctx, src, dst, task.SourceKey, dstKey, destSpec.Provider, srcETag, srcSize, dstMeta)
						if dupErr != nil {
							if recordFatalReflowError(dupErr) {
								continue
							}
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination duplicate comparison failed", dupErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
							continue
						}
						if dup {
							collision = newCollisionInfo(collisionDuplicate, dstMeta, decisionIfAbsentHead)
							if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionDuplicate, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							if collCfg.Mode == reflowCollisionSkip || collCfg.Mode == reflowCollisionQuar || collCfg.Mode == reflowCollisionSrcNew {
								sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), destRel, dstKey, dstURI, dstMeta, reflowRewriteTo, "skipped.duplicate", jobID, collision)
								if sidecarFatal {
									errorCount.Add(1)
									if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
										observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
									}
									rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed")
									rec.Reason = "provenance.write_failed"
									rec.Provenance = sidecarRef
									_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
									continue
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.duplicate"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "skipped")
								rec.Reason = "collision.duplicate"
								rec.Provenance = sidecarRef
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
								continue
							}

							err := fmt.Errorf("destination key exists with identical content: %s", dstKey)
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "collision duplicate", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI, "collision": collision})
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeAlreadyExists, ErrorMessage: err.Error()}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed")
							rec.Reason = "collision.exists.duplicate"
							_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
							continue
						}

						if collCfg.Mode == reflowCollisionSrcNew {
							if dstMeta.LastModified.IsZero() {
								errorCount.Add(1)
								err := fmt.Errorf("destination LastModified is required for --on-collision=%s: %s", reflowCollisionSrcNew, dstKey)
								_ = emitReflowError(context.Background(), w, task.SourceKey, "destination metadata unavailable", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInvalidInput, ErrorMessage: err.Error()}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed")
								rec.Reason = "collision.missing_dest_last_modified"
								_ = w.WriteAny(ctx, reflowRecordType, rec)
								continue
							}

							decisionReason := reasonSrcOlder
							shouldOverwrite := false
							if task.SourceLastMod.After(dstMeta.LastModified) {
								decisionReason = reasonSrcNewer
								shouldOverwrite = true
							} else if task.SourceLastMod.Equal(dstMeta.LastModified) && srcSize != dstMeta.Size {
								decisionReason = reasonEqualSizeDiffers
								shouldOverwrite = true
							}
							if !shouldOverwrite {
								collision = newSourceNewerCollisionInfo(collisionSrcOlder, dstMeta, task.SourceLastMod, decisionReason)
								if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.skipped_src_older"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "skipped")
								rec.Reason = "collision.skipped_src_older"
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
								continue
							}

							collision = newSourceNewerCollisionInfo(collisionOverwritten, dstMeta, task.SourceLastMod, decisionReason)
							bytes, putResult, err = transfer.CopyObjectConditionalWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes, provider.PutPrecondition{IfMatchETag: &dstMeta.ETag}, putOptions)
							if err != nil && isConditionalExists(err) {
								collision = newSourceNewerCollisionInfo(collisionConcurrentMut, dstMeta, task.SourceLastMod, reasonConcurrentMut)
								if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.skipped_concurrent_mutation"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "skipped")
								rec.Reason = "collision.skipped_concurrent_mutation"
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
								continue
							}
							if err == nil {
								if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionOverwrite, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
							}
						} else if collCfg.Mode == reflowCollisionQuar {
							collision = newCollisionInfo(collisionQuarantined, dstMeta, decisionQuarantine)
							if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							quarantineDestRel := buildQuarantineDestRel(collCfg.QuarantinePrefix, task.SourceKey)
							quarantineDstKey := buildReflowDestKey(destSpec, quarantineDestRel)
							quarantineDstURI := buildReflowDestURI(destSpec, quarantineDstKey)
							bytes, err = transfer.CopyObjectWithOptions(ctx, src, dst, task.SourceKey, quarantineDstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes, putOptions)
							if err != nil {
								if recordFatalReflowError(err) {
									continue
								}
								errorCount.Add(1)
								code := reflowErrCode(err)
								_ = emitReflowError(context.Background(), w, task.SourceKey, "quarantine copy failed", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": quarantineDstURI, "original_dest_uri": dstURI, "collision": collision})
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: err.Error()}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(quarantineDstURI, quarantineDstKey, "failed")
								rec.Reason = "collision.quarantine_copy_failed"
								rec.RoutingClass = "quarantine"
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
								continue
							}
							if werr := state.NoteDestKeySource(context.Background(), quarantineDstKey, srcCheckpointURI, srcETag, srcSize); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							destMeta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: quarantineDstKey, Size: bytes}}
							sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), quarantineDestRel, quarantineDstKey, quarantineDstURI, destMeta, reflowRewriteTo, "quarantined", jobID, collision)
							if sidecarFatal {
								errorCount.Add(1)
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(quarantineDstURI, quarantineDstKey, "failed")
								rec.Reason = "provenance.write_failed"
								rec.Bytes = bytes
								rec.RoutingClass = "quarantine"
								rec.Provenance = sidecarRef
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
								continue
							}
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "quarantined", Reason: "collision.conflict.quarantined", Bytes: bytes}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(quarantineDstURI, quarantineDstKey, "quarantined")
							rec.Reason = "collision.conflict.quarantined"
							rec.Bytes = bytes
							rec.RoutingClass = "quarantine"
							rec.Provenance = sidecarRef
							_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
							continue
						} else {
							collision = newCollisionInfo(collisionConflict, dstMeta, decisionIfAbsentHead)
							if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							reason := "collision.conflict"
							if collCfg.Mode == reflowCollisionFail {
								reason = "collision.exists.conflict"
							}
							err := fmt.Errorf("destination key exists with different content: %s", dstKey)
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "collision", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI, "collision": collision})
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeAlreadyExists, ErrorMessage: err.Error()}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed")
							rec.Reason = reason
							_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
							continue
						}
					}
				}
				if err != nil {
					if recordFatalReflowError(err) {
						continue
					}
					errorCount.Add(1)
					code := reflowErrCode(err)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "copy failed", err, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: err.Error()}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					_ = w.WriteAny(ctx, reflowRecordType, task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed"))
					continue
				}
				if srcCfg.PreserveMode && task.SourceProvider == string(provider.ProviderFile) && destSpec.Provider == string(provider.ProviderFile) {
					if chmodErr := os.Chmod(filepath.Join(destSpec.BaseDir, filepath.FromSlash(dstKey)), task.SourceMode.Perm()); chmodErr != nil {
						_ = w.WriteAny(ctx, reflowWarningRecord, reflowWarning{
							Code:    "DESTINATION_MODE_UNREPRESENTABLE",
							Message: fmt.Sprintf("destination mode could not be preserved: %s", chmodErr.Error()),
							Key:     dstKey,
							Details: map[string]any{"reason": "destination.mode.unrepresentable", "source_uri": srcAuditURI, "dest_uri": dstURI},
						})
					}
				}

				if werr := state.NoteDestKeySource(context.Background(), dstKey, srcCheckpointURI, srcETag, srcSize); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				destMeta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: dstKey, Size: bytes, ETag: putResult.ETag}}
				sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), destRel, dstKey, dstURI, destMeta, reflowRewriteTo, reflowActionForTask(task), jobID, collision)
				if sidecarFatal {
					errorCount.Add(1)
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed")
					rec.Reason = "provenance.write_failed"
					rec.Bytes = bytes
					rec.Provenance = sidecarRef
					_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
					continue
				}
				if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "complete", Bytes: bytes}); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "complete")
				rec.Bytes = bytes
				rec.Provenance = sidecarRef
				_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(rec, collision))
			}
		}()
	}

	// Feed tasks from stdin / positional.
	var inputErr error
	srcIdentity := ""
	if reflowStdin {
		s := bufio.NewScanner(cmd.InOrStdin())
		s.Buffer(make([]byte, 64*1024), 1024*1024)
		for s.Scan() {
			line := strings.TrimSpace(s.Text())
			if line == "" {
				continue
			}
			srcIdentity, inputErr = enqueueReflowLine(ctx, line, srcIdentity, srcCfg, getInputProviders, tasks)
			if inputErr != nil {
				if recordFatalReflowError(inputErr) {
					inputErr = nil
					break
				}
				invalidCount.Add(1)
				_ = emitReflowError(context.Background(), w, "", "invalid input", inputErr, map[string]any{"input": line})
				inputErr = nil
				continue
			}
		}
		if err := s.Err(); err != nil {
			inputErr = err
		}
	} else {
		_, inputErr = enqueueReflowLine(ctx, args[0], srcIdentity, srcCfg, getInputProviders, tasks)
		if inputErr != nil && recordFatalReflowError(inputErr) {
			inputErr = nil
		}
	}
	close(tasks)
	wg.Wait()

	fatalRunErr := currentFatalReflowError()
	if fatalRunErr != nil || ctx.Err() != nil {
		classifyErr := fatalRunErr
		if classifyErr == nil {
			classifyErr = ctx.Err()
		}
		classification := classifyTransferReflowRunErrorWithConfig(classifyErr, checkpointCfg)
		checkpointWritten := false
		if classification.Resumable && transferReflowCheckpointEligible(checkpointCfg) {
			progress := transferReflowProgress(invalidCount.Load(), errorCount.Load())
			if heartbeat := resumeLeaseHeartbeatFromContext(ctx); heartbeat != nil {
				if err := stopResumeLeaseHeartbeatBeforeFailedResumableCheckpoint(heartbeat); err != nil {
					return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), err)
				}
			}
			if checkpointErr := writeFailedResumableTransferReflowCheckpoint(context.Background(), state, jobID, checkpointCfg, classification.Class, progress); checkpointErr == nil {
				checkpointWritten = true
				writeOperationErrorSummary(cmd.ErrOrStderr(), "Transfer reflow failed with resumable checkpoint", operationTransferReflow, jobID, classification.Class, progress)
				enc := json.NewEncoder(cmd.OutOrStdout())
				if emitErr := emitOperationErrorRecord(context.Background(), enc, operationTransferReflow, jobID, classification.Class, progress); emitErr != nil {
					return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), fmt.Errorf("%w; write operation error record: %v", classifyErr, emitErr))
				}
			} else {
				return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), fmt.Errorf("%w; write operation checkpoint: %v", classifyErr, checkpointErr))
			}
		}
		return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), classifyErr)
	}
	if inputErr != nil {
		return exitError(foundry.ExitInvalidArgument, "Failed to read input", inputErr)
	}
	if invalidCount.Load() > 0 {
		return exitError(foundry.ExitInvalidArgument, "reflow completed with invalid inputs", fmt.Errorf("invalid_inputs=%d", invalidCount.Load()))
	}
	if errorCount.Load() > 0 {
		return exitError(foundry.ExitExternalServiceUnavailable, "reflow completed with errors", fmt.Errorf("errors=%d", errorCount.Load()))
	}
	return nil
}

func resolveReflowCheckpointPath(jobID string) (string, error) {
	root, err := indexRootDir()
	if err != nil {
		return "", err
	}
	// Keep reflow artifacts near index artifacts for consistent ops tooling.
	return filepath.Join(root, "reflow", "runs", jobID, "state.db"), nil
}

func enqueueReflowLine(ctx context.Context, line string, srcIdentity string, srcCfg reflowSourceConfig, getProviders func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error), out chan<- reflowTask) (string, error) {
	// JSONL: index object record.
	if strings.HasPrefix(line, "{") {
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return srcIdentity, err
		}
		switch env.Type {
		case "gonimbus.index.object.v1":
			var data struct {
				BaseURI      string    `json:"base_uri"`
				Key          string    `json:"key"`
				ETag         string    `json:"etag"`
				SizeBytes    int64     `json:"size_bytes"`
				LastModified time.Time `json:"last_modified"`
				RelKey       string    `json:"rel_key"`
				DeletedAt    *string   `json:"deleted_at"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcIdentity, err
			}
			if data.DeletedAt != nil {
				return srcIdentity, fmt.Errorf("deleted objects are not supported in reflow input")
			}
			base, err := uri.ParseURI(data.BaseURI)
			if err != nil {
				return srcIdentity, fmt.Errorf("invalid base_uri: %w", err)
			}
			if base.Provider != string(provider.ProviderS3) && base.Provider != string(provider.ProviderFile) {
				return srcIdentity, fmt.Errorf("unsupported provider %q", base.Provider)
			}
			identity := reflowSourceIdentity(base)
			if srcIdentity == "" {
				srcIdentity = identity
			} else if srcIdentity != identity {
				return srcIdentity, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcIdentity)
			}
			_, _, err = getProviders(base)
			if err != nil {
				return srcIdentity, err
			}
			key := strings.TrimPrefix(data.Key, "/")
			if key == "" {
				key = strings.TrimPrefix(data.RelKey, "/")
			}
			if key == "" {
				return srcIdentity, fmt.Errorf("missing key in index record")
			}
			srcURI := fmt.Sprintf("%s://%s/%s", base.Provider, base.Bucket, key)
			sourceBucket := base.Bucket
			sourceRoot := ""
			sourceCheckpoint := srcURI
			if base.Provider == string(provider.ProviderFile) {
				sourceBucket = reflowSourceBucketFile
				sourceRoot = base.Key
				sourceCheckpoint = fileCheckpointSourceURI(key)
				srcURI = fileURI(filepath.Join(sourceRoot, filepath.FromSlash(key)))
			}
			select {
			case out <- reflowTask{SourceProvider: base.Provider, SourceBucket: sourceBucket, SourceRoot: sourceRoot, SourceURI: srcURI, SourceCheckpoint: sourceCheckpoint, SourceKey: key, SourceETag: data.ETag, SourceSize: data.SizeBytes, SourceLastMod: data.LastModified, RejectSymlinkPath: base.Provider == string(provider.ProviderFile)}:
				return srcIdentity, nil
			case <-ctx.Done():
				return srcIdentity, ctx.Err()
			}
		case "gonimbus.reflow.input.v1":
			var data struct {
				SourceURI        string            `json:"source_uri"`
				SourceKey        string            `json:"source_key"`
				SourceETag       string            `json:"source_etag"`
				SourceSize       int64             `json:"source_size_bytes"`
				SourceLastMod    time.Time         `json:"source_last_modified"`
				Vars             map[string]string `json:"vars"`
				Probe            *probe.ProbeAudit `json:"probe"`
				DestRelKey       string            `json:"dest_rel_key"`
				RoutingClass     string            `json:"routing_class"`
				QuarantinePrefix string            `json:"quarantine_prefix"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcIdentity, err
			}
			if strings.TrimSpace(data.SourceURI) == "" {
				return srcIdentity, fmt.Errorf("missing data.source_uri")
			}
			u, err := uri.ParseURI(data.SourceURI)
			if err != nil {
				return srcIdentity, err
			}
			if u.Provider != string(provider.ProviderS3) && u.Provider != string(provider.ProviderFile) {
				return srcIdentity, fmt.Errorf("unsupported provider %q", u.Provider)
			}
			if u.IsPrefix() || u.IsPattern() {
				return srcIdentity, fmt.Errorf("reflow input source_uri must be an exact object URI")
			}
			sourceProviderURI := u
			key := u.Key
			srcURI := fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key)
			sourceBucket := u.Bucket
			sourceRoot := ""
			sourceCheckpoint := srcURI
			if u.Provider == string(provider.ProviderFile) {
				sourceBucket = reflowSourceBucketFile
				sourceRoot, key, err = fileReflowInputRootAndKey(u.Key, data.SourceKey)
				if err != nil {
					return srcIdentity, err
				}
				sourceCheckpoint = fileCheckpointSourceURI(key)
				srcURI = fileURI(filepath.Join(sourceRoot, filepath.FromSlash(key)))
				sourceProviderURI = &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: reflowSourceBucketFile, Key: sourceRoot}
			} else if strings.TrimSpace(data.SourceKey) != "" {
				key = strings.TrimPrefix(strings.TrimSpace(data.SourceKey), "/")
				srcURI = fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key)
				sourceCheckpoint = srcURI
			}
			identity := reflowSourceIdentity(sourceProviderURI)
			if srcIdentity == "" {
				srcIdentity = identity
			} else if srcIdentity != identity {
				return srcIdentity, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcIdentity)
			}
			_, _, err = getProviders(sourceProviderURI)
			if err != nil {
				return srcIdentity, err
			}
			destRel := strings.Trim(strings.TrimSpace(data.DestRelKey), "/")
			routingClass := strings.TrimSpace(data.RoutingClass)
			if routingClass == "" {
				routingClass = "normal"
			}
			switch routingClass {
			case "normal", "quarantine":
				// ok
			default:
				return srcIdentity, fmt.Errorf("unsupported routing_class %q", data.RoutingClass)
			}
			quarantinePrefix := strings.Trim(strings.TrimSpace(data.QuarantinePrefix), "/")
			if routingClass == "quarantine" && quarantinePrefix == "" {
				return srcIdentity, fmt.Errorf("quarantine_prefix is required when routing_class=quarantine")
			}
			if routingClass == "quarantine" && !isRelativeQuarantinePrefix(data.QuarantinePrefix) {
				return srcIdentity, fmt.Errorf("quarantine_prefix must be a relative destination prefix")
			}
			select {
			case out <- reflowTask{SourceProvider: u.Provider, SourceBucket: sourceBucket, SourceRoot: sourceRoot, SourceURI: srcURI, SourceCheckpoint: sourceCheckpoint, SourceKey: key, SourceETag: data.SourceETag, SourceSize: data.SourceSize, SourceLastMod: data.SourceLastMod, Vars: data.Vars, Probe: data.Probe, DestRelKey: destRel, RoutingClass: routingClass, QuarantinePrefix: quarantinePrefix, RejectSymlinkPath: u.Provider == string(provider.ProviderFile)}:
				return srcIdentity, nil
			case <-ctx.Done():
				return srcIdentity, ctx.Err()
			}
		default:
			return srcIdentity, fmt.Errorf("unsupported json record type %q", env.Type)
		}
	}

	parsed, err := uri.ParseURI(line)
	if err != nil {
		return srcIdentity, err
	}
	if parsed.Provider != string(provider.ProviderS3) && parsed.Provider != string(provider.ProviderFile) {
		return srcIdentity, fmt.Errorf("unsupported provider %q", parsed.Provider)
	}
	identity := reflowSourceIdentity(parsed)
	if srcIdentity == "" {
		srcIdentity = identity
	} else if srcIdentity != identity {
		return srcIdentity, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcIdentity)
	}
	prov, _, err := getProviders(parsed)
	if err != nil {
		return srcIdentity, err
	}

	if parsed.Provider == string(provider.ProviderFile) {
		return enqueueFileReflowSource(ctx, parsed, srcCfg, srcIdentity, out)
	}

	if !parsed.IsPrefix() && !parsed.IsPattern() {
		select {
		case out <- reflowTask{SourceProvider: parsed.Provider, SourceBucket: parsed.Bucket, SourceURI: parsed.String(), SourceKey: parsed.Key}:
			return srcIdentity, nil
		case <-ctx.Done():
			return srcIdentity, ctx.Err()
		}
	}

	var m *match.Matcher
	if parsed.IsPattern() {
		matcher, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			return srcIdentity, err
		}
		m = matcher
	}

	var token string
	for {
		res, err := prov.List(ctx, provider.ListOptions{Prefix: parsed.Key, ContinuationToken: token})
		if err != nil {
			return srcIdentity, err
		}
		for _, obj := range res.Objects {
			if m != nil && !m.Match(obj.Key) {
				continue
			}
			uri := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, obj.Key)
			select {
			case out <- reflowTask{SourceProvider: parsed.Provider, SourceBucket: parsed.Bucket, SourceURI: uri, SourceKey: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, SourceLastMod: obj.LastModified}:
				// ok
			case <-ctx.Done():
				return srcIdentity, ctx.Err()
			}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}
	return srcIdentity, nil
}

func enqueueFileReflowSource(ctx context.Context, parsed *uri.ObjectURI, srcCfg reflowSourceConfig, srcIdentity string, out chan<- reflowTask) (string, error) {
	st, err := os.Stat(parsed.Key)
	if err != nil {
		return srcIdentity, redactedPathError("source root is not accessible", err.Error())
	}
	if !st.IsDir() {
		return srcIdentity, redactedPathError("file source root must be a directory", fmt.Sprintf("file source root must be a directory: %s", parsed.Key))
	}

	root := filepath.Clean(parsed.Key)
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		canonicalRoot = root
	}
	ancestors := map[string]bool{filepath.Clean(canonicalRoot): true}
	err = walkFileReflowDir(ctx, root, root, canonicalRoot, ancestors, srcCfg, out)
	if err != nil {
		return srcIdentity, err
	}
	return srcIdentity, nil
}

func walkFileReflowDir(ctx context.Context, root string, dir string, canonicalRoot string, ancestors map[string]bool, srcCfg reflowSourceConfig, out chan<- reflowTask) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsPermission(err) {
			return enqueueFileSourceFailure(ctx, out, root, dir, "source.read.permission_denied", srcCfg)
		}
		return enqueueFileSourceFailure(ctx, out, root, dir, "source.read.io_error", srcCfg)
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		path := filepath.Join(dir, entry.Name())
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if fileSourceSkipped(key, srcCfg) {
			continue
		}

		if entry.Type()&os.ModeSymlink != 0 {
			if srcCfg.Symlinks == reflowSymlinkSkip || srcCfg.Symlinks == "" {
				if err := enqueueFileSourceFailure(ctx, out, root, path, "source.symlink.skipped", srcCfg); err != nil {
					return err
				}
				continue
			}
			resolved, err := filepath.EvalSymlinks(path)
			if err != nil {
				if err := enqueueFileSourceFailure(ctx, out, root, path, "source.read.io_error", srcCfg); err != nil {
					return err
				}
				continue
			}
			resolved = filepath.Clean(resolved)
			if !pathWithinRoot(canonicalRoot, resolved) {
				if err := enqueueFileSourceFailure(ctx, out, root, path, "source.symlink.escapes_root", srcCfg); err != nil {
					return err
				}
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				if os.IsPermission(err) {
					return enqueueFileSourceFailure(ctx, out, root, path, "source.read.permission_denied", srcCfg)
				}
				return enqueueFileSourceFailure(ctx, out, root, path, "source.read.io_error", srcCfg)
			}
			if info.IsDir() {
				if ancestors[resolved] {
					if err := enqueueFileSourceFailure(ctx, out, root, path, "source.symlink.cycle", srcCfg); err != nil {
						return err
					}
					continue
				}
				nextAncestors := cloneAncestorSet(ancestors)
				nextAncestors[resolved] = true
				if err := walkFileReflowDir(ctx, root, path, canonicalRoot, nextAncestors, srcCfg, out); err != nil {
					return err
				}
				continue
			}
			if err := enqueueFileReflowTask(ctx, out, root, key, info); err != nil {
				return err
			}
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if os.IsPermission(err) {
				return enqueueFileSourceFailure(ctx, out, root, path, "source.read.permission_denied", srcCfg)
			}
			return enqueueFileSourceFailure(ctx, out, root, path, "source.read.io_error", srcCfg)
		}
		if info.IsDir() {
			resolved := path
			if eval, err := filepath.EvalSymlinks(path); err == nil {
				resolved = eval
			}
			nextAncestors := cloneAncestorSet(ancestors)
			nextAncestors[filepath.Clean(resolved)] = true
			if err := walkFileReflowDir(ctx, root, path, canonicalRoot, nextAncestors, srcCfg, out); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			if err := enqueueFileSourceFailure(ctx, out, root, path, "source.unsupported_type", srcCfg); err != nil {
				return err
			}
			continue
		}
		if err := enqueueFileReflowTask(ctx, out, root, key, info); err != nil {
			return err
		}
	}
	return nil
}

func enqueueFileReflowTask(ctx context.Context, out chan<- reflowTask, root string, key string, info fs.FileInfo) error {
	select {
	case out <- reflowTask{
		SourceProvider:   string(provider.ProviderFile),
		SourceBucket:     reflowSourceBucketFile,
		SourceRoot:       root,
		SourceURI:        fileURI(filepath.Join(root, filepath.FromSlash(key))),
		SourceCheckpoint: fileCheckpointSourceURI(key),
		SourceKey:        key,
		SourceSize:       info.Size(),
		SourceLastMod:    info.ModTime(),
		SourceMode:       info.Mode(),
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func enqueueFileSourceFailure(ctx context.Context, out chan<- reflowTask, root string, path string, reason string, srcCfg reflowSourceConfig) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = filepath.Base(path)
	}
	key := filepath.ToSlash(rel)
	select {
	case out <- reflowTask{
		SourceProvider:   string(provider.ProviderFile),
		SourceBucket:     reflowSourceBucketFile,
		SourceRoot:       root,
		SourceURI:        fileURI(filepath.Join(root, filepath.FromSlash(key))),
		SourceCheckpoint: fileCheckpointSourceURI(key),
		SourceKey:        key,
		SourceFailure:    reason,
	}:
		if srcCfg.OnSourceFailure == reflowSourceFailFail {
			return errors.New(reason)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fileSourceExcluded(rel string, patterns []string) bool {
	rel = filepath.ToSlash(strings.TrimPrefix(rel, "/"))
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		if strings.HasSuffix(pattern, "/*") && strings.TrimSuffix(pattern, "/*") == rel {
			return true
		}
		if ok, _ := filepath.Match(pattern, rel); ok {
			return true
		}
	}
	return false
}

func fileSourceSkipped(rel string, cfg reflowSourceConfig) bool {
	rel = filepath.ToSlash(strings.TrimPrefix(rel, "/"))
	if cfg.Hidden == "" || cfg.Hidden == reflowHiddenSkip {
		for _, segment := range strings.Split(rel, "/") {
			if strings.HasPrefix(segment, ".") && segment != "." && segment != ".." {
				return true
			}
		}
	}
	return fileSourceExcluded(rel, cfg.Excludes)
}

func cloneAncestorSet(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func pathWithinRoot(root string, path string) bool {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func buildQuarantineDestRel(prefix string, sourceKey string) string {
	prefix = strings.Trim(prefix, "/")
	sourceKey = strings.Trim(sourceKey, "/")
	if prefix == "" {
		return sourceKey
	}
	if sourceKey == "" {
		return prefix
	}
	return prefix + "/" + sourceKey
}

func isRelativeQuarantinePrefix(prefix string) bool {
	prefix = strings.TrimSpace(prefix)
	if strings.HasPrefix(prefix, "/") {
		return false
	}
	u, err := url.Parse(prefix)
	return err != nil || u.Scheme == ""
}

func emitReflowError(ctx context.Context, w output.Writer, key, msg string, err error, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	var collision any
	if v, ok := details["collision"]; ok {
		collision = v
		delete(details, "collision")
	}
	details["mode"] = "transfer_reflow"
	code := reflowErrCode(err)
	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Key: key, Details: details, Collision: collision}); werr != nil {
		observability.CLILogger.Debug("Failed to emit reflow error record", zap.Error(werr))
	}
	return nil
}

func reflowErrCode(err error) string {
	var budgetErr *metadataBudgetError
	switch {
	case errors.As(err, &budgetErr):
		return output.ErrCodeInvalidInput
	case provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeProviderUnavailable
	default:
		return output.ErrCodeInternal
	}
}
