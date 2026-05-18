package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
	reflowRecordType       = "gonimbus.reflow.v1"
	reflowRunRecordType    = "gonimbus.reflow.run.v1"
	reflowWarningRecord    = "gonimbus.warning.v1"
	provenanceSchema       = "gonimbus.provenance.v1"
	provenanceSchemaVer    = "1.0.0"
	provenanceModeNone     = "none"
	provenanceModeSidecar  = "sidecar"
	provenancePlaceSibling = "sibling"
	provenancePlaceMirror  = "mirrored-root"
	provenanceErrorWarn    = "warn"
	provenanceErrorFail    = "fail"
	provenanceSuffix       = ".gnb.json"
	reflowCollisionSkip    = "skip-if-duplicate"
	reflowCollisionLog     = "log"
	reflowCollisionFail    = "fail"
	reflowCollisionOver    = "overwrite"
	reflowCollisionQuar    = "quarantine"
	collisionDuplicate     = "duplicate"
	collisionConflict      = "conflict"
	collisionQuarantined   = "conflict_quarantined"
	decisionIfAbsentHead   = "ifabsent_then_head"
	decisionOverwrite      = "unconditional_overwrite"
	decisionQuarantine     = "quarantine_routed"
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
	reflowCheckpoint  string
	reflowOverwrite   bool
	reflowOnCollision string
	reflowCollQuar    string
	reflowProvenance  string
	reflowProvRoot    string
	reflowProvSuffix  string
	reflowProvOnError string
	reflowProvUnsafe  bool

	reflowSrcRegion   string
	reflowSrcProfile  string
	reflowSrcEndpoint string
	reflowDstRegion   string
	reflowDstProfile  string
	reflowDstEndpoint string
)

var (
	newReflowS3Provider = func(ctx context.Context, cfg s3.Config) (provider.Provider, error) {
		return s3.New(ctx, cfg)
	}
	newReflowFileProvider = func(cfg providerfile.Config) (provider.Provider, error) {
		return providerfile.New(cfg)
	}
)

func init() {
	transferCmd.AddCommand(transferReflowCmd)

	transferReflowCmd.Flags().BoolVar(&reflowStdin, "stdin", false, "Read selection from stdin")
	transferReflowCmd.Flags().StringVar(&reflowDest, "dest", "", "Destination base URI (prefix), e.g. s3://bucket/base/ or file:///tmp/out/")
	transferReflowCmd.Flags().StringVar(&reflowRewriteFrom, "rewrite-from", "", "Rewrite source template (segment captures)")
	transferReflowCmd.Flags().StringVar(&reflowRewriteTo, "rewrite-to", "", "Rewrite destination template (segment renders)")
	transferReflowCmd.Flags().IntVar(&reflowParallel, "parallel", 16, "Concurrent copy workers")
	transferReflowCmd.Flags().BoolVar(&reflowDryRun, "dry-run", false, "Emit planned mappings without writing")
	transferReflowCmd.Flags().BoolVar(&reflowResume, "resume", false, "Resume from checkpoint (requires --checkpoint)")
	transferReflowCmd.Flags().StringVar(&reflowCheckpoint, "checkpoint", "", "Checkpoint DB path (sqlite)")
	transferReflowCmd.Flags().BoolVar(&reflowOverwrite, "overwrite", false, "Allow overwriting destination objects")
	transferReflowCmd.Flags().StringVar(&reflowOnCollision, "on-collision", reflowCollisionSkip, "Collision policy: skip-if-duplicate|fail|overwrite|quarantine (log is a deprecated alias)")
	transferReflowCmd.Flags().StringVar(&reflowCollQuar, "collision-quarantine-prefix", "", "Relative destination prefix for --on-collision=quarantine")
	transferReflowCmd.Flags().StringVar(&reflowProvenance, "provenance", provenanceModeNone, "Provenance mode: none|sidecar")
	transferReflowCmd.Flags().StringVar(&reflowProvRoot, "provenance-sidecar-root", "", "Sidecar root URI for mirrored-root provenance placement (must end in /)")
	transferReflowCmd.Flags().StringVar(&reflowProvSuffix, "provenance-suffix", provenanceSuffix, "Sidecar key suffix (default .gnb.json)")
	transferReflowCmd.Flags().StringVar(&reflowProvOnError, "provenance-on-write-error", provenanceErrorWarn, "Sidecar write failure policy: warn|fail")
	transferReflowCmd.Flags().BoolVar(&reflowProvUnsafe, "allow-unsafe-suffix", false, "Allow a provenance suffix that collides with common data extensions")

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

	_ = transferReflowCmd.MarkFlagRequired("dest")
	_ = transferReflowCmd.MarkFlagRequired("rewrite-from")
	_ = transferReflowCmd.MarkFlagRequired("rewrite-to")
}

func validateTransferReflowArgs(cmd *cobra.Command, args []string) error {
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
	SourceBucket     string
	SourceURI        string
	SourceKey        string
	SourceETag       string
	SourceSize       int64
	SourceLastMod    time.Time
	Vars             map[string]string
	Probe            *probe.ProbeAudit
	DestRelKey       string
	RoutingClass     string
	QuarantinePrefix string
}

type reflowRecord struct {
	SourceURI     string         `json:"source_uri"`
	SourceKey     string         `json:"source_key"`
	SourceETag    string         `json:"source_etag,omitempty"`
	SourceSize    int64          `json:"source_size_bytes,omitempty"`
	DestURI       string         `json:"dest_uri"`
	DestKey       string         `json:"dest_key"`
	Bytes         int64          `json:"bytes,omitempty"`
	Status        string         `json:"status"`
	Reason        string         `json:"reason,omitempty"`
	RoutingClass  string         `json:"routing_class,omitempty"`
	CollisionKind string         `json:"collision_kind,omitempty"`
	CollisionETag string         `json:"collision_etag,omitempty"`
	CollisionSize *int64         `json:"collision_size_bytes,omitempty"`
	Collision     *collisionInfo `json:"collision,omitempty"`
	Provenance    *provenanceRef `json:"provenance,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

type reflowRunRecord struct {
	DestURI        string               `json:"dest_uri"`
	CheckpointPath string               `json:"checkpoint_path"`
	DryRun         bool                 `json:"dry_run"`
	Resume         bool                 `json:"resume"`
	Parallel       int                  `json:"parallel"`
	Provenance     *provenanceRunConfig `json:"provenance,omitempty"`
}

type collisionConfig struct {
	Mode             string
	QuarantinePrefix string
	DeprecatedLog    bool
}

type collisionInfo struct {
	Kind             string `json:"kind"`
	DestETagObserved string `json:"dest_etag_observed,omitempty"`
	DestSizeObserved *int64 `json:"dest_size_observed,omitempty"`
	DecisionPath     string `json:"decision_path"`
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

func recordWithCollision(rec reflowRecord, collision *collisionInfo) reflowRecord {
	if collision == nil {
		return rec
	}
	rec.Collision = collision
	rec.CollisionKind = collision.Kind
	rec.CollisionETag = collision.DestETagObserved
	rec.CollisionSize = collision.DestSizeObserved
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

	if strings.HasPrefix(strings.ToLower(raw), "file://") {
		path := strings.TrimPrefix(raw, "file://")
		path = strings.TrimSpace(path)
		if path == "" {
			return nil, fmt.Errorf("file destination path is empty")
		}
		baseDir := filepath.Clean(path)
		baseURI := fileURI(baseDir)
		if !strings.HasSuffix(baseURI, "/") {
			baseURI += "/"
		}
		return &reflowDestSpec{Provider: string(provider.ProviderFile), BaseURI: baseURI, BaseDir: baseDir}, nil
	}

	parsed, err := uri.ParseURI(raw)
	if err != nil {
		return nil, err
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
	case reflowCollisionSkip, reflowCollisionFail, reflowCollisionOver, reflowCollisionQuar:
		// ok
	default:
		return fmt.Errorf("on-collision must be one of: skip-if-duplicate, fail, overwrite, quarantine")
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

func emitCollisionFlatFieldDeprecationBanner() {
	if observability.CLILogger == nil {
		return
	}
	observability.CLILogger.Warn("Reflow collision flat fields are deprecated; use data.collision",
		zap.String("phase", "phase_a"),
		zap.Strings("deprecated_fields", []string{"collision_kind", "collision_etag", "collision_size_bytes"}),
		zap.String("replacement", "collision"),
	)
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
			URI:          task.SourceURI,
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

func newDestProvider(ctx context.Context, dest *reflowDestSpec) (provider.Provider, error) {
	if dest == nil {
		return nil, fmt.Errorf("destination is nil")
	}
	switch dest.Provider {
	case string(provider.ProviderS3):
		return newReflowS3Provider(ctx, s3.Config{
			Bucket:         dest.Bucket,
			Region:         dest.Region,
			Endpoint:       dest.Endpoint,
			Profile:        dest.Profile,
			ForcePathStyle: dest.ForcePathStyle,
		})
	case string(provider.ProviderFile):
		if err := os.MkdirAll(dest.BaseDir, 0o750); err != nil {
			return nil, err
		}
		return newReflowFileProvider(providerfile.Config{BaseDir: dest.BaseDir})
	default:
		return nil, fmt.Errorf("unsupported destination provider %q", dest.Provider)
	}
}

func runTransferReflow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if reflowParallel < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --parallel value", fmt.Errorf("parallel must be >= 1"))
	}
	if reflowResume && strings.TrimSpace(reflowCheckpoint) == "" {
		return exitError(foundry.ExitInvalidArgument, "Invalid --resume usage", fmt.Errorf("--resume requires --checkpoint"))
	}
	collCfg, err := resolveCollisionConfig(cmd)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid collision configuration", err)
	}
	if collCfg.DeprecatedLog {
		_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "warning: --on-collision=log is deprecated; use --on-collision=skip-if-duplicate")
	}
	emitCollisionFlatFieldDeprecationBanner()
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

	provCfg, err := resolveProvenanceConfig(cmd, destSpec)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid provenance configuration", err)
	}
	emitProvenancePlacementWarnings(cmd.ErrOrStderr(), destSpec, provCfg)

	rewrite, err := transfer.CompileReflowRewrite(reflowRewriteFrom, reflowRewriteTo)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid rewrite templates", err)
	}

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, destSpec.Provider)
	defer func() { _ = w.Close() }()

	checkpointPath, err := resolveReflowCheckpointPath(jobID)
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to resolve checkpoint path", err)
	}
	if strings.TrimSpace(reflowCheckpoint) != "" {
		checkpointPath = reflowCheckpoint
	}

	state, err := reflowstate.Open(ctx, reflowstate.Config{Path: checkpointPath})
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to open checkpoint", err)
	}
	defer func() { _ = state.Close() }()

	_ = w.WriteAny(ctx, reflowRunRecordType, reflowRunRecord{
		DestURI:        destURI,
		CheckpointPath: checkpointPath,
		DryRun:         reflowDryRun,
		Resume:         reflowResume,
		Parallel:       reflowParallel,
		Provenance:     provCfg.runConfig(),
	})

	// Providers are created after we discover the source bucket.
	var (
		srcProv     provider.Provider
		dstProv     provider.Provider
		sidecarProv provider.Provider
		srcBucket   string
		provMu      sync.Mutex
	)
	getProviders := func(bucket string) (provider.Provider, provider.Provider, provider.Provider, error) {
		provMu.Lock()
		defer provMu.Unlock()

		if dstProv == nil {
			pNew, err := newDestProvider(ctx, destSpec)
			if err != nil {
				return nil, nil, nil, err
			}
			dstProv = pNew
		}
		if sidecarProv == nil {
			if provCfg.PlacementMode == provenancePlaceMirror && provCfg.SidecarRoot != nil && provCfg.SidecarRoot.Provider == string(provider.ProviderFile) && provCfg.SidecarRoot.BaseDir != destSpec.BaseDir {
				pNew, err := newDestProvider(ctx, provCfg.SidecarRoot)
				if err != nil {
					return nil, nil, nil, err
				}
				sidecarProv = pNew
			} else {
				sidecarProv = dstProv
			}
		}
		if srcProv == nil {
			pNew, err := newReflowS3Provider(ctx, s3.Config{
				Bucket:         bucket,
				Region:         reflowSrcRegion,
				Endpoint:       reflowSrcEndpoint,
				Profile:        reflowSrcProfile,
				ForcePathStyle: reflowSrcEndpoint != "",
			})
			if err != nil {
				return nil, nil, nil, err
			}
			srcProv = pNew
			srcBucket = bucket
		} else if srcBucket != "" && bucket != "" && srcBucket != bucket {
			return nil, nil, nil, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", bucket, srcBucket)
		}
		return srcProv, dstProv, sidecarProv, nil
	}
	getInputProviders := func(bucket string) (provider.Provider, provider.Provider, error) {
		src, dst, _, err := getProviders(bucket)
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
	)

	tasks := make(chan reflowTask, reflowParallel*2)
	var wg sync.WaitGroup
	for i := 0; i < reflowParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}

				src, dst, sidecarDst, err := getProviders(task.SourceBucket)
				if err != nil {
					errorCount.Add(1)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "failed to connect to provider", err, map[string]any{"source_uri": task.SourceURI})
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
						_ = emitReflowError(context.Background(), w, task.SourceKey, "rewrite failed", err, map[string]any{"source_uri": task.SourceURI})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: "", SourceKey: task.SourceKey, DestKey: "", SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
					destRel = mapped
				}

				dstKey := buildReflowDestKey(destSpec, destRel)
				dstURI := buildReflowDestURI(destSpec, dstKey)

				if reflowResume {
					done, status, err := state.ItemDone(ctx, task.SourceURI, dstURI)
					if err != nil {
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "checkpoint read failed", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
						continue
					}
					if done {
						_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, DestURI: dstURI, DestKey: dstKey, Status: "skipped", Reason: "resume." + status})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "skipped", Reason: "resume." + status}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
				}

				if reflowDryRun {
					_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, DestURI: dstURI, DestKey: dstKey, Status: "planned"})
					continue
				}

				_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, DestURI: dstURI, DestKey: dstKey, Status: "in_progress"})

				srcETag := task.SourceETag
				srcSize := task.SourceSize
				if srcETag == "" || srcSize == 0 {
					meta, err := src.Head(ctx, task.SourceKey)
					if err == nil {
						srcETag = meta.ETag
						srcSize = meta.Size
						if !meta.LastModified.IsZero() {
							task.SourceLastMod = meta.LastModified
						}
					}
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
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionOverwrite, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
					} else if !provider.IsNotFound(headErr) {
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed", headErr, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
						continue
					}
					bytes, err = transfer.CopyObject(ctx, src, dst, task.SourceKey, dstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes)
				} else {
					bytes, putResult, err = transfer.CopyObjectConditional(ctx, src, dst, task.SourceKey, dstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes, provider.PutPrecondition{IfAbsent: true})
					if err != nil && isConditionalExists(err) {
						dstMeta, headErr := dst.Head(ctx, dstKey)
						if headErr != nil {
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed after collision", headErr, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
							continue
						}
						dup := isDuplicateCollision(srcETag, srcSize, dstMeta)
						if dup {
							collision = newCollisionInfo(collisionDuplicate, dstMeta, decisionIfAbsentHead)
							if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionDuplicate, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							if collCfg.Mode == reflowCollisionSkip || collCfg.Mode == reflowCollisionQuar {
								sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), destRel, dstKey, dstURI, dstMeta, reflowRewriteTo, "skipped.duplicate", jobID, collision)
								if sidecarFatal {
									errorCount.Add(1)
									if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
										observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
									}
									_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed", Reason: "provenance.write_failed", Provenance: sidecarRef}, collision))
									continue
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.duplicate"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "skipped", Reason: "collision.duplicate", Provenance: sidecarRef}, collision))
								continue
							}

							err := fmt.Errorf("destination key exists with identical content: %s", dstKey)
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "collision duplicate", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI, "collision": collision})
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeAlreadyExists, ErrorMessage: err.Error()}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed", Reason: "collision.exists.duplicate"}, collision))
							continue
						}

						if collCfg.Mode == reflowCollisionQuar {
							collision = newCollisionInfo(collisionQuarantined, dstMeta, decisionQuarantine)
							if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							quarantineDestRel := buildQuarantineDestRel(collCfg.QuarantinePrefix, task.SourceKey)
							quarantineDstKey := buildReflowDestKey(destSpec, quarantineDestRel)
							quarantineDstURI := buildReflowDestURI(destSpec, quarantineDstKey)
							bytes, err = transfer.CopyObject(ctx, src, dst, task.SourceKey, quarantineDstKey, srcSize, transfer.DefaultRetryBufferMaxMemoryBytes)
							if err != nil {
								errorCount.Add(1)
								code := reflowErrCode(err)
								_ = emitReflowError(context.Background(), w, task.SourceKey, "quarantine copy failed", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": quarantineDstURI, "original_dest_uri": dstURI, "collision": collision})
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: err.Error()}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: quarantineDstURI, DestKey: quarantineDstKey, Status: "failed", Reason: "collision.quarantine_copy_failed", RoutingClass: "quarantine"}, collision))
								continue
							}
							if werr := state.NoteDestKeySource(context.Background(), quarantineDstKey, task.SourceURI, srcETag, srcSize); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							destMeta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: quarantineDstKey, Size: bytes}}
							sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), quarantineDestRel, quarantineDstKey, quarantineDstURI, destMeta, reflowRewriteTo, "quarantined", jobID, collision)
							if sidecarFatal {
								errorCount.Add(1)
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: quarantineDstURI, DestKey: quarantineDstKey, Status: "failed", Reason: "provenance.write_failed", Bytes: bytes, RoutingClass: "quarantine", Provenance: sidecarRef}, collision))
								continue
							}
							if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: quarantineDstURI, SourceKey: task.SourceKey, DestKey: quarantineDstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "quarantined", Reason: "collision.conflict.quarantined", Bytes: bytes}); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: quarantineDstURI, DestKey: quarantineDstKey, Status: "quarantined", Reason: "collision.conflict.quarantined", Bytes: bytes, RoutingClass: "quarantine", Provenance: sidecarRef}, collision))
							continue
						}

						collision = newCollisionInfo(collisionConflict, dstMeta, decisionIfAbsentHead)
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						reason := "collision.conflict"
						if collCfg.Mode == reflowCollisionFail {
							reason = "collision.exists.conflict"
						}
						err := fmt.Errorf("destination key exists with different content: %s", dstKey)
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "collision", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI, "collision": collision})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeAlreadyExists, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed", Reason: reason}, collision))
						continue
					}
				}
				if err != nil {
					errorCount.Add(1)
					code := reflowErrCode(err)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "copy failed", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: err.Error()}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed"})
					continue
				}

				if werr := state.NoteDestKeySource(context.Background(), dstKey, task.SourceURI, srcETag, srcSize); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				destMeta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: dstKey, Size: bytes, ETag: putResult.ETag}}
				sidecarRef, sidecarFatal := writeProvenanceSidecar(ctx, w, sidecarDst, provCfg, destSpec, task.withSourceMeta(srcETag, srcSize), destRel, dstKey, dstURI, destMeta, reflowRewriteTo, reflowActionForTask(task), jobID, collision)
				if sidecarFatal {
					errorCount.Add(1)
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: "provenance sidecar write failed"}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed", Reason: "provenance.write_failed", Bytes: bytes, Provenance: sidecarRef}, collision))
					continue
				}
				if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "complete", Bytes: bytes}); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				_ = w.WriteAny(ctx, reflowRecordType, recordWithCollision(reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "complete", Bytes: bytes, Provenance: sidecarRef}, collision))
			}
		}()
	}

	// Feed tasks from stdin / positional.
	var inputErr error
	if reflowStdin {
		s := bufio.NewScanner(cmd.InOrStdin())
		s.Buffer(make([]byte, 64*1024), 1024*1024)
		for s.Scan() {
			line := strings.TrimSpace(s.Text())
			if line == "" {
				continue
			}
			srcBucket, inputErr = enqueueReflowLine(ctx, line, srcBucket, getInputProviders, tasks)
			if inputErr != nil {
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
		srcBucket, inputErr = enqueueReflowLine(ctx, args[0], srcBucket, getInputProviders, tasks)
	}
	close(tasks)
	wg.Wait()

	if inputErr != nil {
		return exitError(foundry.ExitInvalidArgument, "Failed to read input", inputErr)
	}
	if ctx.Err() != nil {
		return exitError(foundry.ExitSignalInt, "reflow cancelled", ctx.Err())
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

func enqueueReflowLine(ctx context.Context, line string, srcBucket string, getProviders func(bucket string) (provider.Provider, provider.Provider, error), out chan<- reflowTask) (string, error) {
	// JSONL: index object record.
	if strings.HasPrefix(line, "{") {
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return srcBucket, err
		}
		switch env.Type {
		case "gonimbus.index.object.v1":
			var data struct {
				BaseURI   string  `json:"base_uri"`
				Key       string  `json:"key"`
				ETag      string  `json:"etag"`
				SizeBytes int64   `json:"size_bytes"`
				RelKey    string  `json:"rel_key"`
				DeletedAt *string `json:"deleted_at"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcBucket, err
			}
			if data.DeletedAt != nil {
				return srcBucket, fmt.Errorf("deleted objects are not supported in reflow input")
			}
			base, err := uri.ParseURI(data.BaseURI)
			if err != nil {
				return srcBucket, fmt.Errorf("invalid base_uri: %w", err)
			}
			if base.Provider != string(provider.ProviderS3) {
				return srcBucket, fmt.Errorf("unsupported provider %q", base.Provider)
			}
			if srcBucket == "" {
				srcBucket = base.Bucket
			} else if srcBucket != base.Bucket {
				return srcBucket, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", base.Bucket, srcBucket)
			}
			_, _, err = getProviders(srcBucket)
			if err != nil {
				return srcBucket, err
			}
			key := strings.TrimPrefix(data.Key, "/")
			if key == "" {
				key = strings.TrimPrefix(data.RelKey, "/")
			}
			if key == "" {
				return srcBucket, fmt.Errorf("missing key in index record")
			}
			uri := fmt.Sprintf("%s://%s/%s", base.Provider, base.Bucket, key)
			select {
			case out <- reflowTask{SourceBucket: base.Bucket, SourceURI: uri, SourceKey: key, SourceETag: data.ETag, SourceSize: data.SizeBytes}:
				return srcBucket, nil
			case <-ctx.Done():
				return srcBucket, ctx.Err()
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
				return srcBucket, err
			}
			if strings.TrimSpace(data.SourceURI) == "" {
				return srcBucket, fmt.Errorf("missing data.source_uri")
			}
			u, err := uri.ParseURI(data.SourceURI)
			if err != nil {
				return srcBucket, err
			}
			if u.Provider != string(provider.ProviderS3) {
				return srcBucket, fmt.Errorf("unsupported provider %q", u.Provider)
			}
			if u.IsPrefix() || u.IsPattern() {
				return srcBucket, fmt.Errorf("reflow input source_uri must be an exact object URI")
			}
			if srcBucket == "" {
				srcBucket = u.Bucket
			} else if srcBucket != u.Bucket {
				return srcBucket, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", u.Bucket, srcBucket)
			}
			_, _, err = getProviders(srcBucket)
			if err != nil {
				return srcBucket, err
			}
			key := u.Key
			if strings.TrimSpace(data.SourceKey) != "" {
				key = strings.TrimPrefix(strings.TrimSpace(data.SourceKey), "/")
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
				return srcBucket, fmt.Errorf("unsupported routing_class %q", data.RoutingClass)
			}
			quarantinePrefix := strings.Trim(strings.TrimSpace(data.QuarantinePrefix), "/")
			if routingClass == "quarantine" && quarantinePrefix == "" {
				return srcBucket, fmt.Errorf("quarantine_prefix is required when routing_class=quarantine")
			}
			if routingClass == "quarantine" && !isRelativeQuarantinePrefix(data.QuarantinePrefix) {
				return srcBucket, fmt.Errorf("quarantine_prefix must be a relative destination prefix")
			}
			srcURI := fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key)
			select {
			case out <- reflowTask{SourceBucket: u.Bucket, SourceURI: srcURI, SourceKey: key, SourceETag: data.SourceETag, SourceSize: data.SourceSize, SourceLastMod: data.SourceLastMod, Vars: data.Vars, Probe: data.Probe, DestRelKey: destRel, RoutingClass: routingClass, QuarantinePrefix: quarantinePrefix}:
				return srcBucket, nil
			case <-ctx.Done():
				return srcBucket, ctx.Err()
			}
		default:
			return srcBucket, fmt.Errorf("unsupported json record type %q", env.Type)
		}
	}

	parsed, err := uri.ParseURI(line)
	if err != nil {
		return srcBucket, err
	}
	if parsed.Provider != string(provider.ProviderS3) {
		return srcBucket, fmt.Errorf("unsupported provider %q", parsed.Provider)
	}
	if srcBucket == "" {
		srcBucket = parsed.Bucket
	} else if srcBucket != parsed.Bucket {
		return srcBucket, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", parsed.Bucket, srcBucket)
	}
	prov, _, err := getProviders(srcBucket)
	if err != nil {
		return srcBucket, err
	}

	if !parsed.IsPrefix() && !parsed.IsPattern() {
		select {
		case out <- reflowTask{SourceBucket: parsed.Bucket, SourceURI: parsed.String(), SourceKey: parsed.Key}:
			return srcBucket, nil
		case <-ctx.Done():
			return srcBucket, ctx.Err()
		}
	}

	var m *match.Matcher
	if parsed.IsPattern() {
		matcher, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			return srcBucket, err
		}
		m = matcher
	}

	var token string
	for {
		res, err := prov.List(ctx, provider.ListOptions{Prefix: parsed.Key, ContinuationToken: token})
		if err != nil {
			return srcBucket, err
		}
		for _, obj := range res.Objects {
			if m != nil && !m.Match(obj.Key) {
				continue
			}
			uri := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, obj.Key)
			select {
			case out <- reflowTask{SourceBucket: parsed.Bucket, SourceURI: uri, SourceKey: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, SourceLastMod: obj.LastModified}:
				// ok
			case <-ctx.Done():
				return srcBucket, ctx.Err()
			}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}
	return srcBucket, nil
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
	switch {
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
