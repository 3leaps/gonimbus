package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
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
	decisionHeadFallback    = "head_compare_fallback"
	ifAbsentFallbackWarning = "REFLOW_IFABSENT_FALLBACK_ACTIVE"
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
	reflowStdin               bool
	reflowDest                string
	reflowRewriteFrom         string
	reflowRewriteTo           string
	reflowParallel            int
	reflowNoAdaptive          bool
	reflowResourceProbeForRun = reflowpkg.DefaultResourceProbe()
	reflowDryRun              bool
	reflowResume              bool
	reflowResumeRun           string
	reflowCheckpoint          string
	reflowOverwrite           bool
	reflowOnCollision         string
	reflowCollQuar            string
	reflowProvenance          string
	reflowProvRoot            string
	reflowProvSuffix          string
	reflowProvOnError         string
	reflowProvUnsafe          bool
	reflowMetaPolicy          string
	reflowMetaSets            []string
	reflowMetaSrcKeys         []string
	reflowMetaDerived         []string
	reflowMetaMissing         string
	reflowMetaContent         bool
	reflowMetaStorage         string
	reflowMetaSuffix          string
	reflowSymlinks            string
	reflowHidden              string
	reflowExcludes            []string
	reflowPreserve            bool
	reflowSrcFailure          string

	reflowSrcRegion     string
	reflowSrcProfile    string
	reflowSrcEndpoint   string
	reflowSrcGCPProject string
	reflowDstRegion     string
	reflowDstProfile    string
	reflowDstEndpoint   string
	reflowDstGCPProject string
)

var (
	newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
		return reflowstate.Open(ctx, cfg)
	}
)

func init() {
	transferCmd.AddCommand(transferReflowCmd)

	transferReflowCmd.Flags().BoolVar(&reflowStdin, "stdin", false, "Read selection from stdin")
	transferReflowCmd.Flags().StringVar(&reflowDest, "dest", "", "Destination base URI (prefix), e.g. s3://bucket/base/ or file:///tmp/out/")
	transferReflowCmd.Flags().StringVar(&reflowRewriteFrom, "rewrite-from", "", "Rewrite source template (segment captures)")
	transferReflowCmd.Flags().StringVar(&reflowRewriteTo, "rewrite-to", "", "Rewrite destination template (segment renders)")
	transferReflowCmd.Flags().IntVar(&reflowParallel, "parallel", 16, "Requested concurrent copy ceiling")
	transferReflowCmd.Flags().BoolVar(&reflowNoAdaptive, "no-adaptive", false, "Disable adaptive concurrency and run fixed at the resource-capped --parallel ceiling")
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
	transferReflowCmd.Flags().StringVar(&reflowSrcGCPProject, "src-gcp-project", "", "Source GCP project hint for GCS")
	transferReflowCmd.Flags().StringVar(&reflowDstRegion, "dest-region", "", "Destination AWS region")
	transferReflowCmd.Flags().StringVar(&reflowDstProfile, "dest-profile", "", "Destination AWS profile")
	transferReflowCmd.Flags().StringVar(&reflowDstEndpoint, "dest-endpoint", "", "Destination custom S3 endpoint")
	transferReflowCmd.Flags().StringVar(&reflowDstGCPProject, "dest-gcp-project", "", "Destination GCP project hint for GCS")

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
	rewriteFromSet := strings.TrimSpace(reflowRewriteFrom) != ""
	rewriteToSet := strings.TrimSpace(reflowRewriteTo) != ""
	if !reflowStdin || rewriteFromSet || rewriteToSet {
		if !rewriteFromSet {
			return exitError(foundry.ExitInvalidArgument, "Missing --rewrite-from", fmt.Errorf("--rewrite-from is required when --rewrite-to is set or when not using --stdin"))
		}
		if !rewriteToSet {
			return exitError(foundry.ExitInvalidArgument, "Missing --rewrite-to", fmt.Errorf("--rewrite-to is required when --rewrite-from is set or when not using --stdin"))
		}
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
	if destSpec.Provider == string(provider.ProviderGCS) {
		destSpec.GCPProject = strings.TrimSpace(reflowDstGCPProject)
	}
	destURI := destSpec.BaseURI
	concurrencyCfg := reflowpkg.ResolveConcurrency(reflowParallel, !reflowNoAdaptive, reflowResourceProbeForRun)
	concurrencyLimiter := reflowpkg.NewConcurrencyLimiter(concurrencyCfg)

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

	var rewrite *transfer.ReflowRewrite
	if rewriteFromSet && rewriteToSet {
		rewrite, err = transfer.CompileReflowRewrite(reflowRewriteFrom, reflowRewriteTo)
		if err != nil {
			return exitError(foundry.ExitInvalidArgument, "Invalid rewrite templates", err)
		}
	}
	cmd.SilenceUsage = true

	jobID := strings.TrimSpace(runID)
	if jobID == "" {
		jobID = uuid.New().String()
	}
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, destSpec.Provider)
	defer func() { _ = w.Close() }()
	if err := emitReflowConcurrencyClampWarning(ctx, w, cmd.ErrOrStderr(), concurrencyCfg); err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to write concurrency warning", err)
	}

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
	dstProv, err = newDestProvider(ctx, destSpec, metaCfg, concurrencyCfg)
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

	if reflowDryRun && isObjectStoreProvider(destSpec.Provider) {
		if err := runObjectStoreReflowDryRunPreflight(ctx, w, dstProv, destSpec); err != nil {
			return exitError(foundry.ExitExternalServiceUnavailable, "Destination write preflight failed", err)
		}
	}
	ifAbsentCapability := detectReflowIfAbsentCapability(ctx, dstProv, destSpec, collCfg, reflowDryRun)
	if err := emitIfAbsentFallbackWarning(ctx, w, collCfg, destSpec, ifAbsentCapability); err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to write IfAbsent fallback warning", err)
	}

	_ = w.WriteAny(ctx, reflowpkg.RunRecordType, reflowpkg.RunRecord{
		DestURI:          destURI,
		CheckpointPath:   checkpointPath,
		DryRun:           reflowDryRun,
		Resume:           reflowResume,
		Parallel:         reflowParallel,
		ConcurrencyStats: concurrencyLimiter.Snapshot(),
		Provenance:       provCfg.runConfig(),
		Metadata:         metaCfg.runConfig(),
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
			pNew, err := newDestProvider(ctx, destSpec, metaCfg, concurrencyCfg)
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
				pNew, err := newDestProvider(ctx, provCfg.SidecarRoot, metaCfg, concurrencyCfg)
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
			pNew, err := newSourceProvider(ctx, srcURI, concurrencyCfg)
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
		fatalRun     *reflowFatalRunError
	)
	stats := newReflowRunStats()
	writeReflowRecord := func(ctx context.Context, rec reflowpkg.Record) {
		stats.record(rec)
		_ = w.WriteAny(ctx, reflowpkg.RecordType, rec)
	}
	copyObjectWithOptions := func(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, opts provider.PutOptions) (int64, error) {
		release, err := concurrencyLimiter.Acquire(ctx)
		if err != nil {
			return 0, err
		}
		defer release()
		bytes, err := transfer.CopyObjectWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, transfer.DefaultRetryBufferMaxMemoryBytes, opts)
		concurrencyLimiter.ObserveProviderResult(err)
		return bytes, err
	}
	copyObjectConditionalWithOptions := func(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey, dstKey string, expectedSize int64, precond provider.PutPrecondition, opts provider.PutOptions) (int64, provider.PutResult, error) {
		release, err := concurrencyLimiter.Acquire(ctx)
		if err != nil {
			return 0, provider.PutResult{}, err
		}
		defer release()
		bytes, result, err := transfer.CopyObjectConditionalWithOptions(ctx, src, dst, srcKey, dstKey, expectedSize, transfer.DefaultRetryBufferMaxMemoryBytes, precond, opts)
		concurrencyLimiter.ObserveProviderResult(err)
		return bytes, result, err
	}
	// acquireProbeHead bounds a standalone provider HEAD probe on the adaptive
	// concurrency limiter, mirroring the copy path. The slot is held only for the
	// duration of the HEAD and is never held across a copy, so probe and copy
	// acquisitions are strictly sequential, never nested — deadlock-free even when
	// the adaptive `current` has backed off to the Floor (1). This closes the
	// asymmetry where HEAD/collision probes ran unbounded at worker-count
	// while only copies obeyed `current`; under throttling, backoff now actually
	// reduces probe pressure (it already fed the throttle signal via
	// ObserveProviderResult, but was not itself bounded). Bounding only — the
	// existing ObserveProviderResult call sites are unchanged.
	acquireProbeHead := func(ctx context.Context, p provider.Provider, key string) (*provider.ObjectMeta, error) {
		release, err := concurrencyLimiter.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
		return p.Head(ctx, key)
	}
	recordFatalReflowError := func(err error) bool {
		classification := classifyTransferReflowRunErrorWithConfig(err, checkpointCfg)
		if !classification.Resumable {
			return false
		}
		fatalMu.Lock()
		defer fatalMu.Unlock()
		if fatalRun == nil {
			fatalRun = &reflowFatalRunError{
				err:   err,
				cause: reflowOperationErrorCause(err, classification),
			}
			cancelWork()
		}
		return true
	}
	currentFatalReflowError := func() *reflowFatalRunError {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		return fatalRun
	}

	tasks := make(chan reflowTask, concurrencyCfg.EffectiveCeiling*2)
	destArbiter := newReflowDestKeyArbiter()
	var wg sync.WaitGroup
	for i := 0; i < concurrencyCfg.EffectiveCeiling; i++ {
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
					writeReflowRecord(ctx, rec)
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
					if rewrite == nil {
						invalidCount.Add(1)
						err := fmt.Errorf("stdin record lacks dest_rel_key and no rewrite templates were supplied")
						_ = emitReflowInputError(context.Background(), w, task.SourceKey, "destination mapping unavailable", err, map[string]any{"source_uri": srcAuditURI})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: "", SourceKey: task.SourceKey, DestKey: "", SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "failed", ErrorCode: output.ErrCodeInvalidInput, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
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
						writeReflowRecord(ctx, rec)
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "skipped", Reason: "resume." + status}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
				}

				if reflowDryRun {
					writeReflowRecord(ctx, task.reflowRecord(dstURI, dstKey, "planned"))
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
						writeReflowRecord(ctx, rec)
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
						writeReflowRecord(ctx, rec)
						continue
					}
				}

				writeReflowRecord(ctx, task.reflowRecord(dstURI, dstKey, "in_progress"))

				srcETag := task.SourceETag
				srcSize := task.SourceSize
				var sourceMeta *provider.ObjectMeta
				needsSourceHeadForCollision := collCfg.Mode == reflowCollisionSrcNew && task.SourceLastMod.IsZero()
				if metaCfg.needsSourceHead() || srcETag == "" || srcSize == 0 || needsSourceHeadForCollision {
					meta, err := acquireProbeHead(ctx, src, task.SourceKey)
					if err == nil {
						sourceMeta = meta
						srcETag = meta.ETag
						srcSize = meta.Size
						if !meta.LastModified.IsZero() {
							task.SourceLastMod = meta.LastModified
						}
					} else if metaCfg.needsSourceHead() || needsSourceHeadForCollision {
						concurrencyLimiter.ObserveProviderResult(err)
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
						bytes, qerr := copyObjectWithOptions(ctx, src, dst, task.SourceKey, quarantineDstKey, srcSize, provider.PutOptions{})
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
							writeReflowRecord(ctx, rec)
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
							writeReflowRecord(ctx, rec)
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
						writeReflowRecord(ctx, rec)
						continue
					}
					errorCount.Add(1)
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: output.ErrCodeInvalidInput, ErrorMessage: err.Error()}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					continue
				}

				var collision *reflowpkg.CollisionInfo
				var bytes int64
				var putResult provider.PutResult
				if collCfg.Mode == reflowCollisionOver {
					dstMeta, headErr := acquireProbeHead(ctx, dst, dstKey)
					if headErr == nil {
						kind := collisionConflict
						if isDuplicateCollision(task.SourceProvider, destSpec.Provider, srcETag, srcSize, dstMeta) {
							kind = collisionDuplicate
						}
						collision = newCollisionInfo(kind, dstMeta, decisionOverwrite)
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionOverwrite, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
					} else if !provider.IsNotFound(headErr) {
						concurrencyLimiter.ObserveProviderResult(headErr)
						if recordFatalReflowError(headErr) {
							continue
						}
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed", headErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
						continue
					}
					bytes, err = copyObjectWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, putOptions)
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
							if recordFatalReflowError(observedErr) {
								continue
							}
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination arbitration state lookup failed", observedErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
							continue
						}
						if observed {
							gate.observed = true
							err = &provider.ProviderError{Op: "PutObjectConditional", Provider: provider.ProviderType(destSpec.Provider), Key: dstKey, Err: provider.ErrAlreadyExists}
						} else {
							if ifAbsentCapability.FallbackActive {
								stats.recordFallbackObject()
								_, headErr := acquireProbeHead(ctx, dst, dstKey)
								switch {
								case headErr == nil:
									err = &provider.ProviderError{Op: "Head", Provider: provider.ProviderType(destSpec.Provider), Key: dstKey, Err: provider.ErrAlreadyExists}
								case provider.IsNotFound(headErr):
									bytes, err = copyObjectWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, putOptions)
								default:
									concurrencyLimiter.ObserveProviderResult(headErr)
									err = headErr
								}
							} else {
								bytes, putResult, err = copyObjectConditionalWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, provider.PutPrecondition{IfAbsent: true}, putOptions)
							}
							if err == nil || isConditionalExists(err) {
								gate.observed = true
								if markErr := state.MarkDestKeyObserved(ctx, dstKey); markErr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(markErr))
									_ = w.WriteAny(ctx, reflowpkg.WarningRecordType, reflowpkg.Warning{
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
						collisionDecisionPath := decisionIfAbsentHead
						sourceNewerDecisionPath := decisionHeadCompare
						if ifAbsentCapability.FallbackActive {
							collisionDecisionPath = decisionHeadFallback
							sourceNewerDecisionPath = decisionHeadFallback
						}
						dstMeta, headErr := acquireProbeHead(ctx, dst, dstKey)
						if headErr != nil {
							concurrencyLimiter.ObserveProviderResult(headErr)
							if recordFatalReflowError(headErr) {
								continue
							}
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed after collision", headErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
							continue
						}
						// Bound the body-compare GETs on the limiter too (held only for
						// the comparison, never across a copy — sequential, deadlock-free).
						dup, dupErr := func() (bool, error) {
							release, acqErr := concurrencyLimiter.Acquire(ctx)
							if acqErr != nil {
								return false, acqErr
							}
							defer release()
							return isDuplicateCollisionForReflow(ctx, src, dst, task.SourceKey, dstKey, task.SourceProvider, destSpec.Provider, srcETag, srcSize, dstMeta)
						}()
						if dupErr != nil {
							concurrencyLimiter.ObserveProviderResult(dupErr)
							if recordFatalReflowError(dupErr) {
								continue
							}
							errorCount.Add(1)
							_ = emitReflowError(context.Background(), w, task.SourceKey, "destination duplicate comparison failed", dupErr, map[string]any{"source_uri": srcAuditURI, "dest_uri": dstURI})
							continue
						}
						if dup {
							collision = newCollisionInfo(collisionDuplicate, dstMeta, collisionDecisionPath)
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
									writeReflowRecord(ctx, recordWithCollision(rec, collision))
									continue
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.duplicate"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "skipped")
								rec.Reason = "collision.duplicate"
								rec.Provenance = sidecarRef
								writeReflowRecord(ctx, recordWithCollision(rec, collision))
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
							writeReflowRecord(ctx, recordWithCollision(rec, collision))
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
								writeReflowRecord(ctx, rec)
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
								collision = newSourceNewerCollisionInfo(collisionSrcOlder, dstMeta, task.SourceLastMod, sourceNewerDecisionPath, decisionReason)
								if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.skipped_src_older"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "skipped")
								rec.Reason = "collision.skipped_src_older"
								writeReflowRecord(ctx, recordWithCollision(rec, collision))
								continue
							}

							collision = newSourceNewerCollisionInfo(collisionOverwritten, dstMeta, task.SourceLastMod, sourceNewerDecisionPath, decisionReason)
							bytes, putResult, err = copyObjectConditionalWithOptions(ctx, src, dst, task.SourceKey, dstKey, srcSize, provider.PutPrecondition{IfMatchETag: &dstMeta.ETag}, putOptions)
							if err != nil && isConditionalExists(err) {
								collision = newSourceNewerCollisionInfo(collisionConcurrentMut, dstMeta, task.SourceLastMod, sourceNewerDecisionPath, reasonConcurrentMut)
								if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.skipped_concurrent_mutation"}); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
								rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "skipped")
								rec.Reason = "collision.skipped_concurrent_mutation"
								writeReflowRecord(ctx, recordWithCollision(rec, collision))
								continue
							}
							if err == nil {
								if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionOverwrite, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
									observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
								}
							}
						} else if collCfg.Mode == reflowCollisionQuar {
							quarantineDecisionPath := decisionQuarantine
							if ifAbsentCapability.FallbackActive {
								quarantineDecisionPath = collisionDecisionPath
							}
							collision = newCollisionInfo(collisionQuarantined, dstMeta, quarantineDecisionPath)
							if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, srcCheckpointURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
								observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
							}
							quarantineDestRel := buildQuarantineDestRel(collCfg.QuarantinePrefix, task.SourceKey)
							quarantineDstKey := buildReflowDestKey(destSpec, quarantineDestRel)
							quarantineDstURI := buildReflowDestURI(destSpec, quarantineDstKey)
							bytes, err = copyObjectWithOptions(ctx, src, dst, task.SourceKey, quarantineDstKey, srcSize, putOptions)
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
								writeReflowRecord(ctx, recordWithCollision(rec, collision))
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
								writeReflowRecord(ctx, recordWithCollision(rec, collision))
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
							writeReflowRecord(ctx, recordWithCollision(rec, collision))
							continue
						} else {
							collision = newCollisionInfo(collisionConflict, dstMeta, collisionDecisionPath)
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
							writeReflowRecord(ctx, recordWithCollision(rec, collision))
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
					rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "failed")
					rec.Reason = reflowReasonForErrCode(code)
					writeReflowRecord(ctx, rec)
					continue
				}
				if srcCfg.PreserveMode && task.SourceProvider == string(provider.ProviderFile) && destSpec.Provider == string(provider.ProviderFile) {
					if chmodErr := os.Chmod(filepath.Join(destSpec.BaseDir, filepath.FromSlash(dstKey)), task.SourceMode.Perm()); chmodErr != nil {
						_ = w.WriteAny(ctx, reflowpkg.WarningRecordType, reflowpkg.Warning{
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
					writeReflowRecord(ctx, recordWithCollision(rec, collision))
					continue
				}
				if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: srcCheckpointURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "complete", Bytes: bytes}); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				rec := task.withSourceMeta(srcETag, srcSize).reflowRecord(dstURI, dstKey, "complete")
				rec.Bytes = bytes
				rec.Provenance = sidecarRef
				writeReflowRecord(ctx, recordWithCollision(rec, collision))
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
	_ = w.WriteAny(context.Background(), reflowpkg.SummaryRecordType, stats.summary(destURI, reflowDryRun, collCfg, ifAbsentCapability, concurrencyLimiter.Snapshot(), invalidCount.Load(), errorCount.Load()))

	fatalRunErr := currentFatalReflowError()
	if fatalRunErr != nil || ctx.Err() != nil {
		var cause *opcheckpoint.ErrorCause
		classifyErr := error(nil)
		if fatalRunErr != nil {
			classifyErr = fatalRunErr.err
			cause = fatalRunErr.cause
		}
		if classifyErr == nil {
			classifyErr = ctx.Err()
		}
		classification := classifyTransferReflowRunErrorWithConfig(classifyErr, checkpointCfg)
		if cause == nil {
			cause = reflowOperationErrorCause(classifyErr, classification)
		}
		reportErr := classifyErr
		if cause != nil && cause.Message != "" {
			reportErr = errors.New(cause.Message)
		}
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
				writeOperationErrorSummaryWithCause(cmd.ErrOrStderr(), "Transfer reflow failed with resumable checkpoint", operationTransferReflow, jobID, classification.Class, cause, progress)
				enc := json.NewEncoder(cmd.OutOrStdout())
				if emitErr := emitOperationErrorRecordWithCause(context.Background(), enc, operationTransferReflow, jobID, classification.Class, cause, progress); emitErr != nil {
					return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), fmt.Errorf("%w; write operation error record: %v", reportErr, emitErr))
				}
			} else {
				return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), fmt.Errorf("%w; write operation checkpoint: %v", reportErr, checkpointErr))
			}
		}
		return exitError(transferReflowFatalExitCode(classification), transferReflowFatalExitMessage(classification, checkpointWritten), reportErr)
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
