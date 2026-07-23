package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

type provenanceConfig struct {
	Mode              string
	Suffix            string
	OnWriteError      string
	AllowUnsafeSuffix bool
	PlacementMode     string
	SidecarRootRaw    string
	SidecarRoot       *reflowDestSpec
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
	// Mode, on-write-error, and suffix rules delegate to the shared pkg/reflow
	// validators so the CLI and the engine enforce one canonical rule set.
	if err := reflowpkg.ValidateProvenanceModeAndPolicy(cfg.Mode, cfg.OnWriteError); err != nil {
		return err
	}
	switch cfg.Mode {
	case "", provenanceModeNone:
		if cfg.SidecarRootRaw != "" {
			return fmt.Errorf("provenance-sidecar-root requires --provenance sidecar")
		}
		return nil
	}
	return reflowpkg.ValidateProvenanceSuffix(cfg.Suffix, cfg.AllowUnsafeSuffix)
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
	// Object-store mirrored roots must resolve to the destination bucket (S3 and
	// GCS): the sidecar is written through the bucket-bound destination handle, so
	// a cross-bucket root would render a URI naming a bucket the object was never
	// written to. Refuse it up front rather than emit a false provenance URI.
	if isObjectStoreProvider(root.Provider) && root.Bucket != dest.Bucket {
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

func (cfg provenanceConfig) enabled() bool {
	return cfg.Mode == provenanceModeSidecar
}

// enginePlan resolves the CLI provenance config into the engine's validated
// ProvenancePlan. The engine is the sole provenance authority for a migrated run:
// it owns validation, per-object sidecar emission, and the RunRecord echo. runID
// is the job identity carried into each sidecar's run.run_id. For the command
// surface only sibling and same-bucket object-store mirrored placement are
// reachable (a file destination is not engine-migrated), so the sidecar is always
// written through the destination handle and needs no injected second provider;
// the resolved root's BaseURI mirrors the raw flag so the RunRecord echo matches
// the pool byte-for-byte.
func (cfg provenanceConfig) enginePlan(runID string) reflowpkg.ProvenancePlan {
	if !cfg.enabled() {
		return reflowpkg.ProvenancePlan{Mode: reflowpkg.ProvenanceModeNone}
	}
	plan := reflowpkg.ProvenancePlan{
		Mode:              reflowpkg.ProvenanceModeSidecar,
		Suffix:            cfg.Suffix,
		AllowUnsafeSuffix: cfg.AllowUnsafeSuffix,
		OnWriteError:      cfg.OnWriteError,
		Placement:         reflowpkg.ProvenancePlacementPlan{Mode: reflowpkg.ProvenancePlacementSibling},
		RunID:             runID,
		ToolVersion:       reflowToolVersion(),
	}
	if cfg.PlacementMode == provenancePlaceMirror && cfg.SidecarRoot != nil {
		root := cfg.SidecarRoot
		plan.Placement = reflowpkg.ProvenancePlacementPlan{
			Mode: reflowpkg.ProvenancePlacementMirror,
			SidecarRoot: &reflowpkg.ProvenanceSidecarRoot{
				Provider:         root.Provider,
				Bucket:           root.Bucket,
				Prefix:           root.Prefix,
				BaseDir:          root.BaseDir,
				BaseURI:          cfg.SidecarRootRaw,
				SameBucketAsDest: true,
			},
		}
	}
	return plan
}

func (cfg provenanceConfig) runConfig() *reflowpkg.ProvenanceRunConfig {
	if !cfg.enabled() {
		return nil
	}
	placement := reflowpkg.ProvenancePlacementContext{Mode: cfg.PlacementMode}
	if cfg.PlacementMode == provenancePlaceMirror {
		placement.SidecarRoot = cfg.SidecarRootRaw
	}
	return &reflowpkg.ProvenanceRunConfig{Mode: cfg.Mode, Suffix: cfg.Suffix, OnWriteError: cfg.OnWriteError, Placement: placement}
}

// poolProvenanceEmitter delivers the shared sidecar writer's warn/fail outcomes
// through the command pool's existing record paths, keeping the pool's emitted
// warning/error records byte-identical after the writer relocation.
type poolProvenanceEmitter struct{ w *output.JSONLWriter }

func (e poolProvenanceEmitter) EmitProvenanceWarning(ctx context.Context, warning reflowpkg.Warning) error {
	return e.w.WriteAny(ctx, reflowpkg.WarningRecordType, warning)
}

func (e poolProvenanceEmitter) EmitProvenanceError(ctx context.Context, key, message string, cause error, details map[string]any) error {
	return emitReflowError(ctx, e.w, key, message, cause, details)
}

// writeProvenanceSidecar resolves the sidecar placement (key/URI) from the CLI
// layout and delegates the content build + write + on-write-error policy to the
// shared pkg/reflow authority, so the command pool and the engine emit
// byte-identical sidecar objects and apply one policy.
func writeProvenanceSidecar(ctx context.Context, w *output.JSONLWriter, sidecarDst provider.Provider, cfg provenanceConfig, destSpec *reflowDestSpec, task reflowTask, destRel string, destKey string, destURI string, destMeta *provider.ObjectMeta, rewriteTemplate string, action string, jobID string, collision *reflowpkg.CollisionInfo) (*reflowpkg.ProvenanceRef, bool) {
	if !cfg.enabled() {
		return nil, false
	}
	sidecarKey := buildProvenanceSidecarKey(cfg, destSpec, destRel, destKey)
	sidecarURI := buildProvenanceSidecarURI(cfg, destSpec, sidecarKey)
	in := reflowpkg.ProvenanceSidecarInput{
		SourceURI:        task.auditSourceURI(),
		SourceETag:       task.SourceETag,
		SourceSize:       task.SourceSize,
		SourceLastMod:    task.SourceLastMod,
		DestURI:          destURI,
		RoutingClass:     task.RoutingClass,
		RewriteTemplate:  rewriteTemplate,
		QuarantinePrefix: task.QuarantinePrefix,
		Collision:        collision,
		Vars:             task.Vars,
		Probe:            task.Probe,
		Action:           action,
	}
	if destMeta != nil {
		in.DestETag = destMeta.ETag
		in.DestSize = destMeta.Size
	}
	// mode="transfer_reflow" (underscore) matches the pool's historical details
	// label exactly; do not use operationTransferReflow ("transfer-reflow").
	ref, sidecarFatal, emitErr := reflowpkg.WriteProvenanceSidecar(ctx, reflowpkg.ProvenancePutViaProvider(sidecarDst), jobID, reflowToolVersion(), reflowpkg.ProvenanceNow(), "transfer_reflow", cfg.OnWriteError, sidecarKey, sidecarURI, destURI, in, poolProvenanceEmitter{w: w})
	// A warn/error whose record could not be delivered must not let the caller
	// proceed to a success terminal: fold an undelivered event into the
	// fatal signal so the item is reported failed rather than silently completed.
	return ref, sidecarFatal || emitErr != nil
}

func buildProvenanceSidecarKey(cfg provenanceConfig, destSpec *reflowDestSpec, destRel string, destKey string) string {
	if cfg.PlacementMode != provenancePlaceMirror || cfg.SidecarRoot == nil {
		return destKey + cfg.Suffix
	}
	rel := strings.Trim(destRel, "/")
	switch cfg.SidecarRoot.Provider {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		// INTENTIONAL FIX: GCS previously fell through to the default branch and
		// used destKey+suffix, silently ignoring the mirrored sidecar-root prefix.
		// Object stores share the prefix-relative layout.
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

func reflowToolVersion() string {
	version := strings.TrimSpace(versionInfo.Version)
	if version == "" {
		version = "dev"
	}
	return "gonimbus " + version
}
