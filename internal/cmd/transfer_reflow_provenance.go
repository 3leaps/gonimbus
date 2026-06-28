package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
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

type provenanceSidecar struct {
	Schema        string                   `json:"schema"`
	SchemaVersion string                   `json:"schema_version"`
	Source        provenanceSource         `json:"source"`
	Destination   provenanceDestination    `json:"destination"`
	Run           provenanceRun            `json:"run"`
	Routing       provenanceRouting        `json:"routing"`
	Collision     *reflowpkg.CollisionInfo `json:"collision,omitempty"`
	Vars          map[string]string        `json:"vars,omitempty"`
	Probe         *probe.ProbeAudit        `json:"probe,omitempty"`
	Action        string                   `json:"action"`
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

func writeProvenanceSidecar(ctx context.Context, w *output.JSONLWriter, sidecarDst provider.Provider, cfg provenanceConfig, destSpec *reflowDestSpec, task reflowTask, destRel string, destKey string, destURI string, destMeta *provider.ObjectMeta, rewriteTemplate string, action string, jobID string, collision *reflowpkg.CollisionInfo) (*reflowpkg.ProvenanceRef, bool) {
	if !cfg.enabled() {
		return nil, false
	}

	sidecarKey := buildProvenanceSidecarKey(cfg, destSpec, destRel, destKey)
	sidecarURI := buildProvenanceSidecarURI(cfg, destSpec, sidecarKey)
	ref := &reflowpkg.ProvenanceRef{Written: false, Key: sidecarKey, URI: sidecarURI}
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

func buildProvenanceSidecar(task reflowTask, destURI string, destMeta *provider.ObjectMeta, rewriteTemplate string, action string, jobID string, collision *reflowpkg.CollisionInfo) provenanceSidecar {
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
	_ = w.WriteAny(ctx, reflowpkg.WarningRecordType, reflowpkg.Warning{
		Code:    "PROVENANCE_WRITE_FAILED",
		Message: reflowpkg.FormatErrorMessage("provenance sidecar write failed", err),
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
