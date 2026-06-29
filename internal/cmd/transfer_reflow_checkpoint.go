package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/output"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
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

func emitReflowConcurrencyClampWarning(ctx context.Context, w *output.JSONLWriter, stderr io.Writer, cfg reflowpkg.ConcurrencyConfig) error {
	if cfg.RequestedCeiling == cfg.EffectiveCeiling {
		return nil
	}
	_, _ = fmt.Fprintf(stderr, "warning: --parallel requested %d; effective concurrency ceiling clamped to %d (%s)\n", cfg.RequestedCeiling, cfg.EffectiveCeiling, cfg.CeilingReason)
	return w.WriteAny(ctx, reflowpkg.WarningRecordType, reflowpkg.Warning{
		Code:    "REFLOW_CONCURRENCY_CEILING_CLAMPED",
		Message: fmt.Sprintf("requested concurrency ceiling clamped from %d to %d", cfg.RequestedCeiling, cfg.EffectiveCeiling),
		Details: map[string]any{
			"concurrency_ceiling_requested": cfg.RequestedCeiling,
			"concurrency_ceiling_effective": cfg.EffectiveCeiling,
			"concurrency_ceiling_reason":    cfg.CeilingReason,
			"adaptive_enabled":              cfg.AdaptiveEnabled,
		},
	})
}

type transferReflowCheckpointPayload struct {
	Config transferReflowCheckpointConfig `json:"config"`
}

type transferReflowCheckpointConfig struct {
	SourceURI   string `json:"source_uri"`
	Stdin       bool   `json:"stdin"`
	Dest        string `json:"dest"`
	RewriteFrom string `json:"rewrite_from"`
	RewriteTo   string `json:"rewrite_to"`
	Parallel    int    `json:"parallel"`
	// omitempty so a default (adaptive) config re-fingerprints identically to a
	// checkpoint written by an earlier release (v0.3.0–v0.3.2), where the field did
	// not exist. Without it, v0.3.3 injected "no_adaptive":false and broke
	// --resume-run across the upgrade boundary (ErrIdentityMismatch).
	NoAdaptive                bool     `json:"no_adaptive,omitempty"`
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
	SrcGCPProject             string   `json:"src_gcp_project,omitempty"`
	DstRegion                 string   `json:"dest_region,omitempty"`
	DstProfile                string   `json:"dest_profile,omitempty"`
	DstEndpoint               string   `json:"dest_endpoint,omitempty"`
	DstGCPProject             string   `json:"dest_gcp_project,omitempty"`
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
		NoAdaptive:                reflowNoAdaptive,
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
		SrcGCPProject:             reflowSrcGCPProject,
		DstRegion:                 reflowDstRegion,
		DstProfile:                reflowDstProfile,
		DstEndpoint:               reflowDstEndpoint,
		DstGCPProject:             reflowDstGCPProject,
	}
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
		{"src-gcp-project", cfg.SrcGCPProject},
		{"dest-region", cfg.DstRegion},
		{"dest-profile", cfg.DstProfile},
		{"dest-endpoint", cfg.DstEndpoint},
		{"dest-gcp-project", cfg.DstGCPProject},
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
		{"no-adaptive", cfg.NoAdaptive},
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

type reflowFatalRunError struct {
	err   error
	cause *opcheckpoint.ErrorCause
}

const reflowOperationCauseDisposition = "aborted_resumable_checkpoint"

func reflowOperationErrorCause(err error, classification opcheckpoint.Classification) *opcheckpoint.ErrorCause {
	if err == nil {
		return nil
	}
	code := reflowErrCode(err)
	reason := reflowReasonForErrCode(code)
	if classification.Class == opcheckpoint.ErrorClassInterrupted && code == output.ErrCodeInternal {
		code = output.ErrCodeTimeout
		reason = "interrupted"
		if errors.Is(err, context.Canceled) {
			reason = "interrupted.canceled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			reason = "interrupted.deadline_exceeded"
		}
	}
	return &opcheckpoint.ErrorCause{
		Code:        code,
		Reason:      reason,
		Message:     reflowpkg.SanitizeOperationCauseMessage(err),
		Resumable:   classification.Resumable,
		Disposition: reflowOperationCauseDisposition,
	}
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

func resolveReflowCheckpointPath(jobID string) (string, error) {
	root, err := indexRootDir()
	if err != nil {
		return "", err
	}
	// Keep reflow artifacts near index artifacts for consistent ops tooling.
	return filepath.Join(root, "reflow", "runs", jobID, "state.db"), nil
}
