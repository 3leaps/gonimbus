package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type transferReflowEnginePlan struct {
	enabled bool
	reason  string
	cfg     reflowpkg.Config
	source  reflowpkg.RecordStreamSource
	input   io.Reader
	close   func()
}

func planTransferReflowEngineAdapter(ctx context.Context, input io.Reader, destSpec *reflowDestSpec, dst provider.Provider, collCfg collisionConfig, metaCfg reflowMetadataConfig, concurrencyCfg reflowpkg.ConcurrencyConfig, state reflowStateStore) transferReflowEnginePlan {
	plan := transferReflowEnginePlan{input: input}
	if !reflowStdin {
		plan.reason = "positional source path not migrated"
		return plan
	}
	if input == nil {
		plan.reason = "stdin reader is nil"
		return plan
	}
	engineInput, ok, err := prepareTransferReflowEngineInput(input)
	plan.input = engineInput
	if err != nil {
		plan.reason = err.Error()
		return plan
	}
	if !ok {
		plan.reason = "stdin record stream not migrated"
		return plan
	}
	if destSpec == nil || dst == nil {
		plan.reason = "destination provider unavailable"
		return plan
	}
	if reflowProvenance != provenanceModeNone {
		plan.reason = "provenance sidecars not migrated"
		return plan
	}
	if reflowOverwrite || collCfg.Mode == reflowCollisionOver || collCfg.Mode == reflowCollisionQuar || collCfg.Mode == reflowCollisionSrcNew {
		plan.reason = "collision mode not migrated"
		return plan
	}
	if collCfg.Mode != reflowCollisionSkip && collCfg.Mode != reflowCollisionFail {
		plan.reason = "collision mode not migrated"
		return plan
	}
	if strings.TrimSpace(reflowSrcFailure) != "" && strings.TrimSpace(reflowSrcFailure) != reflowSourceFailSkip {
		plan.reason = "source-failure policy not migrated"
		return plan
	}
	if reflowPreserve {
		plan.reason = "preserve-mode not migrated"
		return plan
	}
	if destSpec.Provider == string(provider.ProviderFile) {
		plan.reason = "file destination adapter not migrated"
		return plan
	}

	var (
		srcProv             provider.Provider
		srcProviderIdentity string
	)
	srcResolver := func(ctx context.Context, sourceURI string) (provider.Provider, error) {
		parsed, err := uri.ParseURI(sourceURI)
		if err != nil {
			return nil, err
		}
		identity := reflowSourceIdentity(parsed)
		if srcProv != nil {
			if srcProviderIdentity != "" && identity != "" && srcProviderIdentity != identity {
				return nil, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcProviderIdentity)
			}
			return srcProv, nil
		}
		p, err := newSourceProvider(ctx, parsed, concurrencyCfg)
		if err != nil {
			return nil, err
		}
		srcProv = p
		srcProviderIdentity = identity
		return srcProv, nil
	}
	if reflowDryRun {
		srcResolver = nil
	}

	plan.enabled = true
	plan.cfg = reflowpkg.Config{
		Destination: reflowpkg.Destination{
			Provider:   dst,
			ProviderID: destSpec.Provider,
			BaseURI:    destSpec.BaseURI,
		},
		Rewrite: reflowpkg.RewriteConfig{
			From: reflowRewriteFrom,
			To:   reflowRewriteTo,
		},
		Collision: reflowpkg.CollisionPolicy{
			Mode:             collCfg.Mode,
			QuarantinePrefix: collCfg.QuarantinePrefix,
		},
		Concurrency: concurrencyCfg,
		DryRun:      reflowDryRun,
		ReadOnly:    IsReadOnly(),
		Metadata:    metaCfg,
		Checkpoint:  checkpointAdapter(state, reflowResume),
	}
	plan.source = reflowpkg.RecordStreamSource{
		Records: engineInput,
		Resolve: srcResolver,
	}
	plan.close = func() {
		if srcProv != nil {
			_ = srcProv.Close()
		}
	}
	return plan
}

func prepareTransferReflowEngineInput(input io.Reader) (io.Reader, bool, error) {
	reader := bufio.NewReader(input)
	var prefix strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			prefix.WriteString(line)
		}
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			return io.MultiReader(strings.NewReader(prefix.String()), reader), isMigratedReflowInputLine(trimmed), nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.MultiReader(strings.NewReader(prefix.String()), reader), false, nil
			}
			return io.MultiReader(strings.NewReader(prefix.String()), reader), false, err
		}
	}
}

func isMigratedReflowInputLine(line string) bool {
	var env struct {
		Type string `json:"type"`
		Data struct {
			SourceURI string `json:"source_uri"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		return false
	}
	return env.Type == "gonimbus.reflow.input.v1" && strings.HasPrefix(strings.TrimSpace(env.Data.SourceURI), "s3://")
}

func runTransferReflowViaEngine(ctx context.Context, plan transferReflowEnginePlan, w *output.JSONLWriter, checkpointPath string, resume bool, provCfg provenanceConfig, metaCfg reflowMetadataConfig) (reflowpkg.Summary, error) {
	if !plan.enabled {
		return reflowpkg.Summary{}, reflowpkg.ErrNotImplemented
	}
	if plan.close != nil {
		defer plan.close()
	}
	plan.cfg.Events = transferReflowEventSink{
		w:              w,
		checkpointPath: checkpointPath,
		resume:         resume,
		provenance:     provCfg.runConfig(),
		metadata:       metadataRunConfig(metaCfg),
	}
	runner, err := reflowpkg.NewRunner(plan.cfg)
	if err != nil {
		return reflowpkg.Summary{}, err
	}
	return runner.Run(ctx, plan.source)
}

func transferReflowEngineTerminalError(err error) error {
	var invalidErr *reflowpkg.InvalidInputsError
	if errors.As(err, &invalidErr) {
		return exitError(foundry.ExitInvalidArgument, "reflow completed with invalid inputs", fmt.Errorf("invalid_inputs=%d", invalidErr.Count))
	}
	var objectErr *reflowpkg.ObjectErrorsError
	if errors.As(err, &objectErr) {
		return exitError(foundry.ExitExternalServiceUnavailable, "reflow completed with errors", fmt.Errorf("errors=%d", objectErr.Count))
	}
	return err
}

type transferReflowEventSink struct {
	w              *output.JSONLWriter
	checkpointPath string
	resume         bool
	provenance     *reflowpkg.ProvenanceRunConfig
	metadata       *reflowpkg.MetadataRunConfig
}

func (s transferReflowEventSink) OnRun(ctx context.Context, rec reflowpkg.RunRecord) error {
	rec.CheckpointPath = s.checkpointPath
	rec.Resume = s.resume
	rec.Provenance = s.provenance
	rec.Metadata = s.metadata
	return s.w.WriteAny(ctx, reflowpkg.RunRecordType, rec)
}

func (s transferReflowEventSink) OnSource(ctx context.Context, rec reflowpkg.SourceRunRecord) error {
	return s.w.WriteAny(ctx, reflowpkg.SourceRecordType, rec)
}

func (s transferReflowEventSink) OnRecord(ctx context.Context, rec reflowpkg.Record) error {
	return s.w.WriteAny(ctx, reflowpkg.RecordType, rec)
}

func (s transferReflowEventSink) OnWarning(ctx context.Context, warning reflowpkg.Warning) error {
	return s.w.WriteAny(ctx, reflowpkg.WarningRecordType, warning)
}

func (s transferReflowEventSink) OnError(ctx context.Context, event reflowpkg.ErrorEvent) error {
	return s.w.WriteAny(ctx, reflowpkg.ErrorEventType, event)
}

func (s transferReflowEventSink) OnSummary(ctx context.Context, rec reflowpkg.SummaryRecord) error {
	return s.w.WriteAny(ctx, reflowpkg.SummaryRecordType, rec)
}

func checkpointAdapter(state reflowStateStore, resume bool) reflowpkg.CheckpointStore {
	if state == nil {
		return nil
	}
	return transferReflowCheckpointAdapter{state: state, resume: resume}
}

type transferReflowCheckpointAdapter struct {
	state  reflowStateStore
	resume bool
}

func (a transferReflowCheckpointAdapter) Close() error {
	if a.state == nil {
		return nil
	}
	return a.state.Close()
}

func (a transferReflowCheckpointAdapter) ItemDone(ctx context.Context, sourceURI, destURI string) (bool, string, error) {
	if !a.resume {
		return false, "", nil
	}
	return a.state.ItemDone(ctx, sourceURI, destURI)
}

func (a transferReflowCheckpointAdapter) UpsertItem(ctx context.Context, item reflowpkg.CheckpointItem) error {
	return a.state.UpsertItem(ctx, reflowstate.UpsertItemParams{
		SourceURI:    item.SourceURI,
		DestURI:      item.DestURI,
		SourceKey:    item.SourceKey,
		DestKey:      item.DestKey,
		SourceETag:   item.SourceETag,
		SourceSize:   item.SourceSize,
		Status:       item.Status,
		Bytes:        item.Bytes,
		Reason:       item.Reason,
		ErrorCode:    item.ErrorCode,
		ErrorMessage: item.ErrorMessage,
	})
}

func (a transferReflowCheckpointAdapter) DestKeyObserved(ctx context.Context, destKey string) (bool, error) {
	return a.state.DestKeyObserved(ctx, destKey)
}

func (a transferReflowCheckpointAdapter) MarkDestKeyObserved(ctx context.Context, destKey string) error {
	return a.state.MarkDestKeyObserved(ctx, destKey)
}

func (a transferReflowCheckpointAdapter) NoteDestKeySource(ctx context.Context, destKey, sourceURI, sourceETag string, sourceSize int64) error {
	return a.state.NoteDestKeySource(ctx, destKey, sourceURI, sourceETag, sourceSize)
}

func (a transferReflowCheckpointAdapter) NoteCollision(ctx context.Context, collision reflowpkg.CheckpointCollision) error {
	return a.state.NoteCollision(ctx, collision.DestKey, reflowstate.CollisionKind(collision.Kind), collision.SourceURI, collision.SourceETag, collision.SourceSize, collision.DestETag, collision.DestSize)
}
