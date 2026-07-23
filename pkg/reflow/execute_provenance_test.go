package reflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// s3ProvenanceLine is a 7-byte source that lands cleanly (no destination
// collision) with a vars block, so the sidecar payload carries source, dest, run,
// action, and vars.
const s3ProvenanceLine = `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"dest_rel_key":"a/b.xml","vars":{"site":"42"}}}`

// s3ProvenanceConflictLine is an 11-byte source used to force a genuine content
// conflict against a differently-sized destination.
const s3ProvenanceConflictLine = `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":11,"dest_rel_key":"a/b.xml"}}`

func siblingProvenancePlan() ProvenancePlan {
	return ProvenancePlan{
		Mode:         ProvenanceModeSidecar,
		Suffix:       ".gnb.json",
		OnWriteError: ProvenanceOnWriteErrorWarn,
		Placement:    ProvenancePlacementPlan{Mode: ProvenancePlacementSibling},
		RunID:        "run-abc",
		ToolVersion:  "gonimbus test",
	}
}

func provenanceCopyConfig(dst *copyMemoryProvider, sink EventSink, plan ProvenancePlan) Config {
	cfg := copyConfig(dst, sink)
	cfg.Provenance = plan
	return cfg
}

func lastRecord(t *testing.T, sink *collectSink) Record {
	t.Helper()
	require.NotEmpty(t, sink.records)
	return sink.records[len(sink.records)-1]
}

func decodeSidecar(t *testing.T, raw []byte) ProvenanceSidecarPayload {
	t.Helper()
	var payload ProvenanceSidecarPayload
	require.NoError(t, json.Unmarshal(raw, &payload))
	return payload
}

// TestEngineProvenanceSidecarOnComplete pins that a landed terminal writes a
// sibling sidecar with the shipped action, a written ref on the terminal record,
// the destination ETag/size reconstructed from the land, and the run-config echo.
func TestEngineProvenanceSidecarOnComplete(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, siblingProvenancePlan()))
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	require.Equal(t, []byte("payload"), dst.body("data/a/b.xml"), "main object landed")
	raw := dst.body("data/a/b.xml.gnb.json")
	require.NotEmpty(t, raw, "sibling sidecar landed next to the object")

	term := lastRecord(t, sink)
	require.Equal(t, "complete", term.Status)
	require.NotNil(t, term.Provenance)
	require.True(t, term.Provenance.Written)
	require.Equal(t, "data/a/b.xml.gnb.json", term.Provenance.Key)
	require.Equal(t, "s3://dest-bucket/data/a/b.xml.gnb.json", term.Provenance.URI)

	payload := decodeSidecar(t, raw)
	require.Equal(t, ProvenanceSchema, payload.Schema)
	require.Equal(t, "s3://source-bucket/a/b.xml", payload.Source.URI)
	require.Equal(t, "s3://dest-bucket/data/a/b.xml", payload.Destination.URI)
	require.Equal(t, "dest-data/a/b.xml", payload.Destination.ETag)
	require.Equal(t, int64(7), payload.Destination.Size)
	require.Equal(t, "landed", payload.Action)
	require.Equal(t, "run-abc", payload.Run.RunID)
	require.Equal(t, "gonimbus test", payload.Run.ToolVersion)
	require.Equal(t, map[string]string{"site": "42"}, payload.Vars)

	require.Equal(t, int64(1), summary.Statuses["complete"])
	require.Len(t, sink.runs, 1)
	require.NotNil(t, sink.runs[0].Provenance)
	require.Equal(t, ProvenanceModeSidecar, sink.runs[0].Provenance.Mode)
	require.Equal(t, ProvenancePlacementSibling, sink.runs[0].Provenance.Placement.Mode)
}

// TestEngineProvenanceSidecarOnDuplicateSkip pins that a duplicate skip writes the
// skipped.duplicate sidecar with the observed destination ETag/size (not the land
// path's reconstruction) and the duplicate collision block.
func TestEngineProvenanceSidecarOnDuplicateSkip(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putFixture("data/a/b.xml", "payload", "dest-etag") // same bytes -> duplicate by body compare
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, siblingProvenancePlan()))
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	raw := dst.body("data/a/b.xml.gnb.json")
	require.NotEmpty(t, raw)
	payload := decodeSidecar(t, raw)
	require.Equal(t, "skipped.duplicate", payload.Action)
	require.Equal(t, "dest-etag", payload.Destination.ETag)
	require.Equal(t, int64(7), payload.Destination.Size)
	require.NotNil(t, payload.Collision)
	require.Equal(t, "duplicate", payload.Collision.Kind)

	term := lastRecord(t, sink)
	require.Equal(t, "skipped", term.Status)
	require.Equal(t, "collision.duplicate", term.Reason)
	require.NotNil(t, term.Provenance)
	require.True(t, term.Provenance.Written)
}

// TestEngineProvenanceNoSidecarOnErrorTerminal pins the eligibility floor: an
// error terminal (fail-mode conflict) writes no sidecar object and attaches no
// provenance ref.
func TestEngineProvenanceNoSidecarOnErrorTerminal(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "new payload", "src-etag")
	dst.putFixture("data/a/b.xml", "old payload", "old-etag")
	sink := &collectSink{}
	cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
	cfg.Collision = CollisionPolicy{Mode: CollisionFail}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceConflictLine))
	var objectErr *ObjectErrorsError
	require.ErrorAs(t, err, &objectErr)

	require.Empty(t, dst.body("data/a/b.xml.gnb.json"), "no sidecar for an error terminal")
	term := lastRecord(t, sink)
	require.Equal(t, "failed", term.Status)
	require.Nil(t, term.Provenance)
}

// TestEngineProvenanceFailPolicySidecarFailure pins the fail-policy contract: under fail policy a
// sidecar write failure leaves the item failed/provenance.write_failed even though
// the main object landed — never a success ack — and emits the error event.
func TestEngineProvenanceFailPolicySidecarFailure(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putObjectErrByKey["data/a/b.xml.gnb.json"] = provider.ErrProviderUnavailable
	sink := &collectSink{}
	plan := siblingProvenancePlan()
	plan.OnWriteError = ProvenanceOnWriteErrorFail
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, plan))
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	var objectErr *ObjectErrorsError
	require.ErrorAs(t, err, &objectErr)

	require.Equal(t, []byte("payload"), dst.body("data/a/b.xml"), "the main object lands")
	require.Empty(t, dst.body("data/a/b.xml.gnb.json"), "the sidecar did not land")

	term := lastRecord(t, sink)
	require.Equal(t, "failed", term.Status)
	require.Equal(t, "provenance.write_failed", term.Reason)
	require.NotNil(t, term.Provenance)
	require.False(t, term.Provenance.Written)
	require.NotEmpty(t, sink.errs, "fail policy emits the error event")
	for _, r := range sink.records {
		require.NotEqual(t, "complete", r.Status, "no success ack after a fail-policy sidecar failure")
	}
}

// TestEngineProvenanceWarnPolicySidecarFailure pins that under warn policy a
// sidecar write failure still completes the item, emitting the warning and an
// unwritten ref.
func TestEngineProvenanceWarnPolicySidecarFailure(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putObjectErrByKey["data/a/b.xml.gnb.json"] = provider.ErrProviderUnavailable
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, siblingProvenancePlan()))
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err, "warn policy does not fail the run")

	term := lastRecord(t, sink)
	require.Equal(t, "complete", term.Status)
	require.NotNil(t, term.Provenance)
	require.False(t, term.Provenance.Written)

	require.NotEmpty(t, sink.warnings)
	var found bool
	for _, w := range sink.warnings {
		if w.Code == ProvenanceWriteFailedWarningCode {
			found = true
		}
	}
	require.True(t, found, "a PROVENANCE_WRITE_FAILED warning is emitted")
}

// TestEngineProvenanceRefusesDestWithoutObjectPutter pins the pre-I/O
// admission: an enabled live plan whose resolved sidecar authority cannot PUT is
// refused before any stream read, event emission, or destination mutation.
func TestEngineProvenanceRefusesDestWithoutObjectPutter(t *testing.T) {
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	cfg := copyConfig(newCopyMemoryProvider(), sink)
	cfg.Destination.Provider = sentinelProvider{} // no ObjectPutter
	cfg.Provenance = siblingProvenancePlan()
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	reader := &countingReader{r: strings.NewReader(s3ProvenanceLine)}
	_, runErr := runner.Run(context.Background(), RecordStreamSource{
		Records: reader,
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), "PutObject")
	require.Zero(t, reader.n, "no stream read on a refused plan")
	require.False(t, sink.emitted(), "refusal precedes any event emission")
}

// failOnDeliverySink fails a chosen event method to model an EventSink that cannot
// persist the provenance warning/error, exercising the no-false-complete rule.
type failOnDeliverySink struct {
	collectSink
	failWarn bool
	failErr  bool
}

func (s *failOnDeliverySink) OnWarning(ctx context.Context, w Warning) error {
	if s.failWarn {
		return fmt.Errorf("sink unavailable")
	}
	return s.collectSink.OnWarning(ctx, w)
}

func (s *failOnDeliverySink) OnError(ctx context.Context, e ErrorEvent) error {
	if s.failErr {
		return fmt.Errorf("sink unavailable")
	}
	return s.collectSink.OnError(ctx, e)
}

// TestEngineProvenanceWarnEventUndeliverableAbortsWithoutComplete pins the no-false-complete rule: if the
// warn-policy warning cannot be delivered, the run aborts rather than silently
// completing the item.
func TestEngineProvenanceWarnEventUndeliverableAbortsWithoutComplete(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putObjectErrByKey["data/a/b.xml.gnb.json"] = provider.ErrProviderUnavailable
	sink := &failOnDeliverySink{failWarn: true}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, siblingProvenancePlan()))
	require.NoError(t, err)

	_, runErr := runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.Error(t, runErr, "an undeliverable warning aborts the run")
	for _, r := range sink.records {
		require.NotEqual(t, "complete", r.Status, "no silent complete when the audit warning was not recorded")
	}
}

// TestEngineProvenanceSidecarTraversesLimiter pins AIMD participation: the
// sidecar PUT flows through the engine limiter's ObserveProviderResult, so a
// throttled sidecar write registers a throttle backoff on the run summary.
func TestEngineProvenanceSidecarTraversesLimiter(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putObjectErrByKey["data/a/b.xml.gnb.json"] = provider.ErrThrottled
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, siblingProvenancePlan()))
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err, "warn policy tolerates the throttled sidecar")
	require.GreaterOrEqual(t, summary.ConcurrencyThrottleBackoffs, int64(1),
		"the throttled sidecar PUT is observed by the AIMD limiter")
}

// TestEngineProvenanceSidecarLimiterRecovery pins that the sidecar PUT drives both
// halves of the AIMD loop: one sidecar throttles (a backoff), and the subsequent
// clean sidecar observations feed the additive-increase recovery — not just the
// backoff counter.
func TestEngineProvenanceSidecarLimiterRecovery(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	// Enough objects that, after the first object's sidecar throttles early (parallel
	// 4 grabs it in the first batch), the run of clean sidecar/copy observations
	// exceeds the throttle cooldown plus the additive-increase interval.
	const n = 120
	var lines []string
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("o%d.xml", i)
		src.putFixture(key, "payload", fmt.Sprintf("e%d", i))
		lines = append(lines, fmt.Sprintf(
			`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/%s","source_key":"%s","source_etag":"e%d","source_size_bytes":7,"dest_rel_key":"%s"}}`,
			key, key, i, key))
	}
	// Only the first object's sidecar throttles; the rest write cleanly, so the
	// limiter observes a throttle then a run of clean sidecar PUTs.
	dst.putObjectErrByKey["data/o0.xml.gnb.json"] = provider.ErrThrottled
	sink := &collectSink{}
	cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
	cfg.Concurrency = ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 8, CeilingReason: "test", AdaptiveEnabled: true, Floor: 1, Initial: 8}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	summary, err := runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(strings.Join(lines, "\n")),
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.NoError(t, err, "warn policy tolerates the throttled sidecar")

	require.GreaterOrEqual(t, summary.ConcurrencyThrottleBackoffs, int64(1),
		"the throttled sidecar PUT is observed as a backoff")
	require.GreaterOrEqual(t, summary.ConcurrencyAdditiveIncreases, int64(1),
		"clean sidecar (and copy) observations drive the additive-increase recovery")
	require.Empty(t, dst.body("data/o0.xml.gnb.json"), "the throttled sidecar did not land")
	require.NotEmpty(t, dst.body("data/o119.xml.gnb.json"), "later clean sidecars land")
	require.Equal(t, int64(n), summary.Statuses["complete"])
}

// throttleThenCleanPutter throttles its first throttleN PutObject calls, then
// succeeds — used to drive the limiter through backoff and recovery from sidecar
// PUTs alone.
type throttleThenCleanPutter struct {
	throttleN int
	n         int
}

func (p *throttleThenCleanPutter) PutObject(_ context.Context, _ string, body io.Reader, _ int64) error {
	p.n++
	if p.n <= p.throttleN {
		return &provider.ProviderError{Op: "PutObject", Provider: provider.ProviderS3, Err: provider.ErrThrottled}
	}
	_, _ = io.ReadAll(body)
	return nil
}

// TestLimitedProvenancePutDrivesLimiterRecovery isolates the sidecar PUT transport
// (no copy operations): a throttled sidecar PUT backs the limiter off, and clean
// sidecar PUT observations alone drive the additive-increase recovery.
func TestLimitedProvenancePutDrivesLimiterRecovery(t *testing.T) {
	limiter := NewConcurrencyLimiter(ConcurrencyConfig{RequestedCeiling: 8, EffectiveCeiling: 8, CeilingReason: "test", AdaptiveEnabled: true, Floor: 1, Initial: 8})
	putter := &throttleThenCleanPutter{throttleN: 1}
	ctx := context.Background()

	err := limitedProvenancePut(ctx, limiter, putter, "k0.gnb.json", strings.NewReader("x"), 1)
	require.Error(t, err, "the first sidecar PUT throttles")
	require.GreaterOrEqual(t, limiter.Snapshot().ConcurrencyThrottleBackoffs, int64(1),
		"the throttled sidecar PUT is observed as a backoff")

	for i := 0; i < 24; i++ {
		require.NoError(t, limitedProvenancePut(ctx, limiter, putter, fmt.Sprintf("k%d.gnb.json", i+1), strings.NewReader("x"), 1))
	}
	require.GreaterOrEqual(t, limiter.Snapshot().ConcurrencyAdditiveIncreases, int64(1),
		"clean sidecar PUT observations alone drive the additive-increase recovery")
}

// TestProvenancePlanRedactsInjectedHandle pins that the injected sidecar provider
// handle is never dumped by String/GoString (presence only), while the non-secret
// run identity stays visible — the redaction property Config carries for its own
// handles now extends to the provenance plan.
func TestProvenancePlanRedactsInjectedHandle(t *testing.T) {
	plan := siblingProvenancePlan()
	plan.Placement = ProvenancePlacementPlan{
		Mode:            ProvenancePlacementMirror,
		SidecarRoot:     &ProvenanceSidecarRoot{Provider: string(provider.ProviderFile), BaseDir: "/mirror", BaseURI: "file:///mirror/"},
		SidecarProvider: sentinelProvider{},
	}

	s := plan.String()
	require.NotContains(t, s, providerSecretSentinel, "the injected handle must never appear")
	require.Contains(t, s, "<redacted>")
	require.Contains(t, s, "run-abc", "run identity is non-secret and stays visible")
	require.Equal(t, s, plan.GoString())

	cfg := Config{
		Destination: Destination{Provider: sentinelProvider{}, ProviderID: string(provider.ProviderFile), BaseURI: "file:///out/"},
		Provenance:  plan,
	}
	require.NotContains(t, cfg.String(), providerSecretSentinel)
}

// TestEngineProvenanceSameKeyFanInCoherent pins per-key coherence: two DISTINCT sources fanning
// into the same destination key never strand a mismatched object/sidecar pair.
// The sources carry distinct bytes, so the final object content identifies the
// winner; the final sidecar's source must be exactly that winner (not merely "one
// of the two"). A swapped object/sidecar pair would fail this. Run with -race.
func TestEngineProvenanceSameKeyFanInCoherent(t *testing.T) {
	const (
		bytesOne = "aaaaaaaaaaa" // 11 bytes, s1
		bytesTwo = "bbbbbbbbbbb" // 11 bytes, s2 (distinct content, same length)
	)
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("s1/o.xml", bytesOne, "etag-1")
	src.putFixture("s2/o.xml", bytesTwo, "etag-2")
	sink := &collectSink{}
	cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
	cfg.Collision = CollisionPolicy{Mode: CollisionOverwrite}
	cfg.Concurrency = ResolveConcurrency(4, true, DefaultResourceProbe())
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	// Both inputs rewrite to the same destination key out.xml.
	lines := strings.Join([]string{
		`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/s1/o.xml","source_key":"s1/o.xml","source_etag":"etag-1","source_size_bytes":11,"dest_rel_key":"out.xml"}}`,
		`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/s2/o.xml","source_key":"s2/o.xml","source_etag":"etag-2","source_size_bytes":11,"dest_rel_key":"out.xml"}}`,
	}, "\n")
	_, err = runner.Run(context.Background(), RecordStreamSource{
		Records: strings.NewReader(lines),
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.NoError(t, err)

	raw := dst.body("data/out.xml.gnb.json")
	require.NotEmpty(t, raw)
	payload := decodeSidecar(t, raw)

	// The final sidecar must describe the SAME winning operation as the final
	// object: map the final object bytes back to the source that produced them and
	// require the sidecar's source to match it exactly.
	wantSource := map[string]string{
		bytesOne: "s3://source-bucket/s1/o.xml",
		bytesTwo: "s3://source-bucket/s2/o.xml",
	}[string(dst.body("data/out.xml"))]
	require.NotEmpty(t, wantSource, "final object must be one of the two known payloads")
	require.Equal(t, wantSource, payload.Source.URI, "final sidecar source must match the final landed object")
	require.Equal(t, int64(len(dst.body("data/out.xml"))), payload.Destination.Size)
}

// TestEngineProvenanceRecoveryMatrix pins the fail-policy recovery matrix: after
// a run lands the object but fails the sidecar (item failed/provenance.write_failed),
// a resume against the now-existing destination repairs — or does not repair — the
// sidecar according to the collision mode, with the second-run data-object mutation
// count and terminal asserted.
// lastCheckpointItem returns the most recent terminal item the store recorded for
// destKey, so a test asserts the durably-persisted status/reason (the resume
// authority), not merely the emitted record.
func lastCheckpointItem(t *testing.T, ckpt *memCheckpoint, destKey string) CheckpointItem {
	t.Helper()
	ckpt.mu.Lock()
	defer ckpt.mu.Unlock()
	for i := len(ckpt.items) - 1; i >= 0; i-- {
		if ckpt.items[i].DestKey == destKey {
			return ckpt.items[i]
		}
	}
	t.Fatalf("no checkpoint item recorded for %s", destKey)
	return CheckpointItem{}
}

func TestEngineProvenanceRecoveryMatrix(t *testing.T) {
	const (
		mainKey    = "data/a/b.xml"
		sidecarKey = "data/a/b.xml.gnb.json"
		srcKey     = "a/b.xml"
	)
	// Fixed timestamps: tOld < now (run-1 copy time) < tFuture. tOld/tPast are
	// before the run-1 copy time; tFuture is after it.
	tOld := time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	tPast := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tFuture := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	inputLine := func(body, etag string, lastMod time.Time) string {
		data := map[string]any{
			"source_uri":        "s3://source-bucket/" + srcKey,
			"source_key":        srcKey,
			"source_etag":       etag,
			"source_size_bytes": len(body),
			"dest_rel_key":      srcKey,
		}
		if !lastMod.IsZero() {
			data["source_last_modified"] = lastMod.UTC().Format(time.RFC3339Nano)
		}
		b, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
		require.NoError(t, err)
		return string(b)
	}
	// runOnce drives one fail-policy provenance run of a single source object against
	// the shared dst + checkpoint. failSidecar injects the sidecar write failure.
	runOnce := func(t *testing.T, dst *copyMemoryProvider, ckpt CheckpointStore, mode, body, etag string, lastMod time.Time, failSidecar bool) (*collectSink, error) {
		t.Helper()
		src := newCopyMemoryProvider()
		if lastMod.IsZero() {
			src.putFixture(srcKey, body, etag)
		} else {
			src.putFixtureAt(srcKey, body, etag, lastMod)
		}
		if failSidecar {
			dst.putObjectErrByKey[sidecarKey] = provider.ErrProviderUnavailable
		} else {
			delete(dst.putObjectErrByKey, sidecarKey)
		}
		sink := &collectSink{}
		plan := siblingProvenancePlan()
		plan.OnWriteError = ProvenanceOnWriteErrorFail
		cfg := provenanceCopyConfig(dst, sink, plan)
		cfg.Collision = CollisionPolicy{Mode: mode}
		cfg.Checkpoint = ckpt
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, runErr := runner.Run(context.Background(), copySource(src, inputLine(body, etag, lastMod)))
		return sink, runErr
	}
	// requireRun1Failed asserts the shared run-1 premise: the object lands once and
	// the fail-policy sidecar failure persists a durable failed/provenance.write_failed
	// terminal (the resume authority) — never a success status.
	requireRun1Failed := func(t *testing.T, dst *copyMemoryProvider, ckpt *memCheckpoint, sink *collectSink, err error) {
		t.Helper()
		var objErr *ObjectErrorsError
		require.ErrorAs(t, err, &objErr)
		require.Equal(t, "provenance.write_failed", lastRecord(t, sink).Reason)
		require.Empty(t, dst.body(sidecarKey), "run 1 sidecar did not land")
		item := lastCheckpointItem(t, ckpt, mainKey)
		require.Equal(t, "failed", item.Status, "run 1 persists a failed terminal, never complete")
		require.Equal(t, "provenance.write_failed", item.Reason)
		require.Equal(t, 1, dst.writeCount(mainKey), "run 1 lands the object exactly once")
	}
	// run1 runs the shared run-1 premise (land + fail the sidecar) and asserts it.
	run1 := func(t *testing.T, dst *copyMemoryProvider, ckpt *memCheckpoint, mode, body, etag string, lastMod time.Time) {
		t.Helper()
		sink, err := runOnce(t, dst, ckpt, mode, body, etag, lastMod, true)
		requireRun1Failed(t, dst, ckpt, sink, err)
	}
	// requireRun2 asserts the full second-run outcome: the emitted terminal, the
	// DURABLE persisted checkpoint terminal (the resume authority), the exact
	// main-object land count, and the sidecar presence/ref (repaired or not).
	requireRun2 := func(t *testing.T, dst *copyMemoryProvider, ckpt *memCheckpoint, sink *collectSink, wantStatus, wantReason string, wantLands int, repaired bool) {
		t.Helper()
		term := lastRecord(t, sink)
		require.Equal(t, wantStatus, term.Status)
		require.Equal(t, wantReason, term.Reason)
		secondCkpt := lastCheckpointItem(t, ckpt, mainKey)
		require.Equal(t, wantStatus, secondCkpt.Status, "second-run persisted checkpoint status")
		// The durable checkpoint reason now equals the emitted record reason on every
		// row (the emitted-vs-durable split is closed): a failed collision persists
		// the specific reason (e.g. collision.exists.duplicate), not the coarse class.
		require.Equal(t, wantReason, secondCkpt.Reason,
			"second-run persisted checkpoint reason equals the emitted reason")
		if wantStatus == "failed" {
			// error_code still carries the class, and the sanitized cause is retained,
			// so the durable failed terminal stays fully diagnosable.
			require.Equal(t, "INTERNAL", secondCkpt.ErrorCode, "durable failed error_code (class, literal)")
			require.NotEmpty(t, secondCkpt.ErrorMessage, "durable failed terminal records a sanitized cause")
		} else {
			require.Empty(t, secondCkpt.ErrorCode, "non-failed terminal carries no durable error code")
		}
		require.Equal(t, wantLands, dst.writeCount(mainKey), "second-run main-object land count")
		if repaired {
			require.NotNil(t, term.Provenance)
			require.True(t, term.Provenance.Written, "sidecar repaired")
			require.NotEmpty(t, dst.body(sidecarKey))
		} else {
			require.Nil(t, term.Provenance, "no sidecar ref on a non-repairing terminal")
			require.Empty(t, dst.body(sidecarKey), "sidecar not repaired")
		}
	}

	// The matrix is keyed by collision mode x the reachable second-run resolver
	// outcome. Each row: run 1 lands + fails the sidecar; run 2 re-drives against the
	// now-existing object and asserts the exact terminal, main-object land delta, and
	// sidecar repair.
	t.Run("skip-if-duplicate / proven duplicate -> repair, 0 land", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		run1(t, dst, ckpt, CollisionSkipIfDuplicate, "v1", "e1", time.Time{})
		sink2, err2 := runOnce(t, dst, ckpt, CollisionSkipIfDuplicate, "v1", "e1", time.Time{}, false)
		require.NoError(t, err2)
		requireRun2(t, dst, ckpt, sink2, "skipped", "collision.duplicate", 1, true)
	})

	t.Run("skip-if-duplicate / non-duplicate -> conflict, no repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		run1(t, dst, ckpt, CollisionSkipIfDuplicate, "v1", "e1", time.Time{})
		// Source changed between runs (different bytes AND etag) -> defeats both the
		// ETag and body-equality duplicate branches.
		sink2, err2 := runOnce(t, dst, ckpt, CollisionSkipIfDuplicate, "v2-different", "e2", time.Time{}, false)
		var objErr *ObjectErrorsError
		require.ErrorAs(t, err2, &objErr)
		requireRun2(t, dst, ckpt, sink2, "failed", "collision.exists.conflict", 1, false)
	})

	t.Run("overwrite -> re-land, +1 land, repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		dst.putFixture(mainKey, "stale", "stale-etag")
		run1(t, dst, ckpt, CollisionOverwrite, "v1", "e1", time.Time{})
		sink2, err2 := runOnce(t, dst, ckpt, CollisionOverwrite, "v1", "e1", time.Time{}, false)
		require.NoError(t, err2)
		requireRun2(t, dst, ckpt, sink2, "complete", "", 2, true)
	})

	t.Run("fail / duplicate -> failed, no repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		run1(t, dst, ckpt, CollisionFail, "v1", "e1", time.Time{})
		sink2, err2 := runOnce(t, dst, ckpt, CollisionFail, "v1", "e1", time.Time{}, false)
		var objErr *ObjectErrorsError
		require.ErrorAs(t, err2, &objErr)
		requireRun2(t, dst, ckpt, sink2, "failed", "collision.exists.duplicate", 1, false)
	})

	t.Run("fail / non-duplicate -> conflict, no repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		run1(t, dst, ckpt, CollisionFail, "v1", "e1", time.Time{})
		sink2, err2 := runOnce(t, dst, ckpt, CollisionFail, "v2-different", "e2", time.Time{}, false)
		var objErr *ObjectErrorsError
		require.ErrorAs(t, err2, &objErr)
		requireRun2(t, dst, ckpt, sink2, "failed", "collision.exists.conflict", 1, false)
	})

	// Source-newer rows: run 1 conditionally overwrites a pre-existing older object,
	// which stamps the destination LastModified so run 2's timestamp comparison is
	// decidable. The pre-existing dst is placed via putFixtureAt (not counted).
	sourceNewerRun1 := func(t *testing.T, dst *copyMemoryProvider, ckpt *memCheckpoint) {
		dst.putFixtureAt(mainKey, "older-dst", "old-etag", tOld)
		sink1, err1 := runOnce(t, dst, ckpt, CollisionOverwriteIfSourceNewer, "v1", "e1", tPast, true)
		requireRun1Failed(t, dst, ckpt, sink1, err1)
	}

	t.Run("source-newer / proven duplicate -> repair, 0 land", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		sourceNewerRun1(t, dst, ckpt)
		sink2, err2 := runOnce(t, dst, ckpt, CollisionOverwriteIfSourceNewer, "v1", "e1", tPast, false)
		require.NoError(t, err2)
		requireRun2(t, dst, ckpt, sink2, "skipped", "collision.duplicate", 1, true)
	})

	t.Run("source-newer / non-dup still-newer -> re-land, +1, repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		sourceNewerRun1(t, dst, ckpt)
		// Changed bytes, timestamp still after the run-1 copy time -> conditional re-land.
		sink2, err2 := runOnce(t, dst, ckpt, CollisionOverwriteIfSourceNewer, "v2-different", "e2", tFuture, false)
		require.NoError(t, err2)
		requireRun2(t, dst, ckpt, sink2, "complete", "", 2, true)
	})

	t.Run("source-newer / non-dup source-older -> skip, no repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		sourceNewerRun1(t, dst, ckpt)
		// Changed bytes, timestamp before the run-1 copy time -> preserve dst, skip.
		sink2, err2 := runOnce(t, dst, ckpt, CollisionOverwriteIfSourceNewer, "v2-different", "e2", tPast, false)
		require.NoError(t, err2)
		requireRun2(t, dst, ckpt, sink2, "skipped", "collision.skipped_src_older", 1, false)
	})

	t.Run("source-newer / non-dup concurrent mutation -> skip, no repair", func(t *testing.T) {
		dst, ckpt := newCopyMemoryProvider(), newMemCheckpoint()
		sourceNewerRun1(t, dst, ckpt)
		// A dest mutation between the head and the If-Match conditional PUT yields a
		// concurrent-mutation skip on an otherwise-newer changed source.
		dst.mutateBeforeIfMatch = true
		sink2, err2 := runOnce(t, dst, ckpt, CollisionOverwriteIfSourceNewer, "v2-different", "e2", tFuture, false)
		require.NoError(t, err2)
		requireRun2(t, dst, ckpt, sink2, "skipped", "collision.skipped_concurrent_mutation", 1, false)
	})
}

// TestEngineProvenanceEligibilityNegativesLive pins that the ineligible skip/resume
// terminals write no sidecar and attach no ref: source-older and
// concurrent-mutation source-newer skips, and a resume skip.
func TestEngineProvenanceEligibilityNegativesLive(t *testing.T) {
	t.Run("source-older skip", func(t *testing.T) {
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixtureAt("a/b.xml", "payload", "src-etag", time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC))
		dst.putFixtureAt("data/a/b.xml", "older-dst", "dest-etag", time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC))
		sink := &collectSink{}
		cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":7,"source_last_modified":"2026-01-14T00:00:00Z","dest_rel_key":"a/b.xml"}}`
		_, err = runner.Run(context.Background(), copySource(src, line))
		require.NoError(t, err)
		term := lastRecord(t, sink)
		require.Equal(t, "skipped", term.Status)
		require.Equal(t, "collision.skipped_src_older", term.Reason)
		require.Nil(t, term.Provenance)
		require.Empty(t, dst.body("data/a/b.xml.gnb.json"))
	})

	t.Run("concurrent-mutation skip", func(t *testing.T) {
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixtureAt("a/b.xml", "brand-new", "src-etag", time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC))
		dst.putFixtureAt("data/a/b.xml", "older-dst", "dest-etag", time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC))
		dst.mutateBeforeIfMatch = true
		sink := &collectSink{}
		cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwriteIfSourceNewer}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"src-etag","source_size_bytes":9,"source_last_modified":"2026-01-20T00:00:00Z","dest_rel_key":"a/b.xml"}}`
		_, err = runner.Run(context.Background(), copySource(src, line))
		require.NoError(t, err)
		term := lastRecord(t, sink)
		require.Equal(t, "skipped", term.Status)
		require.Equal(t, "collision.skipped_concurrent_mutation", term.Reason)
		require.Nil(t, term.Provenance)
		require.Empty(t, dst.body("data/a/b.xml.gnb.json"))
	})

	t.Run("resume skip", func(t *testing.T) {
		src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
		src.putFixture("a/b.xml", "payload", "etag-a")
		ckpt := newMemCheckpoint()
		ckpt.markDone("s3://source-bucket/a/b.xml", "s3://dest-bucket/data/a/b.xml", "complete")
		sink := &collectSink{}
		cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
		cfg.Checkpoint = ckpt
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
		require.NoError(t, err)
		term := lastRecord(t, sink)
		require.Equal(t, "skipped", term.Status)
		require.Equal(t, "resume.complete", term.Reason)
		require.Nil(t, term.Provenance)
		require.Empty(t, dst.body("data/a/b.xml.gnb.json"))
	})
}

// TestEngineProvenanceOverwriteDestETagPinned pins the overwrite-complete sidecar
// destination block explicitly: an overwrite land yields no post-PUT
// ETag on either path, so the sidecar records an empty dest ETag — asserted
// non-vacuously (the sidecar is written and the size is the landed byte count).
func TestEngineProvenanceOverwriteDestETagPinned(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putFixture("data/a/b.xml", "old-payload", "old-etag")
	sink := &collectSink{}
	cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
	cfg.Collision = CollisionPolicy{Mode: CollisionOverwrite}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	payload := decodeSidecar(t, dst.body("data/a/b.xml.gnb.json"))
	require.Equal(t, "landed", payload.Action)
	require.Empty(t, payload.Destination.ETag, "overwrite land surfaces no post-PUT ETag (parity-consistent)")
	require.Equal(t, int64(7), payload.Destination.Size)
}

// blockingSidecarProvider blocks the sidecar PUT (keyed by suffix) until the run
// context is cancelled, modelling a cancellation in the post-land / pre-terminal
// window. The main object's conditional PUT is unaffected (it uses PutObjectConditional).
type blockingSidecarProvider struct {
	*copyMemoryProvider
	suffix  string
	reached chan struct{}
}

func (p *blockingSidecarProvider) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	if strings.HasSuffix(key, p.suffix) {
		close(p.reached)
		<-ctx.Done()
		return ctx.Err()
	}
	return p.copyMemoryProvider.PutObject(ctx, key, body, size)
}

// TestEngineProvenanceCancellationWindow pins the cancellation window: cancelling after the main object
// lands while the sidecar PUT is pending yields no complete, no success ack, and no
// Written=true under fail policy.
func TestEngineProvenanceCancellationWindow(t *testing.T) {
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := &blockingSidecarProvider{copyMemoryProvider: newCopyMemoryProvider(), suffix: ".gnb.json", reached: make(chan struct{})}
	sink := &collectSink{}
	plan := siblingProvenancePlan()
	plan.OnWriteError = ProvenanceOnWriteErrorFail
	cfg := provenanceCopyConfig(dst.copyMemoryProvider, sink, plan)
	cfg.Destination.Provider = dst
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, copySource(src, s3ProvenanceLine))
		done <- runErr
	}()

	<-dst.reached // the main object has landed; the sidecar PUT is now pending
	require.Equal(t, "payload", string(dst.body("data/a/b.xml")), "main object landed before cancellation")
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
	for _, r := range sink.records {
		require.NotEqual(t, "complete", r.Status, "no complete in the cancellation window")
		if r.Provenance != nil {
			require.False(t, r.Provenance.Written, "no false Written=true after a cancelled sidecar")
		}
	}
	require.Empty(t, dst.body("data/a/b.xml.gnb.json"), "sidecar never landed")
}

// barrierSidecarProvider blocks the FIRST sidecar PUT until release is closed,
// signalling reached, so a test can force the interleaving that would strand a
// mismatched object/sidecar pair without the per-key gate: worker A lands then
// blocks inside its sidecar PUT while worker B contends for the same key.
type barrierSidecarProvider struct {
	*copyMemoryProvider
	suffix  string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *barrierSidecarProvider) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	if strings.HasSuffix(key, p.suffix) {
		first := false
		p.once.Do(func() { first = true })
		if first {
			close(p.reached)
			<-p.release
		}
	}
	return p.copyMemoryProvider.PutObject(ctx, key, body, size)
}

func waitErr(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case e := <-done:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("run did not complete")
		return nil
	}
}

func checkpointItems(ckpt *memCheckpoint) []CheckpointItem {
	ckpt.mu.Lock()
	defer ckpt.mu.Unlock()
	return append([]CheckpointItem(nil), ckpt.items...)
}

func twoSourceLines(destRel string) string {
	l1 := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/s1/o.xml","source_key":"s1/o.xml","source_etag":"e1","source_size_bytes":4,"dest_rel_key":"` + destRel + `"}}`
	l2 := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/s2/o.xml","source_key":"s2/o.xml","source_etag":"e2","source_size_bytes":4,"dest_rel_key":"` + destRel + `"}}`
	return l1 + "\n" + l2
}

// TestEngineProvenanceD1EngineeredBarrier pins D1 deterministically: with a
// barrier that pauses worker A inside its sidecar PUT after its main land, worker
// B — targeting the same destination key — cannot land the main object, reach its
// sidecar, or terminate until A releases the per-key gate. Without the gate,
// main(A)→main(B)→sidecar(B)→sidecar(A) would strand a mismatched pair.
func TestEngineProvenanceD1EngineeredBarrier(t *testing.T) {
	// terminalsFor returns the persisted terminal checkpoint items for destKey, in
	// order — the mutex-guarded checkpoint is safe to read mid-run (the sink is not).
	terminalsFor := func(ckpt *memCheckpoint, destKey string) []CheckpointItem {
		var out []CheckpointItem
		for _, it := range checkpointItems(ckpt) {
			if it.DestKey == destKey {
				out = append(out, it)
			}
		}
		return out
	}

	t.Run("overwrite: B cannot overtake A's held gate", func(t *testing.T) {
		src := newCopyMemoryProvider()
		src.putFixture("s1/o.xml", "AAAA", "e1")
		src.putFixture("s2/o.xml", "BBBB", "e2")
		base := newCopyMemoryProvider()
		dst := &barrierSidecarProvider{copyMemoryProvider: base, suffix: ".gnb.json", reached: make(chan struct{}), release: make(chan struct{})}
		ckpt := newMemCheckpoint()
		sink := &collectSink{}
		cfg := provenanceCopyConfig(base, sink, siblingProvenancePlan())
		cfg.Destination.Provider = dst
		cfg.Checkpoint = ckpt
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwrite}
		cfg.Concurrency = ResolveConcurrency(4, true, DefaultResourceProbe())
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			_, e := runner.Run(context.Background(), RecordStreamSource{
				Records: strings.NewReader(twoSourceLines("out.xml")),
				Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
			})
			done <- e
		}()

		<-dst.reached // A landed the main object and is blocked in its sidecar PUT.
		require.Equal(t, 1, base.writeCount("data/out.xml"),
			"while A holds the per-key gate, B cannot land the main object")
		require.Empty(t, terminalsFor(ckpt, "data/out.xml"),
			"neither A (blocked pre-terminal) nor B (blocked on the gate) has persisted a strict terminal checkpoint")
		close(dst.release)
		require.NoError(t, waitErr(t, done))

		require.Equal(t, 2, base.writeCount("data/out.xml"), "both sources land, serialized by the gate")
		terminals := terminalsFor(ckpt, "data/out.xml")
		require.Len(t, terminals, 2, "both serialized terminals persisted after release")
		// Overwrite fan-in persists the exact ordered sequence [complete, complete]
		// (both with an empty reason): the gate holder lands, then the second source
		// re-lands over it.
		require.Equal(t, "complete", terminals[0].Status, "first serialized terminal (gate holder) completes")
		require.Empty(t, terminals[0].Reason)
		require.Equal(t, "complete", terminals[1].Status)
		require.Empty(t, terminals[1].Reason)
		payload := decodeSidecar(t, base.body("data/out.xml.gnb.json"))
		wantSource := map[string]string{
			"AAAA": "s3://source-bucket/s1/o.xml",
			"BBBB": "s3://source-bucket/s2/o.xml",
		}[string(base.body("data/out.xml"))]
		require.Equal(t, "landed", payload.Action)
		require.Equal(t, wantSource, payload.Source.URI, "final sidecar describes the final landed object")
	})

	t.Run("duplicate fan-in: final sidecar is a truthful skipped.duplicate", func(t *testing.T) {
		src := newCopyMemoryProvider()
		src.putFixture("s1/o.xml", "SAME", "e1")
		src.putFixture("s2/o.xml", "SAME", "e2")
		base := newCopyMemoryProvider()
		dst := &barrierSidecarProvider{copyMemoryProvider: base, suffix: ".gnb.json", reached: make(chan struct{}), release: make(chan struct{})}
		ckpt := newMemCheckpoint()
		sink := &collectSink{}
		cfg := provenanceCopyConfig(base, sink, siblingProvenancePlan())
		cfg.Destination.Provider = dst
		cfg.Checkpoint = ckpt
		cfg.Collision = CollisionPolicy{Mode: CollisionSkipIfDuplicate}
		cfg.Concurrency = ResolveConcurrency(4, true, DefaultResourceProbe())
		runner, err := NewRunner(cfg)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			_, e := runner.Run(context.Background(), RecordStreamSource{
				Records: strings.NewReader(twoSourceLines("out.xml")),
				Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
			})
			done <- e
		}()
		<-dst.reached
		require.Equal(t, 1, base.writeCount("data/out.xml"))
		require.Empty(t, terminalsFor(ckpt, "data/out.xml"),
			"no strict terminal checkpoint is overtaken while A holds the gate")
		close(dst.release)
		require.NoError(t, waitErr(t, done))

		require.Equal(t, 1, base.writeCount("data/out.xml"), "the duplicate never re-lands")
		terminals := terminalsFor(ckpt, "data/out.xml")
		require.Len(t, terminals, 2, "the landed and the duplicate-skip terminals both persist, serialized")
		// Duplicate fan-in persists the exact ordered sequence
		// [complete, skipped/collision.duplicate]: the gate holder lands, then the
		// proven-duplicate second source skips.
		require.Equal(t, "complete", terminals[0].Status, "first serialized terminal (gate holder) lands complete")
		require.Empty(t, terminals[0].Reason)
		require.Equal(t, "skipped", terminals[1].Status)
		require.Equal(t, "collision.duplicate", terminals[1].Reason)
		payload := decodeSidecar(t, base.body("data/out.xml.gnb.json"))
		require.Equal(t, "skipped.duplicate", payload.Action, "the last serialized observation is the duplicate skip")
		require.Equal(t, "SAME", string(base.body("data/out.xml")), "sidecar source bytes equal the final object")
	})
}

// TestEngineProvenanceCancellationResume pins D3 with durable state: cancelling
// after the main land while the sidecar is blocked leaves no strict-success
// checkpoint; the resume then repairs through a named recovery-matrix cell
// (skip-if-duplicate proven duplicate) at zero extra land.
func TestEngineProvenanceCancellationResume(t *testing.T) {
	base := newCopyMemoryProvider()
	ckpt := newMemCheckpoint()
	plan := siblingProvenancePlan()
	plan.OnWriteError = ProvenanceOnWriteErrorFail

	// Run 1: block the sidecar; cancel after the main object lands.
	src1 := newCopyMemoryProvider()
	src1.putFixture("a/b.xml", "payload", "etag-a")
	blocking := &blockingSidecarProvider{copyMemoryProvider: base, suffix: ".gnb.json", reached: make(chan struct{})}
	sink1 := &collectSink{}
	cfg1 := provenanceCopyConfig(base, sink1, plan)
	cfg1.Destination.Provider = blocking
	cfg1.Checkpoint = ckpt
	cfg1.Collision = CollisionPolicy{Mode: CollisionSkipIfDuplicate}
	runner1, err := NewRunner(cfg1)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := runner1.Run(ctx, copySource(src1, s3ProvenanceLine))
		done <- e
	}()
	<-blocking.reached
	require.Equal(t, "payload", string(base.body("data/a/b.xml")), "main object landed before cancellation")
	cancel()
	_ = waitErr(t, done) // run 1 returns a cancellation/object error; the assertions below are the contract

	for _, it := range checkpointItems(ckpt) {
		if it.DestKey == "data/a/b.xml" {
			require.NotEqual(t, "complete", it.Status, "no strict-success checkpoint in the cancellation window")
		}
	}
	require.Empty(t, base.body("data/a/b.xml.gnb.json"), "sidecar never landed on run 1")

	// Run 2: resume against the now-existing object with a working sidecar provider
	// (base directly). The proven-duplicate re-drive repairs at zero extra land.
	src2 := newCopyMemoryProvider()
	src2.putFixture("a/b.xml", "payload", "etag-a")
	sink2 := &collectSink{}
	cfg2 := provenanceCopyConfig(base, sink2, plan)
	cfg2.Checkpoint = ckpt
	cfg2.Collision = CollisionPolicy{Mode: CollisionSkipIfDuplicate}
	runner2, err := NewRunner(cfg2)
	require.NoError(t, err)
	_, err = runner2.Run(context.Background(), copySource(src2, s3ProvenanceLine))
	require.NoError(t, err)

	term := lastRecord(t, sink2)
	require.Equal(t, "skipped", term.Status)
	require.Equal(t, "collision.duplicate", term.Reason)
	require.True(t, term.Provenance.Written, "sidecar repaired on resume")
	require.NotEmpty(t, base.body("data/a/b.xml.gnb.json"))
	require.Equal(t, 1, base.writeCount("data/a/b.xml"), "resume repairs at zero extra land")
	resumeCkpt := lastCheckpointItem(t, ckpt, "data/a/b.xml")
	require.Equal(t, "skipped", resumeCkpt.Status, "the resume persists the durable skipped terminal")
	require.Equal(t, "collision.duplicate", resumeCkpt.Reason,
		"the resume persists the duplicate reason, not only the skipped status")
}

// TestEngineProvenanceStaleReplacementSidecar documents the failed-replacement
// limitation: the sidecar is an unconditional PUT with no invalidation, so when
// source A lands key K and writes sidecar(A), then source B overwrites K but B's
// replacement sidecar write fails, the prior sidecar still describes A while the
// object is B. Coherence holds for a *successful* write; a *failed* replacement
// leaves the prior (now stale) sidecar — under warn the item still completes;
// under fail it is failed and repaired on resume. This pins the honest contract
// the guide and changelog state (not an absolute "same operation" guarantee).
func TestEngineProvenanceStaleReplacementSidecar(t *testing.T) {
	const sidecarKey = "data/out.xml.gnb.json"
	line := func(srcKey, etag string) string {
		return `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/` + srcKey +
			`","source_key":"` + srcKey + `","source_etag":"` + etag + `","source_size_bytes":4,"dest_rel_key":"out.xml"}}`
	}
	runOne := func(t *testing.T, base *copyMemoryProvider, srcKey, body, etag, policy string, failSidecar bool) (*collectSink, error) {
		t.Helper()
		src := newCopyMemoryProvider()
		src.putFixture(srcKey, body, etag)
		if failSidecar {
			base.putObjectErrByKey[sidecarKey] = provider.ErrProviderUnavailable
		} else {
			delete(base.putObjectErrByKey, sidecarKey)
		}
		plan := siblingProvenancePlan()
		plan.OnWriteError = policy
		cfg := provenanceCopyConfig(base, &collectSink{}, plan)
		sink := cfg.Events.(*collectSink)
		cfg.Collision = CollisionPolicy{Mode: CollisionOverwrite}
		runner, err := NewRunner(cfg)
		require.NoError(t, err)
		_, runErr := runner.Run(context.Background(), copySource(src, line(srcKey, etag)))
		return sink, runErr
	}

	for _, policy := range []string{ProvenanceOnWriteErrorWarn, ProvenanceOnWriteErrorFail} {
		t.Run(policy, func(t *testing.T) {
			base := newCopyMemoryProvider()

			// A lands + sidecar succeeds.
			_, errA := runOne(t, base, "s1/o.xml", "AAAA", "eA", policy, false)
			require.NoError(t, errA)
			require.Equal(t, "s3://source-bucket/s1/o.xml", decodeSidecar(t, base.body(sidecarKey)).Source.URI)

			// B overwrites the object but its replacement sidecar write fails.
			sinkB, errB := runOne(t, base, "s2/o.xml", "BBBB", "eB", policy, true)
			if policy == ProvenanceOnWriteErrorWarn {
				require.NoError(t, errB, "warn tolerates the failed replacement sidecar")
				require.Equal(t, "complete", lastRecord(t, sinkB).Status)
			} else {
				var objErr *ObjectErrorsError
				require.ErrorAs(t, errB, &objErr)
				require.Equal(t, "provenance.write_failed", lastRecord(t, sinkB).Reason)
			}

			require.Equal(t, "BBBB", string(base.body("data/out.xml")), "the final object is B")
			require.Equal(t, "s3://source-bucket/s1/o.xml", decodeSidecar(t, base.body(sidecarKey)).Source.URI,
				"a failed replacement leaves the prior sidecar; it is stale (describes A), not current")
		})
	}
}

// TestEngineProvenanceRefusesSelfAssertedCrossBucket pins the authority-integrity
// gate against a caller-controlled boolean: an object-store mirrored root that
// self-asserts SameBucketAsDest=true but names a different bucket than the actual
// destination is refused before any stream read, event, probe, or mutation — the
// assertion is proven against the resolved destination, not trusted.
func TestEngineProvenanceRefusesSelfAssertedCrossBucket(t *testing.T) {
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst := newGCSCapabilityProvider()
	sink := &collectSink{}
	cfg := gcsCopyConfig(dst, sink) // destination bucket is dest-bucket
	plan := siblingProvenancePlan()
	plan.Placement = ProvenancePlacementPlan{
		Mode: ProvenancePlacementMirror,
		// Lies: claims same-bucket but names other-bucket.
		SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderGCS), Bucket: "other-bucket", Prefix: "p/", BaseURI: "gs://other-bucket/p/", SameBucketAsDest: true},
	}
	cfg.Provenance = plan
	// The plain plan validation trusts the boolean and passes; the destination-aware
	// check is what refuses it.
	require.NoError(t, plan.Validate())
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	reader := &countingReader{r: strings.NewReader(s3ProvenanceLine)}
	_, runErr := runner.Run(context.Background(), RecordStreamSource{
		Records: reader,
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), "cross-bucket")
	require.Zero(t, reader.n, "no stream read on a refused authority")
	require.False(t, sink.emitted(), "refusal precedes any event emission")
	require.Empty(t, dst.preconditions(), "no destination probe/mutation on a refused authority")
}

// TestEngineProvenanceGCSMirrorSameBucketWritesUnderRoot pins the live same-bucket
// GCS mirrored-root path: the sidecar lands under the root prefix (not adjacent),
// and its key/URI/write-handle agree.
func TestEngineProvenanceGCSMirrorSameBucketWritesUnderRoot(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newGCSCapabilityProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	cfg := gcsCopyConfig(dst, sink)
	plan := siblingProvenancePlan()
	plan.Placement = ProvenancePlacementPlan{
		Mode:        ProvenancePlacementMirror,
		SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderGCS), Bucket: "dest-bucket", Prefix: "prov/", BaseURI: "gs://dest-bucket/prov/", SameBucketAsDest: true},
	}
	cfg.Provenance = plan
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	require.NotEmpty(t, dst.body("prov/a/b.xml.gnb.json"), "sidecar lands under the mirrored root prefix")
	require.Empty(t, dst.body("data/a/b.xml.gnb.json"), "sidecar must not fall back to adjacent placement")
	term := lastRecord(t, sink)
	require.True(t, term.Provenance.Written)
	require.Equal(t, "prov/a/b.xml.gnb.json", term.Provenance.Key)
	require.Equal(t, "gs://dest-bucket/prov/a/b.xml.gnb.json", term.Provenance.URI, "URI rendered from the write authority")
}

// fileMirrorPlan places sidecars under an injected file mirrored-root distinct
// from the destination handle.
func fileMirrorPlan(sidecarProvider provider.Provider) ProvenancePlan {
	p := siblingProvenancePlan()
	p.Placement = ProvenancePlacementPlan{
		Mode:            ProvenancePlacementMirror,
		SidecarRoot:     &ProvenanceSidecarRoot{Provider: string(provider.ProviderFile), BaseDir: "/mirror", BaseURI: "file:///mirror/"},
		SidecarProvider: sidecarProvider,
	}
	return p
}

// TestEngineProvenanceFileMirrorWritesThroughInjectedProvider pins file mirrored-root authority: a file
// mirrored root writes the sidecar through its injected second provider (not the
// destination handle) and renders the URI from that authority — the two cannot
// diverge.
func TestEngineProvenanceFileMirrorWritesThroughInjectedProvider(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	mirror := newCopyMemoryProvider()
	mirror.providerType = provider.ProviderFile
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, fileMirrorPlan(mirror)))
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	require.Empty(t, dst.body("data/a/b.xml.gnb.json"), "sidecar must not land in the destination handle")
	require.NotEmpty(t, mirror.body("a/b.xml.gnb.json"), "sidecar lands under the mirrored file root, rel-keyed")

	term := lastRecord(t, sink)
	require.True(t, term.Provenance.Written)
	require.Equal(t, "file:///mirror/a/b.xml.gnb.json", term.Provenance.URI, "URI rendered from the write authority")
}

// TestEngineProvenanceFileMirrorWithoutProviderRefused pins the authority-integrity
// guard: a file mirrored root whose second provider was not injected is refused
// pre-I/O rather than silently writing through the destination handle and
// stranding the file:// URI.
func TestEngineProvenanceFileMirrorWithoutProviderRefused(t *testing.T) {
	src := newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(newCopyMemoryProvider(), sink, fileMirrorPlan(nil)))
	require.NoError(t, err)

	reader := &countingReader{r: strings.NewReader(s3ProvenanceLine)}
	_, runErr := runner.Run(context.Background(), RecordStreamSource{
		Records: reader,
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.Error(t, runErr)
	require.Zero(t, reader.n, "no stream read on a refused plan")
	require.False(t, sink.emitted(), "refusal precedes any event emission")
}

// TestEngineProvenanceFailEventUndeliverableAborts pins the fail-policy half:
// if the fail-policy error event cannot be delivered, the run aborts and no
// success terminal is emitted.
func TestEngineProvenanceFailEventUndeliverableAborts(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putObjectErrByKey["data/a/b.xml.gnb.json"] = provider.ErrProviderUnavailable
	sink := &failOnDeliverySink{failErr: true}
	plan := siblingProvenancePlan()
	plan.OnWriteError = ProvenanceOnWriteErrorFail
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, plan))
	require.NoError(t, err)

	_, runErr := runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.Error(t, runErr, "an undeliverable fail-policy error event aborts the run")
	for _, r := range sink.records {
		require.NotEqual(t, "complete", r.Status)
	}
}

// TestEngineProvenanceWarnFailureRedactsCredentialCause pins redaction on the engine
// path: a sidecar write failure whose provider cause carries credential material
// is redacted before it reaches the warning event.
func TestEngineProvenanceWarnFailureRedactsCredentialCause(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	dst.putObjectErrByKey["data/a/b.xml.gnb.json"] = fmt.Errorf("PUT failed: x-amz-signature=SIG-REDACT-MARKER&x-amz-credential=topsecret")
	sink := &collectSink{}
	runner, err := NewRunner(provenanceCopyConfig(dst, sink, siblingProvenancePlan()))
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	require.NotEmpty(t, sink.warnings)
	for _, w := range sink.warnings {
		require.NotContains(t, w.Message, "topsecret")
		require.NotContains(t, w.Message, "SIG-REDACT-MARKER")
		for _, v := range w.Details {
			if s, ok := v.(string); ok {
				require.NotContains(t, s, "topsecret")
			}
		}
	}
}

// TestEngineProvenanceDryRunWritesNoSidecar pins the dry-run floor: a dry-run
// enabled plan emits the RunRecord provenance echo but writes zero sidecar objects
// and claims no per-object Written ref.
func TestEngineProvenanceDryRunWritesNoSidecar(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	cfg := provenanceCopyConfig(dst, sink, siblingProvenancePlan())
	cfg.DryRun = true
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	_, err = runner.Run(context.Background(), copySource(src, s3ProvenanceLine))
	require.NoError(t, err)

	require.Empty(t, dst.body("data/a/b.xml.gnb.json"), "dry-run writes no sidecar object")
	require.Len(t, sink.runs, 1)
	require.NotNil(t, sink.runs[0].Provenance, "the RunRecord provenance echo is still emitted")
	for _, r := range sink.records {
		require.Nil(t, r.Provenance, "no per-object provenance ref in dry-run")
		require.Equal(t, "planned", r.Status)
	}
}
