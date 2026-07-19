package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

// concurrentGetBarrierProvider wraps a source provider and records max concurrent
// GetObject calls. The first barrierFloor entrants wait until that many have
// entered (or a shared fail-open deadline elapses). On a serial execution path
// maxInFlight stays 1 and the deadline unblocks the run so the assertion fails
// cleanly instead of hanging. Helpers stay test-only for reuse by a future
// dual-path behavioral parity gate.
type concurrentGetBarrierProvider struct {
	base         *reflowMemoryProvider
	barrierFloor int64
	deadline     time.Duration

	entered     atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	releaseCh   chan struct{}
	releaseOnce sync.Once
	failOpen    <-chan struct{}
	startOnce   sync.Once
}

func newConcurrentGetBarrierProvider(base *reflowMemoryProvider, floor int, deadline time.Duration) *concurrentGetBarrierProvider {
	return &concurrentGetBarrierProvider{
		base:         base,
		barrierFloor: int64(floor),
		deadline:     deadline,
		releaseCh:    make(chan struct{}),
	}
}

func (p *concurrentGetBarrierProvider) ensureFailOpen() {
	p.startOnce.Do(func() {
		ch := make(chan struct{})
		timer := time.NewTimer(p.deadline)
		go func() {
			<-timer.C
			close(ch)
		}()
		p.failOpen = ch
	})
}

func (p *concurrentGetBarrierProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.ensureFailOpen()

	cur := p.inFlight.Add(1)
	for {
		max := p.maxInFlight.Load()
		if cur <= max || p.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}
	defer p.inFlight.Add(-1)

	n := p.entered.Add(1)
	if n == p.barrierFloor {
		p.releaseOnce.Do(func() { close(p.releaseCh) })
	}

	select {
	case <-p.releaseCh:
	case <-p.failOpen:
		// Shared deadline: serial path proceeds; maxInFlight assertion fails later.
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}

	return p.base.GetObject(ctx, key)
}

func (p *concurrentGetBarrierProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return p.base.List(ctx, opts)
}

func (p *concurrentGetBarrierProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return p.base.Head(ctx, key)
}

func (p *concurrentGetBarrierProvider) Close() error {
	return p.base.Close()
}

func TestPrepareTransferReflowEngineInput_PreservesLeadingBlanksAndSniffedRecord(t *testing.T) {
	first := reflowInputLine("source/first.xml", "etag-0", 8, "", "")
	second := reflowInputLine("source/second.xml", "etag-1", 8, "", "")
	original := "\n\n" + first + "\n" + second + "\n"

	replay, ok, err := prepareTransferReflowEngineInput(strings.NewReader(original))
	require.NoError(t, err)
	require.True(t, ok)

	got, err := io.ReadAll(replay)
	require.NoError(t, err)
	require.Equal(t, original, string(got))
}

func TestPlanTransferReflowEngineAdapter_LiveCopyPlansEngine(t *testing.T) {
	withTransferReflowTestState(t)
	reflowStdin = true
	reflowDryRun = false

	dst := newReflowMemoryProvider()
	dest := &reflowDestSpec{
		Provider: string(provider.ProviderS3),
		Bucket:   "dest-bucket",
		Prefix:   "data/",
		BaseURI:  "s3://dest-bucket/data/",
	}
	input := reflowInputLine("source/a.xml", "etag", 1, "", "") + "\n"
	plan := planTransferReflowEngineAdapter(context.Background(), strings.NewReader(input), dest, dst, collisionConfig{Mode: reflowCollisionSkip}, reflowMetadataConfig{}, reflowpkg.ConcurrencyConfig{EffectiveCeiling: 8}, nil)

	require.True(t, plan.enabled, "live migrated stdin runs dispatch to the engine (reason=%q)", plan.reason)
	require.Empty(t, plan.reason)
	require.False(t, plan.cfg.DryRun)
	require.NotNil(t, plan.source.Resolve, "live plan carries a source resolver")
	if plan.close != nil {
		plan.close()
	}
}

func TestPlanTransferReflowEngineAdapter_ForcedCLIPoolFallsBack(t *testing.T) {
	withTransferReflowTestState(t)
	reflowStdin = true
	reflowDryRun = false
	transferReflowForceCLIPool = true
	t.Cleanup(func() { transferReflowForceCLIPool = false })

	input := reflowInputLine("source/a.xml", "etag", 1, "", "") + "\n"
	plan := planTransferReflowEngineAdapter(context.Background(), strings.NewReader(input), nil, nil, collisionConfig{Mode: reflowCollisionSkip}, reflowMetadataConfig{}, reflowpkg.ConcurrencyConfig{EffectiveCeiling: 8}, nil)

	require.False(t, plan.enabled)
	require.Equal(t, transferReflowLiveCopyCLIPoolReason, plan.reason)

	// Replay reader must still yield the original stdin bytes for the CLI path.
	got, err := io.ReadAll(plan.input)
	require.NoError(t, err)
	require.Equal(t, input, string(got))
}

func TestPlanTransferReflowEngineAdapter_DryRunKeepsEngine(t *testing.T) {
	withTransferReflowTestState(t)
	reflowStdin = true
	reflowDryRun = true

	dst := newReflowMemoryProvider()
	dest := &reflowDestSpec{
		Provider: string(provider.ProviderS3),
		Bucket:   "dest-bucket",
		Prefix:   "data/",
		BaseURI:  "s3://dest-bucket/data/",
	}
	input := reflowInputLine("source/a.xml", "etag", 1, "", "") + "\n"
	plan := planTransferReflowEngineAdapter(context.Background(), strings.NewReader(input), dest, dst, collisionConfig{Mode: reflowCollisionSkip}, reflowMetadataConfig{}, reflowpkg.ConcurrencyConfig{EffectiveCeiling: 8}, nil)

	require.True(t, plan.enabled)
	require.Empty(t, plan.reason)
	require.True(t, plan.cfg.DryRun)
	if plan.close != nil {
		plan.close()
	}
}

// reflowDualPathArm captures one arm of the dual-path behavioral harness: the
// same live stdin input executed through the real command dispatch, on either
// the engine path (default) or the CLI worker pool (force hook).
type reflowDualPathArm struct {
	stdout         string
	stderr         string
	src            *concurrentGetBarrierProvider
	dst            *reflowMemoryProvider
	checkpointPath string
}

// runTransferReflowBarrierArm executes one dual-path arm through the real
// command with fresh, identically-seeded providers and a barrier source that
// records max in-flight GetObject calls.
func runTransferReflowBarrierArm(t *testing.T, forcePool bool, objectCount, parallel, barrierFloor int) reflowDualPathArm {
	t.Helper()

	srcBase := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src := newConcurrentGetBarrierProvider(srcBase, barrierFloor, 2*time.Second)

	// Leading blanks exercise sniff replay (first nonblank record must not drop).
	lines := []string{"", ""}
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		body := fmt.Sprintf("payload-%02d", i)
		srcBase.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
	}
	input := strings.Join(lines, "\n") + "\n"

	// Room for the workers: memory budget must exceed parallel * retry buffer.
	reflowResourceProbeForRun = reflowpkg.ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) {
			return transfer.DefaultRetryBufferMaxMemoryBytes * 64, "test_override", nil
		},
		FDSoftLimit: func() (int64, error) {
			return 100000, nil
		},
	}

	useTransferReflowProviderFactories(t, providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			switch cfg.Bucket {
			case "source-bucket":
				return src, nil
			case "dest-bucket":
				return dst, nil
			default:
				return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
			}
		},
	})

	if forcePool {
		transferReflowForceCLIPool = true
		defer func() { transferReflowForceCLIPool = false }()
	}

	checkpointPath := filepath.Join(t.TempDir(), "state.db")
	var stdout, stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--parallel", fmt.Sprintf("%d", parallel),
		"--checkpoint", checkpointPath,
		// default skip-if-duplicate collision mode
	})
	require.NoError(t, cmd.Execute(), "stderr=%s\nstdout=%s", stderr.String(), stdout.String())

	return reflowDualPathArm{stdout: stdout.String(), stderr: stderr.String(), src: src, dst: dst, checkpointPath: checkpointPath}
}

// assertBarrierArmBehavior carries the locked behavioral ACs from the v0.4.1
// dispatch-regression test (#159) for one arm: in-flight floor, one terminal
// record per input, complete statuses, dest delivery, truthful execution_path,
// and the requested/resolved/observed field relations.
func assertBarrierArmBehavior(t *testing.T, arm reflowDualPathArm, objectCount, barrierFloor int, expectedPath string) {
	t.Helper()

	run := requireRecord(t, arm.stdout, reflowpkg.RunRecordType, "")
	require.Contains(t, string(run.Data), fmt.Sprintf(`"execution_path":%q`, expectedPath))
	require.Contains(t, string(run.Data), `"concurrency_ceiling_effective":8`)
	require.Contains(t, string(run.Data), `"concurrency_ceiling_requested":8`)
	require.Contains(t, string(run.Data), `"adaptive_enabled":true`)

	// Behavioral gate: pooled execution must overlap GetObject calls.
	require.GreaterOrEqual(t, arm.src.maxInFlight.Load(), int64(barrierFloor),
		"max concurrent GetObject calls=%d on %s path; expected pooled execution (>= %d)",
		arm.src.maxInFlight.Load(), expectedPath, barrierFloor)

	// One-to-one replay over terminal object records: N inputs → N terminal
	// results (no drop, no duplicate), including the sniffed first record.
	records := requireReflowRecords(t, arm.stdout)
	var terminal []testReflowData
	for _, rec := range records {
		if rec.Status == "in_progress" {
			continue
		}
		terminal = append(terminal, rec)
	}
	require.Len(t, terminal, objectCount, "terminal object records (all statuses except in_progress)")
	keyCounts := make(map[string]int, objectCount)
	for _, rec := range terminal {
		require.Equal(t, "complete", rec.Status,
			"fresh-destination fixture: every terminal record must be complete; got status=%q key=%q reason=%q",
			rec.Status, rec.SourceKey, rec.Reason)
		keyCounts[rec.SourceKey]++
	}
	require.Len(t, keyCounts, objectCount, "unique source keys among terminal records")
	require.Equal(t, 1, keyCounts["source/obj-00.xml"], "sniffed first record must appear once terminal")
	requireReflowStatusReasonCount(t, records, "complete", "", objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		require.True(t, arm.dst.hasObject("data/"+key), "missing dest object for %s", key)
	}

	// Summary carries the same execution path, and the concurrency relations
	// hold: observed max-active <= resolved effective <= requested.
	sum := requireRecord(t, arm.stdout, reflowpkg.SummaryRecordType, "")
	require.Contains(t, string(sum.Data), fmt.Sprintf(`"execution_path":%q`, expectedPath))
	var sumFields struct {
		MaxActive int `json:"concurrency_max_active"`
		Effective int `json:"concurrency_ceiling_effective"`
		Requested int `json:"concurrency_ceiling_requested"`
	}
	require.NoError(t, json.Unmarshal(sum.Data, &sumFields))
	require.LessOrEqual(t, sumFields.MaxActive, sumFields.Effective)
	require.LessOrEqual(t, sumFields.Effective, sumFields.Requested)

	// Sterility: no URI/key material in stderr diagnostics.
	require.NotContains(t, arm.stderr, "s3://source-bucket")
	require.NotContains(t, arm.stderr, "s3://dest-bucket")
	require.NotContains(t, arm.stderr, "source/obj-")
}

func TestTransferReflowCommand_LiveStdinEngineMaxInFlight(t *testing.T) {
	// Package-global factories / probe; do not t.Parallel.
	withTransferReflowTestState(t)
	oldVerbose := verbose
	verbose = true
	t.Cleanup(func() { verbose = oldVerbose })

	arm := runTransferReflowBarrierArm(t, false, 32, 8, 4)
	assertBarrierArmBehavior(t, arm, 32, 4, reflowpkg.ExecutionPathEngine)
}

func TestTransferReflowCommand_LiveStdinForcedCLIPoolMaxInFlight(t *testing.T) {
	withTransferReflowTestState(t)
	oldVerbose := verbose
	verbose = true
	t.Cleanup(func() { verbose = oldVerbose })

	arm := runTransferReflowBarrierArm(t, true, 32, 8, 4)
	assertBarrierArmBehavior(t, arm, 32, 4, reflowpkg.ExecutionPathCLIPool)
	// Forced-pool diagnostic: exact static reason, no URI/key material.
	require.Contains(t, arm.stderr, "execution_path=cli-pool")
	require.Contains(t, arm.stderr, "reason="+transferReflowLiveCopyCLIPoolReason)
}

// upsertFailingReflowState fails terminal item upserts while passing all other
// store operations through, for the dual-path checkpoint-failure disposition.
type upsertFailingReflowState struct {
	reflowStateStore
	err error
}

func (s upsertFailingReflowState) UpsertItem(context.Context, reflowstate.UpsertItemParams) error {
	return s.err
}

// TestTransferReflowDualPathTerminalUpsertFailureNeverAcksComplete pins the
// strict terminal disposition on BOTH execution paths: when the store cannot
// record the terminal item, neither path acknowledges complete — the object
// reports failed (reason checkpoint.write_failed) and the run exits non-zero.
func TestTransferReflowDualPathTerminalUpsertFailureNeverAcksComplete(t *testing.T) {
	for _, arm := range []struct {
		name      string
		forcePool bool
		path      string
	}{
		{name: "engine", forcePool: false, path: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", forcePool: true, path: reflowpkg.ExecutionPathCLIPool},
	} {
		t.Run(arm.name, func(t *testing.T) {
			withTransferReflowTestState(t)

			oldStateStore := newReflowStateStore
			newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
				store, err := oldStateStore(ctx, cfg)
				if err != nil {
					return nil, err
				}
				return upsertFailingReflowState{reflowStateStore: store, err: fmt.Errorf("injected terminal upsert failure")}, nil
			}
			t.Cleanup(func() { newReflowStateStore = oldStateStore })

			src := newReflowMemoryProvider()
			src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
			dst := newReflowMemoryProvider()

			reflowResourceProbeForRun = reflowpkg.ResourceProbe{
				MemoryLimitBytes: func() (int64, string, error) {
					return transfer.DefaultRetryBufferMaxMemoryBytes * 64, "test_override", nil
				},
				FDSoftLimit: func() (int64, error) { return 100000, nil },
			}
			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					switch cfg.Bucket {
					case "source-bucket":
						return src, nil
					case "dest-bucket":
						return dst, nil
					default:
						return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
					}
				},
			})

			if arm.forcePool {
				transferReflowForceCLIPool = true
				t.Cleanup(func() { transferReflowForceCLIPool = false })
			}

			var stdout, stderr bytes.Buffer
			cmd := newTransferReflowTestCommand()
			cmd.SetIn(strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "") + "\n"))
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs([]string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2"})

			err := cmd.Execute()
			require.Error(t, err, "terminal upsert failure must exit non-zero")

			run := requireRecord(t, stdout.String(), reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), fmt.Sprintf(`"execution_path":%q`, arm.path))

			records := requireReflowRecords(t, stdout.String())
			requireReflowStatusReasonCount(t, records, "complete", "", 0)
			requireReflowStatusReasonCount(t, records, "failed", "checkpoint.write_failed", 1)
			// The destination mutation itself happened; only the acknowledgement
			// is withheld. Resume convergence is proven at the library level.
			require.True(t, dst.hasObject("data/source/file.xml"))
		})
	}
}

// TestTransferReflowCommand_EngineSingleSourceProviderConstruction proves the
// adapter's lazy resolver cache stays single-authority now that the engine
// executes live copies: exactly one source provider is constructed for the
// whole run, every record reuses it, and a divergent source root refuses as
// INVALID_INPUT without constructing a second provider.
func TestTransferReflowCommand_EngineSingleSourceProviderConstruction(t *testing.T) {
	withTransferReflowTestState(t)

	const objectCount = 8
	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	lines := make([]string, 0, objectCount+1)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		body := fmt.Sprintf("payload-%02d", i)
		src.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
	}
	// Divergent source root: must refuse as INVALID_INPUT before resolution.
	lines = append(lines, `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://other-bucket/x/y.xml","source_key":"x/y.xml","source_etag":"etag-x","source_size_bytes":7,"dest_rel_key":"x/y.xml"}}`)
	input := strings.Join(lines, "\n") + "\n"

	reflowResourceProbeForRun = reflowpkg.ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) {
			return transfer.DefaultRetryBufferMaxMemoryBytes * 64, "test_override", nil
		},
		FDSoftLimit: func() (int64, error) { return 100000, nil },
	}

	var sourceConstructions atomic.Int32
	useTransferReflowProviderFactories(t, providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			switch cfg.Bucket {
			case "source-bucket":
				sourceConstructions.Add(1)
				return src, nil
			case "dest-bucket":
				return dst, nil
			default:
				return nil, fmt.Errorf("unexpected bucket construction %q", cfg.Bucket)
			}
		},
	})

	var stdout, stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "4"})

	err := cmd.Execute()
	require.Error(t, err, "divergent-root record must surface as invalid input")
	require.Contains(t, err.Error(), "invalid input")

	require.Equal(t, int32(1), sourceConstructions.Load(),
		"exactly one source provider construction for the run; divergent root must not construct a second")

	run := requireRecord(t, stdout.String(), reflowpkg.RunRecordType, "")
	require.Contains(t, string(run.Data), `"execution_path":"engine"`)
	records := requireReflowRecords(t, stdout.String())
	requireReflowStatusReasonCount(t, records, "complete", "", objectCount)
	for i := 0; i < objectCount; i++ {
		require.True(t, dst.hasObject(fmt.Sprintf("data/source/obj-%02d.xml", i)))
	}
	require.False(t, dst.hasObject("data/x/y.xml"), "divergent-root record must not land")
}

// dropVolatileConcurrencyObservations removes run-to-run-variable limiter
// observation fields before cross-path comparison; their relations are
// asserted per arm by assertBarrierArmBehavior instead.
func dropVolatileConcurrencyObservations(events []normalizedEvent) []normalizedEvent {
	volatile := []string{
		"concurrency_final",
		"concurrency_max_active",
		"concurrency_additive_increases",
		"concurrency_throttle_backoffs",
		"concurrency_connection_error_freezes",
	}
	for _, ev := range events {
		for _, k := range volatile {
			delete(ev.Fields, k)
		}
	}
	return events
}

// TestTransferReflowDualPathBehavioralParity is the standing same-input
// dual-path gate: an identical live record stream through the real command
// dispatch on both execution paths must yield (a) normalized output-record
// parity, (b) the max-in-flight floor on BOTH paths, and (c) checkpoint
// semantic equivalence on the resume surface — plus destination set equality.
func TestTransferReflowDualPathBehavioralParity(t *testing.T) {
	const (
		objectCount  = 24
		parallel     = 8
		barrierFloor = 4
	)
	withTransferReflowTestState(t)

	engine := runTransferReflowBarrierArm(t, false, objectCount, parallel, barrierFloor)
	pool := runTransferReflowBarrierArm(t, true, objectCount, parallel, barrierFloor)

	// (b) both paths overlap to the floor.
	require.GreaterOrEqual(t, engine.src.maxInFlight.Load(), int64(barrierFloor), "engine arm in-flight floor")
	require.GreaterOrEqual(t, pool.src.maxInFlight.Load(), int64(barrierFloor), "cli-pool arm in-flight floor")

	// (a) normalized output parity across paths.
	engineEvents := dropVolatileConcurrencyObservations(normalizeReflowStdout(t, engine.stdout))
	poolEvents := dropVolatileConcurrencyObservations(normalizeReflowStdout(t, pool.stdout))
	require.Equal(t, poolEvents, engineEvents, "engine and cli-pool paths must emit equivalent normalized events")

	// (c) checkpoint semantic equivalence on the resume surface.
	ctx := context.Background()
	engineState, err := reflowstate.Open(ctx, reflowstate.Config{Path: engine.checkpointPath})
	require.NoError(t, err)
	defer func() { _ = engineState.Close() }()
	poolState, err := reflowstate.Open(ctx, reflowstate.Config{Path: pool.checkpointPath})
	require.NoError(t, err)
	defer func() { _ = poolState.Close() }()
	for i := 0; i < objectCount; i++ {
		srcURI := fmt.Sprintf("s3://source-bucket/source/obj-%02d.xml", i)
		dstURI := fmt.Sprintf("s3://dest-bucket/data/source/obj-%02d.xml", i)
		eDone, eStatus, err := engineState.ItemDone(ctx, srcURI, dstURI)
		require.NoError(t, err)
		pDone, pStatus, err := poolState.ItemDone(ctx, srcURI, dstURI)
		require.NoError(t, err)
		require.True(t, eDone, "engine arm item recorded done: %s", srcURI)
		require.Equal(t, pDone, eDone, "ItemDone parity for %s", srcURI)
		require.Equal(t, pStatus, eStatus, "ItemDone status parity for %s", srcURI)
	}

	// Destination set equality.
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("data/source/obj-%02d.xml", i)
		require.True(t, engine.dst.hasObject(key), "engine dest object %s", key)
		require.True(t, pool.dst.hasObject(key), "pool dest object %s", key)
	}
}

func TestTransferReflowCommand_DryRunStdinKeepsEnginePath(t *testing.T) {
	withTransferReflowTestState(t)

	src := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	const objectCount = 4
	lines := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/dry-%02d.xml", i)
		body := "payload"
		src.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
	}
	input := strings.Join(lines, "\n") + "\n"

	probe := reflowpkg.ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) {
			return transfer.DefaultRetryBufferMaxMemoryBytes * 64, "test_override", nil
		},
		FDSoftLimit: func() (int64, error) {
			return 100000, nil
		},
	}
	reflowResourceProbeForRun = probe

	useTransferReflowProviderFactories(t, providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			switch cfg.Bucket {
			case "source-bucket":
				return src, nil
			case "dest-bucket":
				return dst, nil
			default:
				return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
			}
		},
	})

	oldVerbose := verbose
	verbose = true
	t.Cleanup(func() { verbose = oldVerbose })

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--parallel", "8",
		"--dry-run",
	})

	require.NoError(t, cmd.Execute(), "stderr=%s\nstdout=%s", stderr.String(), stdout.String())

	// Dry-run remains on the engine: no CLI-pool fallback diagnostic.
	require.NotContains(t, stderr.String(), "execution_path=cli-pool")
	require.NotContains(t, stderr.String(), transferReflowLiveCopyCLIPoolReason)

	// Engine dry-run emits planned object records; destination objects are not landed
	// (object-store dry-run may still run a write/delete preflight probe).
	records := requireReflowRecords(t, stdout.String())
	requireReflowStatusReasonCount(t, records, "planned", "", objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/dry-%02d.xml", i)
		require.False(t, dst.hasObject("data/"+key), "dry-run must not land %s", key)
	}
}
