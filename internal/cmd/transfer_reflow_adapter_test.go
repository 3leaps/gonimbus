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

func TestPlanTransferReflowEngineAdapter_SourceFailurePolicyRoutesCLIPool(t *testing.T) {
	withTransferReflowTestState(t)
	reflowStdin = true
	reflowDryRun = false
	reflowSrcFailure = reflowSourceFailFail

	dst := newReflowMemoryProvider()
	dest := &reflowDestSpec{
		Provider: string(provider.ProviderS3),
		Bucket:   "dest-bucket",
		Prefix:   "data/",
		BaseURI:  "s3://dest-bucket/data/",
	}
	input := reflowInputLine("source/a.xml", "etag", 1, "", "") + "\n"
	plan := planTransferReflowEngineAdapter(context.Background(), strings.NewReader(input), dest, dst, collisionConfig{Mode: reflowCollisionSkip}, reflowMetadataConfig{}, reflowpkg.ConcurrencyConfig{EffectiveCeiling: 8}, nil)

	require.False(t, plan.enabled, "non-migrated source-failure policy must route to the CLI pool")
	require.Equal(t, "source-failure policy not migrated", plan.reason)

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
// the engine path (default) or the CLI worker pool (selected by genuine
// dispatch routing via a non-migrated policy flag).
type reflowDualPathArm struct {
	stdout         string
	stderr         string
	src            *concurrentGetBarrierProvider
	dst            *reflowMemoryProvider
	checkpointPath string
}

// runTransferReflowBarrierArm executes one dual-path arm through the real
// command with fresh, identically-seeded providers and a barrier source that
// records max in-flight GetObject calls. preseedDest makes the fixture
// skip-heavy: every destination key already holds an identical object (same
// etag and size), so both paths resolve collision-duplicate skips instead of
// copies. The CLI-pool arm is selected by genuine dispatch routing (extraArgs
// carries a non-migrated policy such as --on-source-failure fail, which
// changes nothing on a success fixture) — not by any test hook.
func runTransferReflowBarrierArm(t *testing.T, objectCount, parallel, barrierFloor int, preseedDest bool, extraArgs ...string) reflowDualPathArm {
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
		if preseedDest {
			dst.putFixture("data/"+key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC))
		}
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

	checkpointPath := filepath.Join(t.TempDir(), "state.db")
	var stdout, stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(strings.NewReader(input))
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	args := []string{
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--parallel", fmt.Sprintf("%d", parallel),
		"--checkpoint", checkpointPath,
		// default skip-if-duplicate collision mode
	}
	args = append(args, extraArgs...)
	cmd.SetArgs(args)
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
		MaxActive     int     `json:"concurrency_max_active"`
		Effective     int     `json:"concurrency_ceiling_effective"`
		Requested     int     `json:"concurrency_ceiling_requested"`
		TimeAvgActive float64 `json:"concurrency_time_avg_active"`
	}
	require.NoError(t, json.Unmarshal(sum.Data, &sumFields))
	require.LessOrEqual(t, sumFields.MaxActive, sumFields.Effective)
	require.LessOrEqual(t, sumFields.Effective, sumFields.Requested)
	require.Greater(t, sumFields.TimeAvgActive, 0.0,
		"a run that moved objects must report positive time-averaged occupancy")
	require.LessOrEqual(t, sumFields.TimeAvgActive, float64(sumFields.Effective),
		"time-averaged occupancy can never exceed the effective ceiling")

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

	arm := runTransferReflowBarrierArm(t, 32, 8, 4, false)
	assertBarrierArmBehavior(t, arm, 32, 4, reflowpkg.ExecutionPathEngine)
}

func TestTransferReflowCommand_LiveStdinCLIPoolMaxInFlight(t *testing.T) {
	withTransferReflowTestState(t)
	oldVerbose := verbose
	verbose = true
	t.Cleanup(func() { verbose = oldVerbose })

	// Genuine dispatch routing: a non-migrated source-failure policy selects
	// the CLI pool while changing nothing on this success fixture.
	arm := runTransferReflowBarrierArm(t, 32, 8, 4, false, "--on-source-failure", "fail")
	assertBarrierArmBehavior(t, arm, 32, 4, reflowpkg.ExecutionPathCLIPool)
}

// failKeysProvider wraps a source provider and fails GetObject for the listed
// keys until healed, simulating an interrupted first run.
type failKeysProvider struct {
	*reflowMemoryProvider
	mu       sync.Mutex
	failKeys map[string]bool
}

func (p *failKeysProvider) heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failKeys = nil
}

func (p *failKeysProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	failing := p.failKeys[key]
	p.mu.Unlock()
	if failing {
		return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: provider.ProviderS3, Key: key, Err: provider.ErrAccessDenied}
	}
	return p.reflowMemoryProvider.GetObject(ctx, key)
}

// TestTransferReflowCommand_EngineResumeAfterInterruptedRunRealStore is the
// constraint-10 real-store evidence for the engine path: an interrupted run
// leaves a mixed real sqlite checkpoint; a --resume rerun converges — done
// items skip with resume reasons, failed items re-drive, and no destination
// key lands twice.
func TestTransferReflowCommand_EngineResumeAfterInterruptedRunRealStore(t *testing.T) {
	withTransferReflowTestState(t)

	const objectCount = 8
	srcBase := newReflowMemoryProvider()
	src := &failKeysProvider{reflowMemoryProvider: srcBase, failKeys: map[string]bool{}}
	dst := newReflowMemoryProvider()
	lines := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		body := fmt.Sprintf("payload-%02d", i)
		srcBase.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		if i%2 == 1 {
			src.failKeys[key] = true
		}
		lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
	}
	input := strings.Join(lines, "\n") + "\n"

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

	checkpointPath := filepath.Join(t.TempDir(), "state.db")
	runOnce := func(resume bool) (string, error) {
		var stdout, stderr bytes.Buffer
		cmd := newTransferReflowTestCommand()
		cmd.SetIn(strings.NewReader(input))
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "4", "--checkpoint", checkpointPath}
		if resume {
			args = append(args, "--resume")
		}
		cmd.SetArgs(args)
		execErr := cmd.Execute()
		return stdout.String(), execErr
	}

	// Interrupted first run: half the objects fail typed, half complete.
	stdout1, err := runOnce(false)
	require.Error(t, err, "interrupted run must exit non-zero")
	records1 := requireReflowRecords(t, stdout1)
	requireReflowStatusReasonCount(t, records1, "complete", "", objectCount/2)
	run1 := requireRecord(t, stdout1, reflowpkg.RunRecordType, "")
	require.Contains(t, string(run1.Data), `"execution_path":"engine"`)

	// Healed resume converges: done items skip, failed items re-drive.
	src.heal()
	stdout2, err := runOnce(true)
	require.NoError(t, err, "resume run must converge")
	records2 := requireReflowRecords(t, stdout2)
	requireReflowStatusReasonCount(t, records2, "complete", "", objectCount/2)
	requireReflowStatusReasonCount(t, records2, "skipped", "resume.complete", objectCount/2)

	// Every object landed exactly once across both runs (capability-preflight
	// probe keys excluded).
	objectPuts := map[string]int{}
	for _, key := range dst.conditionalPutCallsSnapshot() {
		if strings.Contains(key, ".gonimbus-preflight/") {
			continue
		}
		objectPuts[key]++
	}
	require.Len(t, objectPuts, objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("data/source/obj-%02d.xml", i)
		require.True(t, dst.hasObject(key), "missing dest object for %s", key)
		require.Equal(t, 1, objectPuts[key],
			"each object lands via exactly one conditional PUT across interrupt + resume")
	}
}

// headBarrierDest wraps a destination provider and records max concurrent
// object-key Head calls (capability-preflight keys pass through uncounted).
// The first floor entrants wait until floor calls are in flight, with a
// bounded fail-open deadline so a serial skip path fails the in-flight
// assertion instead of hanging — the skip-path analog of the GetObject
// barrier, since collision-duplicate resolution Heads the destination and
// never fetches source bodies.
type headBarrierDest struct {
	*reflowMemoryProvider
	floor    int64
	deadline time.Duration

	entered     atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	release     chan struct{}
	releaseOnce sync.Once
	failOpen    <-chan struct{}
	startOnce   sync.Once
}

func newHeadBarrierDest(base *reflowMemoryProvider, floor int, deadline time.Duration) *headBarrierDest {
	return &headBarrierDest{reflowMemoryProvider: base, floor: int64(floor), deadline: deadline, release: make(chan struct{})}
}

func (p *headBarrierDest) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	if isPreflightKey(key) {
		return p.reflowMemoryProvider.Head(ctx, key)
	}
	p.startOnce.Do(func() {
		ch := make(chan struct{})
		timer := time.NewTimer(p.deadline)
		go func() {
			<-timer.C
			close(ch)
		}()
		p.failOpen = ch
	})
	cur := p.inFlight.Add(1)
	for {
		max := p.maxInFlight.Load()
		if cur <= max || p.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}
	defer p.inFlight.Add(-1)
	if p.entered.Add(1) == p.floor {
		p.releaseOnce.Do(func() { close(p.release) })
	}
	select {
	case <-p.release:
	case <-p.failOpen:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return p.reflowMemoryProvider.Head(ctx, key)
}

// TestTransferReflowDualPathSkipHeavyParity is the skip-heavy companion of the
// behavioral parity harness: with every destination key preseeded identically,
// both genuine paths must resolve the same collision-duplicate skips with
// equivalent normalized outputs, checkpoint rows, and untouched destinations —
// AND overlap the skip path itself, proven by a destination-Head barrier.
func TestTransferReflowDualPathSkipHeavyParity(t *testing.T) {
	const (
		objectCount  = 24
		parallel     = 8
		barrierFloor = 4
	)
	withTransferReflowTestState(t)

	type skipHeavyArm struct {
		stdout         string
		dstBase        *reflowMemoryProvider
		dstBarrier     *headBarrierDest
		checkpointPath string
	}
	runArm := func(extraArgs ...string) skipHeavyArm {
		srcBase := newReflowMemoryProvider()
		dstBase := newReflowMemoryProvider()
		dstBarrier := newHeadBarrierDest(dstBase, barrierFloor, 2*time.Second)
		lines := []string{"", ""}
		for i := 0; i < objectCount; i++ {
			key := fmt.Sprintf("source/obj-%02d.xml", i)
			body := fmt.Sprintf("payload-%02d", i)
			srcBase.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
			dstBase.putFixture("data/"+key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC))
			lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
		}
		input := strings.Join(lines, "\n") + "\n"

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
					return srcBase, nil
				case "dest-bucket":
					return dstBarrier, nil
				default:
					return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
				}
			},
		})

		checkpointPath := filepath.Join(t.TempDir(), "state.db")
		var stdout, stderr bytes.Buffer
		cmd := newTransferReflowTestCommand()
		cmd.SetIn(strings.NewReader(input))
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		cmd.SetArgs(append([]string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", fmt.Sprintf("%d", parallel), "--checkpoint", checkpointPath}, extraArgs...))
		require.NoError(t, cmd.Execute(), "stderr=%s\nstdout=%s", stderr.String(), stdout.String())
		return skipHeavyArm{stdout: stdout.String(), dstBase: dstBase, dstBarrier: dstBarrier, checkpointPath: checkpointPath}
	}

	engine := runArm()
	pool := runArm("--on-source-failure", "fail")

	for _, arm := range []struct {
		name string
		arm  skipHeavyArm
		path string
	}{
		{name: "engine", arm: engine, path: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", arm: pool, path: reflowpkg.ExecutionPathCLIPool},
	} {
		run := requireRecord(t, arm.arm.stdout, reflowpkg.RunRecordType, "")
		require.Contains(t, string(run.Data), fmt.Sprintf(`"execution_path":%q`, arm.path), arm.name)

		records := requireReflowRecords(t, arm.arm.stdout)
		requireReflowStatusReasonCount(t, records, "skipped", "collision.duplicate", objectCount)
		requireReflowStatusReasonCount(t, records, "complete", "", 0)

		sum := requireRecord(t, arm.arm.stdout, reflowpkg.SummaryRecordType, "")
		var sumFields struct {
			MaxActive int `json:"concurrency_max_active"`
			Effective int `json:"concurrency_ceiling_effective"`
		}
		require.NoError(t, json.Unmarshal(sum.Data, &sumFields))
		require.LessOrEqual(t, sumFields.MaxActive, sumFields.Effective, arm.name)

		// Skip-path fan-out lower bound: collision resolution must overlap on
		// the operation skips actually execute (destination Head), bounded
		// above by the effective ceiling.
		require.GreaterOrEqual(t, arm.arm.dstBarrier.maxInFlight.Load(), int64(barrierFloor),
			"%s: skip-heavy path must overlap destination Heads (>= %d)", arm.name, barrierFloor)
		require.LessOrEqual(t, arm.arm.dstBarrier.maxInFlight.Load(), int64(parallel),
			"%s: skip-heavy in-flight must respect the effective ceiling", arm.name)

		// Destinations untouched: preseeded content and etag survive.
		for i := 0; i < objectCount; i++ {
			key := fmt.Sprintf("data/source/obj-%02d.xml", i)
			require.Equal(t, fmt.Sprintf("payload-%02d", i), string(arm.arm.dstBase.mustObject(key)))
			require.Equal(t, fmt.Sprintf("etag-%02d", i), arm.arm.dstBase.metaSnapshot(key).ETag,
				"%s: preseeded destination must not be overwritten", arm.name)
		}
	}

	// Cross-path parity: normalized outputs and checkpoint rows.
	engineEvents := dropVolatileConcurrencyObservations(normalizeReflowStdout(t, engine.stdout))
	poolEvents := dropVolatileConcurrencyObservations(normalizeReflowStdout(t, pool.stdout))
	require.Equal(t, poolEvents, engineEvents, "skip-heavy normalized event parity")

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
		require.Equal(t, pDone, eDone, "skip-heavy ItemDone parity for %s", srcURI)
		require.Equal(t, pStatus, eStatus, "skip-heavy ItemDone status parity for %s", srcURI)
	}
}

// landCountingDest wraps a destination provider and counts SUCCESSFUL
// object-level lands per key (conditional creates that returned nil), records
// any non-IfAbsent conditional attempt, and any unconditional write. Capability
// preflight keys are excluded. This distinguishes "landed exactly once" from
// attempt-count heuristics: an IfAbsent-refused re-drive is an attempt, never a
// land.
type landCountingDest struct {
	*reflowMemoryProvider
	mu               sync.Mutex
	lands            map[string]int
	nonIfAbsent      int
	unconditionalPut int
}

func newLandCountingDest(base *reflowMemoryProvider) *landCountingDest {
	return &landCountingDest{reflowMemoryProvider: base, lands: map[string]int{}}
}

func isPreflightKey(key string) bool { return strings.Contains(key, ".gonimbus-preflight/") }

func (p *landCountingDest) recordConditional(key string, precond provider.PutPrecondition, err error) {
	if isPreflightKey(key) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !precond.IfAbsent {
		p.nonIfAbsent++
	}
	if err == nil {
		p.lands[key]++
	}
}

func (p *landCountingDest) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	if !isPreflightKey(key) {
		p.mu.Lock()
		p.unconditionalPut++
		p.mu.Unlock()
	}
	return p.reflowMemoryProvider.PutObject(ctx, key, body, contentLength)
}

func (p *landCountingDest) PutObjectWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, opts provider.PutOptions) error {
	if !isPreflightKey(key) {
		p.mu.Lock()
		p.unconditionalPut++
		p.mu.Unlock()
	}
	return p.reflowMemoryProvider.PutObjectWithOptions(ctx, key, body, contentLength, opts)
}

func (p *landCountingDest) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	result, err := p.reflowMemoryProvider.PutObjectConditional(ctx, key, body, contentLength, precond)
	p.recordConditional(key, precond, err)
	return result, err
}

func (p *landCountingDest) PutObjectConditionalWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition, opts provider.PutOptions) (provider.PutResult, error) {
	result, err := p.reflowMemoryProvider.PutObjectConditionalWithOptions(ctx, key, body, contentLength, precond, opts)
	p.recordConditional(key, precond, err)
	return result, err
}

func (p *landCountingDest) landsSnapshot() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.lands))
	for k, v := range p.lands {
		out[k] = v
	}
	return out
}

func (p *landCountingDest) requireExactlyOneLandPerKey(t *testing.T, keys []string) {
	t.Helper()
	lands := p.landsSnapshot()
	for _, key := range keys {
		require.Equal(t, 1, lands[key], "destination key %s must land exactly once", key)
	}
	require.Len(t, lands, len(keys), "no destination key outside the expected set may land")
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Zero(t, p.nonIfAbsent, "every object-level conditional attempt must carry IfAbsent")
	require.Zero(t, p.unconditionalPut, "no unconditional object-level write may occur")
}

// gateAfterProvider passes the first passCount GetObject calls through, then
// blocks every subsequent call until released (or the call's context ends),
// signalling on blocked once at least one worker is held mid-pool.
type gateAfterProvider struct {
	*reflowMemoryProvider
	mu          sync.Mutex
	passed      int
	passCount   int
	blocked     chan struct{}
	blockedOnce sync.Once
	release     chan struct{}
}

func newGateAfterProvider(base *reflowMemoryProvider, passCount int) *gateAfterProvider {
	return &gateAfterProvider{
		reflowMemoryProvider: base,
		passCount:            passCount,
		blocked:              make(chan struct{}),
		release:              make(chan struct{}),
	}
}

func (p *gateAfterProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.mu.Lock()
	if p.passed < p.passCount {
		p.passed++
		p.mu.Unlock()
		return p.reflowMemoryProvider.GetObject(ctx, key)
	}
	p.mu.Unlock()
	p.blockedOnce.Do(func() { close(p.blocked) })
	select {
	case <-p.release:
		return p.reflowMemoryProvider.GetObject(ctx, key)
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// TestTransferReflowCommand_EngineCancelMidPoolResumeRealStore is the genuine
// interruption gate: the run is context-canceled while workers are blocked
// mid-pool with in-flight copies, emits no terminal summary, and leaves a mixed
// real sqlite checkpoint. Every complete record emitted before the cancel is
// durably recorded (no false completes). Reopening the same store and resuming
// with a healthy provider converges: durable completes skip as resume,
// landed-but-unrecorded objects skip as collision duplicates, never-landed
// objects copy — and no object is ever landed twice.
func TestTransferReflowCommand_EngineCancelMidPoolResumeRealStore(t *testing.T) {
	withTransferReflowTestState(t)

	const (
		objectCount = 12
		passCount   = 4
	)
	srcBase := newReflowMemoryProvider()
	src := newGateAfterProvider(srcBase, passCount)
	dst := newLandCountingDest(newReflowMemoryProvider())
	lines := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		body := fmt.Sprintf("payload-%02d", i)
		srcBase.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
	}
	input := strings.Join(lines, "\n") + "\n"

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

	checkpointPath := filepath.Join(t.TempDir(), "state.db")

	// Run 1: cancel while at least one worker is blocked mid-pool.
	runCtx, cancelRun := context.WithCancel(context.Background())
	var stdout1 bytes.Buffer
	cmd1 := newTransferReflowTestCommand()
	cmd1.SetIn(strings.NewReader(input))
	cmd1.SetOut(&stdout1)
	cmd1.SetErr(&bytes.Buffer{})
	cmd1.SetArgs([]string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "4", "--checkpoint", checkpointPath})
	done := make(chan error, 1)
	go func() { done <- cmd1.ExecuteContext(runCtx) }()
	select {
	case <-src.blocked:
	case err := <-done:
		t.Fatalf("run finished before any worker blocked mid-pool: %v", err)
	}
	// Cancel only after at least one completion is durable in the real store,
	// so the resume arm deterministically observes a resume.complete skip.
	pollCtx := context.Background()
	pollStore, err := reflowstate.Open(pollCtx, reflowstate.Config{Path: checkpointPath})
	require.NoError(t, err)
	durableBeforeCancel := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < passCount; i++ {
			srcURI := fmt.Sprintf("s3://source-bucket/source/obj-%02d.xml", i)
			dstURI := fmt.Sprintf("s3://dest-bucket/data/source/obj-%02d.xml", i)
			if recorded, status, itemErr := pollStore.ItemDone(pollCtx, srcURI, dstURI); itemErr == nil && recorded && status == "complete" {
				durableBeforeCancel = true
			}
		}
		if durableBeforeCancel {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.NoError(t, pollStore.Close())
	require.True(t, durableBeforeCancel, "at least one completion must be durable before cancellation")
	cancelRun()
	select {
	case err := <-done:
		require.Error(t, err, "interrupted run must not succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("canceled run did not return")
	}

	// No terminal summary; every emitted complete is durably recorded.
	requireNoRecordType(t, stdout1.String(), reflowpkg.SummaryRecordType)
	records1 := requireReflowRecords(t, stdout1.String())
	ctx := context.Background()
	store, err := reflowstate.Open(ctx, reflowstate.Config{Path: checkpointPath})
	require.NoError(t, err)
	completesEmitted := 0
	for _, rec := range records1 {
		if rec.Status != "complete" {
			continue
		}
		completesEmitted++
		srcURI := "s3://source-bucket/" + rec.SourceKey
		dstURI := "s3://dest-bucket/data/" + rec.SourceKey
		recorded, status, err := store.ItemDone(ctx, srcURI, dstURI)
		require.NoError(t, err)
		require.True(t, recorded, "emitted complete for %s must be durably recorded (no false completes)", rec.SourceKey)
		require.Equal(t, "complete", status)
	}
	require.Less(t, completesEmitted, objectCount, "interruption must leave unfinished work")
	require.NoError(t, store.Close())

	// Heal and resume against the SAME store: convergence with no second land.
	close(src.release)
	var stdout2 bytes.Buffer
	cmd2 := newTransferReflowTestCommand()
	cmd2.SetIn(strings.NewReader(input))
	cmd2.SetOut(&stdout2)
	cmd2.SetErr(&bytes.Buffer{})
	cmd2.SetArgs([]string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "4", "--checkpoint", checkpointPath, "--resume"})
	require.NoError(t, cmd2.Execute(), "resume after interruption must converge")

	records2 := requireReflowRecords(t, stdout2.String())
	terminal2 := map[string]int{}
	for _, rec := range records2 {
		if rec.Status == "in_progress" {
			continue
		}
		switch {
		case rec.Status == "complete",
			rec.Status == "skipped" && rec.Reason == "resume.complete",
			rec.Status == "skipped" && rec.Reason == "collision.duplicate":
			terminal2[rec.SourceKey]++
		default:
			t.Fatalf("unexpected resume terminal status=%q reason=%q key=%q", rec.Status, rec.Reason, rec.SourceKey)
		}
	}
	require.Len(t, terminal2, objectCount, "every object reaches a convergent terminal on resume")
	for key, n := range terminal2 {
		require.Equal(t, 1, n, "exactly one terminal for %s on resume", key)
	}
	resumeCompletes := 0
	for _, rec := range records2 {
		if rec.Status == "skipped" && rec.Reason == "resume.complete" {
			resumeCompletes++
		}
	}
	require.GreaterOrEqual(t, resumeCompletes, 1,
		"the durable pre-cancel completion must be observed as a resume.complete skip")

	expectedKeys := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		require.True(t, dst.hasObject("data/"+key), "missing dest object for %s", key)
		require.Equal(t, fmt.Sprintf("payload-%02d", i), string(dst.mustObject("data/"+key)), "dest content intact for %s", key)
		expectedKeys = append(expectedKeys, "data/"+key)
	}
	// The claimed property, measured directly: exactly one SUCCESSFUL
	// object-level conditional land per destination key across cancel + resume,
	// all attempts IfAbsent, no unconditional writes.
	dst.requireExactlyOneLandPerKey(t, expectedKeys)
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

// TestTransferReflowDualPathTerminalUpsertFailureNeverAcksTerminal pins the
// strict terminal disposition on BOTH execution paths for BOTH terminal
// outcomes: when the store cannot record the terminal item, neither path
// acknowledges complete OR collision-skipped — the object reports failed
// (reason checkpoint.write_failed) and the run exits non-zero. After the store
// heals, a resume converges without a second object land.
func TestTransferReflowDualPathTerminalUpsertFailureNeverAcksTerminal(t *testing.T) {
	arms := []struct {
		name      string
		routeArgs []string
		path      string
	}{
		{name: "engine", path: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", routeArgs: []string{"--on-source-failure", "fail"}, path: reflowpkg.ExecutionPathCLIPool},
	}
	scenarios := []struct {
		name       string
		preseed    bool // identical object already at the destination -> collision skip
		ackStatus  string
		resumeWant string
	}{
		{name: "fresh complete", preseed: false, ackStatus: "complete", resumeWant: "skipped"},
		{name: "collision skip", preseed: true, ackStatus: "skipped", resumeWant: "skipped"},
	}
	for _, arm := range arms {
		for _, sc := range scenarios {
			t.Run(arm.name+"/"+sc.name, func(t *testing.T) {
				withTransferReflowTestState(t)

				injecting := true
				oldStateStore := newReflowStateStore
				newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
					store, err := oldStateStore(ctx, cfg)
					if err != nil {
						return nil, err
					}
					if injecting {
						return upsertFailingReflowState{reflowStateStore: store, err: fmt.Errorf("injected terminal upsert failure")}, nil
					}
					return store, nil
				}
				t.Cleanup(func() { newReflowStateStore = oldStateStore })

				src := newReflowMemoryProvider()
				src.putFixture("source/file.xml", "payload", "src-etag", time.Time{})
				dst := newLandCountingDest(newReflowMemoryProvider())
				if sc.preseed {
					dst.putFixture("data/source/file.xml", "payload", "src-etag", time.Time{})
				}

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

				checkpointPath := filepath.Join(t.TempDir(), "state.db")
				runOnce := func(resume bool) (string, error) {
					var stdout, stderr bytes.Buffer
					cmd := newTransferReflowTestCommand()
					cmd.SetIn(strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "") + "\n"))
					cmd.SetOut(&stdout)
					cmd.SetErr(&stderr)
					args := append([]string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2", "--checkpoint", checkpointPath}, arm.routeArgs...)
					if resume {
						args = append(args, "--resume")
					}
					cmd.SetArgs(args)
					execErr := cmd.Execute()
					return stdout.String(), execErr
				}

				stdout1, err := runOnce(false)
				require.Error(t, err, "terminal upsert failure must exit non-zero")
				run := requireRecord(t, stdout1, reflowpkg.RunRecordType, "")
				require.Contains(t, string(run.Data), fmt.Sprintf(`"execution_path":%q`, arm.path))
				records1 := requireReflowRecords(t, stdout1)
				requireReflowStatusReasonCount(t, records1, sc.ackStatus, "", 0)
				requireReflowStatusReasonCount(t, records1, "failed", "checkpoint.write_failed", 1)
				require.True(t, dst.hasObject("data/source/file.xml"))

				// Healed store: resume converges without a second object land.
				injecting = false
				stdout2, err := runOnce(true)
				require.NoError(t, err, "healthy-store resume must converge")
				records2 := requireReflowRecords(t, stdout2)
				statusSeen := 0
				for _, rec := range records2 {
					if rec.Status == sc.resumeWant {
						statusSeen++
					}
				}
				require.Equal(t, 1, statusSeen, "resume terminal is %s", sc.resumeWant)
				require.True(t, dst.hasObject("data/source/file.xml"))
				// Measured directly, per key: a fresh object lands exactly once
				// across failure + resume; a preseeded object never lands at all.
				wantLands := 1
				if sc.preseed {
					wantLands = 0
				}
				require.Equal(t, wantLands, dst.landsSnapshot()["data/source/file.xml"],
					"successful object-level lands across failure + resume")
			})
		}
	}
}

// TestTransferReflowPoolPathCLIOnlyTerminalUpsertFailureNeverAcks is the
// E-A3-I3 / §6 P1 parity gate for the CLI-only success terminals the engine does
// not yet implement, so they run on the legacy worker-pool fallback: the
// overwrite-if-source-newer skips (skipped_src_older, skipped_concurrent_mutation)
// and the collision-conflict quarantine (quarantined). Reached through REAL
// adapter selection (the engine returns ErrNotImplemented for these modes, so the
// command falls through to the pool). When the checkpoint store cannot record the
// terminal, the pool must NOT acknowledge the success terminal: it reports
// failed/checkpoint.write_failed and the run exits non-zero — matching the engine
// and the pool's own complete/collision.duplicate terminals. (The metadata
// derivation quarantine terminal shares the identical strict template; quarantine
// resume-authority is separately deferred to the engine-convergence arc.)
func TestTransferReflowPoolPathCLIOnlyTerminalUpsertFailureNeverAcks(t *testing.T) {
	scenarios := []struct {
		name          string
		successStatus string
		successReason string
		run           func(t *testing.T, checkpoint string) (string, error)
	}{
		{
			name:          "overwrite-if-source-newer/src_older",
			successStatus: "skipped",
			successReason: "collision.skipped_src_older",
			run: func(t *testing.T, checkpoint string) (string, error) {
				src := newReflowMemoryProvider()
				src.putFixture("source/file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
				dst := newReflowMemoryProvider()
				dst.ignoreIfAbsent = true
				dst.putFixture("data/source/file.xml", "old payload", "old-etag", time.Date(2026, 1, 16, 20, 53, 44, 0, time.UTC))
				stdout, _, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""),
					"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1",
					"--on-collision", "overwrite-if-source-newer", "--checkpoint", checkpoint)
				return stdout, err
			},
		},
		{
			name:          "overwrite-if-source-newer/concurrent_mutation",
			successStatus: "skipped",
			successReason: "collision.skipped_concurrent_mutation",
			run: func(t *testing.T, checkpoint string) (string, error) {
				src := newReflowMemoryProvider()
				src.putFixture("source/file.xml", "new payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
				dst := newReflowMemoryProvider()
				dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Date(2026, 1, 14, 20, 53, 44, 0, time.UTC))
				dst.mutateBeforeIfMatch = true
				return runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""),
					"--on-collision", "overwrite-if-source-newer", "--checkpoint", checkpoint)
			},
		},
		{
			name:          "quarantine/quarantined",
			successStatus: "quarantined",
			successReason: "collision.conflict.quarantined",
			run: func(t *testing.T, checkpoint string) (string, error) {
				src := newReflowMemoryProvider()
				src.putFixture("source/file.xml", "new payload", "src-etag", time.Time{})
				dst := newReflowMemoryProvider()
				dst.ignoreIfAbsent = true
				dst.putFixture("data/source/file.xml", "old payload", "old-etag", time.Time{})
				stdout, _, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.xml", "src-etag", int64(len("new payload")), "", ""),
					"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1",
					"--on-collision", "quarantine", "--collision-quarantine-prefix", "_conflict", "--checkpoint", checkpoint)
				return stdout, err
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			oldStateStore := newReflowStateStore
			newReflowStateStore = func(ctx context.Context, cfg reflowstate.Config) (reflowStateStore, error) {
				store, err := oldStateStore(ctx, cfg)
				if err != nil {
					return nil, err
				}
				return upsertFailingReflowState{reflowStateStore: store, err: fmt.Errorf("injected terminal upsert failure")}, nil
			}
			t.Cleanup(func() { newReflowStateStore = oldStateStore })

			checkpoint := filepath.Join(t.TempDir(), "state.db")
			stdout, err := sc.run(t, checkpoint)
			require.Error(t, err, "terminal upsert failure must exit non-zero")
			records := requireReflowRecords(t, stdout)
			// The success terminal is suppressed: no skipped/quarantined ack survives
			// a store that could not durably record it.
			requireReflowStatusReasonCount(t, records, sc.successStatus, sc.successReason, 0)
			// It is replaced by a typed failed terminal, matching the engine.
			requireReflowStatusReasonCount(t, records, "failed", "checkpoint.write_failed", 1)
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
		"concurrency_time_avg_active",
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

	engine := runTransferReflowBarrierArm(t, objectCount, parallel, barrierFloor, false)
	pool := runTransferReflowBarrierArm(t, objectCount, parallel, barrierFloor, false, "--on-source-failure", "fail")

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

	// Dry-run remains on the engine.
	run := requireRecord(t, stdout.String(), reflowpkg.RunRecordType, "")
	require.Contains(t, string(run.Data), `"execution_path":"engine"`)

	// Engine dry-run emits planned object records; destination objects are not landed
	// (object-store dry-run may still run a write/delete preflight probe).
	records := requireReflowRecords(t, stdout.String())
	requireReflowStatusReasonCount(t, records, "planned", "", objectCount)
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/dry-%02d.xml", i)
		require.False(t, dst.hasObject("data/"+key), "dry-run must not land %s", key)
	}
}
