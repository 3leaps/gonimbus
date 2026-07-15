package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

func TestPlanTransferReflowEngineAdapter_LiveCopyFallsBackToCLIPool(t *testing.T) {
	withTransferReflowTestState(t)
	reflowStdin = true
	reflowDryRun = false

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

func TestTransferReflowCommand_LiveStdinUsesCLIPoolMaxInFlight(t *testing.T) {
	// Package-global factories / probe; do not t.Parallel.
	withTransferReflowTestState(t)

	const (
		objectCount  = 32
		parallel     = 8
		barrierFloor = 4
	)

	srcBase := newReflowMemoryProvider()
	dst := newReflowMemoryProvider()
	src := newConcurrentGetBarrierProvider(srcBase, barrierFloor, 2*time.Second)

	expectedKeys := make(map[string]struct{}, objectCount)
	lines := make([]string, 0, objectCount)
	// Leading blanks exercise sniff replay (first nonblank record must not drop).
	lines = append(lines, "", "")
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		body := fmt.Sprintf("payload-%02d", i)
		srcBase.putFixture(key, body, fmt.Sprintf("etag-%02d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		expectedKeys[key] = struct{}{}
		lines = append(lines, reflowInputLine(key, fmt.Sprintf("etag-%02d", i), int64(len(body)), "", ""))
	}
	input := strings.Join(lines, "\n") + "\n"

	// Room for eight workers: memory budget must exceed 8 * retry buffer.
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
		"--parallel", fmt.Sprintf("%d", parallel),
		// default skip-if-duplicate collision mode
	})

	require.NoError(t, cmd.Execute(), "stderr=%s\nstdout=%s", stderr.String(), stdout.String())

	// Concurrency precondition: host probes must not clamp below requested 8.
	// Run record embeds ConcurrencyStats flat on the data payload.
	run := requireRecord(t, stdout.String(), reflowpkg.RunRecordType, "")
	require.Contains(t, string(run.Data), `"concurrency_ceiling_effective":8`)
	require.Contains(t, string(run.Data), `"concurrency_ceiling_requested":8`)
	require.Contains(t, string(run.Data), `"concurrency_initial":8`)
	require.Contains(t, string(run.Data), `"adaptive_enabled":true`)

	// Diagnostic: exact static reason, no URI/key material.
	require.Contains(t, stderr.String(), "execution_path=cli-pool")
	require.Contains(t, stderr.String(), "reason="+transferReflowLiveCopyCLIPoolReason)
	require.NotContains(t, stderr.String(), "s3://source-bucket")
	require.NotContains(t, stderr.String(), "s3://dest-bucket")
	require.NotContains(t, stderr.String(), "source/obj-")
	require.NotContains(t, stderr.String(), "GON-")

	// Behavioral gate: pooled execution must overlap GetObject calls.
	require.GreaterOrEqual(t, src.maxInFlight.Load(), int64(barrierFloor),
		"max concurrent GetObject calls=%d; expected pooled execution (>= %d)", src.maxInFlight.Load(), barrierFloor)

	// One-to-one replay over *terminal* object records.
	// CLI also emits non-terminal in_progress per object; the locked AC is
	// N inputs → N terminal results (no drop, no duplicate). Counting only
	// status==complete keys would miss complete+skipped duplication of the
	// sniffed first record.
	records := requireReflowRecords(t, stdout.String())
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
	for key := range expectedKeys {
		require.Equal(t, 1, keyCounts[key], "expected source key %s once among terminal records", key)
	}
	for key, n := range keyCounts {
		_, ok := expectedKeys[key]
		require.True(t, ok, "unexpected source key %q among terminal records (count=%d)", key, n)
	}
	require.Equal(t, 1, keyCounts["source/obj-00.xml"], "sniffed first record must appear once terminal")
	require.Equal(t, 1, keyCounts["source/obj-31.xml"], "final record must appear once terminal")
	requireReflowStatusReasonCount(t, records, "complete", "", objectCount)

	// Dest received each object under the dest prefix.
	for i := 0; i < objectCount; i++ {
		key := fmt.Sprintf("source/obj-%02d.xml", i)
		require.True(t, dst.hasObject("data/"+key), "missing dest object for %s", key)
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
