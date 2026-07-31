package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/runbudget"
)

var (
	testDomainA = runbudget.Domain{Version: 1, ID: "test-service"}
	testDomainB = runbudget.Domain{Version: 1, ID: "test-account"}
)

type laneTestProvider struct {
	list func(context.Context, provider.ListOptions) (*provider.ListResult, error)
}

func (p *laneTestProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return p.list(ctx, opts)
}

func (*laneTestProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, errors.New("unexpected Head")
}

func (*laneTestProvider) Close() error { return nil }

func laneTestAuthority(t *testing.T, prefixes []string, maxLanes int) *partition.Authority {
	t.Helper()
	authority, err := partition.CompileAuthority(partition.PlanRequest{
		Prefixes:          prefixes,
		Coverage:          partition.CoverageComplete,
		BaseIdentity:      "test-source",
		ConfigFingerprint: "test-config",
		MaxLanes:          maxLanes,
	})
	if err != nil {
		t.Fatalf("CompileAuthority: %v", err)
	}
	return authority
}

func laneTestBudget(t *testing.T, listSlots int) *runbudget.Budget {
	t.Helper()
	budget, err := runbudget.New(runbudget.Limits{
		InFlight: map[runbudget.OpClass]int{runbudget.OpList: listSlots},
	})
	if err != nil {
		t.Fatalf("runbudget.New: %v", err)
	}
	return budget
}

func collectLaneObjects(t *testing.T, objects <-chan LaneObject, errc <-chan error) []LaneObject {
	t.Helper()
	var out []LaneObject
	for object := range objects {
		out = append(out, object)
	}
	if err := <-errc; err != nil {
		t.Fatalf("lane stream: %v", err)
	}
	return out
}

func TestLaneExecutorStreamsEveryLaneWithAuthorityIssuedIdentity(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/", "b/", "c/"}, 3)
	var wrongPageSize atomic.Bool
	p := &laneTestProvider{list: func(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
		if opts.MaxKeys != 17 {
			wrongPageSize.Store(true)
		}
		return &provider.ListResult{
			Objects: []provider.ObjectSummary{{Key: opts.Prefix + "object"}},
		}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority:     authority,
		Provider:      p,
		Budget:        laneTestBudget(t, 3),
		Domains:       []runbudget.Domain{testDomainA},
		PageSize:      17,
		QueueCapacity: 2,
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	objects, errc := executor.Stream(context.Background())
	got := collectLaneObjects(t, objects, errc)
	if len(got) != 3 {
		t.Fatalf("emitted %d objects, want 3", len(got))
	}
	if wrongPageSize.Load() {
		t.Fatal("executor did not pass the configured page bound to Provider.List")
	}
	seen := make(map[string]int, len(got))
	for _, item := range got {
		if err := authority.AuthorizeLane(item.Lane); err != nil {
			t.Fatalf("executor emitted an unauthorized lane reference: %v", err)
		}
		seen[item.Object.Key] = item.Lane.Ordinal
	}
	for _, prefix := range []string{"a/", "b/", "c/"} {
		if seen[prefix+"object"] == 0 {
			t.Errorf("object under prefix %q was not emitted", prefix)
		}
	}
}

func TestLaneExecutorSharesOneListCeilingAcrossAllLanes(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/", "b/", "c/", "d/"}, 4)
	var active, maxActive atomic.Int32
	p := &laneTestProvider{list: func(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := maxActive.Load()
			if n <= old || maxActive.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &provider.ListResult{Objects: []provider.ObjectSummary{{Key: opts.Prefix}}}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority: authority,
		Provider:  p,
		Budget:    laneTestBudget(t, 1),
		Domains:   []runbudget.Domain{testDomainA},
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	objects, errc := executor.Stream(context.Background())
	got := collectLaneObjects(t, objects, errc)
	if len(got) != 4 {
		t.Fatalf("emitted %d objects, want 4", len(got))
	}
	if max := maxActive.Load(); max != 1 {
		t.Fatalf("observed %d concurrent LIST calls with a shared ceiling of 1", max)
	}
}

func TestLaneExecutorBoundedQueueBackpressuresPagination(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/"}, 1)
	var calls atomic.Int32
	p := &laneTestProvider{list: func(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
		call := calls.Add(1)
		return &provider.ListResult{
			Objects:           []provider.ObjectSummary{{Key: fmt.Sprintf("%s%d", opts.Prefix, call)}},
			ContinuationToken: fmt.Sprint(call),
			IsTruncated:       call < 10,
		}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority:     authority,
		Provider:      p,
		Budget:        laneTestBudget(t, 1),
		Domains:       []runbudget.Domain{testDomainA},
		PageSize:      1,
		QueueCapacity: 2,
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	objects, errc := executor.Stream(ctx)
	t.Cleanup(cancel)

	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("provider reached %d pages before the capacity-2 queue blocked it, want 3", got)
	}
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 3 {
		t.Fatalf("provider advanced to %d pages while the capacity-2 queue had no consumer", got)
	}

	cancel()
	for range objects {
	}
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("stream error %v, want context cancellation", err)
	}
}

func TestLaneExecutorCancellationUnblocksFullQueue(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/", "b/"}, 2)
	p := &laneTestProvider{list: func(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
		return &provider.ListResult{
			Objects: []provider.ObjectSummary{
				{Key: opts.Prefix + "1"},
				{Key: opts.Prefix + "2"},
			},
		}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority: authority,
		Provider:  p,
		Budget:    laneTestBudget(t, 2),
		Domains:   []runbudget.Domain{testDomainA},
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	objects, errc := executor.Stream(ctx)
	cancel()

	select {
	case _, ok := <-objects:
		if ok {
			for range objects {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("object stream did not close after cancellation blocked its unbuffered sends")
	}
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("stream error %v, want context cancellation", err)
	}
}

func TestLaneExecutorBroadcastsThrottleToCompositeDomains(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/"}, 1)
	budget, err := runbudget.New(runbudget.Limits{
		RequestsPerSecond: 100,
		Burst:             100,
		InFlight:          map[runbudget.OpClass]int{runbudget.OpList: 1},
	})
	if err != nil {
		t.Fatalf("runbudget.New: %v", err)
	}
	p := &laneTestProvider{list: func(context.Context, provider.ListOptions) (*provider.ListResult, error) {
		return nil, provider.ErrThrottled
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority: authority,
		Provider:  p,
		Budget:    budget,
		Domains:   []runbudget.Domain{testDomainA, testDomainB},
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	objects, errc := executor.Stream(context.Background())
	for range objects {
	}
	if err := <-errc; !provider.IsThrottled(err) {
		t.Fatalf("stream error %v, want provider throttle identity", err)
	}

	usage := budget.Snapshot()
	if len(usage.Domains) != 2 {
		t.Fatalf("budget recorded %d domains, want both composite domains", len(usage.Domains))
	}
	for _, domain := range usage.Domains {
		if domain.RateFactor != 0.5 {
			t.Errorf("domain %s rate factor %v, want shared throttle factor 0.5", domain.Domain, domain.RateFactor)
		}
	}
}

func TestLaneExecutorRefusesInvalidConfigBeforeProviderUse(t *testing.T) {
	var calls atomic.Int32
	p := &laneTestProvider{list: func(context.Context, provider.ListOptions) (*provider.ListResult, error) {
		calls.Add(1)
		return &provider.ListResult{}, nil
	}}
	authority := laneTestAuthority(t, []string{"a/"}, 1)
	budget := laneTestBudget(t, 1)

	tests := []struct {
		name string
		cfg  LaneExecutorConfig
	}{
		{name: "nil authority", cfg: LaneExecutorConfig{Provider: p, Budget: budget, Domains: []runbudget.Domain{testDomainA}}},
		{name: "nil provider", cfg: LaneExecutorConfig{Authority: authority, Budget: budget, Domains: []runbudget.Domain{testDomainA}}},
		{name: "nil budget", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Domains: []runbudget.Domain{testDomainA}}},
		{name: "no domains", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget}},
		{name: "invalid domain", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget, Domains: []runbudget.Domain{{}}}},
		{name: "duplicate domain", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget, Domains: []runbudget.Domain{testDomainA, testDomainA}}},
		{name: "negative page size", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget, Domains: []runbudget.Domain{testDomainA}, PageSize: -1}},
		{name: "page size above ceiling", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget, Domains: []runbudget.Domain{testDomainA}, PageSize: PageSizeCeiling + 1}},
		{name: "negative queue capacity", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget, Domains: []runbudget.Domain{testDomainA}, QueueCapacity: -1}},
		{name: "queue capacity above ceiling", cfg: LaneExecutorConfig{Authority: authority, Provider: p, Budget: budget, Domains: []runbudget.Domain{testDomainA}, QueueCapacity: QueueCapacityCeiling + 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewLaneExecutor(tc.cfg); err == nil {
				t.Fatal("NewLaneExecutor accepted invalid configuration")
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("invalid configurations made %d provider calls", got)
	}
}

func TestLaneExecutorRefusesProviderPageAboveRequestedBound(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/"}, 1)
	p := &laneTestProvider{list: func(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
		if opts.MaxKeys != 1 {
			t.Errorf("List MaxKeys %d, want explicit bound 1", opts.MaxKeys)
		}
		return &provider.ListResult{
			Objects: []provider.ObjectSummary{{Key: "a/1"}, {Key: "a/2"}},
		}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority: authority,
		Provider:  p,
		Budget:    laneTestBudget(t, 1),
		Domains:   []runbudget.Domain{testDomainA},
		PageSize:  1,
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	objects, errc := executor.Stream(context.Background())
	for range objects {
	}
	if err := <-errc; err == nil {
		t.Fatal("provider page above the requested bound was admitted")
	}
}

func TestLaneExecutorRefusesTruncatedPageWithoutContinuation(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/"}, 1)
	p := &laneTestProvider{list: func(context.Context, provider.ListOptions) (*provider.ListResult, error) {
		return &provider.ListResult{IsTruncated: true}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority: authority,
		Provider:  p,
		Budget:    laneTestBudget(t, 1),
		Domains:   []runbudget.Domain{testDomainA},
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	objects, errc := executor.Stream(context.Background())
	for range objects {
	}
	if err := <-errc; err == nil {
		t.Fatal("truncated page without a continuation token was reported as lane EOF")
	}
}

func TestLaneExecutorConcurrentStreamCallsDoNotMutateConfig(t *testing.T) {
	authority := laneTestAuthority(t, []string{"a/", "b/"}, 2)
	p := &laneTestProvider{list: func(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
		return &provider.ListResult{Objects: []provider.ObjectSummary{{Key: opts.Prefix}}}, nil
	}}
	executor, err := NewLaneExecutor(LaneExecutorConfig{
		Authority: authority,
		Provider:  p,
		Budget:    laneTestBudget(t, 4),
		Domains:   []runbudget.Domain{testDomainA},
	})
	if err != nil {
		t.Fatalf("NewLaneExecutor: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan int, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			objects, errc := executor.Stream(context.Background())
			count := 0
			for range objects {
				count++
			}
			if err := <-errc; err != nil {
				count = -1
			}
			results <- count
		}()
	}
	wg.Wait()
	close(results)
	for count := range results {
		if count != 2 {
			t.Errorf("stream result %d, want 2 objects and no error", count)
		}
	}
}
