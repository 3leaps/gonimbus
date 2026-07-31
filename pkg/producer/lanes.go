package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/runbudget"
)

// LaneObject is one listed object bound to the validated lane that enumerated
// it. The reference is issued by the executor's Authority; it is never accepted
// from provider data.
type LaneObject struct {
	Lane   partition.LaneRef
	Object provider.ObjectSummary
}

// LaneEnumerator is the partitioned enumeration seam consumed by workflow
// engines. The consumer holds its own partition Authority and authenticates the
// LaneRef carried by each object before handing that object to a mutating stage.
type LaneEnumerator interface {
	Stream(context.Context) (<-chan LaneObject, <-chan error)
}

const (
	// DefaultPageSize is the explicit provider-page bound used when a caller
	// leaves PageSize at zero.
	DefaultPageSize = 1000
	// PageSizeCeiling prevents one lane from retaining an arbitrarily large
	// provider response while it is blocked on the shared output queue.
	PageSizeCeiling = 1000
	// QueueCapacityCeiling bounds caller-controlled aggregate retention in the
	// output channel. Per-lane page retention is separately bounded by
	// PageSizeCeiling and partition.LaneCeiling.
	QueueCapacityCeiling = partition.LaneCeiling * PageSizeCeiling
)

// LaneExecutorConfig supplies the run-owned dependencies for partitioned
// provider enumeration.
//
// Domains are the quota domains the provider adapter says each LIST consumes.
// They are deliberately separate from Authority.BaseIdentity: resume identity
// and provider quota identity answer different questions and are not
// interchangeable.
type LaneExecutorConfig struct {
	Authority *partition.Authority
	Provider  provider.Provider
	Budget    *runbudget.Budget
	Domains   []runbudget.Domain

	// PageSize is passed to Provider.List. Zero resolves to DefaultPageSize.
	PageSize int
	// QueueCapacity bounds the aggregate object frontier between all lane
	// enumerators and their consumer. Zero selects an unbuffered handoff.
	QueueCapacity int
}

// LaneExecutor runs every lane of one validated plan against a shared provider
// budget and emits objects onto one bounded stream.
//
// The executor is non-mutating: it lists and assigns lane identity, but does not
// probe content, write destinations, or decide lane completion. A downstream
// coordinator owns those state transitions.
type LaneExecutor struct {
	authority     *partition.Authority
	provider      provider.Provider
	budget        *runbudget.Budget
	domains       []runbudget.Domain
	pageSize      int
	queueCapacity int
}

// NewLaneExecutor validates the complete execution contract before any provider
// call or goroutine can start.
func NewLaneExecutor(cfg LaneExecutorConfig) (*LaneExecutor, error) {
	if cfg.Authority == nil {
		return nil, errors.New("producer: lane executor requires a validated partition authority")
	}
	if cfg.Provider == nil {
		return nil, errors.New("producer: lane executor requires a provider")
	}
	if cfg.Budget == nil {
		return nil, errors.New("producer: lane executor requires a shared run budget")
	}
	if len(cfg.Domains) == 0 {
		return nil, errors.New("producer: lane executor requires at least one provider quota domain")
	}
	if cfg.PageSize < 0 {
		return nil, fmt.Errorf("producer: lane executor page size must not be negative, got %d", cfg.PageSize)
	}
	if cfg.PageSize > PageSizeCeiling {
		return nil, fmt.Errorf("producer: lane executor page size %d exceeds the ceiling of %d", cfg.PageSize, PageSizeCeiling)
	}
	if cfg.QueueCapacity < 0 {
		return nil, fmt.Errorf("producer: lane executor queue capacity must not be negative, got %d", cfg.QueueCapacity)
	}
	if cfg.QueueCapacity > QueueCapacityCeiling {
		return nil, fmt.Errorf("producer: lane executor queue capacity %d exceeds the ceiling of %d", cfg.QueueCapacity, QueueCapacityCeiling)
	}
	pageSize := cfg.PageSize
	if pageSize == 0 {
		pageSize = DefaultPageSize
	}

	domains := append([]runbudget.Domain(nil), cfg.Domains...)
	seen := make(map[runbudget.Domain]struct{}, len(domains))
	for _, domain := range domains {
		if err := domain.Validate(); err != nil {
			return nil, fmt.Errorf("producer: lane executor quota domain: %w", err)
		}
		if _, duplicate := seen[domain]; duplicate {
			return nil, fmt.Errorf("producer: lane executor quota domain %s is duplicated", domain)
		}
		seen[domain] = struct{}{}
	}

	return &LaneExecutor{
		authority:     cfg.Authority,
		provider:      cfg.Provider,
		budget:        cfg.Budget,
		domains:       domains,
		pageSize:      pageSize,
		queueCapacity: cfg.QueueCapacity,
	}, nil
}

// Stream starts one enumerator per lane and returns its bounded object stream.
//
// The object channel closes after every enumerator exits. The error channel then
// yields at most one error: the first lane failure, or the parent context error
// when the caller cancelled the run. A lane failure cancels its siblings; sends
// and budget waits observe that cancellation, so a full output queue cannot
// strand the executor.
//
// Cancellation cannot interrupt a Provider.List implementation that ignores
// its context. Bounding a stuck provider read remains the provider transport's
// deadline responsibility.
func (e *LaneExecutor) Stream(ctx context.Context) (<-chan LaneObject, <-chan error) {
	objects := make(chan LaneObject, e.queueCapacity)
	errc := make(chan error, 1)

	runCtx, cancel := context.WithCancelCause(ctx)
	lanes := e.authority.Lanes()

	var (
		wg       sync.WaitGroup
		failOnce sync.Once
		firstErr error
	)
	fail := func(err error) {
		if err == nil {
			return
		}
		failOnce.Do(func() {
			firstErr = err
			cancel(err)
		})
	}

	wg.Add(len(lanes))
	for _, lane := range lanes {
		lane := lane
		go func() {
			defer wg.Done()
			if err := e.enumerateLane(runCtx, lane, objects); err != nil {
				fail(err)
			}
		}()
	}

	go func() {
		wg.Wait()
		cancel(nil)
		close(objects)
		defer close(errc)

		if firstErr != nil {
			errc <- firstErr
			return
		}
		if err := ctx.Err(); err != nil {
			errc <- err
		}
	}()

	return objects, errc
}

func (e *LaneExecutor) enumerateLane(ctx context.Context, lane partition.Lane, out chan<- LaneObject) error {
	ref, err := e.authority.LaneRef(lane.Ordinal)
	if err != nil {
		return err
	}
	for _, prefix := range lane.Prefixes {
		if err := e.enumeratePrefix(ctx, ref, prefix, out); err != nil {
			return err
		}
	}
	return nil
}

func (e *LaneExecutor) enumeratePrefix(ctx context.Context, ref partition.LaneRef, prefix string, out chan<- LaneObject) error {
	var token string
	for {
		result, err := e.listPage(ctx, ref, prefix, token)
		if err != nil {
			return err
		}
		for _, object := range result.Objects {
			select {
			case out <- LaneObject{Lane: ref, Object: object}:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		if !result.IsTruncated {
			return nil
		}
		if result.ContinuationToken == "" {
			return fmt.Errorf("producer: provider returned a truncated LIST page without a continuation token for %s", ref)
		}
		token = result.ContinuationToken
	}
}

func (e *LaneExecutor) listPage(ctx context.Context, ref partition.LaneRef, prefix, token string) (*provider.ListResult, error) {
	lease, err := e.budget.Acquire(ctx, runbudget.Request{
		Domains:  e.domains,
		Class:    runbudget.OpList,
		Consumer: ref.String(),
	})
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	result, listErr := e.provider.List(ctx, provider.ListOptions{
		Prefix:            prefix,
		ContinuationToken: token,
		MaxKeys:           e.pageSize,
	})
	lease.CompleteRequest()

	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if listErr != nil {
		if provider.IsThrottled(listErr) {
			var throttleErr error
			for _, domain := range e.domains {
				throttleErr = errors.Join(throttleErr, e.budget.Throttle(domain, 0))
			}
			if throttleErr != nil {
				return nil, errors.Join(listErr, fmt.Errorf("producer: record shared provider throttle: %w", throttleErr))
			}
		}
		return nil, fmt.Errorf("producer: LIST %s: %w", ref, listErr)
	}
	if result == nil {
		return nil, fmt.Errorf("producer: LIST %s: provider returned no result", ref)
	}
	if len(result.Objects) > e.pageSize {
		return nil, fmt.Errorf("producer: LIST %s: provider returned %d objects above the requested page bound of %d",
			ref, len(result.Objects), e.pageSize)
	}
	return result, nil
}
