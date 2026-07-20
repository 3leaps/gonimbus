package reflow

import (
	"context"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

const (
	concurrencyFloor              = 1
	concurrencyDefaultInitial     = 16
	concurrencyDefaultRequested   = 16
	concurrencyCleanIncreaseEvery = 8
	concurrencyThrottleCooldown   = 4
	resourceDefaultMemoryLimit    = int64(1 << 30)
	resourceMemoryFraction        = 0.25
	resourceFDHeadroom            = 128
	resourceFDsPerCopy            = 1
	resourceMinCap                = 1
	resourceMaxCap                = int(^uint(0) >> 1)
)

// Memory-limit source labels reported in concurrency_ceiling_reason and the
// run-record memory fields. Platform probes additionally report cgroup_v2 /
// cgroup_v1.
const (
	memorySourceRuntime              = "runtime"
	memorySourcePhysicalRAM          = "physical_ram"
	memorySourceDetectionUnavailable = "detection_unavailable"
)

// Memory-budget source labels reported in the run-record memory fields.
const (
	memoryBudgetSourceDerived         = "derived"
	memoryBudgetSourceOperator        = "operator"
	memoryBudgetSourceOperatorClamped = "operator_clamped_to_limit"
	memoryBudgetReasonLabel           = "operator_budget"
)

// ResourceProbe supplies host resource limits used to clamp requested transfer
// concurrency before any worker pools, queues, or retry buffers are created.
type ResourceProbe struct {
	MemoryLimitBytes func() (int64, string, error)
	FDSoftLimit      func() (int64, error)
}

// ConcurrencyConfig is the resolved GON-048 concurrency contract. RequestedCeiling
// is the user/operator ceiling; EffectiveCeiling is the resource-safe hard cap.
//
// The Memory* fields record the arithmetic behind a memory-derived ceiling:
// the detected limit, and the budget (either derived as a fraction of the
// limit or operator-supplied). Configs constructed directly with an explicit
// EffectiveCeiling and no probe report zero memory fields — the ceiling was
// not memory-resolved and the records say so.
type ConcurrencyConfig struct {
	RequestedCeiling int
	EffectiveCeiling int
	CeilingReason    string
	AdaptiveEnabled  bool
	Floor            int
	Initial          int

	MemoryLimitBytes           int64
	MemoryLimitSource          string
	MemoryBudgetRequestedBytes int64
	MemoryBudgetEffectiveBytes int64
	MemoryBudgetSource         string
	// RetryBufferCapBytes is the resolved per-copy in-memory retry-buffer
	// bound (min of the transfer default and the effective budget); the copy
	// paths must allocate against this value, spooling above it. Zero means
	// not memory-resolved — consumers fall back to the transfer default.
	RetryBufferCapBytes int64
}

// ConcurrencyStats is embedded in reflow run and summary records.
type ConcurrencyStats struct {
	AdaptiveEnabled                   bool   `json:"adaptive_enabled"`
	ConcurrencyFloor                  int    `json:"concurrency_floor"`
	ConcurrencyInitial                int    `json:"concurrency_initial"`
	ConcurrencyCeilingRequested       int    `json:"concurrency_ceiling_requested"`
	ConcurrencyCeilingEffective       int    `json:"concurrency_ceiling_effective"`
	ConcurrencyCeilingReason          string `json:"concurrency_ceiling_reason"`
	ConcurrencyFinal                  int    `json:"concurrency_final"`
	ConcurrencyThrottleBackoffs       int64  `json:"concurrency_throttle_backoffs"`
	ConcurrencyAdditiveIncreases      int64  `json:"concurrency_additive_increases"`
	ConcurrencyConnectionErrorFreezes int64  `json:"concurrency_connection_error_freezes"`
	ConcurrencyMaxActive              int    `json:"concurrency_max_active"`
	// ConcurrencyTimeAvgActive is limiter-active provider-operation-seconds
	// divided by the common execution window (rounded to 3 decimals) — the
	// window starts when the run record is emitted on either path, so setup
	// and preflight are excluded identically. The run record's startup sample
	// is always 0; the summary value is the completed-run diagnostic. Low
	// time-average under a high effective ceiling is a signal to read
	// together with throttle/freeze counters and memory-pressure evidence,
	// not standalone proof of a starved producer.
	ConcurrencyTimeAvgActive float64 `json:"concurrency_time_avg_active"`
	// Memory fields are omitted when the ceiling was not memory-resolved
	// (configs constructed directly with an explicit EffectiveCeiling and no
	// probe); probe-resolved runs always populate limit, budget, and sources.
	MemoryLimitBytes           int64  `json:"memory_limit_bytes,omitempty"`
	MemoryLimitSource          string `json:"memory_limit_source,omitempty"`
	MemoryBudgetRequestedBytes int64  `json:"memory_budget_requested_bytes,omitempty"`
	MemoryBudgetEffectiveBytes int64  `json:"memory_budget_effective_bytes,omitempty"`
	MemoryBudgetSource         string `json:"memory_budget_source,omitempty"`
	RetryBufferCapBytes        int64  `json:"retry_buffer_cap_bytes,omitempty"`
	// Ledger pressure telemetry: peak outstanding copy-buffer reservation and
	// admission-wait evidence. Read together with occupancy — waits with peak
	// near the budget indicate memory admission, not a starved producer.
	MemoryReservedPeakBytes int64 `json:"memory_reserved_peak_bytes,omitempty"`
	MemoryReservationWaits  int64 `json:"memory_reservation_waits,omitempty"`
	MemoryReservationWaitMS int64 `json:"memory_reservation_wait_ms,omitempty"`
}

// ConcurrencyLimiter gates active copy work and applies AIMD feedback under the
// resource-derived effective ceiling.
type ConcurrencyLimiter struct {
	mu sync.Mutex

	cfg       ConcurrencyConfig
	current   int
	active    int
	maxActive int
	clean     int
	cooldown  int

	throttleBackoffs  int64
	additiveIncreases int64
	connectionFreezes int64

	// Time-averaged occupancy accounting: activeIntegral accumulates
	// worker-seconds at every active-count transition, so Snapshot can report
	// the run's time-averaged active concurrency — a bound producer shows a
	// low time-average under a high ceiling.
	clock          func() time.Time
	startedAt      time.Time
	lastTransition time.Time
	activeIntegral float64

	// ledger admits copy buffer bytes under the effective memory budget; nil
	// for configs that were not memory-resolved (no budget to govern).
	ledger *memoryLedger
}

// DefaultResourceProbe returns the platform probes used by transfer reflow.
func DefaultResourceProbe() ResourceProbe {
	return ResourceProbe{
		MemoryLimitBytes: defaultMemoryLimitBytes,
		FDSoftLimit:      defaultFDSoftLimit,
	}
}

func defaultMemoryLimitBytes() (int64, string, error) {
	return memoryLimitFromChain(defaultPlatformMemoryLimitBytes, runtimeMemoryLimitBytes, defaultPhysicalMemoryBytes)
}

func runtimeMemoryLimitBytes() int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit > 0 && limit < math.MaxInt64 {
		return limit
	}
	return 0
}

// memoryLimitFromChain resolves the memory limit that derives the transfer
// concurrency budget. All viable candidates are probed — container/cgroup hard
// limit, explicit runtime limit (GOMEMLIMIT), detected physical RAM — and the
// LOWEST positive value binds, with its source recorded. An explicit runtime
// limit may tighten ambient capacity but never authorizes exceeding a lower
// known bound. The conservative default applies only when no candidate
// succeeds — a probe failure never silently assumes host capacity.
func memoryLimitFromChain(platform func() (int64, string, error), runtimeLimit func() int64, physical func() (int64, error)) (int64, string, error) {
	binding := int64(0)
	bindingSource := ""
	consider := func(limit int64, source string) {
		if limit > 0 && (binding == 0 || limit < binding) {
			binding, bindingSource = limit, source
		}
	}
	if limit, source, err := platform(); err == nil {
		consider(limit, source)
	}
	consider(runtimeLimit(), memorySourceRuntime)
	if limit, err := physical(); err == nil {
		consider(limit, memorySourcePhysicalRAM)
	}
	if binding <= 0 {
		return resourceDefaultMemoryLimit, memorySourceDetectionUnavailable, nil
	}
	return binding, bindingSource, nil
}

// ResolveConcurrency clamps the requested ceiling by memory and FD limits before
// the caller allocates concurrency-sized resources. The memory budget is derived
// as a fraction of the detected limit; use ResolveConcurrencyWithBudget to
// supply an operator budget instead.
func ResolveConcurrency(requested int, adaptiveEnabled bool, probe ResourceProbe) ConcurrencyConfig {
	return ResolveConcurrencyWithBudget(requested, adaptiveEnabled, probe, 0)
}

// ResolveConcurrencyWithBudget resolves like ResolveConcurrency with an
// explicit operator memory budget (bytes) replacing the fraction-derived
// budget. The operator budget is bounded above by the detected memory limit
// (recorded as operator_clamped_to_limit); when detection is unavailable the
// operator value is authoritative. operatorBudgetBytes <= 0 means no override.
// Callers own rejecting invalid operator values before resolution — this
// function never refuses; it clamps and records.
func ResolveConcurrencyWithBudget(requested int, adaptiveEnabled bool, probe ResourceProbe, operatorBudgetBytes int64) ConcurrencyConfig {
	if requested < 1 {
		requested = 1
	}
	if probe.MemoryLimitBytes == nil {
		probe.MemoryLimitBytes = defaultMemoryLimitBytes
	}
	if probe.FDSoftLimit == nil {
		probe.FDSoftLimit = defaultFDSoftLimit
	}

	memoryLimit, memorySource, memoryErr := probe.MemoryLimitBytes()
	if memoryLimit <= 0 || memoryErr != nil {
		memoryLimit = resourceDefaultMemoryLimit
		memorySource = memorySourceDetectionUnavailable
	}

	budgetRequested := int64(0)
	budgetSource := memoryBudgetSourceDerived
	memoryBudget := int64(float64(memoryLimit) * resourceMemoryFraction)
	if operatorBudgetBytes > 0 {
		budgetRequested = operatorBudgetBytes
		budgetSource = memoryBudgetSourceOperator
		memoryBudget = operatorBudgetBytes
		if memorySource != memorySourceDetectionUnavailable && memoryBudget > memoryLimit {
			memoryBudget = memoryLimit
			budgetSource = memoryBudgetSourceOperatorClamped
		}
	}
	if memoryBudget < 1 {
		memoryBudget = 1
	}
	// The per-copy retry-buffer cap shrinks to the budget rather than the
	// budget rising to the cap: a detected hard limit is never exceeded — the
	// records and the copy-path allocator share this one resolved bound, and
	// objects above it spool instead of buffering.
	retryBufferCap := transfer.DefaultRetryBufferMaxMemoryBytes
	if memoryBudget < retryBufferCap {
		retryBufferCap = memoryBudget
	}
	memoryCap := int(memoryBudget / retryBufferCap)

	fdLimit, fdErr := probe.FDSoftLimit()
	if fdLimit <= 0 || fdErr != nil {
		fdLimit = int64(resourceFDHeadroom + resourceMinCap)
	}
	fdCap := int((fdLimit - resourceFDHeadroom) / resourceFDsPerCopy)
	if fdCap < resourceMinCap {
		fdCap = resourceMinCap
	}

	effective := requested
	var reasons []string
	if memoryCap < effective {
		effective = memoryCap
		memoryReasonLabel := memorySource
		if budgetSource != memoryBudgetSourceDerived {
			memoryReasonLabel = memoryBudgetReasonLabel
		}
		reasons = append(reasons, "memory:"+memoryReasonLabel)
	}
	if fdCap < effective {
		effective = fdCap
		reasons = append(reasons, "fd")
	}
	if effective < resourceMinCap {
		effective = resourceMinCap
	}
	reason := "requested"
	if len(reasons) > 0 {
		reason = "resource_capped:" + strings.Join(reasons, ",")
	}

	initial := effective
	if adaptiveEnabled {
		initial = minInt(concurrencyDefaultInitial, effective)
	}

	return ConcurrencyConfig{
		RequestedCeiling:           requested,
		EffectiveCeiling:           effective,
		CeilingReason:              reason,
		AdaptiveEnabled:            adaptiveEnabled,
		Floor:                      concurrencyFloor,
		Initial:                    initial,
		MemoryLimitBytes:           memoryLimit,
		MemoryLimitSource:          memorySource,
		MemoryBudgetRequestedBytes: budgetRequested,
		MemoryBudgetEffectiveBytes: memoryBudget,
		MemoryBudgetSource:         budgetSource,
		RetryBufferCapBytes:        retryBufferCap,
	}
}

// normalizeConcurrency resolves the documented zero-value Config.Concurrency to
// resource-resolved defaults and floors partial configs into internal
// consistency, so pool size, limiter configuration, and run-record fields all
// derive from one normalized config. An unresolved effective ceiling (zero)
// always goes through ResolveConcurrency — a run never executes with more (or
// different) concurrency than its records report.
func normalizeConcurrency(cfg ConcurrencyConfig) ConcurrencyConfig {
	if cfg.EffectiveCeiling < 1 {
		requested := cfg.RequestedCeiling
		adaptive := cfg.AdaptiveEnabled
		if requested < 1 {
			requested = concurrencyDefaultRequested
			adaptive = true
		}
		return clampConcurrencyInvariants(ResolveConcurrency(requested, adaptive, DefaultResourceProbe()))
	}
	return clampConcurrencyInvariants(cfg)
}

// clampConcurrencyInvariants floors a config with a resolved effective ceiling
// into the invariant 1 <= Floor <= Initial <= EffectiveCeiling <=
// RequestedCeiling. The floor may never exceed the effective ceiling: AIMD
// multiplicative decrease recovers to max(Floor, current/2), so an over-large
// floor would push observed concurrency above the resolved ceiling the records
// report. Fixed (non-adaptive) mode has no ramp — the limiter runs at Initial
// forever — so Initial IS the effective ceiling (ResolveConcurrency's canon)
// and a partial fixed config must not execute below what its records report.
func clampConcurrencyInvariants(cfg ConcurrencyConfig) ConcurrencyConfig {
	if cfg.EffectiveCeiling < 1 {
		cfg.EffectiveCeiling = 1
	}
	if cfg.RequestedCeiling < 1 {
		cfg.RequestedCeiling = cfg.EffectiveCeiling
	}
	if cfg.EffectiveCeiling > cfg.RequestedCeiling {
		cfg.EffectiveCeiling = cfg.RequestedCeiling
	}
	if cfg.Floor < 1 {
		cfg.Floor = concurrencyFloor
	}
	if cfg.Floor > cfg.EffectiveCeiling {
		cfg.Floor = cfg.EffectiveCeiling
	}
	if cfg.AdaptiveEnabled {
		if cfg.Initial < 1 {
			cfg.Initial = minInt(concurrencyDefaultInitial, cfg.EffectiveCeiling)
		}
		if cfg.Initial < cfg.Floor {
			cfg.Initial = cfg.Floor
		}
		if cfg.Initial > cfg.EffectiveCeiling {
			cfg.Initial = cfg.EffectiveCeiling
		}
	} else {
		cfg.Initial = cfg.EffectiveCeiling
	}
	// A positive effective budget activates the admission ledger, so the
	// allocator cap must be positive, recorded, and never above the budget —
	// otherwise a partially specified public config could buffer more bytes
	// than the ledger admits. Genuinely unbudgeted configs (zero budget) keep
	// a zero cap and fall back to the transfer default at the copy sites.
	if cfg.MemoryBudgetEffectiveBytes > 0 {
		if cfg.RetryBufferCapBytes <= 0 {
			cfg.RetryBufferCapBytes = transfer.DefaultRetryBufferMaxMemoryBytes
		}
		if cfg.RetryBufferCapBytes > cfg.MemoryBudgetEffectiveBytes {
			cfg.RetryBufferCapBytes = cfg.MemoryBudgetEffectiveBytes
		}
	}
	if cfg.CeilingReason == "" {
		cfg.CeilingReason = "requested"
	}
	// Post-conditions: 1 <= Floor <= Initial <= EffectiveCeiling <= RequestedCeiling.
	return cfg
}

// NewConcurrencyLimiter returns an AIMD limiter initialized from cfg. The
// constructor enforces the same invariant as the runner's config
// normalization, so a directly-constructed limiter can never observe (or
// recover to, via throttle) a concurrency above the effective ceiling its
// snapshots report.
func NewConcurrencyLimiter(cfg ConcurrencyConfig) *ConcurrencyLimiter {
	cfg = clampConcurrencyInvariants(cfg)
	now := time.Now()
	limiter := &ConcurrencyLimiter{
		cfg:            cfg,
		current:        cfg.Initial,
		clock:          time.Now,
		startedAt:      now,
		lastTransition: now,
	}
	if cfg.MemoryBudgetEffectiveBytes > 0 {
		limiter.ledger = newMemoryLedger(cfg.MemoryBudgetEffectiveBytes)
	}
	return limiter
}

// ReserveCopyMemory admits a copy's bounded buffer bytes under the effective
// memory budget before any limiter token or provider action, using the
// allocator-identical arithmetic (min of known size and the resolved retry
// cap; unknown sizes reserve the cap). The returned release is exactly-once
// safe and must run on every terminal path. Configs without a resolved
// budget return a no-op release.
func (l *ConcurrencyLimiter) ReserveCopyMemory(ctx context.Context, sourceSize int64) (func(), error) {
	if l.ledger == nil {
		return func() {}, nil
	}
	return l.ledger.Reserve(ctx, copyReservationBytes(sourceSize, l.RetryBufferCap()))
}

// accrueActiveLocked folds the elapsed interval at the current active count
// into the occupancy integral. Callers must hold l.mu.
func (l *ConcurrencyLimiter) accrueActiveLocked(now time.Time) {
	if elapsed := now.Sub(l.lastTransition); elapsed > 0 {
		l.activeIntegral += float64(l.active) * elapsed.Seconds()
	}
	l.lastTransition = now
}

// Acquire waits until the current adaptive concurrency permits another active
// copy and returns a release function for the acquired token.
func (l *ConcurrencyLimiter) Acquire(ctx context.Context) (func(), error) {
	for {
		l.mu.Lock()
		if l.active < l.current {
			l.accrueActiveLocked(l.clock())
			l.active++
			if l.active > l.maxActive {
				l.maxActive = l.active
			}
			l.mu.Unlock()
			return func() {
				l.mu.Lock()
				l.accrueActiveLocked(l.clock())
				l.active--
				l.mu.Unlock()
			}, nil
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// ObserveSuccess records a successful provider operation and may additively
// increase current concurrency after a clean streak.
func (l *ConcurrencyLimiter) ObserveSuccess() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cfg.AdaptiveEnabled || l.current >= l.cfg.EffectiveCeiling {
		return
	}
	if l.cooldown > 0 {
		l.cooldown--
		l.clean = 0
		return
	}
	l.clean++
	if l.clean < concurrencyCleanIncreaseEvery {
		return
	}
	l.current++
	l.additiveIncreases++
	l.clean = 0
}

// ObserveThrottle multiplicatively decreases current concurrency.
func (l *ConcurrencyLimiter) ObserveThrottle() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cfg.AdaptiveEnabled {
		return
	}
	l.current = maxInt(l.cfg.Floor, l.current/2)
	l.clean = 0
	l.cooldown = concurrencyThrottleCooldown
	l.throttleBackoffs++
}

// ObserveConnectionError freezes additive increase without decreasing current
// concurrency.
func (l *ConcurrencyLimiter) ObserveConnectionError() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cfg.AdaptiveEnabled {
		return
	}
	l.clean = 0
	l.connectionFreezes++
}

// ObserveProviderResult maps provider outcomes onto the GON-048 signal model.
func (l *ConcurrencyLimiter) ObserveProviderResult(err error) {
	switch {
	case err == nil:
		l.ObserveSuccess()
	case provider.IsThrottled(err):
		l.ObserveThrottle()
	case ConcurrencyConnectionError(err):
		l.ObserveConnectionError()
	}
}

// ResetOccupancyWindow restarts the time-averaged occupancy interval. Both
// execution paths call this immediately before emitting the run record, so
// the occupancy denominator is the common execution window — setup, preflight,
// and provider construction are excluded identically on both paths. The run
// record's startup sample therefore reports zero; the summary value is the
// completed-run diagnostic.
func (l *ConcurrencyLimiter) ResetOccupancyWindow() {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock()
	l.startedAt = now
	l.lastTransition = now
	l.activeIntegral = 0
}

// RetryBufferCap returns the resolved per-copy in-memory retry-buffer bound
// the copy paths must allocate against, falling back to the transfer default
// for configs that were not memory-resolved.
func (l *ConcurrencyLimiter) RetryBufferCap() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cfg.RetryBufferCapBytes > 0 {
		return l.cfg.RetryBufferCapBytes
	}
	return transfer.DefaultRetryBufferMaxMemoryBytes
}

// Snapshot returns a stable stats view for run and summary records.
func (l *ConcurrencyLimiter) Snapshot() ConcurrencyStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.accrueActiveLocked(l.clock())
	timeAvg := 0.0
	if elapsed := l.lastTransition.Sub(l.startedAt).Seconds(); elapsed > 0 {
		timeAvg = math.Round(l.activeIntegral/elapsed*1000) / 1000
	}
	stats := ConcurrencyStats{
		AdaptiveEnabled:                   l.cfg.AdaptiveEnabled,
		ConcurrencyFloor:                  l.cfg.Floor,
		ConcurrencyInitial:                l.cfg.Initial,
		ConcurrencyCeilingRequested:       l.cfg.RequestedCeiling,
		ConcurrencyCeilingEffective:       l.cfg.EffectiveCeiling,
		ConcurrencyCeilingReason:          l.cfg.CeilingReason,
		ConcurrencyFinal:                  l.current,
		ConcurrencyThrottleBackoffs:       l.throttleBackoffs,
		ConcurrencyAdditiveIncreases:      l.additiveIncreases,
		ConcurrencyConnectionErrorFreezes: l.connectionFreezes,
		ConcurrencyMaxActive:              l.maxActive,
		ConcurrencyTimeAvgActive:          timeAvg,
		MemoryLimitBytes:                  l.cfg.MemoryLimitBytes,
		MemoryLimitSource:                 l.cfg.MemoryLimitSource,
		MemoryBudgetRequestedBytes:        l.cfg.MemoryBudgetRequestedBytes,
		MemoryBudgetEffectiveBytes:        l.cfg.MemoryBudgetEffectiveBytes,
		MemoryBudgetSource:                l.cfg.MemoryBudgetSource,
		RetryBufferCapBytes:               l.cfg.RetryBufferCapBytes,
	}
	if l.ledger != nil {
		ledgerStats := l.ledger.Stats()
		stats.MemoryReservedPeakBytes = ledgerStats.PeakReservedBytes
		stats.MemoryReservationWaits = ledgerStats.Waits
		stats.MemoryReservationWaitMS = ledgerStats.WaitTotal.Milliseconds()
	}
	return stats
}

// ConcurrencyConnectionError reports provider/transport failures that freeze
// additive increase without triggering multiplicative decrease.
func ConcurrencyConnectionError(err error) bool {
	return provider.IsProviderUnavailable(err) || transfer.IsTransientNetworkError(err)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
