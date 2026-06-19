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
	concurrencyCleanIncreaseEvery = 8
	concurrencyThrottleCooldown   = 4
	resourceDefaultMemoryLimit    = int64(1 << 30)
	resourceMemoryFraction        = 0.25
	resourceFDHeadroom            = 128
	resourceFDsPerCopy            = 1
	resourceMinCap                = 1
	resourceMaxCap                = int(^uint(0) >> 1)
)

// ResourceProbe supplies host resource limits used to clamp requested transfer
// concurrency before any worker pools, queues, or retry buffers are created.
type ResourceProbe struct {
	MemoryLimitBytes func() (int64, string, error)
	FDSoftLimit      func() (int64, error)
}

// ConcurrencyConfig is the resolved GON-048 concurrency contract. RequestedCeiling
// is the user/operator ceiling; EffectiveCeiling is the resource-safe hard cap.
type ConcurrencyConfig struct {
	RequestedCeiling int
	EffectiveCeiling int
	CeilingReason    string
	AdaptiveEnabled  bool
	Floor            int
	Initial          int
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
}

// DefaultResourceProbe returns the platform probes used by transfer reflow.
func DefaultResourceProbe() ResourceProbe {
	return ResourceProbe{
		MemoryLimitBytes: defaultMemoryLimitBytes,
		FDSoftLimit:      defaultFDSoftLimit,
	}
}

func defaultMemoryLimitBytes() (int64, string, error) {
	if limit, source, err := defaultPlatformMemoryLimitBytes(); err == nil && limit > 0 {
		return limit, source, nil
	}
	limit := debug.SetMemoryLimit(-1)
	if limit > 0 && limit < math.MaxInt64 {
		return limit, "runtime", nil
	}
	return resourceDefaultMemoryLimit, "conservative_default", nil
}

// ResolveConcurrency clamps the requested ceiling by memory and FD limits before
// the caller allocates concurrency-sized resources.
func ResolveConcurrency(requested int, adaptiveEnabled bool, probe ResourceProbe) ConcurrencyConfig {
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
		memorySource = "conservative_default"
	}
	memoryBudget := int64(float64(memoryLimit) * resourceMemoryFraction)
	if memoryBudget < transfer.DefaultRetryBufferMaxMemoryBytes {
		memoryBudget = transfer.DefaultRetryBufferMaxMemoryBytes
	}
	memoryCap := int(memoryBudget / transfer.DefaultRetryBufferMaxMemoryBytes)
	if memoryCap < resourceMinCap {
		memoryCap = resourceMinCap
	}

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
		reasons = append(reasons, "memory:"+memorySource)
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
		RequestedCeiling: requested,
		EffectiveCeiling: effective,
		CeilingReason:    reason,
		AdaptiveEnabled:  adaptiveEnabled,
		Floor:            concurrencyFloor,
		Initial:          initial,
	}
}

// NewConcurrencyLimiter returns an AIMD limiter initialized from cfg.
func NewConcurrencyLimiter(cfg ConcurrencyConfig) *ConcurrencyLimiter {
	if cfg.EffectiveCeiling < 1 {
		cfg.EffectiveCeiling = 1
	}
	if cfg.Floor < 1 {
		cfg.Floor = 1
	}
	if cfg.Initial < cfg.Floor {
		cfg.Initial = cfg.Floor
	}
	if cfg.Initial > cfg.EffectiveCeiling {
		cfg.Initial = cfg.EffectiveCeiling
	}
	return &ConcurrencyLimiter{cfg: cfg, current: cfg.Initial}
}

// Acquire waits until the current adaptive concurrency permits another active
// copy and returns a release function for the acquired token.
func (l *ConcurrencyLimiter) Acquire(ctx context.Context) (func(), error) {
	for {
		l.mu.Lock()
		if l.active < l.current {
			l.active++
			if l.active > l.maxActive {
				l.maxActive = l.active
			}
			l.mu.Unlock()
			return func() {
				l.mu.Lock()
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

// Snapshot returns a stable stats view for run and summary records.
func (l *ConcurrencyLimiter) Snapshot() ConcurrencyStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return ConcurrencyStats{
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
	}
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
