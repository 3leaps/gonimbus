package cmd

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
	reflowConcurrencyFloor              = 1
	reflowConcurrencyDefaultInitial     = 16
	reflowConcurrencyCleanIncreaseEvery = 8
	reflowConcurrencyThrottleCooldown   = 4
	reflowResourceDefaultMemoryLimit    = int64(1 << 30)
	reflowResourceMemoryFraction        = 0.25
	reflowResourceFDHeadroom            = 128
	reflowResourceFDsPerCopy            = 1
	reflowResourceMinCap                = 1
	reflowResourceMaxCap                = int(^uint(0) >> 1)
)

var reflowResourceProbeForRun = defaultReflowResourceProbe()

type reflowResourceProbe struct {
	MemoryLimitBytes func() (int64, string, error)
	FDSoftLimit      func() (int64, error)
}

type reflowConcurrencyConfig struct {
	RequestedCeiling int
	EffectiveCeiling int
	CeilingReason    string
	AdaptiveEnabled  bool
	Floor            int
	Initial          int
}

type reflowConcurrencyStats struct {
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

type reflowConcurrencyLimiter struct {
	mu sync.Mutex

	cfg       reflowConcurrencyConfig
	current   int
	active    int
	maxActive int
	clean     int
	cooldown  int

	throttleBackoffs  int64
	additiveIncreases int64
	connectionFreezes int64
}

func defaultReflowResourceProbe() reflowResourceProbe {
	return reflowResourceProbe{
		MemoryLimitBytes: defaultReflowMemoryLimitBytes,
		FDSoftLimit:      defaultReflowFDSoftLimit,
	}
}

func defaultReflowMemoryLimitBytes() (int64, string, error) {
	if limit, source, err := defaultReflowPlatformMemoryLimitBytes(); err == nil && limit > 0 {
		return limit, source, nil
	}
	limit := debug.SetMemoryLimit(-1)
	if limit > 0 && limit < math.MaxInt64 {
		return limit, "runtime", nil
	}
	return reflowResourceDefaultMemoryLimit, "conservative_default", nil
}

func resolveReflowConcurrency(requested int, adaptiveEnabled bool, probe reflowResourceProbe) reflowConcurrencyConfig {
	if requested < 1 {
		requested = 1
	}
	if probe.MemoryLimitBytes == nil {
		probe.MemoryLimitBytes = defaultReflowMemoryLimitBytes
	}
	if probe.FDSoftLimit == nil {
		probe.FDSoftLimit = defaultReflowFDSoftLimit
	}

	memoryLimit, memorySource, memoryErr := probe.MemoryLimitBytes()
	if memoryLimit <= 0 || memoryErr != nil {
		memoryLimit = reflowResourceDefaultMemoryLimit
		memorySource = "conservative_default"
	}
	memoryBudget := int64(float64(memoryLimit) * reflowResourceMemoryFraction)
	if memoryBudget < transfer.DefaultRetryBufferMaxMemoryBytes {
		memoryBudget = transfer.DefaultRetryBufferMaxMemoryBytes
	}
	memoryCap := int(memoryBudget / transfer.DefaultRetryBufferMaxMemoryBytes)
	if memoryCap < reflowResourceMinCap {
		memoryCap = reflowResourceMinCap
	}

	fdLimit, fdErr := probe.FDSoftLimit()
	if fdLimit <= 0 || fdErr != nil {
		fdLimit = int64(reflowResourceFDHeadroom + reflowResourceMinCap)
	}
	fdCap := int((fdLimit - reflowResourceFDHeadroom) / reflowResourceFDsPerCopy)
	if fdCap < reflowResourceMinCap {
		fdCap = reflowResourceMinCap
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
	if effective < reflowResourceMinCap {
		effective = reflowResourceMinCap
	}
	reason := "requested"
	if len(reasons) > 0 {
		reason = "resource_capped:" + strings.Join(reasons, ",")
	}

	initial := effective
	if adaptiveEnabled {
		initial = minInt(reflowConcurrencyDefaultInitial, effective)
	}

	return reflowConcurrencyConfig{
		RequestedCeiling: requested,
		EffectiveCeiling: effective,
		CeilingReason:    reason,
		AdaptiveEnabled:  adaptiveEnabled,
		Floor:            reflowConcurrencyFloor,
		Initial:          initial,
	}
}

func newReflowConcurrencyLimiter(cfg reflowConcurrencyConfig) *reflowConcurrencyLimiter {
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
	return &reflowConcurrencyLimiter{cfg: cfg, current: cfg.Initial}
}

func (l *reflowConcurrencyLimiter) acquire(ctx context.Context) (func(), error) {
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

func (l *reflowConcurrencyLimiter) observeSuccess() {
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
	if l.clean < reflowConcurrencyCleanIncreaseEvery {
		return
	}
	l.current++
	l.additiveIncreases++
	l.clean = 0
}

func (l *reflowConcurrencyLimiter) observeThrottle() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cfg.AdaptiveEnabled {
		return
	}
	l.current = maxInt(l.cfg.Floor, l.current/2)
	l.clean = 0
	l.cooldown = reflowConcurrencyThrottleCooldown
	l.throttleBackoffs++
}

func (l *reflowConcurrencyLimiter) observeConnectionError() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cfg.AdaptiveEnabled {
		return
	}
	l.clean = 0
	l.connectionFreezes++
}

func (l *reflowConcurrencyLimiter) snapshot() reflowConcurrencyStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return reflowConcurrencyStats{
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

func reflowConcurrencyConnectionError(err error) bool {
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
