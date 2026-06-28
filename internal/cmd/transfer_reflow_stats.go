package cmd

import (
	"sync"

	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

type reflowRunStats struct {
	mu              sync.Mutex
	statuses        map[string]int64
	collisions      map[string]int64
	fallbackObjects int64
}

func newReflowRunStats() *reflowRunStats {
	return &reflowRunStats{
		statuses:   map[string]int64{},
		collisions: map[string]int64{},
	}
}

func (s *reflowRunStats) record(rec reflowpkg.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec.Status != "" {
		s.statuses[rec.Status]++
	}
	if rec.Collision != nil && rec.Collision.Kind != "" {
		s.collisions[rec.Collision.Kind]++
	}
}

func (s *reflowRunStats) recordFallbackObject() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fallbackObjects++
}

func (s *reflowRunStats) summary(destURI string, dryRun bool, collCfg collisionConfig, capability reflowIfAbsentCapability, concurrency reflowpkg.ConcurrencyStats, invalidCount, errorCount int64) reflowpkg.SummaryRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return reflowpkg.SummaryRecord{
		DestURI:                 destURI,
		DryRun:                  dryRun,
		OnCollision:             collCfg.Mode,
		ConcurrencyStats:        concurrency,
		DestIfAbsentHonored:     capability.Honored,
		DestIfAbsentProbeStatus: string(capability.ProbeStatus),
		FallbackActive:          capability.FallbackActive,
		IfAbsentFallbackObjects: s.fallbackObjects,
		Statuses:                cloneInt64Map(s.statuses),
		Collisions:              cloneInt64Map(s.collisions),
		InvalidInputs:           invalidCount,
		Errors:                  errorCount,
	}
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
