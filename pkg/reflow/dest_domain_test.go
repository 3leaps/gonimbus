package reflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDestDomainCapIsTwiceSourceWhenMultiplierTwo(t *testing.T) {
	cfg := clampConcurrencyInvariants(ConcurrencyConfig{
		RequestedCeiling: 4,
		EffectiveCeiling: 4,
		Initial:          4,
		Floor:            1,
		AdaptiveEnabled:  false,
		// DestDomainMultiplier 0 → clamp defaults to 2
	})
	if cfg.DestDomainMultiplier != 2 {
		t.Fatalf("multiplier=%d want 2", cfg.DestDomainMultiplier)
	}
	if cfg.DestDomainCeilingEffective != 8 {
		t.Fatalf("dest ceiling=%d want 8", cfg.DestDomainCeilingEffective)
	}
	if DestAdmittedN(cfg) != 8 {
		t.Fatalf("DestAdmittedN=%d want 8", DestAdmittedN(cfg))
	}

	lim := NewConcurrencyLimiter(cfg)
	// Fill source to current (4).
	var releases []func()
	for i := 0; i < 4; i++ {
		rel, err := lim.AcquireSource(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, rel)
	}
	// Fifth source must block briefly then cancel.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := lim.AcquireSource(ctx); err == nil {
		t.Fatal("expected source acquire to fail when full")
	}
	// Dest admits 8 under multiplier 2 even while source is full.
	for i := 0; i < 8; i++ {
		rel, err := lim.AcquireDest(context.Background())
		if err != nil {
			t.Fatalf("dest acquire %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if _, err := lim.AcquireDest(ctx2); err == nil {
		t.Fatal("expected dest acquire to fail when dest cap full")
	}
	snap := lim.Snapshot()
	if snap.ConcurrencyMaxActiveSource != 4 {
		t.Fatalf("max source=%d want 4", snap.ConcurrencyMaxActiveSource)
	}
	if snap.ConcurrencyMaxActiveDest != 8 {
		t.Fatalf("max dest=%d want 8", snap.ConcurrencyMaxActiveDest)
	}
	if snap.DestDomainCeilingEffective != 8 || snap.DestDomainMultiplier != 2 {
		t.Fatalf("dest policy mult=%d ceil=%d", snap.DestDomainMultiplier, snap.DestDomainCeilingEffective)
	}
	for _, r := range releases {
		r()
	}
}

func TestDestDomainTracksThrottleDown(t *testing.T) {
	cfg := clampConcurrencyInvariants(ConcurrencyConfig{
		RequestedCeiling:     8,
		EffectiveCeiling:     8,
		Initial:              8,
		Floor:                1,
		AdaptiveEnabled:      true,
		DestDomainMultiplier: 2,
	})
	lim := NewConcurrencyLimiter(cfg)
	// Force throttle: current becomes 4, dest cap becomes 8.
	lim.ObserveThrottle()
	// Fill dest to 8 (2× current after throttle: current=4 → dest cap 8).
	var releases []func()
	for i := 0; i < 8; i++ {
		rel, err := lim.AcquireDest(context.Background())
		if err != nil {
			t.Fatalf("dest %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	// Source only admits 4 after throttle.
	for i := 0; i < 4; i++ {
		rel, err := lim.AcquireSource(context.Background())
		if err != nil {
			t.Fatalf("source %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := lim.AcquireSource(ctx); err == nil {
		t.Fatal("source should be full at current=4")
	}
	for _, r := range releases {
		r()
	}
}

func TestDestAcquireCancellation(t *testing.T) {
	cfg := clampConcurrencyInvariants(ConcurrencyConfig{
		RequestedCeiling: 1,
		EffectiveCeiling: 1,
		Initial:          1,
		Floor:            1,
		AdaptiveEnabled:  false,
	})
	lim := NewConcurrencyLimiter(cfg)
	hold, err := lim.AcquireDest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Second dest (cap=2 with mult 2) still available... fill both.
	hold2, err := lim.AcquireDest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	var got error
	go func() {
		defer wg.Done()
		_, got = lim.AcquireDest(ctx)
	}()
	wg.Wait()
	if got == nil {
		t.Fatal("expected canceled dest acquire")
	}
	hold()
	hold2()
}
