package indexstore

import (
	"sync"
	"testing"
	"time"
)

// The identifiers here are PRIMARY KEY columns, and their uniqueness used to
// rest entirely on the wall clock advancing between two readings. That is a
// platform property rather than a guarantee: Windows resolves time coarsely
// enough that two identifiers minted in the same tick read the same nanosecond,
// which surfaced as a failed insert on that platform and never on Linux.
//
// A test that merely mints identifiers quickly would reproduce nothing on a
// host with a fine-grained clock — it would pass for the same reason the bug
// hid. These tests pin the clock instead, so the coarse-clock case is exercised
// wherever they run.

// pinClock stops the id clock at a fixed instant for the duration of a test,
// which is the strongest form of the condition: a clock that never advances.
func pinClock(t *testing.T) {
	t.Helper()
	fixed := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	original := idClock
	idClock = func() time.Time { return fixed }
	t.Cleanup(func() { idClock = original })
}

func TestNewRunIDIsUniqueUnderAStoppedClock(t *testing.T) {
	pinClock(t)

	const count = 10_000
	seen := make(map[string]struct{}, count)
	for i := 0; i < count; i++ {
		id := NewRunID()
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("run id %q was issued twice at call %d; run_id is a PRIMARY KEY", id, i)
		}
		seen[id] = struct{}{}
	}
}

func TestNewEventIDIsUniqueUnderAStoppedClock(t *testing.T) {
	pinClock(t)

	const count = 10_000
	seen := make(map[string]struct{}, count)
	for i := 0; i < count; i++ {
		id := NewEventID()
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("event id %q was issued twice at call %d; event_id is a PRIMARY KEY", id, i)
		}
		seen[id] = struct{}{}
	}
}

// Runs and events are minted from one stamp source, so uniqueness has to hold
// across both rather than only within each.
func TestRunAndEventIDsDoNotShareAStampUnderAStoppedClock(t *testing.T) {
	pinClock(t)

	seen := map[string]struct{}{}
	for i := 0; i < 1_000; i++ {
		for _, id := range []string{NewRunID(), NewEventID()} {
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("identifier %q was issued twice", id)
			}
			seen[id] = struct{}{}
		}
	}
}

// Concurrent minting must not hand the same stamp to two callers. Run with
// -race this also pins that the stamp source is free of data races.
func TestIDsAreUniqueUnderConcurrencyWithAStoppedClock(t *testing.T) {
	pinClock(t)

	const goroutines = 8
	const perGoroutine = 500

	var mu sync.Mutex
	seen := make(map[string]struct{}, goroutines*perGoroutine)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]string, 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				ids = append(ids, NewRunID())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				if _, duplicate := seen[id]; duplicate {
					t.Errorf("run id %q was issued to two callers", id)
				}
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()
}

// The counter must not permanently outrun the clock. A burst inside one tick
// leaves the stamp ahead by the size of the burst, but once the clock passes
// that point the stamp must be the clock reading again — otherwise identifiers
// would drift away from the time they encode, a little further with every burst.
func TestStampReturnsToTheClockOnceItAdvancesPast(t *testing.T) {
	original := idClock
	t.Cleanup(func() { idClock = original })

	// Far enough ahead to exceed any stamp issued earlier in this package: the
	// counter is process-global, so a fixed date could sit behind it.
	base := time.Now().Add(24 * time.Hour)
	current := base
	idClock = func() time.Time { return current }

	if got := nextIDStamp(); got != base.UnixNano() {
		t.Fatalf("stamp = %d, want the clock reading %d", got, base.UnixNano())
	}

	// A burst within one tick pushes the stamp ahead of the clock, by design.
	for i := 0; i < 100; i++ {
		_ = nextIDStamp()
	}

	current = base.Add(time.Hour)
	if got := nextIDStamp(); got != current.UnixNano() {
		t.Fatalf("stamp = %d after the clock advanced past the burst, want the new reading %d; the counter must not outrun the clock permanently",
			got, current.UnixNano())
	}
}

// The run_<digits> form is a contract the hub layout and the schema both rely
// on, so the fix must not have changed the shape of what is issued.
func TestIDsKeepTheirDigitsOnlyForm(t *testing.T) {
	pinClock(t)

	for _, tc := range []struct {
		name   string
		id     string
		prefix string
	}{
		{"run", NewRunID(), "run_"},
		{"event", NewEventID(), "evt_"},
	} {
		if len(tc.id) <= len(tc.prefix) || tc.id[:len(tc.prefix)] != tc.prefix {
			t.Fatalf("%s id %q does not carry the %q prefix", tc.name, tc.id, tc.prefix)
		}
		for _, r := range tc.id[len(tc.prefix):] {
			if r < '0' || r > '9' {
				t.Fatalf("%s id %q must be prefix plus digits only", tc.name, tc.id)
			}
		}
	}
}
