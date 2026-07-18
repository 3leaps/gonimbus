package reflow

import "sync"

// destKeyArbiter provides per-destination-key mutual exclusion for the engine
// worker pool. Concurrent workers targeting the same destination key serialize
// through a per-key gate around the collision-check/copy critical section, so a
// conditional PUT can never race another in-process worker that is about to
// create (or has just created) the same key. Durable cross-run observations
// remain in the checkpoint store; the gate's observed memo only covers the
// in-process window between a successful copy and its durable mark.
type destKeyArbiter struct {
	mu    sync.Mutex
	gates map[string]*destKeyGate
}

type destKeyGate struct {
	mu sync.Mutex
	// observed records that this process has already established the
	// destination key exists (copied it, or confirmed it via checkpoint or
	// conditional-PUT refusal). Guarded by the gate mutex.
	observed bool
	refs     int
}

func newDestKeyArbiter() *destKeyArbiter {
	return &destKeyArbiter{gates: map[string]*destKeyGate{}}
}

// acquire locks the gate for key and returns it with a release func. Active
// gates are bounded to in-flight keys: the last releaser removes the entry.
func (a *destKeyArbiter) acquire(key string) (*destKeyGate, func()) {
	a.mu.Lock()
	g, ok := a.gates[key]
	if !ok {
		g = &destKeyGate{}
		a.gates[key] = g
	}
	g.refs++
	a.mu.Unlock()

	g.mu.Lock()
	return g, func() {
		g.mu.Unlock()
		a.mu.Lock()
		defer a.mu.Unlock()
		g.refs--
		if g.refs == 0 && a.gates[key] == g {
			delete(a.gates, key)
		}
	}
}
