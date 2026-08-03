package provider

import "fmt"

// ConnectionPoolPolicy is the numbers-only HTTP connection pool policy for a
// single provider construction. It does not create clients, mutate
// http.DefaultTransport or http.DefaultClient, or share state across
// constructions. Callers apply these integers to concrete provider configs
// (for example s3.Config / gcs.Config MaxIdleConnsPerHost and MaxConnsPerHost).
//
// API stability: Stable (additive). Breaking changes follow docs/api-stability.md.
type ConnectionPoolPolicy struct {
	// MaxIdleConnsPerHost is idle retention capacity per host. Zero leaves
	// SDK / transport defaults.
	MaxIdleConnsPerHost int
	// MaxConnsPerHost is the hard cap across active, dialing, and idle
	// connections per host. Zero leaves SDK / transport defaults.
	MaxConnsPerHost int
}

// ResolveConnectionPool returns pool settings for a construction that will run
// up to admittedN concurrent provider operations that each need at most one
// simultaneous HTTP connection on this client.
//
// Semantics (locked GON-065 S0):
//   - admittedN < 0 → error
//   - admittedN 0 or 1 → zero policy (SDK / transport defaults)
//   - admittedN >= 2 → both fields equal admittedN (pass-through, including large N)
//
// Callers must resolve engine defaults and clamps before calling this function.
// Never pass a raw "unset" flag value of 0 when the run will default concurrency
// to a higher value. Composite admitted-N formulas (transfer source C+L, etc.)
// must use checked arithmetic outside this resolver and refuse construction on
// overflow; this function does not apply a second soft cap.
func ResolveConnectionPool(admittedN int) (ConnectionPoolPolicy, error) {
	if admittedN < 0 {
		return ConnectionPoolPolicy{}, fmt.Errorf("admitted connection pool N must be >= 0, got %d", admittedN)
	}
	if admittedN < 2 {
		return ConnectionPoolPolicy{}, nil
	}
	return ConnectionPoolPolicy{
		MaxIdleConnsPerHost: admittedN,
		MaxConnsPerHost:     admittedN,
	}, nil
}
