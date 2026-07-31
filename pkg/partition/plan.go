// Package partition compiles canonical, hashed, lane-assigned enumeration plans
// and defines the lane identity that names one unit of that plan.
//
// Experimental: the plan encoding and the lane-identity contract will churn
// until a consumer persists them across runs.
//
// The package is deliberately neutral. One plan governs enumeration, the
// identity carried alongside the records enumeration emits, resume, and the
// verification a mutating consumer performs before it writes. Owning the plan
// inside any one of those consumers would make a downstream stage the authority
// over an upstream one and force an import in the wrong direction; a shared
// contract package lets producers and consumers depend on the plan without
// depending on each other.
package partition

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// PlanVersion is the schema version of the canonical plan encoding that
// PlanHash is taken over. Any change to the hashed payload shape must bump
// this, because a resume comparing plan digests across versions would otherwise
// read an encoding change as a plan change.
const PlanVersion = 1

// LaneCeiling is the highest lane count a plan may resolve to. It is a refusal,
// not a clamp: an operator asking for more lanes than the engine will run
// should be told so rather than silently getting fewer, because the lane count
// they believe is running is what they will reason about when reading
// throughput.
const LaneCeiling = 64

// Coverage is the caller's declaration of what the plan claims about the scope
// it was compiled from. The compiler cannot derive this: it receives prefixes,
// not the scope configuration they came from (see the note on CompilePlan), so
// completeness is an assertion the caller makes and the plan records.
type Coverage string

const (
	// CoverageComplete asserts the prefixes union to the entire intended scope.
	CoverageComplete Coverage = "complete"
	// CoveragePartial asserts the plan deliberately covers only part of the
	// scope. Recorded so a downstream consumer never mistakes a partial sweep
	// for an authoritative one.
	CoveragePartial Coverage = "intentionally_partial"
)

// Lane is one enumeration unit: an ordinal plus the prefixes it lists. Ordinal
// is derived from assignment order rather than completion order, so lane
// identity does not vary with scheduling.
//
// An ordinal alone is not an identity — see LaneRef.
type Lane struct {
	Ordinal  int      `json:"ordinal"`
	Prefixes []string `json:"prefixes"`
}

// Plan is a canonical, hashed, lane-assigned enumeration plan. It is compiled
// and hashed before enumeration begins so the digest describes the work that
// was planned, not the work that happened to complete.
type Plan struct {
	Version           int      `json:"version"`
	Coverage          Coverage `json:"coverage"`
	BaseIdentity      string   `json:"base_identity"`
	ConfigFingerprint string   `json:"config_fingerprint"`
	Lanes             []Lane   `json:"lanes"`

	// PlanHash is the digest of the canonical payload. It is an integrity
	// fingerprint, not an authentication tag: anyone who can construct the same
	// inputs can construct the same digest. A consumer verifies a claimed lane
	// against a plan it was handed by the run that owns it — never against a
	// plan reconstructed from the claim itself. See AuthorizeLane.
	PlanHash string `json:"plan_hash"`

	// Subsumed records prefixes removed because another retained prefix already
	// covers them. Normalization here is union-preserving — dropping a/b/ when
	// a/ is present enumerates exactly the same objects — but it does reduce the
	// number of partitionable units, so an operator who wrote three prefixes and
	// got one lane needs to be able to see why. Reported rather than refused
	// precisely because coverage is unchanged.
	Subsumed []SubsumedPrefix `json:"subsumed,omitempty"`
}

// SubsumedPrefix is one dropped prefix and the retained prefix that covers it.
type SubsumedPrefix struct {
	Prefix    string `json:"prefix"`
	CoveredBy string `json:"covered_by"`
}

// LaneRef is the identity of one lane: the plan encoding version, the plan
// digest, and the lane ordinal within that plan.
//
// All three are required. An ordinal alone is not stable identity — lane 3
// names entirely different work under a different plan — so a claim carrying
// only an ordinal cannot be checked against anything. The version is carried
// because two encodings of the same planned work produce different digests, and
// a consumer needs to say which mismatch it is looking at.
//
// A LaneRef is issued and checked by an Authority, never by a Plan. A Plan is
// data: its digest field is whatever the last writer put there, so comparing a
// claim against it proves only that the claim agrees with an unverified value.
type LaneRef struct {
	PlanVersion int    `json:"plan_version"`
	PlanDigest  string `json:"plan_digest"`
	Ordinal     int    `json:"ordinal"`
}

// Equal reports whether two lane references name the same lane of the same plan.
func (r LaneRef) Equal(other LaneRef) bool {
	return r.PlanVersion == other.PlanVersion &&
		r.PlanDigest == other.PlanDigest &&
		r.Ordinal == other.Ordinal
}

// String renders a lane reference for diagnostics. The digest is abbreviated
// because a message naming the lane is read far more often than one needing the
// full digest, and the full value is available on the struct.
func (r LaneRef) String() string {
	digest := r.PlanDigest
	if len(digest) > 12 {
		digest = digest[:12]
	}
	return fmt.Sprintf("v%d/%s/lane-%d", r.PlanVersion, digest, r.Ordinal)
}

func abbreviate(digest string) string {
	if digest == "" {
		return "(empty)"
	}
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}

// PlanRequest is the compiler's input.
//
// Prefixes are explicit. The compiler deliberately does not accept a scope
// configuration: scope compilation reaches a schema dependency that the
// embeddable engine's dependency boundary denies, so it belongs to the calling
// adapter. The adapter compiles configuration into prefixes; this package
// receives facts.
type PlanRequest struct {
	Prefixes []string
	Coverage Coverage
	// BaseIdentity names the source provider and base URI this plan enumerates.
	// It is part of the digest because a plan is only the same plan when it runs
	// against the same source: identical prefixes under a different provider or
	// bucket describe entirely different work, and a resume must not match across
	// that boundary.
	//
	// This is resume identity. It is deliberately not a rate-limit authority,
	// which may be broader than one bucket, and it must never carry credentials.
	BaseIdentity string
	// ConfigFingerprint is the caller digest of behavior-affecting configuration.
	// The compiler cannot compute it — the adapter owns what is
	// behavior-affecting — but the plan must carry it so a resume refuses a run
	// whose semantics changed under it.
	ConfigFingerprint string
	// MaxLanes bounds the lane count. Zero means one lane per retained prefix.
	MaxLanes int
}

// CompilePlan normalizes the requested prefixes, refuses inputs whose partition
// semantics are not decidable, assigns stable lane ordinals, and hashes the
// result.
//
// Prefix containment uses literal byte-prefix semantics, not path semantics,
// because that is what an object-store LIST prefix means: listing "a" returns
// "ab/x" as well as "a/x". Treating containment as path-aware would leave a
// genuinely redundant prefix in the plan and enumerate those objects twice.
func CompilePlan(req PlanRequest) (*Plan, error) {
	switch req.Coverage {
	case CoverageComplete, CoveragePartial:
	case "":
		return nil, fmt.Errorf("partition: plan coverage must be declared (%q or %q)", CoverageComplete, CoveragePartial)
	default:
		return nil, fmt.Errorf("partition: unknown plan coverage %q", req.Coverage)
	}

	// Both identity inputs are required rather than optional-with-empty-default:
	// an empty value would produce a digest that cannot distinguish two runs that
	// must be distinguishable, and it would do so silently.
	if strings.TrimSpace(req.BaseIdentity) == "" {
		return nil, errors.New("partition: plan requires a source base identity")
	}
	if strings.TrimSpace(req.ConfigFingerprint) == "" {
		return nil, errors.New("partition: plan requires a configuration fingerprint")
	}

	if req.MaxLanes < 0 {
		return nil, fmt.Errorf("partition: plan max lanes must not be negative, got %d", req.MaxLanes)
	}
	if req.MaxLanes > LaneCeiling {
		return nil, fmt.Errorf("partition: requested %d lanes, which exceeds the ceiling of %d", req.MaxLanes, LaneCeiling)
	}

	retained, subsumed, err := normalizePrefixes(req.Prefixes)
	if err != nil {
		return nil, err
	}
	if len(retained) == 0 {
		return nil, errors.New("partition: plan requires at least one prefix")
	}

	// The ceiling is checked against the count the plan will actually resolve
	// to, not only against the requested bound. MaxLanes is a bound, and its
	// zero value means "one lane per retained prefix" — so checking the request
	// alone leaves the default path unbounded, and a prefix set larger than the
	// ceiling would run more lanes than the ceiling admits without ever naming a
	// lane count that was refused.
	lanes := resolveLaneCount(len(retained), req.MaxLanes)
	if lanes > LaneCeiling {
		return nil, fmt.Errorf("partition: %d prefixes resolve to %d lanes, which exceeds the ceiling of %d; "+
			"bound the lane count at or below the ceiling, or reduce the prefix set",
			len(retained), lanes, LaneCeiling)
	}

	plan := &Plan{
		Version:           PlanVersion,
		Coverage:          req.Coverage,
		BaseIdentity:      strings.TrimSpace(req.BaseIdentity),
		ConfigFingerprint: strings.TrimSpace(req.ConfigFingerprint),
		Lanes:             assignLanes(retained, lanes),
		Subsumed:          subsumed,
	}
	hash, err := hashPlan(plan)
	if err != nil {
		return nil, err
	}
	plan.PlanHash = hash
	return plan, nil
}

// normalizePrefixes rejects undecidable entries, deduplicates, and removes
// prefixes already covered by a retained one.
//
// Prefix bytes are preserved exactly. A leading or trailing space is a valid
// object-key byte, so " a/" and "a/" name different sets of objects and "a/ "
// is a narrower prefix than "a/"; trimming any of them would enumerate a
// different set than the operator asked for while still reporting the plan as
// union-preserving.
func normalizePrefixes(prefixes []string) ([]string, []SubsumedPrefix, error) {
	seen := make(map[string]struct{}, len(prefixes))
	cleaned := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		if prefix == "" {
			// A whole-scope entry alongside named partitions makes the operator's
			// partitioning intent undecidable: it subsumes every sibling, so the
			// plan they wrote and the plan that would run share no structure.
			// Refuse rather than silently collapse to a single full sweep.
			return nil, nil, errors.New("partition: plan contains an empty prefix, which covers the whole scope; " +
				"supply explicit prefixes or run unpartitioned")
		}
		if !utf8.ValidString(prefix) {
			return nil, nil, errors.New("partition: prefix is not valid UTF-8")
		}
		if strings.ContainsRune(prefix, 0) {
			return nil, nil, errors.New("partition: prefix contains a NUL byte")
		}
		if _, dup := seen[prefix]; dup {
			continue
		}
		seen[prefix] = struct{}{}
		cleaned = append(cleaned, prefix)
	}
	if len(cleaned) == 0 {
		return nil, nil, nil
	}

	// Sorting puts any covering prefix ahead of what it covers: a byte-prefix of
	// s sorts before s. So a single forward pass holding the last retained
	// prefix is sufficient — no O(n^2) comparison needed.
	sort.Strings(cleaned)

	retained := make([]string, 0, len(cleaned))
	var subsumed []SubsumedPrefix
	for _, prefix := range cleaned {
		if n := len(retained); n > 0 {
			if cover := retained[n-1]; strings.HasPrefix(prefix, cover) {
				subsumed = append(subsumed, SubsumedPrefix{Prefix: prefix, CoveredBy: cover})
				continue
			}
		}
		retained = append(retained, prefix)
	}
	return retained, subsumed, nil
}

// resolveLaneCount reports how many lanes a plan will actually run: one per
// retained prefix, bounded by an explicit request. It never returns more lanes
// than there are prefixes, because an empty lane is not work.
func resolveLaneCount(retained, maxLanes int) int {
	if maxLanes > 0 && maxLanes < retained {
		return maxLanes
	}
	return retained
}

// assignLanes distributes retained prefixes over the resolved lane count
// round-robin.
//
// Round-robin over the sorted set spreads lexicographically adjacent prefixes —
// which tend to correlate in population, since they usually share a parent —
// across lanes rather than concentrating that skew in one lane.
func assignLanes(retained []string, count int) []Lane {
	lanes := make([]Lane, count)
	for i := range lanes {
		lanes[i].Ordinal = i + 1
	}
	for i, prefix := range retained {
		lanes[i%count].Prefixes = append(lanes[i%count].Prefixes, prefix)
	}
	return lanes
}

// planPayload is the canonical hashed encoding. It is a distinct type from Plan
// so that adding an observational field to the plan — which Subsumed is —
// cannot silently change the digest and invalidate every resume.
type planPayload struct {
	Version           int           `json:"version"`
	Coverage          string        `json:"coverage"`
	BaseIdentity      string        `json:"base_identity"`
	ConfigFingerprint string        `json:"config_fingerprint"`
	Lanes             []lanePayload `json:"lanes"`
}

type lanePayload struct {
	Ordinal  int      `json:"ordinal"`
	Prefixes []string `json:"prefixes"`
}

func hashPlan(plan *Plan) (string, error) {
	payload := planPayload{
		Version:           plan.Version,
		Coverage:          string(plan.Coverage),
		BaseIdentity:      plan.BaseIdentity,
		ConfigFingerprint: plan.ConfigFingerprint,
		Lanes:             make([]lanePayload, 0, len(plan.Lanes)),
	}
	for _, lane := range plan.Lanes {
		// Fields are assigned explicitly rather than by type conversion, which
		// the two currently-identical shapes would permit. A conversion would
		// weld the hashed payload to the public type: a field later added to
		// Lane would enter the digest by default and silently invalidate every
		// stored plan hash. Explicit assignment makes not-hashed the default,
		// and hashing a deliberate edit here.
		//nolint:staticcheck // S1016: intentional decoupling, do not convert.
		payload.Lanes = append(payload.Lanes, lanePayload{
			Ordinal:  lane.Ordinal,
			Prefixes: lane.Prefixes,
		})
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal partition plan payload: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
