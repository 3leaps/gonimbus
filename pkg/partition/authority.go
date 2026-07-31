package partition

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// Authority is a validated, immutable enumeration plan that a run holds while
// it executes.
//
// Plan and Authority are deliberately different things. Plan is data: it is
// JSON-tagged, exported, and mutable, because it is what gets persisted, read
// back, and handed around. Its digest field is whatever the last writer put
// there. Checking a claim against a Plan therefore proves only that the claim
// agrees with an unverified value — a plan literal carrying a made-up digest, or
// no digest at all, would authorize claims built from itself.
//
// An Authority is the same information after that value has been proven: the
// digest is recomputed from the canonical payload and compared, every compiler
// invariant is rechecked, and the contents are deep-copied into unexported
// fields so that mutating the Plan afterwards cannot change what the run
// enforces. Issuing and checking lane identity is available only here, so there
// is no unvalidated path to either.
type Authority struct {
	version           int
	coverage          Coverage
	baseIdentity      string
	configFingerprint string
	lanes             []Lane
	subsumed          []SubsumedPrefix
	digest            string
}

// NewAuthority validates a plan and returns the run authority for it.
//
// It fails closed: any invariant the compiler guarantees but the supplied plan
// does not satisfy is a refusal, because a plan that reached this point without
// satisfying them was not produced by the compiler, and the run cannot tell
// whether that is corruption, hand-editing, or tampering.
func NewAuthority(plan *Plan) (*Authority, error) {
	if plan == nil {
		return nil, fmt.Errorf("partition: cannot build an authority from a nil plan")
	}
	if plan.Version != PlanVersion {
		return nil, fmt.Errorf("partition: plan encoding version %d is not supported (this build compiles version %d)",
			plan.Version, PlanVersion)
	}
	switch plan.Coverage {
	case CoverageComplete, CoveragePartial:
	default:
		return nil, fmt.Errorf("partition: plan declares unknown coverage %q", plan.Coverage)
	}
	if strings.TrimSpace(plan.BaseIdentity) == "" {
		return nil, fmt.Errorf("partition: plan carries no source base identity")
	}
	if strings.TrimSpace(plan.ConfigFingerprint) == "" {
		return nil, fmt.Errorf("partition: plan carries no configuration fingerprint")
	}
	if len(plan.Lanes) == 0 {
		return nil, fmt.Errorf("partition: plan contains no lanes")
	}
	if len(plan.Lanes) > LaneCeiling {
		return nil, fmt.Errorf("partition: plan contains %d lanes, which exceeds the ceiling of %d",
			len(plan.Lanes), LaneCeiling)
	}

	// The digest is only an integrity fingerprint if it is recomputed. Until
	// this comparison it is an assertion by whoever wrote the plan.
	//
	// It runs before the structural checks below because a plan that fails it was
	// altered after compilation, and that is the useful thing to say: which
	// invariant a corrupted or hand-edited plan happens to break first is a
	// downstream detail. The structural checks are not thereby unreachable — a
	// writer that reseals the plan over what it wrote passes this comparison, and
	// that is the case they exist for.
	recomputed, err := hashPlan(plan)
	if err != nil {
		return nil, err
	}
	if plan.PlanHash == "" {
		return nil, fmt.Errorf("partition: plan carries no digest; expected %s", abbreviate(recomputed))
	}
	if plan.PlanHash != recomputed {
		return nil, fmt.Errorf("partition: plan digest %s does not match its contents (recomputed %s); "+
			"the plan was altered after it was compiled",
			abbreviate(plan.PlanHash), abbreviate(recomputed))
	}

	// Ordinals must be exactly 1..N in slice order. The digest is taken over the
	// lanes in the order they appear, so canonical order and correct ordinals are
	// the same statement; a gap or a reordering means the plan is not the one the
	// compiler produced, whatever its digest says.
	for i, lane := range plan.Lanes {
		if lane.Ordinal != i+1 {
			return nil, fmt.Errorf("partition: lane at position %d carries ordinal %d, want %d; "+
				"lane ordinals must be 1..N in canonical order", i, lane.Ordinal, i+1)
		}
		if len(lane.Prefixes) == 0 {
			return nil, fmt.Errorf("partition: lane %d has no prefixes; an empty lane is not work", lane.Ordinal)
		}
		for _, prefix := range lane.Prefixes {
			if err := validatePrefix(prefix); err != nil {
				return nil, fmt.Errorf("partition: lane %d: %w", lane.Ordinal, err)
			}
		}
	}

	// No retained prefix may cover another, and the lane assignment must be the
	// one the compiler produces for this prefix set and lane count. Both are
	// decided from a single sorted view of the plan's prefixes.
	if err := verifyCanonicalAssignment(plan.Lanes); err != nil {
		return nil, err
	}

	return &Authority{
		version:           plan.Version,
		coverage:          plan.Coverage,
		baseIdentity:      plan.BaseIdentity,
		configFingerprint: plan.ConfigFingerprint,
		lanes:             copyLanes(plan.Lanes),
		subsumed:          copySubsumed(plan.Subsumed),
		digest:            plan.PlanHash,
	}, nil
}

// CompileAuthority compiles a plan and returns the run authority for it. It is
// the ordinary entry point: a caller that is planning work wants the authority,
// and CompilePlan exists for callers that need the persistable data.
func CompileAuthority(req PlanRequest) (*Authority, error) {
	plan, err := CompilePlan(req)
	if err != nil {
		return nil, err
	}
	return NewAuthority(plan)
}

// Version reports the plan encoding version this authority enforces.
func (a *Authority) Version() int { return a.version }

// Digest reports the verified plan digest.
func (a *Authority) Digest() string { return a.digest }

// Coverage reports what the plan claims about the scope it was compiled from.
func (a *Authority) Coverage() Coverage { return a.coverage }

// BaseIdentity reports the source this plan enumerates. It is resume identity,
// not a rate-limit authority.
func (a *Authority) BaseIdentity() string { return a.baseIdentity }

// ConfigFingerprint reports the caller digest of behavior-affecting
// configuration this plan was compiled under.
func (a *Authority) ConfigFingerprint() string { return a.configFingerprint }

// LaneCount reports how many lanes the run executes.
func (a *Authority) LaneCount() int { return len(a.lanes) }

// Lanes returns a copy of the lane assignment. It copies rather than exposing
// the slice so a caller ranging over lanes cannot alter what the run enforces.
func (a *Authority) Lanes() []Lane { return copyLanes(a.lanes) }

// Plan returns a copy of the validated plan as persistable data.
func (a *Authority) Plan() *Plan {
	return &Plan{
		Version:           a.version,
		Coverage:          a.coverage,
		BaseIdentity:      a.baseIdentity,
		ConfigFingerprint: a.configFingerprint,
		Lanes:             copyLanes(a.lanes),
		PlanHash:          a.digest,
		Subsumed:          copySubsumed(a.subsumed),
	}
}

// LaneRef returns the identity of the lane with the given ordinal.
//
// It refuses an ordinal this plan does not contain rather than returning a
// reference to a lane that does not exist: a fabricated reference would compare
// equal to itself and could otherwise travel as though it named real work.
func (a *Authority) LaneRef(ordinal int) (LaneRef, error) {
	if ordinal < 1 || ordinal > len(a.lanes) {
		return LaneRef{}, fmt.Errorf("partition: plan has no lane with ordinal %d (it has %d lanes)",
			ordinal, len(a.lanes))
	}
	return LaneRef{PlanVersion: a.version, PlanDigest: a.digest, Ordinal: ordinal}, nil
}

// Lane returns the lane with the given ordinal, refusing one this plan does not
// contain.
func (a *Authority) Lane(ordinal int) (Lane, error) {
	if ordinal < 1 || ordinal > len(a.lanes) {
		return Lane{}, fmt.Errorf("partition: plan has no lane with ordinal %d (it has %d lanes)",
			ordinal, len(a.lanes))
	}
	return copyLane(a.lanes[ordinal-1]), nil
}

// AuthorizeLane checks a claimed lane reference against the plan this run holds.
//
// A claim arriving alongside data — records read from a stream, a resumed
// checkpoint, an API request — is untrusted input: it says which lane it
// believes it belongs to, and this answers whether that is true of the plan the
// run is actually executing. It refuses rather than warns, because the caller's
// next step is a mutation the claim would otherwise scope.
//
// Authorization is not completion. A verified reference says a record belongs
// to a lane of this plan; it says nothing about whether that lane's enumerator
// reached the end of its input or whether its work is durably finished. Only
// the run-owned coordinator may record those.
func (a *Authority) AuthorizeLane(claim LaneRef) error {
	if claim.PlanVersion != a.version {
		return fmt.Errorf("partition: lane claim is for plan version %d, run executes version %d",
			claim.PlanVersion, a.version)
	}
	if claim.PlanDigest != a.digest {
		return fmt.Errorf("partition: lane claim digest %s does not match the plan this run executes",
			abbreviate(claim.PlanDigest))
	}
	if claim.Ordinal < 1 || claim.Ordinal > len(a.lanes) {
		return fmt.Errorf("partition: lane claim names ordinal %d, which this plan does not contain",
			claim.Ordinal)
	}
	return nil
}

func validatePrefix(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("prefix is empty, which covers the whole scope")
	}
	if !utf8.ValidString(prefix) {
		return fmt.Errorf("prefix is not valid UTF-8")
	}
	if strings.ContainsRune(prefix, 0) {
		return fmt.Errorf("prefix contains a NUL byte")
	}
	return nil
}

// verifyCanonicalAssignment proves two things about a plan's lanes from one
// sorted view of its prefixes: that no retained prefix covers another, and that
// the prefixes are distributed exactly as the compiler would distribute them.
//
// Overlap matters because the compiler removes covered prefixes precisely so no
// object is enumerated on two lanes; a plan retaining an overlap would
// double-enumerate, and the run has no way to recover the operator's intent.
//
// Canonical assignment matters because a self-consistent plan is not
// necessarily the compiler's plan. A writer can move prefixes between lanes, or
// reorder them within a lane, reseal the digest, and produce something
// structurally valid whose lane identities name different work than compiling
// the same prefix set at the same lane count would. Lane identity has to mean
// one thing.
//
// The scan is O(P log P) in the total prefix count, not O(P²). The lane ceiling
// bounds lanes, not prefixes per lane, and a compiled plan over a discovered
// prefix set can be very large — exactly the high-cardinality case the planner
// exists for. Sorting once and comparing adjacent entries is sufficient for
// containment: if a is a byte-prefix of c and a <= b <= c, then a is also a
// byte-prefix of b, so any containment shows up between neighbours.
func verifyCanonicalAssignment(lanes []Lane) error {
	type owned struct {
		prefix  string
		ordinal int
	}

	total := 0
	for _, lane := range lanes {
		total += len(lane.Prefixes)
	}
	flat := make([]owned, 0, total)
	for _, lane := range lanes {
		for _, prefix := range lane.Prefixes {
			flat = append(flat, owned{prefix: prefix, ordinal: lane.Ordinal})
		}
	}
	sort.Slice(flat, func(i, j int) bool { return flat[i].prefix < flat[j].prefix })

	for i := 1; i < len(flat); i++ {
		prev, cur := flat[i-1], flat[i]
		if prev.prefix == cur.prefix {
			return fmt.Errorf("partition: prefix %q appears on lanes %d and %d; it would be enumerated twice",
				prev.prefix, prev.ordinal, cur.ordinal)
		}
		if strings.HasPrefix(cur.prefix, prev.prefix) {
			return fmt.Errorf("partition: prefix %q (lane %d) is covered by %q (lane %d); "+
				"the objects it names would be enumerated twice",
				cur.prefix, cur.ordinal, prev.prefix, prev.ordinal)
		}
	}

	sorted := make([]string, len(flat))
	for i := range flat {
		sorted[i] = flat[i].prefix
	}
	expected := assignLanes(sorted, len(lanes))
	for i, want := range expected {
		got := lanes[i]
		if len(got.Prefixes) != len(want.Prefixes) {
			return fmt.Errorf("partition: lane %d carries %d prefixes, canonical assignment gives it %d; "+
				"the plan is not the compiler's assignment for this prefix set and lane count",
				want.Ordinal, len(got.Prefixes), len(want.Prefixes))
		}
		for j := range want.Prefixes {
			if got.Prefixes[j] != want.Prefixes[j] {
				return fmt.Errorf("partition: lane %d position %d carries %q, canonical assignment gives it %q; "+
					"the plan is not the compiler's assignment for this prefix set and lane count",
					want.Ordinal, j, got.Prefixes[j], want.Prefixes[j])
			}
		}
	}
	return nil
}

func copyLane(lane Lane) Lane {
	prefixes := make([]string, len(lane.Prefixes))
	copy(prefixes, lane.Prefixes)
	return Lane{Ordinal: lane.Ordinal, Prefixes: prefixes}
}

func copyLanes(lanes []Lane) []Lane {
	out := make([]Lane, len(lanes))
	for i, lane := range lanes {
		out[i] = copyLane(lane)
	}
	return out
}

func copySubsumed(subsumed []SubsumedPrefix) []SubsumedPrefix {
	if subsumed == nil {
		return nil
	}
	out := make([]SubsumedPrefix, len(subsumed))
	copy(out, subsumed)
	return out
}
