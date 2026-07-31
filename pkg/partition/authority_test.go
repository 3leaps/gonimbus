package partition

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func mustAuthority(t *testing.T, req PlanRequest) *Authority {
	t.Helper()
	authority, err := CompileAuthority(req)
	if err != nil {
		t.Fatalf("CompileAuthority(%+v): unexpected error: %v", req, err)
	}
	return authority
}

func validRequest() PlanRequest {
	return PlanRequest{
		Prefixes:          []string{"site/a/", "site/b/", "site/c/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: testConfigFingerprint,
	}
}

// A freshly compiled plan must satisfy every invariant the authority enforces.
// If it did not, the validator would be describing a plan shape the compiler
// does not produce.
func TestCompiledPlanValidatesAsAuthority(t *testing.T) {
	plan := mustCompile(t, validRequest())
	authority, err := NewAuthority(plan)
	if err != nil {
		t.Fatalf("a compiled plan was refused by its own validator: %v", err)
	}
	if authority.Digest() != plan.PlanHash {
		t.Fatalf("authority digest %s does not match the compiled plan %s", authority.Digest(), plan.PlanHash)
	}
	if authority.LaneCount() != len(plan.Lanes) {
		t.Fatalf("authority reports %d lanes, plan has %d", authority.LaneCount(), len(plan.Lanes))
	}
}

// The digest is only an integrity fingerprint once it is recomputed. Until
// then it is an assertion by whoever wrote the plan, and a plan literal can
// assert anything.
func TestNewAuthorityRefusesUnverifiablePlans(t *testing.T) {
	valid := mustCompile(t, validRequest())

	fabricated := &Plan{
		Version:           PlanVersion,
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: testConfigFingerprint,
		PlanHash:          "made-up",
		Lanes:             []Lane{{Ordinal: 1, Prefixes: []string{"a/"}}},
	}

	noDigest := *fabricated
	noDigest.PlanHash = ""
	noDigest.Lanes = copyLanes(fabricated.Lanes)

	// A prefix edited without resealing. This is the digest's own case: the plan
	// is otherwise well-formed, so nothing but the recomputation can catch it.
	tamperedPrefix := *valid
	tamperedPrefix.Lanes = copyLanes(valid.Lanes)
	tamperedPrefix.Lanes[0].Prefixes[0] = "site/z/"

	// Hashed but non-structural fields, edited without resealing. These cannot
	// be caught by any structural check — they are individually valid values —
	// so they isolate the digest comparison exactly.
	tamperedCoverage := *valid
	tamperedCoverage.Lanes = copyLanes(valid.Lanes)
	tamperedCoverage.Coverage = CoveragePartial

	tamperedIdentity := *valid
	tamperedIdentity.Lanes = copyLanes(valid.Lanes)
	tamperedIdentity.BaseIdentity = "s3://other-bucket/base/"

	tamperedFingerprint := *valid
	tamperedFingerprint.Lanes = copyLanes(valid.Lanes)
	tamperedFingerprint.ConfigFingerprint = "cfg-2"

	badVersion := *valid
	badVersion.Lanes = copyLanes(valid.Lanes)
	badVersion.Version = PlanVersion + 1

	noIdentity := *valid
	noIdentity.Lanes = copyLanes(valid.Lanes)
	noIdentity.BaseIdentity = "  "

	noFingerprint := *valid
	noFingerprint.Lanes = copyLanes(valid.Lanes)
	noFingerprint.ConfigFingerprint = ""

	noLanes := *valid
	noLanes.Lanes = nil

	badCoverage := *valid
	badCoverage.Lanes = copyLanes(valid.Lanes)
	badCoverage.Coverage = "mostly"

	cases := []struct {
		name    string
		plan    *Plan
		wantSub string
	}{
		{"nil plan", nil, "nil plan"},
		{"fabricated literal", fabricated, "does not match its contents"},
		{"empty digest", &noDigest, "carries no digest"},
		{"tampered prefix with stale digest", &tamperedPrefix, "does not match its contents"},
		{"tampered coverage with stale digest", &tamperedCoverage, "does not match its contents"},
		{"tampered base identity with stale digest", &tamperedIdentity, "does not match its contents"},
		{"tampered fingerprint with stale digest", &tamperedFingerprint, "does not match its contents"},
		{"unsupported version", &badVersion, "not supported"},
		{"missing base identity", &noIdentity, "no source base identity"},
		{"missing config fingerprint", &noFingerprint, "no configuration fingerprint"},
		{"no lanes", &noLanes, "no lanes"},
		{"unknown coverage", &badCoverage, "unknown coverage"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			authority, err := NewAuthority(tc.plan)
			if err == nil {
				t.Fatalf("plan was accepted as authority: %+v", authority.Plan())
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// The digest comparison would refuse most tampering on its own, which means the
// structural checks above are never exercised by a plan whose digest is stale.
// An attacker or a buggy writer that recomputes the digest over what it wrote
// defeats the digest check entirely, so each structural invariant must refuse on
// its own grounds with the digest consistent.
func TestNewAuthorityRefusesStructurallyInvalidPlansWithConsistentDigest(t *testing.T) {
	reseal := func(t *testing.T, plan *Plan) *Plan {
		t.Helper()
		digest, err := hashPlan(plan)
		if err != nil {
			t.Fatalf("hashPlan: %v", err)
		}
		plan.PlanHash = digest
		return plan
	}

	mutate := func(t *testing.T, fn func(*Plan)) *Plan {
		t.Helper()
		plan := mustCompile(t, validRequest())
		plan.Lanes = copyLanes(plan.Lanes)
		fn(plan)
		return reseal(t, plan)
	}

	overCeiling := func(t *testing.T) *Plan {
		t.Helper()
		base := mustCompile(t, validRequest())
		lanes := make([]Lane, 0, LaneCeiling+1)
		for i := 0; i <= LaneCeiling; i++ {
			lanes = append(lanes, Lane{Ordinal: i + 1, Prefixes: []string{manyPrefixes(LaneCeiling + 1)[i]}})
		}
		base.Lanes = lanes
		return reseal(t, base)
	}

	cases := []struct {
		name    string
		plan    *Plan
		wantSub string
	}{
		{
			name:    "duplicate ordinal, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[1].Ordinal = 1 }),
			wantSub: "ordinals must be 1..N",
		},
		{
			name:    "ordinal gap, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[2].Ordinal = 9 }),
			wantSub: "ordinals must be 1..N",
		},
		{
			name:    "empty lane, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[1].Prefixes = nil }),
			wantSub: "not work",
		},
		{
			name:    "empty prefix, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[0].Prefixes[0] = "" }),
			wantSub: "covers the whole scope",
		},
		{
			name:    "NUL byte in prefix, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[0].Prefixes[0] = "site/a\x00/" }),
			wantSub: "NUL byte",
		},
		{
			name:    "overlapping prefixes, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[1].Prefixes[0] = "site/a/2026/" }),
			wantSub: "enumerated twice",
		},
		{
			name:    "duplicate prefix on two lanes, digest consistent",
			plan:    mutate(t, func(p *Plan) { p.Lanes[1].Prefixes[0] = "site/a/" }),
			wantSub: "enumerated twice",
		},
		{
			name:    "lane count above ceiling, digest consistent",
			plan:    overCeiling(t),
			wantSub: "exceeds the ceiling",
		},
		{
			// Structurally valid and non-overlapping, but not the assignment the
			// compiler produces: lane identity would name different work than
			// compiling the same prefix set at the same lane count.
			name: "prefixes swapped between lanes, digest consistent",
			plan: mutate(t, func(p *Plan) {
				p.Lanes[0].Prefixes[0], p.Lanes[1].Prefixes[0] = p.Lanes[1].Prefixes[0], p.Lanes[0].Prefixes[0]
			}),
			wantSub: "canonical assignment",
		},
		{
			name: "prefixes reordered within one lane, digest consistent",
			plan: func() *Plan {
				plan := mustCompile(t, PlanRequest{
					Prefixes:          []string{"site/a/", "site/b/", "site/c/", "site/d/"},
					Coverage:          CoverageComplete,
					BaseIdentity:      testBaseIdentity,
					ConfigFingerprint: testConfigFingerprint,
					MaxLanes:          2,
				})
				plan.Lanes = copyLanes(plan.Lanes)
				lane := plan.Lanes[0]
				lane.Prefixes[0], lane.Prefixes[1] = lane.Prefixes[1], lane.Prefixes[0]
				return reseal(t, plan)
			}(),
			wantSub: "canonical assignment",
		},
		{
			// The same canonical prefix set, resealed under a contiguous-block
			// assignment instead of round-robin. Every prefix appears exactly
			// once and nothing overlaps.
			name: "non-round-robin assignment of the canonical set, digest consistent",
			plan: func() *Plan {
				plan := mustCompile(t, PlanRequest{
					Prefixes:          []string{"site/a/", "site/b/", "site/c/", "site/d/"},
					Coverage:          CoverageComplete,
					BaseIdentity:      testBaseIdentity,
					ConfigFingerprint: testConfigFingerprint,
					MaxLanes:          2,
				})
				plan.Lanes = []Lane{
					{Ordinal: 1, Prefixes: []string{"site/a/", "site/b/"}},
					{Ordinal: 2, Prefixes: []string{"site/c/", "site/d/"}},
				}
				return reseal(t, plan)
			}(),
			wantSub: "canonical assignment",
		},
		{
			name: "identity stripped, digest consistent",
			plan: mutate(t, func(p *Plan) { p.BaseIdentity = "" }),
			// Refused before the digest is ever recomputed: an unidentified plan
			// cannot be told apart from another run's plan, sealed or not.
			wantSub: "no source base identity",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Guard the premise: if the digest were stale, this control would
			// pass through the digest comparison and prove nothing about the
			// structural check it names.
			if tc.wantSub != "no source base identity" {
				if recomputed, err := hashPlan(tc.plan); err != nil {
					t.Fatalf("hashPlan: %v", err)
				} else if recomputed != tc.plan.PlanHash {
					t.Fatalf("test setup left a stale digest; this case would be caught by the digest check, not by %q", tc.wantSub)
				}
			}

			authority, err := NewAuthority(tc.plan)
			if err == nil {
				t.Fatalf("structurally invalid plan accepted as authority: %+v", authority.Plan())
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// A plan that arrives over the wire is the ordinary case, not the exotic one:
// it is exactly the input that never passed through the compiler.
func TestNewAuthorityRefusesMalformedUnmarshaledPlan(t *testing.T) {
	valid := mustCompile(t, validRequest())
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Round-tripping unchanged must still validate — otherwise the encoding
	// itself, not the tampering, is what this control detects.
	var roundTripped Plan
	if err := json.Unmarshal(encoded, &roundTripped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, err := NewAuthority(&roundTripped); err != nil {
		t.Fatalf("an unmodified round-trip was refused: %v", err)
	}

	tamperedJSON := strings.Replace(string(encoded), `"site/b/"`, `"site/zzz/"`, 1)
	if tamperedJSON == string(encoded) {
		t.Fatal("test setup did not tamper with the encoded plan")
	}
	var tampered Plan
	if err := json.Unmarshal([]byte(tamperedJSON), &tampered); err != nil {
		t.Fatalf("unmarshal tampered: %v", err)
	}
	if _, err := NewAuthority(&tampered); err == nil {
		t.Fatal("a plan whose prefixes were edited in transit was accepted as authority")
	}
}

// Validation that can be undone afterwards is not validation. Mutating the
// source plan, or anything the authority hands back, must not change what the
// run enforces.
func TestAuthorityIsImmuneToPostConstructionMutation(t *testing.T) {
	plan := mustCompile(t, validRequest())
	authority, err := NewAuthority(plan)
	if err != nil {
		t.Fatalf("NewAuthority: %v", err)
	}
	digest := authority.Digest()
	ref, err := authority.LaneRef(1)
	if err != nil {
		t.Fatalf("LaneRef: %v", err)
	}

	// Mutate every reachable part of the source plan.
	plan.PlanHash = "rewritten"
	plan.Version = PlanVersion + 5
	plan.BaseIdentity = "s3://somewhere-else/"
	plan.Coverage = CoveragePartial
	plan.Lanes[0].Ordinal = 99
	plan.Lanes[0].Prefixes[0] = "hijacked/"
	plan.Lanes = append(plan.Lanes, Lane{Ordinal: 7, Prefixes: []string{"extra/"}})

	if authority.Digest() != digest {
		t.Fatalf("authority digest changed with the source plan: %s -> %s", digest, authority.Digest())
	}
	if authority.Version() != PlanVersion {
		t.Fatalf("authority version changed with the source plan: %d", authority.Version())
	}
	if authority.BaseIdentity() != testBaseIdentity {
		t.Fatalf("authority base identity changed with the source plan: %q", authority.BaseIdentity())
	}
	if authority.Coverage() != CoverageComplete {
		t.Fatalf("authority coverage changed with the source plan: %q", authority.Coverage())
	}
	if authority.LaneCount() != 3 {
		t.Fatalf("authority lane count changed with the source plan: %d", authority.LaneCount())
	}
	if err := authority.AuthorizeLane(ref); err != nil {
		t.Fatalf("a reference the authority issued stopped verifying after the source plan changed: %v", err)
	}

	// The accessors must copy too, or a caller ranging over lanes can reach in.
	lanes := authority.Lanes()
	lanes[0].Prefixes[0] = "hijacked-again/"
	lanes[0].Ordinal = 42
	if got, _ := authority.Lane(1); got.Prefixes[0] == "hijacked-again/" || got.Ordinal != 1 {
		t.Fatalf("mutating the returned lane slice altered the authority: %+v", got)
	}

	exported := authority.Plan()
	exported.PlanHash = "rewritten-again"
	exported.Lanes[0].Prefixes[0] = "hijacked-once-more/"
	if authority.Digest() != digest {
		t.Fatal("mutating the exported plan altered the authority digest")
	}
	if got, _ := authority.Lane(1); got.Prefixes[0] == "hijacked-once-more/" {
		t.Fatal("mutating the exported plan altered the authority lanes")
	}
}

// The exported plan must be persistable and must validate again: a round trip
// through storage cannot be what invalidates a run's authority.
func TestAuthorityPlanRoundTripsThroughValidation(t *testing.T) {
	authority := mustAuthority(t, validRequest())
	again, err := NewAuthority(authority.Plan())
	if err != nil {
		t.Fatalf("the authority's own exported plan was refused: %v", err)
	}
	if again.Digest() != authority.Digest() {
		t.Fatalf("digest changed across export and revalidation: %s -> %s", authority.Digest(), again.Digest())
	}
}

// A lane ordinal alone is not an identity: lane 2 names entirely different work
// under a different plan, so a reference carrying only an ordinal cannot be
// checked against anything.
func TestLaneRefCarriesPlanVersionDigestAndOrdinal(t *testing.T) {
	authority := mustAuthority(t, validRequest())

	ref, err := authority.LaneRef(2)
	if err != nil {
		t.Fatalf("LaneRef(2): %v", err)
	}
	if ref.PlanVersion != authority.Version() || ref.PlanDigest != authority.Digest() || ref.Ordinal != 2 {
		t.Fatalf("lane reference %+v does not name plan %d/%s lane 2", ref, authority.Version(), authority.Digest())
	}
	if !ref.Equal(LaneRef{PlanVersion: authority.Version(), PlanDigest: authority.Digest(), Ordinal: 2}) {
		t.Fatal("a reconstructed reference to the same lane did not compare equal")
	}

	other := mustAuthority(t, PlanRequest{
		Prefixes:          []string{"site/a/", "site/b/", "site/d/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: testConfigFingerprint,
	})
	otherRef, err := other.LaneRef(2)
	if err != nil {
		t.Fatalf("LaneRef(2) on second authority: %v", err)
	}
	if ref.Equal(otherRef) {
		t.Fatal("ordinal 2 of two different plans compared equal; ordinal is being treated as identity")
	}
}

// widePlan compiles a plan over a large discovered prefix set. The prefixes are
// fixed width so none is a byte-prefix of another.
func widePlan(t testing.TB, prefixCount int) *Plan {
	t.Helper()
	prefixes := make([]string, 0, prefixCount)
	for i := 0; i < prefixCount; i++ {
		prefixes = append(prefixes, fmt.Sprintf("site/%08d/", i))
	}
	plan, err := CompilePlan(PlanRequest{
		Prefixes:          prefixes,
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: testConfigFingerprint,
		MaxLanes:          LaneCeiling,
	})
	if err != nil {
		t.Fatalf("CompilePlan over %d prefixes: %v", prefixCount, err)
	}
	return plan
}

// The lane ceiling bounds lanes, not prefixes per lane, so a compiled plan over
// a discovered prefix set can be very large — the high-cardinality case the
// planner exists for. Validation must stay O(P log P); a pairwise scan would be
// O(P²) at exactly that boundary.
//
// This guards the complexity class, not a performance target, and the size was
// chosen by measuring both shapes rather than by guessing. At 20k prefixes a
// pairwise scan completes in ~0.9s — a bound loose enough to tolerate CI noise
// would not have failed it, so a guard at that size proves nothing. At 100k the
// separation is unambiguous: the sorted scan runs in tens of milliseconds and a
// pairwise scan takes ~20s, because one grows by 5x and the other by 25x.
//
// The bound sits roughly two orders of magnitude above the sorted scan and well
// below the pairwise one, so it fails on a restored nested loop without being
// sensitive to machine speed or CI load.
func TestNewAuthorityValidationStaysSubQuadratic(t *testing.T) {
	if testing.Short() {
		t.Skip("scale guard")
	}
	const prefixCount = 100000
	const bound = 3 * time.Second

	plan := widePlan(t, prefixCount)

	start := time.Now()
	authority, err := NewAuthority(plan)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("NewAuthority over %d prefixes: %v", prefixCount, err)
	}
	if authority.LaneCount() != LaneCeiling {
		t.Fatalf("expected %d lanes, got %d", LaneCeiling, authority.LaneCount())
	}
	if elapsed > bound {
		t.Fatalf("validating %d prefixes took %s, above the %s bound; "+
			"validation has likely regressed to a pairwise scan", prefixCount, elapsed, bound)
	}
	t.Logf("validated %d prefixes across %d lanes in %s", prefixCount, authority.LaneCount(), elapsed)
}

func BenchmarkNewAuthorityWidePlan(b *testing.B) {
	plan := widePlan(b, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := NewAuthority(plan); err != nil {
			b.Fatalf("NewAuthority: %v", err)
		}
	}
}

func TestAuthorityLaneRefRefusesUnknownOrdinal(t *testing.T) {
	authority := mustAuthority(t, PlanRequest{
		Prefixes:          []string{"a/", "b/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: testConfigFingerprint,
	})
	for _, ordinal := range []int{0, -1, 3} {
		if ref, err := authority.LaneRef(ordinal); err == nil {
			t.Fatalf("LaneRef(%d) returned %+v for a plan with %d lanes", ordinal, ref, authority.LaneCount())
		}
		if lane, err := authority.Lane(ordinal); err == nil {
			t.Fatalf("Lane(%d) returned %+v for a plan with %d lanes", ordinal, lane, authority.LaneCount())
		}
	}
}

// An ordinal alone is not identity, and the plan the run holds is what decides.
func TestAuthorityAuthorizeLaneRefusesForeignClaims(t *testing.T) {
	authority := mustAuthority(t, validRequest())
	valid, err := authority.LaneRef(2)
	if err != nil {
		t.Fatalf("LaneRef(2): %v", err)
	}
	if err := authority.AuthorizeLane(valid); err != nil {
		t.Fatalf("a reference this authority issued was refused: %v", err)
	}

	otherSource := mustAuthority(t, PlanRequest{
		Prefixes:          []string{"site/a/", "site/b/", "site/c/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      "s3://other-bucket/base/",
		ConfigFingerprint: testConfigFingerprint,
	})
	foreign, err := otherSource.LaneRef(2)
	if err != nil {
		t.Fatalf("LaneRef(2) on second authority: %v", err)
	}
	if foreign.Equal(valid) {
		t.Fatal("lane 2 of two different plans compared equal")
	}

	cases := []struct {
		name    string
		claim   LaneRef
		wantSub string
	}{
		{"foreign plan digest", foreign, "does not match the plan this run executes"},
		{"empty claim", LaneRef{}, "plan version"},
		{"right digest, wrong version", LaneRef{PlanVersion: authority.Version() + 1, PlanDigest: authority.Digest(), Ordinal: 1}, "plan version"},
		{"right plan, unknown ordinal", LaneRef{PlanVersion: authority.Version(), PlanDigest: authority.Digest(), Ordinal: 9}, "does not contain"},
		{"missing digest", LaneRef{PlanVersion: authority.Version(), Ordinal: 1}, "does not match the plan this run executes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := authority.AuthorizeLane(tc.claim)
			if err == nil {
				t.Fatalf("claim %+v was authorized against a plan that does not contain it", tc.claim)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}
