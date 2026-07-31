package partition

import (
	"strings"
	"testing"
)

const (
	testBaseIdentity      = "s3://bucket/base/"
	testConfigFingerprint = "cfg-1"
)

func mustCompile(t *testing.T, req PlanRequest) *Plan {
	t.Helper()
	plan, err := CompilePlan(req)
	if err != nil {
		t.Fatalf("CompilePlan(%+v): unexpected error: %v", req, err)
	}
	return plan
}

func planPrefixes(plan *Plan) []string {
	var out []string
	for _, lane := range plan.Lanes {
		out = append(out, lane.Prefixes...)
	}
	return out
}

func manyPrefixes(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, string(rune('a'+i/26))+string(rune('a'+i%26))+"/")
	}
	return out
}

func TestCompilePlanRefusesUndecidableInput(t *testing.T) {
	cases := []struct {
		name    string
		req     PlanRequest
		wantSub string
	}{
		{
			name:    "coverage must be declared",
			req:     PlanRequest{Prefixes: []string{"a/"}},
			wantSub: "coverage must be declared",
		},
		{
			name:    "unknown coverage",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: "mostly", BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint},
			wantSub: "unknown plan coverage",
		},
		{
			name:    "negative max lanes",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint, MaxLanes: -1},
			wantSub: "must not be negative",
		},
		{
			name:    "requested lane count above ceiling",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint, MaxLanes: LaneCeiling + 1},
			wantSub: "exceeds the ceiling",
		},
		{
			// The whole-scope entry subsumes every sibling, so the plan the
			// operator wrote and the plan that would run share no structure.
			name:    "empty prefix alongside named partitions",
			req:     PlanRequest{Prefixes: []string{"a/", ""}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint},
			wantSub: "covers the whole scope",
		},
		{
			name:    "no prefixes at all",
			req:     PlanRequest{Prefixes: nil, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint},
			wantSub: "at least one prefix",
		},
		{
			name:    "NUL byte in prefix",
			req:     PlanRequest{Prefixes: []string{"a\x00b/"}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint},
			wantSub: "NUL byte",
		},
		{
			name:    "invalid UTF-8 prefix",
			req:     PlanRequest{Prefixes: []string{"a\xffb/"}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint},
			wantSub: "valid UTF-8",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := CompilePlan(tc.req)
			if err == nil {
				t.Fatalf("expected refusal, got plan %+v", plan)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// Leading and trailing spaces are valid object-key bytes: " a/" and "a/" name
// different sets of objects, and "a/ " is a narrower prefix than "a/". A
// normalization that trims them enumerates a different set than the operator
// asked for while still reporting the plan as union-preserving.
func TestCompilePlanPreservesPrefixBytesExactly(t *testing.T) {
	spaced := []string{" lead/", "trail/ ", "  ", "plain/"}
	plan := mustCompile(t, PlanRequest{
		Prefixes: spaced,
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})

	got := planPrefixes(plan)
	if len(got) != len(spaced) {
		t.Fatalf("expected all %d prefixes retained, got %v", len(spaced), got)
	}
	retained := map[string]bool{}
	for _, p := range got {
		retained[p] = true
	}
	for _, want := range spaced {
		if !retained[want] {
			t.Fatalf("prefix %q was not retained byte-exactly; got %q", want, got)
		}
	}
	if len(plan.Subsumed) != 0 {
		t.Fatalf("space-bearing prefixes must not subsume one another, got %+v", plan.Subsumed)
	}
}

// A whitespace-only prefix is a legitimate narrowing of the scope — keys
// beginning with a space — and is not the whole-scope sentinel. Only a
// zero-length prefix covers everything.
func TestCompilePlanAcceptsWhitespaceOnlyPrefix(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{" "},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if got := planPrefixes(plan); len(got) != 1 || got[0] != " " {
		t.Fatalf("whitespace-only prefix not preserved, got %q", got)
	}
}

// The digest must distinguish prefix sets that enumerate different objects.
// Under a trimming normalization these three requests collapse to one plan and
// could resume against each other.
func TestPlanHashDistinguishesSpaceBearingPrefixes(t *testing.T) {
	base := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	leading := mustCompile(t, PlanRequest{
		Prefixes: []string{" a/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	trailing := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/ "},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})

	if base.PlanHash == leading.PlanHash {
		t.Fatal("a leading space did not change the digest; the plan enumerates a different set")
	}
	if base.PlanHash == trailing.PlanHash {
		t.Fatal("a trailing space did not change the digest; the plan enumerates a different set")
	}
	if leading.PlanHash == trailing.PlanHash {
		t.Fatal("leading- and trailing-space prefixes produced the same digest")
	}
}

// MaxLanes is a bound whose zero value means one lane per retained prefix, so
// checking only the requested value leaves the default path unbounded: a prefix
// set larger than the ceiling would run more lanes than the ceiling admits
// without any lane count ever being refused.
func TestCompilePlanRefusesResolvedLaneCountAboveCeiling(t *testing.T) {
	over := manyPrefixes(LaneCeiling + 1)
	plan, err := CompilePlan(PlanRequest{
		Prefixes: over,
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if err == nil {
		t.Fatalf("expected refusal at %d resolved lanes, got %d lanes", len(over), len(plan.Lanes))
	}
	if !strings.Contains(err.Error(), "exceeds the ceiling") {
		t.Fatalf("error %q does not name the ceiling", err.Error())
	}

	// Refusal, not a clamp: the same prefix set is accepted when the operator
	// bounds it explicitly, and it then runs the lane count they asked for.
	bounded := mustCompile(t, PlanRequest{
		Prefixes: over,
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
		MaxLanes: LaneCeiling,
	})
	if len(bounded.Lanes) != LaneCeiling {
		t.Fatalf("bounded plan ran %d lanes, want %d", len(bounded.Lanes), LaneCeiling)
	}
	if got := len(planPrefixes(bounded)); got != len(over) {
		t.Fatalf("bounding the lane count dropped prefixes: %d of %d retained", got, len(over))
	}
}

func TestCompilePlanAcceptsResolvedCountAtCeiling(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: manyPrefixes(LaneCeiling),
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if len(plan.Lanes) != LaneCeiling {
		t.Fatalf("expected exactly %d lanes at the ceiling, got %d", LaneCeiling, len(plan.Lanes))
	}
}

func TestCompilePlanSubsumesCoveredPrefixes(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{"site/a/", "site/a/2026/", "site/a/2026/01/", "site/b/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})

	got := planPrefixes(plan)
	if len(got) != 2 {
		t.Fatalf("expected 2 retained prefixes, got %v", got)
	}
	retained := map[string]bool{}
	for _, p := range got {
		retained[p] = true
	}
	if !retained["site/a/"] || !retained["site/b/"] {
		t.Fatalf("expected site/a/ and site/b/ retained, got %v", got)
	}

	if len(plan.Subsumed) != 2 {
		t.Fatalf("expected 2 subsumed entries, got %+v", plan.Subsumed)
	}
	// Both dropped entries must attribute to the prefix that actually covers
	// them, not merely to "something" — an operator reading this needs to know
	// which prefix ate their partition.
	for _, s := range plan.Subsumed {
		if s.CoveredBy != "site/a/" {
			t.Fatalf("subsumed %q attributed to %q, want site/a/", s.Prefix, s.CoveredBy)
		}
		if !strings.HasPrefix(s.Prefix, s.CoveredBy) {
			t.Fatalf("subsumed %q is not actually covered by %q", s.Prefix, s.CoveredBy)
		}
	}
}

// Object-store LIST prefixes are byte prefixes, not path segments: listing "a"
// returns "ab/x". A path-aware containment check would retain "ab/" alongside
// "a" and enumerate those objects on two lanes.
func TestCompilePlanUsesBytePrefixNotPathSemantics(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{"a", "ab/", "a/x/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})

	got := planPrefixes(plan)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected byte-prefix containment to retain only %q, got %v", "a", got)
	}
	if len(plan.Subsumed) != 2 {
		t.Fatalf("expected ab/ and a/x/ subsumed, got %+v", plan.Subsumed)
	}
}

func TestCompilePlanDeduplicatesAndAssignsStableOrdinals(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{"c/", "a/", "b/", "a/"},
		Coverage: CoveragePartial, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})

	if len(plan.Lanes) != 3 {
		t.Fatalf("expected 3 lanes after dedupe, got %d", len(plan.Lanes))
	}
	for i, lane := range plan.Lanes {
		if lane.Ordinal != i+1 {
			t.Fatalf("lane %d has ordinal %d, want %d", i, lane.Ordinal, i+1)
		}
	}
	if plan.Coverage != CoveragePartial {
		t.Fatalf("coverage %q not preserved", plan.Coverage)
	}
}

func TestCompilePlanBoundsLanesAndDistributesRoundRobin(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/", "b/", "c/", "d/", "e/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
		MaxLanes: 2,
	})

	if len(plan.Lanes) != 2 {
		t.Fatalf("expected 2 lanes, got %d", len(plan.Lanes))
	}
	// Round-robin over the sorted set: lane 1 takes a/, c/, e/ and lane 2 takes
	// b/, d/. Adjacent prefixes land on different lanes rather than one lane
	// taking the whole lexicographic head.
	if got := plan.Lanes[0].Prefixes; len(got) != 3 {
		t.Fatalf("lane 1 got %v, want 3 prefixes", got)
	}
	if got := plan.Lanes[1].Prefixes; len(got) != 2 {
		t.Fatalf("lane 2 got %v, want 2 prefixes", got)
	}
	if plan.Lanes[0].Prefixes[0] != "a/" || plan.Lanes[1].Prefixes[0] != "b/" {
		t.Fatalf("adjacent prefixes not spread across lanes: %v / %v",
			plan.Lanes[0].Prefixes, plan.Lanes[1].Prefixes)
	}

	// Every input prefix must still appear exactly once across all lanes.
	seen := map[string]int{}
	for _, lane := range plan.Lanes {
		for _, p := range lane.Prefixes {
			seen[p]++
		}
	}
	for _, want := range []string{"a/", "b/", "c/", "d/", "e/"} {
		if seen[want] != 1 {
			t.Fatalf("prefix %q appears %d times across lanes, want exactly 1", want, seen[want])
		}
	}
}

// A lane count larger than the prefix set must not produce empty lanes: an
// empty lane is not work, and a run reporting it would overstate its topology.
func TestCompilePlanNeverProducesEmptyLanes(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/", "b/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
		MaxLanes: 8,
	})
	if len(plan.Lanes) != 2 {
		t.Fatalf("expected lane count bounded by the prefix set, got %d lanes", len(plan.Lanes))
	}
	for _, lane := range plan.Lanes {
		if len(lane.Prefixes) == 0 {
			t.Fatalf("lane %d has no prefixes", lane.Ordinal)
		}
	}
}

func TestPlanHashIsOrderIndependentAndContentSensitive(t *testing.T) {
	a := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/", "b/", "c/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	reordered := mustCompile(t, PlanRequest{
		Prefixes: []string{"c/", "a/", "b/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if a.PlanHash != reordered.PlanHash {
		t.Fatalf("hash changed under input reordering: %s vs %s", a.PlanHash, reordered.PlanHash)
	}
	if a.PlanHash == "" {
		t.Fatal("plan hash is empty")
	}

	different := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/", "b/", "d/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if a.PlanHash == different.PlanHash {
		t.Fatal("hash did not change when a prefix changed")
	}

	// Coverage is part of the plan's meaning, so it must be part of its identity:
	// a partial sweep must not resume against a complete one's digest.
	partial := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/", "b/", "c/"},
		Coverage: CoveragePartial, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if a.PlanHash == partial.PlanHash {
		t.Fatal("hash did not change when coverage declaration changed")
	}

	// Lane count changes the work assignment, so it must change the digest.
	bounded := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/", "b/", "c/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
		MaxLanes: 2,
	})
	if a.PlanHash == bounded.PlanHash {
		t.Fatal("hash did not change when lane assignment changed")
	}
}

// Subsumed is observational: it explains why a lane count is lower than the
// operator expected. It must not participate in the digest, because two plans
// that enumerate the same objects on the same lanes are the same plan for resume
// purposes regardless of how the caller happened to spell the input.
func TestPlanHashExcludesObservationalSubsumedField(t *testing.T) {
	direct := mustCompile(t, PlanRequest{
		Prefixes: []string{"site/a/", "site/b/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	viaSubsumption := mustCompile(t, PlanRequest{
		Prefixes: []string{"site/a/", "site/a/2026/", "site/b/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})

	if len(direct.Subsumed) != 0 {
		t.Fatalf("expected no subsumption in direct plan, got %+v", direct.Subsumed)
	}
	if len(viaSubsumption.Subsumed) == 0 {
		t.Fatal("expected subsumption to be reported")
	}
	if direct.PlanHash != viaSubsumption.PlanHash {
		t.Fatalf("observational Subsumed field changed the digest: %s vs %s",
			direct.PlanHash, viaSubsumption.PlanHash)
	}
}

func TestPlanVersionIsHashed(t *testing.T) {
	plan := mustCompile(t, PlanRequest{
		Prefixes: []string{"a/"},
		Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: testConfigFingerprint,
	})
	if plan.Version != PlanVersion {
		t.Fatalf("plan version %d, want %d", plan.Version, PlanVersion)
	}

	bumped := *plan
	bumped.Version = PlanVersion + 1
	rehashed, err := hashPlan(&bumped)
	if err != nil {
		t.Fatalf("hashPlan: %v", err)
	}
	if rehashed == plan.PlanHash {
		t.Fatal("payload version is not covered by the digest; a resume could read an encoding change as no change")
	}
}

// The digest must cover source identity and the behavior-affecting
// configuration fingerprint. Without them two runs against different providers,
// buckets, or configurations produce the same plan hash and could resume
// against each other.
func TestPlanHashCoversIdentityAndConfig(t *testing.T) {
	base := mustCompile(t, PlanRequest{
		Prefixes:          []string{"a/", "b/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: testConfigFingerprint,
	})

	otherSource := mustCompile(t, PlanRequest{
		Prefixes:          []string{"a/", "b/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      "s3://other-bucket/base/",
		ConfigFingerprint: testConfigFingerprint,
	})
	if base.PlanHash == otherSource.PlanHash {
		t.Fatal("identical prefixes under a different source produced the same digest")
	}

	otherConfig := mustCompile(t, PlanRequest{
		Prefixes:          []string{"a/", "b/"},
		Coverage:          CoverageComplete,
		BaseIdentity:      testBaseIdentity,
		ConfigFingerprint: "cfg-2",
	})
	if base.PlanHash == otherConfig.PlanHash {
		t.Fatal("a changed configuration fingerprint did not change the digest")
	}

	if base.BaseIdentity != testBaseIdentity || base.ConfigFingerprint != testConfigFingerprint {
		t.Fatalf("plan did not retain identity inputs: %q / %q", base.BaseIdentity, base.ConfigFingerprint)
	}
}

func TestCompilePlanRequiresIdentityInputs(t *testing.T) {
	cases := []struct {
		name    string
		req     PlanRequest
		wantSub string
	}{
		{
			name:    "missing base identity",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: CoverageComplete, ConfigFingerprint: testConfigFingerprint},
			wantSub: "source base identity",
		},
		{
			name:    "whitespace base identity",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: CoverageComplete, BaseIdentity: "  ", ConfigFingerprint: testConfigFingerprint},
			wantSub: "source base identity",
		},
		{
			name:    "missing config fingerprint",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity},
			wantSub: "configuration fingerprint",
		},
		{
			name:    "whitespace config fingerprint",
			req:     PlanRequest{Prefixes: []string{"a/"}, Coverage: CoverageComplete, BaseIdentity: testBaseIdentity, ConfigFingerprint: "\t"},
			wantSub: "configuration fingerprint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := CompilePlan(tc.req)
			if err == nil {
				t.Fatalf("expected refusal, got plan %+v", plan)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}
