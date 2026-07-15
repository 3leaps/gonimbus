package reflowthroughput

import "testing"

func TestCheckHonesty(t *testing.T) {
	t.Parallel()
	ok := CheckHonesty(ParsedReflowOutput{
		Requested: 32, Effective: 32, Reason: "requested", MaxActive: 16,
	}, 32)
	if !ok.OK {
		t.Fatal(ok.Message)
	}
	// Wrong reason when effective==requested must fail.
	badReason := CheckHonesty(ParsedReflowOutput{
		Requested: 32, Effective: 32, Reason: "not-requested", MaxActive: 16,
	}, 32)
	if badReason.OK {
		t.Fatal("expected failure for non-requested reason")
	}
	clamp := CheckHonesty(ParsedReflowOutput{
		Requested: 256, Effective: 16, Reason: "resource_capped:memory:conservative_default",
		MaxActive: 16, WarningClampCount: 1, ClampWarningOK: true,
	}, 256)
	if !clamp.OK {
		t.Fatal(clamp.Message)
	}
	// Clamp without warning must fail.
	noWarn := CheckHonesty(ParsedReflowOutput{
		Requested: 256, Effective: 16, Reason: "resource_capped:memory:conservative_default",
		MaxActive: 16, WarningClampCount: 0,
	}, 256)
	if noWarn.OK {
		t.Fatal("expected failure without clamp warning")
	}
	bad := CheckHonesty(ParsedReflowOutput{
		Requested: 8, Effective: 16, Reason: "requested", MaxActive: 4,
	}, 8)
	if bad.OK {
		t.Fatal("expected failure when effective > requested")
	}
}

func TestCheckOccupancy(t *testing.T) {
	t.Parallel()
	occ := CheckOccupancy([]int{7, 5, 8}, 8, 2)
	if !occ.OK {
		t.Fatal(occ.Message)
	}
	if occ.Floor != 6 {
		t.Fatalf("floor=%d", occ.Floor)
	}
	fail := CheckOccupancy([]int{1, 2, 3}, 8, 2)
	if fail.OK {
		t.Fatal("expected occupancy fail")
	}
}

func TestCheckCounts(t *testing.T) {
	t.Parallel()
	if err := CheckCounts(10, 0, 10, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := CheckCounts(10, 10, 10, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := CheckCounts(10, 9, 10, 0, 0); err == nil {
		t.Fatal("expected tap mismatch")
	}
}

func TestResolveProfile(t *testing.T) {
	t.Parallel()
	p, err := ResolveProfile("")
	if err != nil || p.Name != ProfileSmoke {
		t.Fatalf("default: %+v %v", p, err)
	}
	if _, err := ResolveProfile("not-a-profile"); err == nil {
		t.Fatal("expected unknown profile error")
	}
	sat, err := ResolveProfile(ProfileReflowSaturation)
	if err != nil || !sat.RequireOccupancy {
		t.Fatalf("sat: %+v %v", sat, err)
	}
	ps, err := ResolveProfile(ProfileProbeSaturation)
	if err != nil || ps.ExecutionShape != "probe_drain" {
		t.Fatalf("probe sat shape: %+v %v", ps, err)
	}
}

func TestChildEnvGOMEMLIMIT(t *testing.T) {
	t.Parallel()
	base := []string{"PATH=/bin", "GOMEMLIMIT=1GiB", "HOME=/tmp"}
	got := ChildEnv(base, "")
	for _, kv := range got {
		if len(kv) >= 11 && kv[:11] == "GOMEMLIMIT=" {
			t.Fatalf("ambient GOMEMLIMIT should be stripped: %v", got)
		}
	}
	got2 := ChildEnv(base, "4GiB")
	found := false
	for _, kv := range got2 {
		if kv == "GOMEMLIMIT=4GiB" {
			found = true
		}
		if kv == "GOMEMLIMIT=1GiB" {
			t.Fatal("old GOMEMLIMIT should be replaced")
		}
	}
	if !found {
		t.Fatalf("missing new GOMEMLIMIT: %v", got2)
	}
}
