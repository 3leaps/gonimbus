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
	scale, err := ResolveProfile(ProfileCheckpointScale)
	if err != nil {
		t.Fatalf("checkpoint-scale: %v", err)
	}
	if scale.Recipe.ObjectCount != DefaultScaleObjects {
		t.Fatalf("checkpoint-scale object count = %d, want %d", scale.Recipe.ObjectCount, DefaultScaleObjects)
	}
	if !scale.NoAdaptive {
		t.Fatalf("checkpoint-scale must run fixed-mode: %+v", scale)
	}
	if !hasTmpfs(scale) {
		t.Fatal("checkpoint-scale must declare a tmpfs class")
	}
}

func TestApplyRecipeOverrides(t *testing.T) {
	t.Parallel()
	base := ScaleRecipe()

	// Zero overrides keep the profile default.
	got := applyRecipeOverrides(base, Options{})
	if got != base {
		t.Fatalf("zero overrides changed recipe: %+v", got)
	}

	// Non-zero overrides win; unset fields stay at the profile default.
	got = applyRecipeOverrides(base, Options{RecipeObjectCount: 100000, RecipePartitions: 32})
	if got.ObjectCount != 100000 || got.Partitions != 32 {
		t.Fatalf("overrides not applied: %+v", got)
	}
	if got.SizeBytes != base.SizeBytes {
		t.Fatalf("unset override should keep default size: got %d want %d", got.SizeBytes, base.SizeBytes)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("scaled recipe should validate: %v", err)
	}
}

func TestRecipeValidateBounds(t *testing.T) {
	t.Parallel()
	// Absurd overrides fail closed rather than materializing an oversized corpus.
	for name, mutate := range map[string]func(Recipe) Recipe{
		"object_count too high": func(r Recipe) Recipe { r.ObjectCount = MaxRecipeObjectCount + 1; return r },
		"size_bytes too high":   func(r Recipe) Recipe { r.SizeBytes = MaxRecipeSizeBytes + 1; return r },
		"partitions too high":   func(r Recipe) Recipe { r.Partitions = MaxRecipePartitions + 1; return r },
	} {
		t.Run(name, func(t *testing.T) {
			if err := mutate(ScaleRecipe()).Validate(); err == nil {
				t.Fatalf("%s: expected validation error", name)
			}
		})
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
