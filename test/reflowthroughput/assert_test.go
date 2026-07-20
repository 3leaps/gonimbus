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

// TestResolveRecipe covers the resolved override path (DR-A3-F1): negative
// overrides fail closed rather than silently reverting to the profile default,
// zero keeps the default, and positive values apply and validate.
func TestResolveRecipe(t *testing.T) {
	t.Parallel()
	base := ScaleRecipe()

	// Every negative override, through the resolved path, is rejected — never
	// treated as unset. A reviewer negative control at exact head reached the
	// binary-path check instead of a rejection.
	for name, opts := range map[string]Options{
		"negative object_count": {RecipeObjectCount: -1},
		"negative size_bytes":   {RecipeSizeBytes: -1},
		"negative partitions":   {RecipePartitions: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveRecipe(base, opts); err == nil {
				t.Fatalf("%s: expected rejection, got nil", name)
			}
		})
	}

	// Zero keeps the default.
	got, err := resolveRecipe(base, Options{})
	if err != nil || got != base {
		t.Fatalf("zero overrides: recipe=%+v err=%v", got, err)
	}

	// Positive overrides apply and validate.
	got, err = resolveRecipe(base, Options{RecipeObjectCount: 1000, RecipeSizeBytes: 128, RecipePartitions: 3})
	if err != nil {
		t.Fatalf("positive overrides: %v", err)
	}
	if got.ObjectCount != 1000 || got.SizeBytes != 128 || got.Partitions != 3 {
		t.Fatalf("positive overrides not applied: %+v", got)
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

	// DR-A3-F3: each dimension individually legal but the aggregate corpus is
	// not — must fail closed before materialization.
	aggregate := ScaleRecipe()
	aggregate.ObjectCount = MaxRecipeObjectCount // legal
	aggregate.SizeBytes = MaxRecipeSizeBytes     // legal
	if err := aggregate.Validate(); err == nil {
		t.Fatal("aggregate corpus over the byte cap must be rejected")
	}

	// Boundary-accept: a corpus exactly at the aggregate cap validates.
	boundary := ScaleRecipe()
	boundary.SizeBytes = 1 << 20                                // 1 MiB objects
	boundary.ObjectCount = int(MaxTotalCorpusBytes / (1 << 20)) // exactly the cap
	boundary.Partitions = 1
	if int64(boundary.ObjectCount)*int64(boundary.SizeBytes) != MaxTotalCorpusBytes {
		t.Fatalf("boundary setup wrong: %d", int64(boundary.ObjectCount)*int64(boundary.SizeBytes))
	}
	if err := boundary.Validate(); err != nil {
		t.Fatalf("corpus exactly at the aggregate cap should validate: %v", err)
	}
}

// TestManifestRecordsResolvedCorpus covers DR-A3-F2: the resolved size_bytes and
// partitions must reach the sanitized report so a reader can name the corpus
// shape that produced the evidence — partitions in particular cannot be
// reconstructed from the entry digest. Mutating each knob moves the recorded value.
func TestManifestRecordsResolvedCorpus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		size       int
		partitions int
		count      int
	}{
		{"partitions=3", 200, 3, 6},
		{"partitions=1 size=512", 512, 1, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := DefaultSmokeRecipe()
			r.ObjectCount = tc.count
			r.SizeBytes = tc.size
			r.Partitions = tc.partitions
			corpus, err := Generate(GenerateOptions{Recipe: r, RunRoot: t.TempDir()})
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			c := corpus.Manifest.Compact()
			if c.Partitions != tc.partitions {
				t.Fatalf("report partitions = %d, want %d", c.Partitions, tc.partitions)
			}
			if c.SizeBytes != tc.size {
				t.Fatalf("report size_bytes = %d, want %d", c.SizeBytes, tc.size)
			}
			if c.ObjectCount != tc.count {
				t.Fatalf("report object_count = %d, want %d", c.ObjectCount, tc.count)
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
