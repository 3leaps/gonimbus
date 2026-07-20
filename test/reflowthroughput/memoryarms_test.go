package reflowthroughput

import (
	"strings"
	"testing"
)

// memoryRecords renders a run+summary pair carrying the memory resolution
// fields, so parse/report behavior can be exercised without a child process.
func memoryRecords(runLimitSource, summaryLimitSource, runBudgetSource, summaryBudgetSource string) string {
	run := `{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":8,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":0,"concurrency_time_avg_active":0,"memory_limit_bytes":1073741824,"memory_limit_source":"` + runLimitSource + `","memory_budget_effective_bytes":268435456,"memory_budget_source":"` + runBudgetSource + `","retry_buffer_cap_bytes":16777216}}`
	summary := `{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":4,"concurrency_time_avg_active":2.5,"memory_limit_bytes":1073741824,"memory_limit_source":"` + summaryLimitSource + `","memory_budget_effective_bytes":268435456,"memory_budget_source":"` + summaryBudgetSource + `","retry_buffer_cap_bytes":16777216,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`
	return strings.Join([]string{run, summary}, "\n") + "\n"
}

func TestParseCapturesMemoryResolution(t *testing.T) {
	t.Parallel()
	p, err := ParseReflowStdout([]byte(memoryRecords("physical_ram", "physical_ram", "derived", "derived")))
	if err != nil {
		t.Fatal(err)
	}
	if p.MemoryLimitSource != "physical_ram" {
		t.Fatalf("limit source %q", p.MemoryLimitSource)
	}
	if p.MemoryBudgetSource != "derived" {
		t.Fatalf("budget source %q", p.MemoryBudgetSource)
	}
	if p.MemoryLimitBytes != 1073741824 || p.MemoryBudgetEffectiveBytes != 268435456 || p.RetryBufferCapBytes != 16777216 {
		t.Fatalf("memory arithmetic %+v", p)
	}
	// The summary carries the completed-run occupancy; the run's startup
	// sample is zero by contract and must not be the reported value.
	if p.SummaryTimeAvgActive != 2.5 {
		t.Fatalf("time avg %v", p.SummaryTimeAvgActive)
	}
}

func TestParseRejectsMemoryProvenanceMismatch(t *testing.T) {
	t.Parallel()
	if _, err := ParseReflowStdout([]byte(memoryRecords("physical_ram", "runtime", "derived", "derived"))); err == nil {
		t.Fatal("expected memory_limit_source mismatch to fail")
	}
	if _, err := ParseReflowStdout([]byte(memoryRecords("runtime", "runtime", "derived", "operator"))); err == nil {
		t.Fatal("expected memory_budget_source mismatch to fail")
	}
}

func TestCeilingLiftDeclaresThreeMemoryArms(t *testing.T) {
	t.Parallel()
	spec, err := ResolveProfile(ProfileCeilingLift)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{MemoryArmGOMEMLIMIT, MemoryArmProbeBound, MemoryArmOperatorBudget}
	if len(spec.MemoryArms) != len(want) {
		t.Fatalf("arms %+v", spec.MemoryArms)
	}
	for i, label := range want {
		if spec.MemoryArms[i].Label != label {
			t.Fatalf("arm %d = %q want %q", i, spec.MemoryArms[i].Label, label)
		}
	}
	// The probe-bound arm is the one that must carry no operator override:
	// under minimum-selection it is bound by the product's own detection.
	if spec.MemoryArms[1].UseGOMEMLIMIT || spec.MemoryArms[1].UseMemoryBudget {
		t.Fatalf("probe-bound arm must take no override: %+v", spec.MemoryArms[1])
	}
}

func TestRequireMemoryArmInputs(t *testing.T) {
	t.Parallel()
	spec, err := ResolveProfile(ProfileCeilingLift)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireMemoryArmInputs(spec, Options{}); err == nil {
		t.Fatal("expected missing GOMEMLIMIT to be refused")
	}
	if err := requireMemoryArmInputs(spec, Options{GOMEMLIMIT: "2GiB"}); err == nil {
		t.Fatal("expected missing memory budget to be refused")
	}
	if err := requireMemoryArmInputs(spec, Options{GOMEMLIMIT: "2GiB", MemoryBudget: "8GiB"}); err != nil {
		t.Fatalf("fully supplied: %v", err)
	}
	// The legacy spelling still supplies the constraining envelope.
	if err := requireMemoryArmInputs(spec, Options{ConstrainedGOMEMLIMIT: "2GiB", MemoryBudget: "8GiB"}); err != nil {
		t.Fatalf("constrained spelling: %v", err)
	}
	// Profiles with no declared arms need nothing.
	smoke, err := ResolveProfile(ProfileSmoke)
	if err != nil {
		t.Fatal(err)
	}
	if err := requireMemoryArmInputs(smoke, Options{}); err != nil {
		t.Fatalf("smoke: %v", err)
	}
}

func TestResolveMemoryArmsBindsOperatorValues(t *testing.T) {
	t.Parallel()
	spec, err := ResolveProfile(ProfileCeilingLift)
	if err != nil {
		t.Fatal(err)
	}
	arms := resolveMemoryArms(spec, Options{ConstrainedGOMEMLIMIT: "2GiB", MemoryBudget: "8GiB"})
	if len(arms) != 3 {
		t.Fatalf("arms %+v", arms)
	}
	if arms[0].GOMEMLIMIT != "2GiB" || arms[0].MemoryBudget != "" {
		t.Fatalf("constrained arm %+v", arms[0])
	}
	if arms[1].GOMEMLIMIT != "" || arms[1].MemoryBudget != "" {
		t.Fatalf("probe-bound arm %+v", arms[1])
	}
	if arms[2].MemoryBudget != "8GiB" || arms[2].GOMEMLIMIT != "" {
		t.Fatalf("budget arm %+v", arms[2])
	}
	// A profile with no arms still yields exactly one unlabeled run.
	smoke, err := ResolveProfile(ProfileSmoke)
	if err != nil {
		t.Fatal(err)
	}
	single := resolveMemoryArms(smoke, Options{GOMEMLIMIT: "2GiB"})
	if len(single) != 1 || single[0].Label != "" || single[0].GOMEMLIMIT != "" {
		t.Fatalf("single arm %+v", single)
	}
}

// A report must not be able to claim an envelope the child did not run under.
func TestValidateReportRejectsMislabeledMemoryEnvelope(t *testing.T) {
	t.Parallel()
	base := func(p PointReport) Report {
		r := NewReport(ProfileCeilingLift, "file", "inv", "sha", CompactManifest{Digest: "corpus-digest"}, false)
		r.Points = []PointReport{p}
		return r
	}
	cases := []struct {
		name  string
		point PointReport
	}{
		{"gomemlimit arm without GOMEMLIMIT", PointReport{
			ExecutionShape: "reflow_only", MemoryEnvelope: MemoryArmGOMEMLIMIT,
		}},
		{"probe-bound arm with an override", PointReport{
			ExecutionShape: "reflow_only", MemoryEnvelope: MemoryArmProbeBound,
			GOMEMLIMITSet: true, GOMEMLIMITValue: "2GiB",
		}},
		{"probe-bound arm reporting an operator budget", PointReport{
			ExecutionShape: "reflow_only", MemoryEnvelope: MemoryArmProbeBound,
			MemoryBudgetSource: "operator",
		}},
		{"budget arm without a budget", PointReport{
			ExecutionShape: "reflow_only", MemoryEnvelope: MemoryArmOperatorBudget,
			MemoryBudgetSource: "operator",
		}},
		{"budget arm the product did not honor", PointReport{
			ExecutionShape: "reflow_only", MemoryEnvelope: MemoryArmOperatorBudget,
			MemoryBudgetRequested: "8GiB", MemoryBudgetSource: "derived",
		}},
		{"unknown envelope", PointReport{
			ExecutionShape: "reflow_only", MemoryEnvelope: "raised",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateReportEnvelope(base(tc.point)); err == nil {
				t.Fatalf("expected rejection for %s", tc.name)
			}
		})
	}

	ok := base(PointReport{
		ExecutionShape: "reflow_only", MemoryEnvelope: MemoryArmOperatorBudget,
		MemoryBudgetRequested: "8GiB", MemoryBudgetSource: "operator_clamped_to_limit",
	})
	if err := ValidateReportEnvelope(ok); err != nil {
		t.Fatalf("honored budget arm: %v", err)
	}
}
