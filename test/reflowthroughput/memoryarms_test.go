package reflowthroughput

import (
	"strconv"
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

// memoryRecordsTuple renders a run+summary pair whose numeric memory tuples
// can differ, to pin the startup-fixed agreement check.
func memoryRecordsTuple(runLimit, runBudget, runCap, sumLimit, sumBudget, sumCap int64) string {
	run := `{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":8,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":0,"memory_limit_bytes":` + i64s(runLimit) + `,"memory_limit_source":"physical_ram","memory_budget_effective_bytes":` + i64s(runBudget) + `,"memory_budget_source":"derived","retry_buffer_cap_bytes":` + i64s(runCap) + `}}`
	summary := `{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":4,"memory_limit_bytes":` + i64s(sumLimit) + `,"memory_limit_source":"physical_ram","memory_budget_effective_bytes":` + i64s(sumBudget) + `,"memory_budget_source":"derived","retry_buffer_cap_bytes":` + i64s(sumCap) + `,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`
	return strings.Join([]string{run, summary}, "\n") + "\n"
}

func i64s(v int64) string { return strconv.FormatInt(v, 10) }

// The startup-fixed tuple must agree field by field, not just by source label:
// matching sources over differing arithmetic would publish the run's numbers as
// though both records had agreed.
func TestParseRejectsMemoryTupleMismatch(t *testing.T) {
	t.Parallel()
	const gib, mib = int64(1) << 30, int64(1) << 20
	if _, err := ParseReflowStdout([]byte(memoryRecordsTuple(gib, 256*mib, 16*mib, gib, 256*mib, 16*mib))); err != nil {
		t.Fatalf("identical tuples: %v", err)
	}
	cases := map[string]string{
		"limit":  memoryRecordsTuple(gib, 256*mib, 16*mib, 2*gib, 256*mib, 16*mib),
		"budget": memoryRecordsTuple(gib, 256*mib, 16*mib, gib, 128*mib, 16*mib),
		"cap":    memoryRecordsTuple(gib, 256*mib, 16*mib, gib, 256*mib, 8*mib),
		// One case per compared numeric field, so the coverage claim is literal.
		"requested budget": strings.Replace(
			memoryRecordsTuple(gib, 256*mib, 16*mib, gib, 256*mib, 16*mib),
			`"memory_budget_effective_bytes":268435456,"memory_budget_source":"derived","retry_buffer_cap_bytes":16777216,"dest_ifabsent_honored"`,
			`"memory_budget_requested_bytes":134217728,"memory_budget_effective_bytes":268435456,"memory_budget_source":"derived","retry_buffer_cap_bytes":16777216,"dest_ifabsent_honored"`,
			1),
	}
	for name, stdout := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseReflowStdout([]byte(stdout)); err == nil {
				t.Fatalf("expected %s mismatch to fail", name)
			}
		})
	}
}

// A profile without declared arms must still run the envelope the operator
// supplied; silently substituting another one is the same evidence failure the
// labeled arms exist to prevent.
func TestSingletonArmKeepsSuppliedControls(t *testing.T) {
	t.Parallel()
	for _, profile := range []string{ProfileSmoke, ProfileReflowSaturation, ProfileFullPipe} {
		t.Run(profile, func(t *testing.T) {
			spec, err := ResolveProfile(profile)
			if err != nil {
				t.Fatal(err)
			}
			arms := resolveMemoryArms(spec, Options{GOMEMLIMIT: "64MiB", MemoryBudget: "128MiB"})
			if len(arms) != 1 {
				t.Fatalf("arms %+v", arms)
			}
			if arms[0].GOMEMLIMIT != "64MiB" {
				t.Fatalf("dropped GOMEMLIMIT: %+v", arms[0])
			}
			if arms[0].MemoryBudget != "128MiB" {
				t.Fatalf("dropped memory budget: %+v", arms[0])
			}
			if arms[0].Label != "" {
				t.Fatalf("singleton must stay unlabeled: %+v", arms[0])
			}
		})
	}
}

// The supplied budget must reach the child on the full-pipe path too.
func TestFullPipeOptsCarryMemoryBudgetToChild(t *testing.T) {
	t.Parallel()
	spec, err := ResolveProfile(ProfileFullPipe)
	if err != nil {
		t.Fatal(err)
	}
	arm := resolveMemoryArms(spec, Options{MemoryBudget: "128MiB"})[0]
	opts := FullPipeOpts{MemoryBudget: arm.MemoryBudget}
	if opts.MemoryBudget != "128MiB" {
		t.Fatalf("full-pipe budget %q", opts.MemoryBudget)
	}
}

// completeMatrix renders exactly the cells a profile declares.
func completeMatrix(spec ProfileSpec) []PointReport {
	classes := spec.CheckpointClasses
	if len(classes) == 0 {
		classes = []string{"disk"}
	}
	var out []PointReport
	for _, arm := range spec.MemoryArms {
		for _, parallel := range spec.ParallelPoints {
			for _, class := range classes {
				out = append(out, PointReport{
					ExecutionShape:  spec.ExecutionShape,
					MemoryEnvelope:  arm.Label,
					Parallel:        parallel,
					CheckpointClass: class,
				})
			}
		}
	}
	return out
}

func TestValidateArmMatrixPinsDeclaredCells(t *testing.T) {
	t.Parallel()
	for _, profile := range []string{ProfileCeilingLift, ProfileCheckpoint} {
		t.Run(profile, func(t *testing.T) {
			spec, err := ResolveProfile(profile)
			if err != nil {
				t.Fatal(err)
			}
			full := completeMatrix(spec)
			if err := ValidateArmMatrix(spec, Report{Points: full}); err != nil {
				t.Fatalf("complete matrix: %v", err)
			}

			clone := func(mutate func([]PointReport) []PointReport) Report {
				cp := append([]PointReport{}, full...)
				return Report{Points: mutate(cp)}
			}
			// Every mutation below preserves per-arm cardinality, so a
			// count-only gate accepts all of them.
			cases := map[string]Report{
				"duplicate one cell, miss another": clone(func(p []PointReport) []PointReport {
					p[1] = p[0]
					return p
				}),
				"all points at one parallel": clone(func(p []PointReport) []PointReport {
					for i := range p {
						p[i].Parallel = spec.ParallelPoints[0]
					}
					return p
				}),
				"wrong checkpoint class": clone(func(p []PointReport) []PointReport {
					for i := range p {
						p[i].CheckpointClass = "tmpfs-not-declared"
					}
					return p
				}),
				"wrong execution shape": clone(func(p []PointReport) []PointReport {
					for i := range p {
						p[i].ExecutionShape = "full_pipe"
					}
					return p
				}),
				"undeclared arm": clone(func(p []PointReport) []PointReport {
					p[0].MemoryEnvelope = "raised"
					return p
				}),
				"missing a whole arm": clone(func(p []PointReport) []PointReport {
					return p[:len(p)-len(spec.ParallelPoints)]
				}),
				// A probe_drain point is structurally valid on its own, so
				// envelope validation accepts it; the matrix must still reject
				// it as a shape this profile never declared.
				"extra undeclared probe_drain point": clone(func(p []PointReport) []PointReport {
					return append(p, PointReport{ExecutionShape: "probe_drain", ProbeConcurrency: 4})
				}),
			}
			for name, r := range cases {
				t.Run(name, func(t *testing.T) {
					if err := ValidateArmMatrix(spec, r); err == nil {
						t.Fatalf("expected %s to be rejected", name)
					}
				})
			}

			// The publication gates run together, so pin the combination the
			// panel reproduced: a spliced probe_drain point passes envelope
			// validation on its own merits and must be caught by the matrix.
			t.Run("spliced probe_drain passes envelope but fails matrix", func(t *testing.T) {
				spliced := NewReport(spec.Name, "file", "inv", "sha", CompactManifest{Digest: "corpus-digest"}, false)
				for _, p := range full {
					resolved := resolvedPoint(p.MemoryEnvelope)
					resolved.Parallel, resolved.CheckpointClass, resolved.ExecutionShape = p.Parallel, p.CheckpointClass, p.ExecutionShape
					if p.MemoryEnvelope == MemoryArmGOMEMLIMIT {
						resolved.GOMEMLIMITSet, resolved.GOMEMLIMITValue = true, "1GiB"
						resolved.MemoryLimitSource = memorySourceRuntime
					}
					if p.MemoryEnvelope == MemoryArmOperatorBudget {
						resolved.MemoryBudgetRequested = "8GiB"
						resolved.MemoryBudgetSource = memoryBudgetSourceOperator
					}
					spliced.Points = append(spliced.Points, resolved)
				}
				if err := ValidateReportEnvelope(spliced); err != nil {
					t.Fatalf("complete matrix must pass envelope validation: %v", err)
				}
				spliced.Points = append(spliced.Points, PointReport{ExecutionShape: "probe_drain", ProbeConcurrency: 4})
				if err := ValidateReportEnvelope(spliced); err != nil {
					t.Fatalf("envelope validation is expected to accept the spliced point: %v", err)
				}
				if err := ValidateArmMatrix(spec, spliced); err == nil {
					t.Fatal("expected the matrix to reject the undeclared probe_drain point")
				}
			})
		})
	}

	// Profiles without declared arms are not matrix-constrained: they
	// legitimately append points the matrix does not describe.
	smoke, err := ResolveProfile(ProfileSmoke)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateArmMatrix(smoke, Report{Points: []PointReport{{ExecutionShape: "reflow_only"}}}); err != nil {
		t.Fatalf("smoke: %v", err)
	}

	// A declared-arm profile that repeats cells by design is refused rather
	// than validated under a rule that does not describe it.
	fullPipeWithArms, err := ResolveProfile(ProfileFullPipe)
	if err != nil {
		t.Fatal(err)
	}
	fullPipeWithArms.MemoryArms = []MemoryArm{{Label: MemoryArmProbeBound}}
	if err := ValidateArmMatrix(fullPipeWithArms, Report{}); err == nil {
		t.Fatal("expected a declared-arm full-pipe profile to be refused")
	}
}

// Baseline provenance integrity applies to unlabeled singleton points too:
// they declare no envelope, but they still executed under a resolved one.
func TestValidateReportRejectsUnlabeledPlaceholderProvenance(t *testing.T) {
	t.Parallel()
	valid := func() PointReport {
		p := resolvedPoint("")
		p.Parallel = 2
		p.CheckpointClass = "disk"
		return p
	}
	cases := map[string]PointReport{
		"placeholder limit source": func() PointReport {
			p := valid()
			p.MemoryLimitSource = "unknown/not_reported"
			return p
		}(),
		"absent limit source": func() PointReport {
			p := valid()
			p.MemoryLimitSource = ""
			return p
		}(),
		"unrecognized limit source": func() PointReport {
			p := valid()
			p.MemoryLimitSource = "invented"
			return p
		}(),
		"absent budget source": func() PointReport {
			p := valid()
			p.MemoryBudgetSource = ""
			return p
		}(),
		"zero limit bytes": func() PointReport {
			p := valid()
			p.MemoryLimitBytes = 0
			return p
		}(),
		"zero effective budget": func() PointReport {
			p := valid()
			p.MemoryBudgetEffectiveBytes = 0
			return p
		}(),
		"zero retry cap": func() PointReport {
			p := valid()
			p.RetryBufferCapBytes = 0
			return p
		}(),
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			if err := ValidateReportEnvelope(reportWith(p)); err == nil {
				t.Fatalf("expected %s to be rejected on an unlabeled point", name)
			}
		})
	}

	// Valid unlabeled shapes stay accepted, including an honored operator
	// budget supplied to a profile with no declared arms.
	operator := valid()
	operator.MemoryBudgetRequested = "128MiB"
	operator.MemoryBudgetSource = memoryBudgetSourceOperator
	for _, p := range []PointReport{valid(), operator} {
		if err := ValidateReportEnvelope(reportWith(p)); err != nil {
			t.Fatalf("valid unlabeled point (budget source %s): %v", p.MemoryBudgetSource, err)
		}
	}

	// probe_drain stays exempt: it runs no reflow, so it carries none of
	// these fields and must not be held to them.
	if err := ValidateReportEnvelope(reportWith(PointReport{ExecutionShape: "probe_drain", ProbeConcurrency: 4})); err != nil {
		t.Fatalf("probe_drain exemption: %v", err)
	}
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
	// A profile with no arms yields exactly one unlabeled run that still
	// carries the supplied envelope — see TestSingletonArmKeepsSuppliedControls.
	smoke, err := ResolveProfile(ProfileSmoke)
	if err != nil {
		t.Fatal(err)
	}
	single := resolveMemoryArms(smoke, Options{GOMEMLIMIT: "2GiB"})
	if len(single) != 1 || single[0].Label != "" || single[0].GOMEMLIMIT != "2GiB" {
		t.Fatalf("single arm %+v", single)
	}
}

// resolvedPoint is a well-formed labeled point; cases below mutate one facet so
// each rejection is attributable to that facet alone.
func resolvedPoint(envelope string) PointReport {
	return PointReport{
		ExecutionShape:             "reflow_only",
		MemoryEnvelope:             envelope,
		MemoryLimitSource:          memorySourcePhysicalRAM,
		MemoryBudgetSource:         memoryBudgetSourceDerived,
		MemoryLimitBytes:           1 << 30,
		MemoryBudgetEffectiveBytes: 256 << 20,
		RetryBufferCapBytes:        16 << 20,
	}
}

func reportWith(p PointReport) Report {
	r := NewReport(ProfileCeilingLift, "file", "inv", "sha", CompactManifest{Digest: "corpus-digest"}, false)
	r.Points = []PointReport{p}
	return r
}

// A report must not be able to claim an envelope the child did not run under —
// and, critically, an arm label must name the candidate that actually BOUND the
// run, not merely the lever that was passed in.
func TestValidateReportRejectsMislabeledMemoryEnvelope(t *testing.T) {
	t.Parallel()
	gomemBound := func() PointReport {
		p := resolvedPoint(MemoryArmGOMEMLIMIT)
		p.GOMEMLIMITSet, p.GOMEMLIMITValue = true, "2GiB"
		p.MemoryLimitSource = memorySourceRuntime
		return p
	}
	budgetHonored := func() PointReport {
		p := resolvedPoint(MemoryArmOperatorBudget)
		p.MemoryBudgetRequested = "8GiB"
		p.MemoryBudgetSource = memoryBudgetSourceOperator
		return p
	}

	cases := []struct {
		name  string
		point PointReport
	}{
		{"gomemlimit arm without GOMEMLIMIT", func() PointReport {
			p := resolvedPoint(MemoryArmGOMEMLIMIT)
			p.MemoryLimitSource = memorySourceRuntime
			return p
		}()},
		// The finding the seats reproduced: a GOMEMLIMIT above a lower
		// candidate is injected but never binds, so the arm is not constrained
		// by it and must not be published as such.
		{"gomemlimit supplied but did not bind", func() PointReport {
			p := gomemBound()
			p.MemoryLimitSource = memorySourcePhysicalRAM
			return p
		}()},
		{"gomemlimit arm with a non-derived budget", func() PointReport {
			p := gomemBound()
			p.MemoryBudgetSource = memoryBudgetSourceOperator
			return p
		}()},
		{"probe-bound arm with an override", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.GOMEMLIMITSet, p.GOMEMLIMITValue = true, "2GiB"
			return p
		}()},
		// The symmetric false accept: nothing was passed, but a runtime bound
		// won, so the detection chain is not what bound this arm.
		{"probe-bound arm bound by the runtime limit", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.MemoryLimitSource = memorySourceRuntime
			return p
		}()},
		{"probe-bound arm reporting an operator budget", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.MemoryBudgetSource = memoryBudgetSourceOperator
			return p
		}()},
		{"budget arm without a budget", func() PointReport {
			p := budgetHonored()
			p.MemoryBudgetRequested = ""
			return p
		}()},
		{"budget arm the product did not honor", func() PointReport {
			p := budgetHonored()
			p.MemoryBudgetSource = memoryBudgetSourceDerived
			return p
		}()},
		{"placeholder limit provenance", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.MemoryLimitSource = "unknown/not_reported"
			return p
		}()},
		{"absent limit provenance", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.MemoryLimitSource = ""
			return p
		}()},
		{"absent budget provenance", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.MemoryBudgetSource = ""
			return p
		}()},
		{"non-positive resolved arithmetic", func() PointReport {
			p := resolvedPoint(MemoryArmProbeBound)
			p.RetryBufferCapBytes = 0
			return p
		}()},
		{"unknown envelope", resolvedPoint("raised")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateReportEnvelope(reportWith(tc.point)); err == nil {
				t.Fatalf("expected rejection for %s", tc.name)
			}
		})
	}
}

// The shapes the current measurement actually produces must stay accepted, so
// the strengthened gate does not require remeasurement.
func TestValidateReportAcceptsBoundMemoryEnvelopes(t *testing.T) {
	t.Parallel()
	gomem := resolvedPoint(MemoryArmGOMEMLIMIT)
	gomem.GOMEMLIMITSet, gomem.GOMEMLIMITValue = true, "1GiB"
	gomem.MemoryLimitSource = memorySourceRuntime

	cgroup := resolvedPoint(MemoryArmProbeBound)
	cgroup.MemoryLimitSource = memorySourceCgroupV2

	undetected := resolvedPoint(MemoryArmProbeBound)
	undetected.MemoryLimitSource = memorySourceDetectionUnavailable

	budget := resolvedPoint(MemoryArmOperatorBudget)
	budget.MemoryBudgetRequested = "8GiB"
	budget.MemoryBudgetSource = memoryBudgetSourceOperator

	clamped := resolvedPoint(MemoryArmOperatorBudget)
	clamped.MemoryBudgetRequested = "8GiB"
	clamped.MemoryBudgetSource = memoryBudgetSourceOperatorClamped

	for _, p := range []PointReport{gomem, resolvedPoint(MemoryArmProbeBound), cgroup, undetected, budget, clamped} {
		if err := ValidateReportEnvelope(reportWith(p)); err != nil {
			t.Fatalf("envelope %s limit=%s budget=%s: %v", p.MemoryEnvelope, p.MemoryLimitSource, p.MemoryBudgetSource, err)
		}
	}
}
