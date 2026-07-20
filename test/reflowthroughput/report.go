package reflowthroughput

import (
	"encoding/json"
	"fmt"
	"time"
)

// ReportSchemaVersion is the test-tool report schema (not a gonimbus.* product type).
const ReportSchemaVersion = "reflowthroughput.report.v1"

// Measurement-scope labels (public, product-safe — no private planning IDs).
const (
	MeasurementScopeSmallScaleRelative = "small_scale_relative"
	AbsoluteEnvelopeNotEvaluated       = "not_evaluated"
)

// Report is the sanitized, versioned measurement report.
// It must never include endpoints, credentials, bucket names, signed URLs,
// checkpoint paths, or local absolute paths.
type Report struct {
	SchemaVersion string `json:"schema_version"`

	BinaryVersion string `json:"binary_version,omitempty"`
	BinaryCommit  string `json:"binary_commit,omitempty"`
	BinarySHA256  string `json:"binary_sha256"`

	Corpus CompactManifest `json:"corpus"`

	Profile                 string `json:"profile"`
	ProviderClass           string `json:"provider_class"`
	ThroughputEvidenceClass string `json:"throughput_evidence_class"`
	// MeasurementScope declares this cut's intent (small-scale honesty/relative).
	MeasurementScope string `json:"measurement_scope"`
	// AbsoluteEnvelopeValidation is not_evaluated until an opt-in BYO-cloud
	// scale lane is used; avoids presenting local/moto as a throughput oracle.
	AbsoluteEnvelopeValidation string `json:"absolute_envelope_validation"`

	OS   string `json:"os"`
	Arch string `json:"arch"`

	InvocationID string    `json:"invocation_id"`
	StartedAt    time.Time `json:"started_at"`

	Keep bool `json:"keep"`

	Points []PointReport `json:"points"`
}

// PointReport is one measured arm with allowlisted aggregate fields only.
// Reflow concurrency fields are pointers so probe_drain omits them (no product
// reflow telemetry exists for that shape).
type PointReport struct {
	PointID          string `json:"point_id"`
	ExecutionShape   string `json:"execution_shape"` // reflow_only|full_pipe|probe_drain
	Parallel         int    `json:"reflow_parallel_requested,omitempty"`
	ProbeConcurrency int    `json:"probe_concurrency,omitempty"`
	// AdaptiveMode is reflow adaptive/fixed when reflow ran; empty for probe_drain.
	AdaptiveMode    string `json:"adaptive_mode,omitempty"`
	GOMEMLIMITSet   bool   `json:"gomemlimit_set"`
	GOMEMLIMITValue string `json:"gomemlimit_value,omitempty"`
	// MemoryBudgetRequested is the operator --memory-budget passed to the child
	// (empty when the arm let the product derive the budget).
	MemoryBudgetRequested string `json:"memory_budget_requested,omitempty"`
	// MemoryLimitSource is product memory-limit provenance when reflow ran; omit for probe_drain.
	MemoryLimitSource string `json:"memory_limit_source,omitempty"`
	// MemoryBudgetSource is product budget provenance: derived|operator|operator_clamped_to_limit.
	MemoryBudgetSource string `json:"memory_budget_source,omitempty"`
	// Resolved memory arithmetic as the product reported it (aggregate bytes only).
	MemoryLimitBytes           int64 `json:"memory_limit_bytes,omitempty"`
	MemoryBudgetEffectiveBytes int64 `json:"memory_budget_effective_bytes,omitempty"`
	RetryBufferCapBytes        int64 `json:"retry_buffer_cap_bytes,omitempty"`
	// MemoryEnvelope names which lever bound this arm's memory:
	// gomemlimit_constrained | probe_bound | operator_budget | "".
	MemoryEnvelope string `json:"memory_envelope,omitempty"`
	// ConcurrencyTimeAvgActive is the completed-run occupancy diagnostic.
	ConcurrencyTimeAvgActive float64 `json:"concurrency_time_avg_active,omitempty"`
	CheckpointClass          string  `json:"checkpoint_class,omitempty"`

	// Product reflow concurrency telemetry (omit when not applicable).
	ConcurrencyRequested *int    `json:"concurrency_ceiling_requested,omitempty"`
	ConcurrencyEffective *int    `json:"concurrency_ceiling_effective,omitempty"`
	ConcurrencyReason    *string `json:"concurrency_ceiling_reason,omitempty"`
	ConcurrencyMaxActive *int    `json:"concurrency_max_active,omitempty"`
	ConcurrencyFinal     *int    `json:"concurrency_final,omitempty"`
	AdaptiveEnabled      *bool   `json:"adaptive_enabled,omitempty"`

	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	CompletedObjects    int64   `json:"completed_objects"`
	EndToEndRate        float64 `json:"end_to_end_rate_objects_per_s,omitempty"`
	ProbeDeliveredRate  float64 `json:"probe_delivered_rate_objects_per_s,omitempty"`
	ProbeSaturationRate float64 `json:"probe_saturation_rate_objects_per_s,omitempty"`
	TapValidRows        int64   `json:"tap_valid_reflow_input_rows,omitempty"`
	// TapCopyIntervalSeconds is the wall time of the tap Copy loop (includes
	// producer pacing and downstream backpressure). Not calibrated overhead.
	TapCopyIntervalSeconds float64 `json:"tap_copy_interval_seconds,omitempty"`

	// HonestyOK is nil when honesty is not applicable (e.g. probe_drain).
	HonestyOK      *bool  `json:"honesty_ok,omitempty"`
	HonestyMessage string `json:"honesty_message,omitempty"`

	OccupancySamples []int  `json:"occupancy_samples,omitempty"`
	OccupancyOK      *bool  `json:"occupancy_ok,omitempty"`
	OccupancyMessage string `json:"occupancy_message,omitempty"`

	// ContentParityOK is true when landed key+size+content-digest multisets match.
	ContentParityOK *bool `json:"content_parity_ok,omitempty"`

	StageExitCodes map[string]int `json:"stage_exit_codes"`
}

// NewReport seeds a report shell.
func NewReport(profile, providerClass, invocationID, binarySHA string, corpus CompactManifest, keep bool) Report {
	evidence := "non_provider"
	switch providerClass {
	case "s3-compatible", "gcs":
		evidence = "provider_opt_in"
	}
	return Report{
		SchemaVersion:              ReportSchemaVersion,
		BinarySHA256:               binarySHA,
		Corpus:                     corpus,
		Profile:                    profile,
		ProviderClass:              providerClass,
		ThroughputEvidenceClass:    evidence,
		MeasurementScope:           MeasurementScopeSmallScaleRelative,
		AbsoluteEnvelopeValidation: AbsoluteEnvelopeNotEvaluated,
		InvocationID:               invocationID,
		StartedAt:                  monoNow().UTC(),
		Keep:                       keep,
		Points:                     nil,
	}
}

// MarshalJSONReport returns the report JSON.
func MarshalJSONReport(r Report) ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// SterilityForbidden returns tokens that must never appear in a serialized report.
func SterilityForbidden(extra ...string) []string {
	base := []string{
		"://",
		"aws_access",
		"aws_secret",
		"begin private",
		"authorization:",
		"x-amz-security-token",
		"signature=",
		"x-amz-credential",
	}
	return append(base, extra...)
}

// ValidateReportEnvelope performs light structural checks on the report itself.
func ValidateReportEnvelope(r Report) error {
	if r.SchemaVersion != ReportSchemaVersion {
		return fmt.Errorf("unexpected schema_version %q", r.SchemaVersion)
	}
	if r.BinarySHA256 == "" {
		return fmt.Errorf("binary_sha256 required")
	}
	if r.Corpus.Digest == "" {
		return fmt.Errorf("corpus digest required")
	}
	if r.Profile == "" {
		return fmt.Errorf("profile required")
	}
	if r.ThroughputEvidenceClass == "" {
		return fmt.Errorf("throughput_evidence_class required")
	}
	if r.MeasurementScope == "" {
		return fmt.Errorf("measurement_scope required")
	}
	if r.AbsoluteEnvelopeValidation == "" {
		return fmt.Errorf("absolute_envelope_validation required")
	}
	if r.ProviderClass == "file" || r.ProviderClass == "moto" {
		if r.ThroughputEvidenceClass != "non_provider" {
			return fmt.Errorf("local/moto must label throughput_evidence_class=non_provider")
		}
	}
	for i, p := range r.Points {
		if p.ExecutionShape == "probe_drain" {
			if p.ConcurrencyRequested != nil || p.ConcurrencyEffective != nil || p.ConcurrencyReason != nil ||
				p.ConcurrencyMaxActive != nil || p.ConcurrencyFinal != nil || p.AdaptiveEnabled != nil {
				return fmt.Errorf("point %d: probe_drain must omit all reflow concurrency fields", i)
			}
			if p.AdaptiveMode != "" {
				return fmt.Errorf("point %d: probe_drain must omit adaptive_mode", i)
			}
			if p.CheckpointClass != "" {
				return fmt.Errorf("point %d: probe_drain must omit checkpoint_class", i)
			}
			if p.MemoryLimitSource != "" {
				return fmt.Errorf("point %d: probe_drain must omit memory_limit_source", i)
			}
			if p.MemoryBudgetSource != "" || p.MemoryBudgetEffectiveBytes != 0 || p.RetryBufferCapBytes != 0 || p.MemoryLimitBytes != 0 {
				return fmt.Errorf("point %d: probe_drain must omit memory resolution fields", i)
			}
			if p.MemoryEnvelope != "" || p.MemoryBudgetRequested != "" {
				return fmt.Errorf("point %d: probe_drain must omit memory envelope fields", i)
			}
			if p.HonestyOK != nil {
				return fmt.Errorf("point %d: probe_drain must omit honesty_ok (not applicable)", i)
			}
			if p.Parallel != 0 {
				return fmt.Errorf("point %d: probe_drain must omit reflow_parallel_requested", i)
			}
			continue
		}
		// Baseline integrity, independent of arm semantics: every point that
		// actually ran a reflow must carry real resolution evidence. This is
		// separate from the label rules below — an unlabeled singleton profile
		// declares no envelope, but it still executed under a resolved one.
		if err := validateResolvedMemoryProvenance(i, p); err != nil {
			return err
		}
		// A labeled arm must additionally match the candidate that actually
		// BOUND the run, not merely the lever that was passed to it. Under
		// minimum-selection an operator value can lose to a lower candidate,
		// so validating the request alone would accept exactly the class of
		// mislabeling that motivated these arms.
		switch p.MemoryEnvelope {
		case MemoryArmGOMEMLIMIT:
			if !p.GOMEMLIMITSet {
				return fmt.Errorf("point %d: envelope %s but no GOMEMLIMIT was set", i, p.MemoryEnvelope)
			}
			if p.MemoryBudgetRequested != "" {
				return fmt.Errorf("point %d: envelope %s must not also set a memory budget", i, p.MemoryEnvelope)
			}
			if p.MemoryLimitSource != memorySourceRuntime {
				return fmt.Errorf("point %d: envelope %s but the binding limit was %q — the supplied GOMEMLIMIT did not constrain this run", i, p.MemoryEnvelope, p.MemoryLimitSource)
			}
			if p.MemoryBudgetSource != memoryBudgetSourceDerived {
				return fmt.Errorf("point %d: envelope %s reported budget source %q, want %s", i, p.MemoryEnvelope, p.MemoryBudgetSource, memoryBudgetSourceDerived)
			}
		case MemoryArmProbeBound:
			if p.GOMEMLIMITSet || p.MemoryBudgetRequested != "" {
				return fmt.Errorf("point %d: envelope %s must run without memory overrides", i, p.MemoryEnvelope)
			}
			if p.MemoryLimitSource == memorySourceRuntime {
				return fmt.Errorf("point %d: envelope %s but the binding limit was %q — that is a runtime bound, not a detected one", i, p.MemoryEnvelope, p.MemoryLimitSource)
			}
			if p.MemoryBudgetSource != memoryBudgetSourceDerived {
				return fmt.Errorf("point %d: envelope %s reported budget source %q, want %s", i, p.MemoryEnvelope, p.MemoryBudgetSource, memoryBudgetSourceDerived)
			}
		case MemoryArmOperatorBudget:
			if p.MemoryBudgetRequested == "" {
				return fmt.Errorf("point %d: envelope %s but no memory budget was passed", i, p.MemoryEnvelope)
			}
			if p.GOMEMLIMITSet {
				return fmt.Errorf("point %d: envelope %s must not also constrain GOMEMLIMIT", i, p.MemoryEnvelope)
			}
			if p.MemoryBudgetSource != memoryBudgetSourceOperator && p.MemoryBudgetSource != memoryBudgetSourceOperatorClamped {
				return fmt.Errorf("point %d: envelope %s reported budget source %q — the operator budget did not govern this run", i, p.MemoryEnvelope, p.MemoryBudgetSource)
			}
		case "":
			// Unlabeled single-arm profiles (smoke, saturation) declare nothing.
		default:
			return fmt.Errorf("point %d: unknown memory_envelope %q", i, p.MemoryEnvelope)
		}
	}
	return nil
}

// Product memory provenance labels, mirrored here so the harness validates
// against the vocabulary the product actually emits rather than free text.
const (
	memorySourceCgroupV2             = "cgroup_v2"
	memorySourceCgroupV1             = "cgroup_v1"
	memorySourceRuntime              = "runtime"
	memorySourcePhysicalRAM          = "physical_ram"
	memorySourceDetectionUnavailable = "detection_unavailable"

	memoryBudgetSourceDerived         = "derived"
	memoryBudgetSourceOperator        = "operator"
	memoryBudgetSourceOperatorClamped = "operator_clamped_to_limit"
)

func recognizedMemoryLimitSource(s string) bool {
	switch s {
	case memorySourceCgroupV2, memorySourceCgroupV1, memorySourceRuntime,
		memorySourcePhysicalRAM, memorySourceDetectionUnavailable:
		return true
	}
	return false
}

func recognizedMemoryBudgetSource(s string) bool {
	switch s {
	case memoryBudgetSourceDerived, memoryBudgetSourceOperator, memoryBudgetSourceOperatorClamped:
		return true
	}
	return false
}

// validateResolvedMemoryProvenance fails closed on any executed reflow point
// that carries no usable resolution evidence. A placeholder or absent source
// cannot support a report claim of any kind — labeled or not — so it must not
// reach a published report.
func validateResolvedMemoryProvenance(i int, p PointReport) error {
	where := "unlabeled point"
	if p.MemoryEnvelope != "" {
		where = "envelope " + p.MemoryEnvelope
	}
	if !recognizedMemoryLimitSource(p.MemoryLimitSource) {
		return fmt.Errorf("point %d: %s has unrecognized memory_limit_source %q", i, where, p.MemoryLimitSource)
	}
	if !recognizedMemoryBudgetSource(p.MemoryBudgetSource) {
		return fmt.Errorf("point %d: %s has unrecognized memory_budget_source %q", i, where, p.MemoryBudgetSource)
	}
	if p.MemoryLimitBytes <= 0 || p.MemoryBudgetEffectiveBytes <= 0 || p.RetryBufferCapBytes <= 0 {
		return fmt.Errorf("point %d: %s has non-positive resolved memory arithmetic (limit=%d budget=%d cap=%d)",
			i, where, p.MemoryLimitBytes, p.MemoryBudgetEffectiveBytes, p.RetryBufferCapBytes)
	}
	return nil
}

// matrixCell identifies one declared sweep point. Counting per-arm totals is
// not enough: equal totals can hide a duplicated cell standing in for a missing
// one, so the identity of each cell is what has to be pinned.
type matrixCell struct {
	Envelope        string
	ExecutionShape  string
	Parallel        int
	CheckpointClass string
}

func (c matrixCell) String() string {
	return fmt.Sprintf("(arm=%s shape=%s parallel=%d checkpoint=%s)", c.Envelope, c.ExecutionShape, c.Parallel, c.CheckpointClass)
}

// ValidateArmMatrix checks that a report contains exactly the arm × execution
// shape × parallel × checkpoint cells its profile declares — each one once.
// Per-point validation cannot see a whole cell that went missing, and "a
// declared arm is never silently dropped" is precisely what these profiles
// promise.
func ValidateArmMatrix(spec ProfileSpec, r Report) error {
	if len(spec.MemoryArms) == 0 {
		// Profiles without declared arms are not matrix-constrained: they
		// legitimately append points the matrix does not describe (occupancy
		// samples, full-pipe A/B repeats).
		return nil
	}
	if spec.ExecutionShape != "reflow_only" {
		// A declared-arm profile that repeats points per cell (full-pipe A/B)
		// would need declared per-cell multiplicity. Refuse rather than
		// validate it under a rule that does not describe it.
		return fmt.Errorf("profile %s declares memory arms with execution shape %q; the arm matrix gate covers reflow_only sweeps only", spec.Name, spec.ExecutionShape)
	}
	classes := spec.CheckpointClasses
	if len(classes) == 0 {
		classes = []string{"disk"}
	}
	expected := map[matrixCell]bool{}
	for _, arm := range spec.MemoryArms {
		for _, parallel := range spec.ParallelPoints {
			for _, class := range classes {
				expected[matrixCell{
					Envelope:        arm.Label,
					ExecutionShape:  spec.ExecutionShape,
					Parallel:        parallel,
					CheckpointClass: class,
				}] = false
			}
		}
	}
	for i, p := range r.Points {
		if p.ExecutionShape == "probe_drain" {
			continue
		}
		cell := matrixCell{
			Envelope:        p.MemoryEnvelope,
			ExecutionShape:  p.ExecutionShape,
			Parallel:        p.Parallel,
			CheckpointClass: p.CheckpointClass,
		}
		seen, declared := expected[cell]
		if !declared {
			return fmt.Errorf("profile %s point %d reported undeclared cell %s", spec.Name, i, cell)
		}
		if seen {
			return fmt.Errorf("profile %s point %d duplicates cell %s", spec.Name, i, cell)
		}
		expected[cell] = true
	}
	for cell, seen := range expected {
		if !seen {
			return fmt.Errorf("profile %s is missing declared cell %s", spec.Name, cell)
		}
	}
	return nil
}

func intPtr(v int) *int       { return &v }
func strPtr(v string) *string { return &v }
func boolPtrVal(v bool) *bool { return &v }
