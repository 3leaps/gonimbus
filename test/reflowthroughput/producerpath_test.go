package reflowthroughput

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/reflow"
)

// The gate tests in memoryarms_test.go hand-build PointReport literals and
// mutate them, which proves the report gate rejects absent provenance. It does
// not prove the measuring path surfaces absence honestly, because the absence
// was fed to the gate rather than produced by the code under test.
//
// The controls below close that gap for the memory-provenance gate. They drive
// the real producing path — product record types, the real parser, the real
// buildPointReport — with an input that withholds the memory tuple, and check
// that the builder reports the absence rather than manufacturing a value the
// product never established. The negative and positive fixtures differ in the
// memory tuple and nothing else, so the rejection is attributable to the
// omission alone.
//
// Note on what the omission trips: withholding the tuple zeroes both the
// provenance sources and the resolved arithmetic, so both sub-assertions in
// validateResolvedMemoryProvenance would fail. That is the genuine product
// shape — reflow marshals the whole tuple omitempty, so a run whose ceiling was
// not memory-resolved emits exactly these records — and half-populating it
// (sources withheld, byte values retained) would fabricate a state the product
// never emits. The source assertion is reached first and is the one asserted
// on; the control pins that specifically so the failure cannot silently become
// the arithmetic one.

// resolvedMemoryTuple is the provenance a memory-resolved run reports.
func resolvedMemoryTuple() reflow.ConcurrencyStats {
	return reflow.ConcurrencyStats{
		MemoryLimitBytes:           1 << 30,
		MemoryLimitSource:          memorySourcePhysicalRAM,
		MemoryBudgetRequestedBytes: 256 << 20,
		MemoryBudgetEffectiveBytes: 256 << 20,
		MemoryBudgetSource:         memoryBudgetSourceDerived,
		RetryBufferCapBytes:        16 << 20,
	}
}

// memoryTupleKeys is the whole startup-fixed memory tuple as it appears on the
// wire. The negative fixture must withhold all six: asserting only on the
// source would let a fixture that still carries, say, a requested-bytes value
// pass as "absent", and the controls below accept both the provenance and the
// arithmetic as genuinely zero on the strength of that absence.
var memoryTupleKeys = []string{
	"memory_limit_bytes",
	"memory_limit_source",
	"memory_budget_requested_bytes",
	"memory_budget_effective_bytes",
	"memory_budget_source",
	"retry_buffer_cap_bytes",
}

// assertMemoryTupleAbsent decodes each emitted record's payload and requires
// that no field of the memory tuple was serialized at all.
func assertMemoryTupleAbsent(t *testing.T, stdout []byte) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		var env struct {
			Type string         `json:"type"`
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			t.Fatalf("decode fixture line: %v", err)
		}
		for _, k := range memoryTupleKeys {
			if _, ok := env.Data[k]; ok {
				t.Fatalf("fixture record %s still carries %s: %s", env.Type, k, line)
			}
		}
	}
}

// productStdoutWithMemory renders the product's own run and summary records
// through the product's own JSONL writer — the same WriteAny call both reflow
// paths use to emit them — carrying the supplied memory tuple. Going through
// the real writer rather than a hand-rolled envelope means the fixture carries
// the product's record schema, not a second dialect the parser merely happens
// to tolerate. Passing the zero ConcurrencyStats memory fields withholds the
// tuple; they are omitempty on the product type, so the records serialize
// without them.
func productStdoutWithMemory(t *testing.T, mem reflow.ConcurrencyStats) []byte {
	t.Helper()

	concurrency := reflow.ConcurrencyStats{
		AdaptiveEnabled:             true,
		ConcurrencyFloor:            1,
		ConcurrencyInitial:          4,
		ConcurrencyCeilingRequested: 8,
		ConcurrencyCeilingEffective: 8,
		ConcurrencyCeilingReason:    "requested",
		ConcurrencyFinal:            8,
		ConcurrencyMaxActive:        0,

		MemoryLimitBytes:           mem.MemoryLimitBytes,
		MemoryLimitSource:          mem.MemoryLimitSource,
		MemoryBudgetRequestedBytes: mem.MemoryBudgetRequestedBytes,
		MemoryBudgetEffectiveBytes: mem.MemoryBudgetEffectiveBytes,
		MemoryBudgetSource:         mem.MemoryBudgetSource,
		RetryBufferCapBytes:        mem.RetryBufferCapBytes,
	}

	run := reflow.RunRecord{
		DestURI:          "file:///dest/",
		Parallel:         8,
		ExecutionPath:    "engine",
		ConcurrencyStats: concurrency,
	}

	summaryConcurrency := concurrency
	summaryConcurrency.ConcurrencyMaxActive = 8
	summaryConcurrency.ConcurrencyTimeAvgActive = 6.5

	summary := reflow.SummaryRecord{
		DestURI:          "file:///dest/",
		OnCollision:      "skip",
		ExecutionPath:    "engine",
		ConcurrencyStats: summaryConcurrency,
		Statuses:         map[string]int64{"complete": 100},
	}

	var b bytes.Buffer
	w := output.NewJSONLWriter(&b, "job", "file")
	defer func() { _ = w.Close() }()
	for _, rec := range []struct {
		typ  string
		data any
	}{
		{reflow.RunRecordType, run},
		{reflow.SummaryRecordType, summary},
	} {
		if err := w.WriteAny(context.Background(), rec.typ, rec.data); err != nil {
			t.Fatalf("emit %s: %v", rec.typ, err)
		}
	}
	return b.Bytes()
}

// measurementFor is the arm configuration and observed timings the two controls
// share. Only the parsed product output differs between them.
func measurementFor(parsed ParsedReflowOutput) pointMeasurement {
	return pointMeasurement{
		PointID:          "producer-path-control",
		Shape:            "reflow_only",
		Parallel:         8,
		CheckpointClass:  "disk",
		ElapsedSeconds:   10,
		CompletedObjects: 100,
		EndToEndRate:     10,
		HonestyOK:        boolPtrVal(true),
		StageExitCodes:   map[string]int{"reflow": 0},
		Parsed:           parsed,
	}
}

// The producer must not manufacture memory provenance the product never
// reported. Driving the real builder with real product records that withhold
// the tuple, the report must carry the absence through to the gate — and the
// gate must be where it fails, not the parser or any earlier setup step.
func TestBuildPointReportDoesNotSynthesizeAbsentMemoryProvenance(t *testing.T) {
	t.Parallel()

	// Withheld: the zero memory tuple, which the product types omit entirely.
	// The whole tuple has to be absent, not just the source — the assertions
	// below treat the zero arithmetic as genuinely unreported too.
	stdout := productStdoutWithMemory(t, reflow.ConcurrencyStats{})
	assertMemoryTupleAbsent(t, stdout)

	// The rejection below has to be attributable to the report gate, so the
	// run must first get there cleanly: no upstream parser or setup failure.
	// agreeMemoryResolution is a run-vs-summary equality check, so a tuple
	// absent from both records agrees at zero and the parse succeeds cleanly.
	parsed, err := ParseReflowStdout(stdout)
	if err != nil {
		t.Fatalf("parse must succeed so the rejection is attributable to the report gate: %v", err)
	}
	if parsed.MemoryLimitSource != "" || parsed.MemoryBudgetSource != "" {
		t.Fatalf("parser invented provenance: limit=%q budget=%q", parsed.MemoryLimitSource, parsed.MemoryBudgetSource)
	}

	pt := buildPointReport(measurementFor(parsed))

	// The substitution hazard itself: no placeholder label, no invented bytes.
	if pt.MemoryLimitSource != "" {
		t.Fatalf("builder synthesized memory_limit_source %q for a run that reported none", pt.MemoryLimitSource)
	}
	if pt.MemoryBudgetSource != "" {
		t.Fatalf("builder synthesized memory_budget_source %q for a run that reported none", pt.MemoryBudgetSource)
	}
	if pt.MemoryLimitBytes != 0 || pt.MemoryBudgetEffectiveBytes != 0 || pt.RetryBufferCapBytes != 0 {
		t.Fatalf("builder synthesized resolved arithmetic (limit=%d budget=%d cap=%d)",
			pt.MemoryLimitBytes, pt.MemoryBudgetEffectiveBytes, pt.RetryBufferCapBytes)
	}
	// The row is an allowlist, not a copy of the product's output. Every
	// non-memory telemetry field it does carry is still reported, so the
	// memory tuple is the only thing the omission took out of it.
	for _, f := range []struct {
		name string
		got  *int
		want int
	}{
		{"concurrency_ceiling_requested", pt.ConcurrencyRequested, 8},
		{"concurrency_ceiling_effective", pt.ConcurrencyEffective, 8},
		{"concurrency_max_active", pt.ConcurrencyMaxActive, 8},
		{"concurrency_final", pt.ConcurrencyFinal, 8},
	} {
		if f.got == nil || *f.got != f.want {
			t.Fatalf("builder dropped reported %s: %v", f.name, f.got)
		}
	}
	if pt.ConcurrencyReason == nil || *pt.ConcurrencyReason != "requested" {
		t.Fatalf("builder dropped reported concurrency_ceiling_reason: %v", pt.ConcurrencyReason)
	}
	if pt.ConcurrencyTimeAvgActive != 6.5 {
		t.Fatalf("builder dropped reported concurrency_time_avg_active: %v", pt.ConcurrencyTimeAvgActive)
	}
	if pt.AdaptiveEnabled == nil || !*pt.AdaptiveEnabled {
		t.Fatalf("builder dropped reported adaptive_enabled: %v", pt.AdaptiveEnabled)
	}

	// And the gate must reject it at its own assertion.
	err = ValidateReportEnvelope(reportWith(pt))
	if err == nil {
		t.Fatal("report with absent memory provenance was accepted")
	}
	if !strings.Contains(err.Error(), "unrecognized memory_limit_source") {
		t.Fatalf("rejection is not the memory-provenance source assertion: %v", err)
	}
	// Pinned so the rejection cannot silently migrate to the arithmetic
	// sub-assertion, which the same omission also trips but does not reach.
	if strings.Contains(err.Error(), "non-positive resolved memory arithmetic") {
		t.Fatalf("rejection came from the arithmetic assertion, not provenance: %v", err)
	}
}

// The same fixture with provenance present must be accepted, and the builder
// must report the values the product reported rather than any of its own. This
// isolates the omission as the sole cause of the rejection above.
func TestBuildPointReportCarriesReportedMemoryProvenance(t *testing.T) {
	t.Parallel()

	mem := resolvedMemoryTuple()
	parsed, err := ParseReflowStdout(productStdoutWithMemory(t, mem))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	pt := buildPointReport(measurementFor(parsed))

	if pt.MemoryLimitSource != mem.MemoryLimitSource {
		t.Fatalf("memory_limit_source %q != reported %q", pt.MemoryLimitSource, mem.MemoryLimitSource)
	}
	if pt.MemoryBudgetSource != mem.MemoryBudgetSource {
		t.Fatalf("memory_budget_source %q != reported %q", pt.MemoryBudgetSource, mem.MemoryBudgetSource)
	}
	if pt.MemoryLimitBytes != mem.MemoryLimitBytes {
		t.Fatalf("memory_limit_bytes %d != reported %d", pt.MemoryLimitBytes, mem.MemoryLimitBytes)
	}
	if pt.MemoryBudgetEffectiveBytes != mem.MemoryBudgetEffectiveBytes {
		t.Fatalf("memory_budget_effective_bytes %d != reported %d", pt.MemoryBudgetEffectiveBytes, mem.MemoryBudgetEffectiveBytes)
	}
	if pt.RetryBufferCapBytes != mem.RetryBufferCapBytes {
		t.Fatalf("retry_buffer_cap_bytes %d != reported %d", pt.RetryBufferCapBytes, mem.RetryBufferCapBytes)
	}

	if err := ValidateReportEnvelope(reportWith(pt)); err != nil {
		t.Fatalf("report built from a fully provenanced run was rejected: %v", err)
	}
}
