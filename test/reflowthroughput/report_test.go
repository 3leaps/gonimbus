package reflowthroughput

import (
	"strings"
	"testing"
)

func TestReportSterilitySentinel(t *testing.T) {
	t.Parallel()
	r := NewReport(ProfileSmoke, "file", "deadbeef", "abc123", CompactManifest{
		RecipeVersion: RecipeVersion,
		Seed:          42,
		ObjectCount:   8,
		TotalBytes:    100,
		SizeHistogram: map[string]int{"256_1k": 8},
		Digest:        "digestdigest",
	}, false)
	r.OS = "darwin"
	r.Arch = "arm64"
	// Sterility is about what a report must not leak, not a licence to publish
	// fabricated provenance: this point carries a real resolved tuple.
	r.Points = []PointReport{{
		PointID:                    "smoke-p01-deadbeef",
		ExecutionShape:             "reflow_only",
		Parallel:                   2,
		AdaptiveMode:               "fixed",
		MemoryLimitSource:          memorySourcePhysicalRAM,
		MemoryBudgetSource:         memoryBudgetSourceDerived,
		MemoryLimitBytes:           1 << 30,
		MemoryBudgetEffectiveBytes: 256 << 20,
		RetryBufferCapBytes:        16 << 20,
		CheckpointClass:            "disk",
		ConcurrencyRequested:       intPtr(2),
		ConcurrencyEffective:       intPtr(2),
		ConcurrencyReason:          strPtr("requested"),
		HonestyOK:                  boolPtrVal(true),
		StageExitCodes:             map[string]int{"reflow": 0},
	}}
	// Inject would-be sensitive fields incorrectly — ensure our marshal path
	// does not include them by construction. Serialize and check forbidden.
	b, err := MarshalJSONReport(r)
	if err != nil {
		t.Fatal(err)
	}
	// Fake endpoint/bucket/local path must not appear.
	forbidden := append(SterilityForbidden(
		"s3://evil-bucket",
		"https://s3.example.invalid",
		"/Users/someone/secret",
		"AKIAEXAMPLE",
	), []string{
		"s3://",
		"gs://",
		"/Users/",
		"/home/",
		"AKIA",
	}...)
	// Report itself should not contain these; if we wrongly added dest paths it would fail.
	if err := AssertNoSensitiveTokens(string(b), forbidden); err != nil {
		t.Fatal(err)
	}
	// Positive: schema present.
	if !strings.Contains(string(b), ReportSchemaVersion) {
		t.Fatal("missing schema version")
	}
	if err := ValidateReportEnvelope(r); err != nil {
		t.Fatal(err)
	}
}

func TestValidateReportEnvelopeRejectsProbeDrainReflowFields(t *testing.T) {
	t.Parallel()
	r := NewReport(ProfileProbeSaturation, "file", "id", "sha", CompactManifest{
		RecipeVersion: RecipeVersion, Seed: 1, ObjectCount: 1, TotalBytes: 1,
		SizeHistogram: map[string]int{"lt_256": 1}, Digest: "d",
	}, false)
	r.Points = []PointReport{{
		PointID:          "p",
		ExecutionShape:   "probe_drain",
		ProbeConcurrency: 4,
		CheckpointClass:  "disk", // non-applicable
		HonestyOK:        boolPtrVal(true),
	}}
	if err := ValidateReportEnvelope(r); err == nil {
		t.Fatal("expected rejection of probe_drain reflow-adjacent fields")
	}
}
