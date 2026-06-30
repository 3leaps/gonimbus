package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

// CLI<->library parity harness.
//
// The merge gate for the reflow engine migration is "CLI behavior unchanged, by
// construction": the CLI adapter and the pkg/reflow runner, driven against the
// same in-memory providers, must emit equivalent typed events and summaries. This
// file establishes that harness — the in-memory providers, the presentation-field
// normalizer, the capturing EventSink, and the canonical case table — before any
// provider-touching engine code moves, so the comparison is ready to assert as the
// runner is implemented.
//
// For scenarios Runner.Run has not migrated yet, the harness captures the CLI
// golden output and exercises the library seam (NewRunner validation +
// provider-handle injection), then short-circuits on ErrNotImplemented. The
// comparison activates automatically for each scenario once Run emits events.

// presentationDropKeys are payload fields that legitimately differ between the CLI
// and library paths (or run-to-run) and are removed before comparison. Semantic
// fields — statuses, collision decisions, IfAbsent probe results, dest keys — are
// kept. The set grows as needed when the runner begins emitting events.
var presentationDropKeys = map[string]bool{
	"checkpoint_path": true,
}

// normalizedEvent is the presentation-stripped form of a reflow event, comparable
// across the CLI JSONL output and the library EventSink.
type normalizedEvent struct {
	Type   string
	Fields map[string]any
}

// normalizeReflowStdout parses CLI JSONL stdout into normalized reflow events,
// keeping only the reflow/warning record types and dropping presentation fields.
func normalizeReflowStdout(t *testing.T, stdout string) []normalizedEvent {
	t.Helper()
	keep := map[string]bool{
		reflowpkg.RunRecordType:     true,
		reflowpkg.SourceRecordType:  true,
		reflowpkg.RecordType:        true,
		reflowpkg.WarningRecordType: true,
		reflowpkg.SummaryRecordType: true,
		reflowpkg.ErrorEventType:    true,
	}
	var out []normalizedEvent
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var env testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &env))
		if !keep[env.Type] {
			continue
		}
		var fields map[string]any
		require.NoError(t, json.Unmarshal(env.Data, &fields))
		out = append(out, normalizedEvent{Type: env.Type, Fields: dropPresentation(fields)})
	}
	return sortNormalized(out)
}

// dropPresentation removes presentation-only keys from a payload map.
func dropPresentation(fields map[string]any) map[string]any {
	for k := range presentationDropKeys {
		delete(fields, k)
	}
	return fields
}

// sortNormalized orders events deterministically so comparison is independent of
// concurrent emission order: by type, then dest key, then source key.
func sortNormalized(events []normalizedEvent) []normalizedEvent {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Type != events[j].Type {
			return events[i].Type < events[j].Type
		}
		di, _ := events[i].Fields["dest_key"].(string)
		dj, _ := events[j].Fields["dest_key"].(string)
		if di != dj {
			return di < dj
		}
		si, _ := events[i].Fields["source_key"].(string)
		sj, _ := events[j].Fields["source_key"].(string)
		return si < sj
	})
	return events
}

// capturingSink implements reflowpkg.EventSink, recording each engine event in the
// same normalized form as the CLI path for parity comparison.
type capturingSink struct {
	events []normalizedEvent
}

func (s *capturingSink) record(t string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	s.events = append(s.events, normalizedEvent{Type: t, Fields: dropPresentation(fields)})
	return nil
}

func (s *capturingSink) OnRun(_ context.Context, rec reflowpkg.RunRecord) error {
	return s.record(reflowpkg.RunRecordType, rec)
}
func (s *capturingSink) OnSource(_ context.Context, rec reflowpkg.SourceRunRecord) error {
	return s.record(reflowpkg.SourceRecordType, rec)
}
func (s *capturingSink) OnRecord(_ context.Context, rec reflowpkg.Record) error {
	return s.record(reflowpkg.RecordType, rec)
}
func (s *capturingSink) OnWarning(_ context.Context, w reflowpkg.Warning) error {
	return s.record(reflowpkg.WarningRecordType, w)
}
func (s *capturingSink) OnError(_ context.Context, e reflowpkg.ErrorEvent) error {
	return s.record(reflowpkg.ErrorEventType, e)
}
func (s *capturingSink) OnSummary(_ context.Context, rec reflowpkg.SummaryRecord) error {
	return s.record(reflowpkg.SummaryRecordType, rec)
}

func (s *capturingSink) normalized() []normalizedEvent { return sortNormalized(s.events) }

// parityCase defines one scenario in both forms: the CLI flag invocation and the
// library Config/Source construction, run against equivalently-seeded in-memory
// providers. The dest base is s3://dest-bucket/data/ and sources address
// s3://source-bucket/ (the buckets the shared provider factory recognizes).
type parityCase struct {
	name    string
	seed    func(src, dst *reflowMemoryProvider)
	input   string
	cliArgs []string
	config  func(dst *reflowMemoryProvider) reflowpkg.Config
	source  func(src *reflowMemoryProvider) reflowpkg.Source
	// expectInvalidInputs marks a case whose stream contains invalid records: both
	// paths must report failure (CLI non-zero exit, library InvalidInputsError),
	// and all engine events, including gonimbus.error.v1, must match.
	expectInvalidInputs bool
}

const (
	parityDestBase = "s3://dest-bucket/data/"
	paritySrcKey   = "source/file.xml"
)

func reflowInputLineForBucket(bucket string, key string, etag string, size int64, routingClass string, quarantinePrefix string) string {
	data := map[string]any{
		"source_uri":        "s3://" + bucket + "/" + key,
		"source_key":        key,
		"source_etag":       etag,
		"source_size_bytes": size,
		"dest_rel_key":      key,
		"vars": map[string]string{
			"key": key,
		},
	}
	if routingClass != "" {
		data["routing_class"] = routingClass
	}
	if quarantinePrefix != "" {
		data["quarantine_prefix"] = quarantinePrefix
	}
	line, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
	if err != nil {
		panic(err)
	}
	return string(line)
}

func baseParityConfig(dst *reflowMemoryProvider) reflowpkg.Config {
	return reflowpkg.Config{
		Destination: reflowpkg.Destination{Provider: dst, ProviderID: "s3", BaseURI: parityDestBase},
		Rewrite:     reflowpkg.RewriteConfig{From: "{key}", To: "{key}"},
		Collision:   reflowpkg.CollisionPolicy{Mode: "skip-if-duplicate"},
		// Mirror the CLI cases' `--parallel 1` (adaptive default on) so the
		// scaffold compares equivalent concurrency policy, not a library default.
		// For requested=1 this resolves deterministically (effective ceiling 1,
		// reason "requested") independent of the host resource probe.
		Concurrency: reflowpkg.ResolveConcurrency(1, true, reflowpkg.DefaultResourceProbe()),
	}
}

func parityRecordStream(src *reflowMemoryProvider, input string) reflowpkg.Source {
	return reflowpkg.RecordStreamSource{
		Records: strings.NewReader(input),
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	}
}

// reflowParityCases is the canonical parity case table. It starts with
// representative behaviors and grows alongside the runner migration to cover the
// full matrix (collision modes, metadata, provenance, resume, throttle/AIMD).
var reflowParityCases = []parityCase{
	{
		name: "basic_copy_ifabsent_honored",
		seed: func(src, _ *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "payload", "etag-a", time.Time{})
		},
		input:   reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"},
		config:  baseParityConfig,
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""))
		},
	},
	{
		name: "dry_run_no_mutation",
		seed: func(src, _ *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "payload", "etag-a", time.Time{})
		},
		input:   reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1", "--dry-run"},
		config: func(dst *reflowMemoryProvider) reflowpkg.Config {
			cfg := baseParityConfig(dst)
			cfg.DryRun = true
			return cfg
		},
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""))
		},
	},
	{
		name: "ifabsent_not_honored_fallback_skip_duplicate",
		seed: func(src, dst *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "payload", "same-etag", time.Time{})
			dst.ignoreIfAbsent = true
			dst.putFixture("data/"+paritySrcKey, "payload", "same-etag", time.Time{})
		},
		input:   reflowInputLine(paritySrcKey, "same-etag", int64(len("payload")), "", ""),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"},
		config:  baseParityConfig,
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, reflowInputLine(paritySrcKey, "same-etag", int64(len("payload")), "", ""))
		},
	},
	{
		name: "collision_fail_conflict_leaves_dest_unchanged",
		seed: func(src, dst *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "new payload", "src-etag", time.Time{})
			dst.ignoreIfAbsent = true
			dst.putFixture("data/"+paritySrcKey, "old payload", "old-etag", time.Time{})
		},
		input:   reflowInputLine(paritySrcKey, "src-etag", int64(len("new payload")), "", ""),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1", "--on-collision", "fail"},
		config: func(dst *reflowMemoryProvider) reflowpkg.Config {
			cfg := baseParityConfig(dst)
			cfg.Collision = reflowpkg.CollisionPolicy{Mode: "fail"}
			return cfg
		},
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, reflowInputLine(paritySrcKey, "src-etag", int64(len("new payload")), "", ""))
		},
	},
	{
		// A reflow-input record without dest_rel_key and no rewrite templates is
		// invalid: both paths emit the run/warning/summary, count the invalid input,
		// and report failure.
		name: "invalid_input_missing_dest_rel_key_dry_run",
		seed: func(src, _ *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "payload", "etag-a", time.Time{})
		},
		input:   reflowInputLineNoDestRel(paritySrcKey, "etag-a", int64(len("payload"))),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--parallel", "1", "--dry-run"},
		config: func(dst *reflowMemoryProvider) reflowpkg.Config {
			cfg := baseParityConfig(dst)
			cfg.DryRun = true
			cfg.Rewrite = reflowpkg.RewriteConfig{}
			return cfg
		},
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, reflowInputLineNoDestRel(paritySrcKey, "etag-a", int64(len("payload"))))
		},
		expectInvalidInputs: true,
	},
	{
		name: "mixed_reflow_and_index_object_same_bucket",
		seed: func(src, _ *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "payload", "etag-a", time.Time{})
			src.putFixture("file.txt", "payload-b", "etag-b", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		},
		input: strings.Join([]string{
			reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""),
			reflowIndexObjectInputLine("file.txt", "etag-b", int64(len("payload-b")), "2026-01-15T20:53:44Z"),
		}, "\n"),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"},
		config:  baseParityConfig,
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, strings.Join([]string{
				reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""),
				reflowIndexObjectInputLine("file.txt", "etag-b", int64(len("payload-b")), "2026-01-15T20:53:44Z"),
			}, "\n"))
		},
	},
	{
		name: "different_s3_bucket_invalid_before_planning_dry_run",
		seed: func(src, _ *reflowMemoryProvider) {
			src.putFixture(paritySrcKey, "payload", "etag-a", time.Time{})
		},
		input: strings.Join([]string{
			reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""),
			reflowInputLineForBucket("other-bucket", "other/file.xml", "etag-b", int64(len("payload")), "", ""),
		}, "\n"),
		cliArgs: []string{"--stdin", "--dest", parityDestBase, "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1", "--dry-run"},
		config: func(dst *reflowMemoryProvider) reflowpkg.Config {
			cfg := baseParityConfig(dst)
			cfg.DryRun = true
			return cfg
		},
		source: func(src *reflowMemoryProvider) reflowpkg.Source {
			return parityRecordStream(src, strings.Join([]string{
				reflowInputLine(paritySrcKey, "etag-a", int64(len("payload")), "", ""),
				reflowInputLineForBucket("other-bucket", "other/file.xml", "etag-b", int64(len("payload")), "", ""),
			}, "\n"))
		},
		expectInvalidInputs: true,
	},
}

// TestReflowCLILibraryParity captures the CLI golden output for each case and
// exercises the library seam. For not-yet-migrated scenarios it short-circuits on
// ErrNotImplemented; once Run emits events the require.Equal asserts CLI<->library
// parity.
func TestReflowCLILibraryParity(t *testing.T) {
	for _, tc := range reflowParityCases {
		t.Run(tc.name, func(t *testing.T) {
			// CLI golden output.
			srcCLI, dstCLI := newReflowMemoryProvider(), newReflowMemoryProvider()
			tc.seed(srcCLI, dstCLI)
			stdout, _, cliErr := runTransferReflowWithProviderFactory(t, srcCLI, dstCLI, tc.input, tc.cliArgs...)
			cliEvents := normalizeReflowStdout(t, stdout)
			require.NotEmpty(t, cliEvents, "CLI path should emit normalized reflow events")

			// Library path: same provider shapes, injected as handles.
			srcLib, dstLib := newReflowMemoryProvider(), newReflowMemoryProvider()
			tc.seed(srcLib, dstLib)
			sink := &capturingSink{}
			cfg := tc.config(dstLib)
			cfg.Events = sink
			runner, err := reflowpkg.NewRunner(cfg)
			require.NoError(t, err, "library runner must construct from the case config")

			_, runErr := runner.Run(context.Background(), tc.source(srcLib))
			if errors.Is(runErr, reflowpkg.ErrNotImplemented) {
				t.Logf("library runner has not migrated this scenario; captured %d CLI golden events. Parity comparison activates when the scenario migrates.", len(cliEvents))
				return
			}

			if tc.expectInvalidInputs {
				require.Error(t, cliErr, "CLI must report failure on invalid inputs")
				var invErr *reflowpkg.InvalidInputsError
				require.ErrorAs(t, runErr, &invErr, "library must report invalid-inputs failure, not success")
				require.Equal(t, cliEvents, sink.normalized(), "CLI and library must emit equivalent events on invalid input")
				return
			}

			if cliErr != nil {
				var objectErr *reflowpkg.ObjectErrorsError
				require.ErrorAs(t, runErr, &objectErr, "library must report object-error failure when CLI fails")
			} else {
				require.NoError(t, runErr)
			}
			require.Equal(t, cliEvents, sink.normalized(), "CLI and library must emit equivalent normalized events")
		})
	}
}

// TestNormalizeReflowStdoutStripsPresentationFields verifies the normalizer drops
// presentation-only fields while preserving semantic payload.
func TestNormalizeReflowStdoutStripsPresentationFields(t *testing.T) {
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","ts":"2026-01-01T00:00:00Z","job_id":"job-xyz","provider":"s3","data":{"dest_uri":"s3://dest-bucket/data/","checkpoint_path":"/var/run/state.db","dry_run":false,"parallel":1}}`,
		`{"type":"gonimbus.reflow.v1","ts":"2026-01-01T00:00:01Z","job_id":"job-xyz","provider":"s3","data":{"source_key":"source/file.xml","dest_key":"data/source/file.xml","status":"complete"}}`,
		`{"type":"gonimbus.heartbeat.v1","ts":"2026-01-01T00:00:02Z","data":{"ignored":true}}`,
	}, "\n")
	events := normalizeReflowStdout(t, stdout)
	require.Len(t, events, 2, "only reflow record types are kept")

	run := events[0]
	require.Equal(t, "gonimbus.reflow.run.v1", run.Type)
	require.NotContains(t, run.Fields, "checkpoint_path", "presentation field must be dropped")
	require.Equal(t, "s3://dest-bucket/data/", run.Fields["dest_uri"], "semantic field must be kept")

	rec := events[1]
	require.Equal(t, "gonimbus.reflow.v1", rec.Type)
	require.Equal(t, "complete", rec.Fields["status"])
	require.Equal(t, "data/source/file.xml", rec.Fields["dest_key"])
}
