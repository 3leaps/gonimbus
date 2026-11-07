package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/stretchr/testify/require"
)

func TestReflowProbeSummaryControlDoesNotAdmitObject(t *testing.T) {
	raw, err := json.Marshal(probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"})
	require.NoError(t, err)
	line := `{"type":"` + probe.SummaryRecordType + `","data":` + string(raw) + `}`
	class, _ := classifyReflowFirstRecord(line)
	require.Equal(t, firstRecordFallback, class)
	queue := make(chan reflowTask, 1)
	identity, err := enqueueReflowLine(context.Background(), line, "existing", reflowSourceConfig{}, func(*uri.ObjectURI) (provider.Provider, provider.Provider, error) {
		t.Fatal("control record must not resolve a provider")
		return nil, nil, nil
	}, queue)
	require.NoError(t, err)
	require.Equal(t, "existing", identity)
	require.Empty(t, queue)
	malformed := `{"type":"` + probe.SummaryRecordType + `","data":{}}`
	class, _ = classifyReflowFirstRecord(malformed)
	require.Equal(t, firstRecordRefuse, class)
	_, err = enqueueReflowLine(context.Background(), malformed, "existing", reflowSourceConfig{}, nil, queue)
	require.Error(t, err)
}

func TestReflowProbeSummaryCommandCompatibilityBothConsumers(t *testing.T) {
	raw, err := json.Marshal(probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"})
	require.NoError(t, err)
	control := `{"type":"` + probe.SummaryRecordType + `","data":` + string(raw) + `}`
	for _, pool := range []bool{false, true} {
		args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"}
		if pool {
			args = append(args, poolRoute...)
		}
		src, dst := newReflowMemoryProvider(), newReflowMemoryProvider()
		src.putFixture("source/file.xml", "payload", "etag", time.Time{})
		stdout, _, err := runTransferReflowWithProviderFactory(t, src, dst, reflowInputLine("source/file.xml", "etag", 7, "", "")+"\n"+control, args...)
		require.NoError(t, err)
		if pool {
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
		} else {
			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		}
		require.Equal(t, "complete", requireReflowData(t, stdout, "complete").Status)
		requireNoRecordType(t, stdout, "gonimbus.error.v1")
		// Existing upstream error behavior stays a refusal, even if a valid
		// producer terminal follows; control evidence is not authorization.
		_, _, err = runTransferReflowWithProviderFactory(t, newReflowMemoryProvider(), newReflowMemoryProvider(), `{"type":"gonimbus.error.v1","data":{"code":"NOT_FOUND","message":"upstream"}}`+"\n"+control, args...)
		require.Error(t, err)
	}
	stdout, err := runTransferReflowWithProviders(t, newReflowMemoryProvider(), newReflowMemoryProvider(), control)
	require.NoError(t, err)
	requireNoRecordType(t, stdout, reflowpkg.RecordType)
}

func TestReflowPoolRejectsMalformedProbeSummaryHistogram(t *testing.T) {
	for _, histogram := range []string{`{"INTERNAL":null}`, `{"INTERNAL":0.5}`, `{"INTERNAL":"0"}`, `{"INTERNAL":-1}`, `{"ACCESS_DENIED":1}`, `{"TIMEOUT":1}`, `{"THROTTLED":1}`, `{"TRANSIENT":1}`, `{"PROVIDER_UNAVAILABLE":1}`, `{"INVALID_INPUT":1}`} {
		s := probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"}
		if strings.HasSuffix(histogram, ":1}") {
			s.Errors = 1
			s.Termination = "completed_with_errors"
			s.ExitCode = 60
		}
		raw, err := json.Marshal(s)
		require.NoError(t, err)
		payload := strings.Replace(string(raw), `"errors_by_code":{}`, `"errors_by_code":`+histogram, 1)
		line := `{"type":"` + probe.SummaryRecordType + `","data":` + payload + `}`
		class, _ := classifyReflowFirstRecord(line)
		require.Equal(t, firstRecordRefuse, class, histogram)
		queue := make(chan reflowTask, 1)
		_, err = enqueueReflowLine(context.Background(), line, "existing", reflowSourceConfig{}, func(*uri.ObjectURI) (provider.Provider, provider.Provider, error) {
			t.Fatal("malformed control must not resolve provider")
			return nil, nil, nil
		}, queue)
		require.Error(t, err, histogram)
		require.Empty(t, queue)
	}
}

func TestProbeTerminalWriterMarksSummaryAdvisoryOnly(t *testing.T) {
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tw := newProbeTerminalWriter(output.NewJSONLWriter(&buf, "job-1", "s3"), cancel)

	require.NoError(t, tw.WriteAny(ctx, "gonimbus.reflow.input.v1", map[string]any{"source_uri": "s3://b/k"}))
	require.NoError(t, tw.WriteError(ctx, &output.ErrorRecord{Code: output.ErrCodeNotFound, Message: "missing"}))
	require.NoError(t, tw.WriteAdvisoryAny(ctx, probe.SummaryRecordType, probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"}))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 3)

	var r0, r1, r2 output.Record
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &r0))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &r1))
	require.NoError(t, json.Unmarshal([]byte(lines[2]), &r2))

	require.Equal(t, "gonimbus.reflow.input.v1", r0.Type)
	require.False(t, r0.Advisory)
	require.NotContains(t, lines[0], `"advisory"`)

	require.Equal(t, output.TypeError, r1.Type)
	require.False(t, r1.Advisory)
	require.NotContains(t, lines[1], `"advisory"`)

	require.Equal(t, probe.SummaryRecordType, r2.Type)
	require.True(t, r2.Advisory)
	require.Contains(t, lines[2], `"advisory":true`)
}

func recordsByType(t *testing.T, stdout string, recordType string) []testRecordEnvelope {
	t.Helper()
	var out []testRecordEnvelope
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		if rec.Type == recordType {
			out = append(out, rec)
		}
	}
	return out
}

func TestReflowAdvisoryEnvelopeAllThreeBoundaries(t *testing.T) {
	validSummaryRaw, err := json.Marshal(probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"})
	require.NoError(t, err)
	workLine := reflowInputLine("source/file.xml", "etag", 7, "", "")

	t.Run("known malformed + marked is refused at all boundaries", func(t *testing.T) {
		for _, line := range []string{
			`{"type":"` + probe.SummaryRecordType + `","advisory":true,"data":{}}`,
			`{"type":"gonimbus.reflow.input.v1","advisory":true,"data":{}}`,
			`{"type":"gonimbus.index.object.v1","advisory":true,"data":{}}`,
		} {
			class, _ := classifyReflowFirstRecord(line)
			require.Equal(t, firstRecordRefuse, class, line)

			queue := make(chan reflowTask, 1)
			_, err := enqueueReflowLine(context.Background(), line, "existing", reflowSourceConfig{}, nil, queue)
			require.Error(t, err, line)
			require.Empty(t, queue)
		}
	})

	t.Run("recognized work and error families at unknown version + marker are refused at all boundaries", func(t *testing.T) {
		for _, recType := range []string{
			"gonimbus.reflow.input.v0",
			"gonimbus.reflow.input.v2",
			"gonimbus.index.object.v2",
			"gonimbus.index.query_receipt.v2",
			"gonimbus.index.complete.v1",
			"gonimbus.index.complete.v2",
			"gonimbus.index.manifest.v1",
			"gonimbus.index.latest.v1",
			"gonimbus.index.journal_header.v1",
			"gonimbus.index.journal_footer.v1",
			"gonimbus.index.spill_run_footer.v1",
			"gonimbus.index.object_record.v1",
			"gonimbus.index.hub_marker.v2",
			"gonimbus.index.acquired_bundle.v2",
			"gonimbus.index.build_result.v1",
			"gonimbus.crawl.selection_summary.v1",
			"gonimbus.selection.summary.v1",
			"gonimbus.content.probe.v1",
			"gonimbus.content.head.v1",
			"gonimbus.error.v1",
			"gonimbus.operation.error.v1",
			"gonimbus.warning.v1",
			"gonimbus.summary.v1",
			"gonimbus.cas.fail.v1",
		} {
			line := `{"type":"` + recType + `","advisory":true,"data":{"source_uri":"s3://b/k"}}`
			class, reason := classifyReflowFirstRecord(line)
			require.Equal(t, firstRecordRefuse, class, recType)
			require.Contains(t, reason, recType)
			require.Contains(t, reason, "cannot be skipped as advisory")

			queue := make(chan reflowTask, 1)
			_, err := enqueueReflowLine(context.Background(), line, "existing", reflowSourceConfig{}, nil, queue)
			require.Error(t, err, recType)
			require.Contains(t, err.Error(), recType)
			require.Contains(t, err.Error(), "cannot be skipped as advisory")
		}
	})

	t.Run("duplicate or case-variant envelope keys and uppercase record types are refused at all boundaries", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			line string
			want string
		}{
			{
				name: "uppercase_known_family",
				line: `{"type":"GONIMBUS.REFLOW.INPUT.V9","advisory":true,"data":{}}`,
				want: "cannot be skipped as advisory",
			},
			{
				name: "uppercase_unknown_family",
				line: `{"type":"GONIMBUS.FUTURE.METRIC.V1","advisory":true,"data":{}}`,
				want: `"GONIMBUS.FUTURE.METRIC.V1"`,
			},
			{
				name: "case_variant_advisory_key",
				line: `{"type":"gonimbus.future.metric.v1","Advisory":true,"data":{}}`,
				want: "advisory must be a boolean",
			},
			{
				name: "case_variant_type_key",
				line: `{"TYPE":"gonimbus.future.metric.v1","advisory":true,"data":{}}`,
				want: `"<empty>"`,
			},
			{
				name: "duplicate_advisory_false_then_true",
				line: `{"type":"gonimbus.future.metric.v1","advisory":false,"advisory":true,"data":{}}`,
				want: `duplicate top-level "advisory" key`,
			},
			{
				name: "duplicate_advisory_true_then_false",
				line: `{"type":"gonimbus.future.metric.v1","advisory":true,"advisory":false,"data":{}}`,
				want: `duplicate top-level "advisory" key`,
			},
			{
				name: "duplicate_type_key",
				line: `{"type":"gonimbus.future.metric.v1","type":"gonimbus.future.metric.v2","advisory":true,"data":{}}`,
				want: `duplicate top-level "type" key`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				class, reason := classifyReflowFirstRecord(tc.line)
				require.Equal(t, firstRecordRefuse, class)
				require.Contains(t, reason, tc.want)

				queue := make(chan reflowTask, 1)
				_, err := enqueueReflowLine(context.Background(), tc.line, "existing", reflowSourceConfig{}, nil, queue)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.want)
				require.Empty(t, queue)
			})
		}
	})

	t.Run("malformed advisory marker is refused at all boundaries", func(t *testing.T) {
		for _, marker := range []string{`"yes"`, `1`, `0`, `null`, `{}`, `[]`} {
			line := `{"type":"gonimbus.future.control.v1","advisory":` + marker + `,"data":` + string(validSummaryRaw) + `}`
			class, reason := classifyReflowFirstRecord(line)
			require.Equal(t, firstRecordRefuse, class, marker)
			require.Contains(t, reason, "advisory must be a boolean")
			require.Contains(t, reason, "gonimbus.future.control.v1")

			queue := make(chan reflowTask, 1)
			_, err := enqueueReflowLine(context.Background(), line, "existing", reflowSourceConfig{}, nil, queue)
			require.Error(t, err, marker)
			require.Contains(t, err.Error(), "advisory must be a boolean")
			require.Contains(t, err.Error(), "gonimbus.future.control.v1")
		}
	})

	t.Run("unknown marked and unmarked at first and later positions across CLI engine and pool", func(t *testing.T) {
		sentinel := "DO_NOT_ECHO_RAW_PAYLOAD"
		markedA1 := `{"type":"gonimbus.future.audit.v1","advisory":true,"data":{"secret":"` + sentinel + `","seq":1}}`
		markedA2 := `{"type":"gonimbus.future.audit.v1","advisory":true,"data":{"secret":"` + sentinel + `","seq":2}}`
		markedB := `{"type":"gonimbus.future.telemetry.v2","advisory":true,"data":{"secret":"` + sentinel + `"}}`
		markedSummary := `{"type":"` + probe.SummaryRecordType + `","advisory":true,"data":` + string(validSummaryRaw) + `}`
		unmarkedUnknown := `{"type":"gonimbus.future.unmarked.v1","producer_version":"v0.5.0","data":{"secret":"` + sentinel + `"}}`

		// First-record classification accepts marked unknown as fallback and refuses unmarked unknown.
		class, reason := classifyReflowFirstRecord(markedA1)
		require.Equal(t, firstRecordFallback, class)
		require.Empty(t, reason)

		class, reason = classifyReflowFirstRecord(unmarkedUnknown)
		require.Equal(t, firstRecordRefuse, class)
		require.Contains(t, reason, `"gonimbus.future.unmarked.v1"`)
		require.Contains(t, reason, `producer_version="v0.5.0"`)
		require.NotContains(t, reason, sentinel)

		// Later position on both engine (pool=false) and CLI pool (pool=true):
		// skips marked unknown with 1 warning per distinct type, accepts marked summary without warning.
		for _, pool := range []bool{false, true} {
			args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"}
			if pool {
				args = append(args, poolRoute...)
			}
			src, dst := newReflowMemoryProvider(), newReflowMemoryProvider()
			src.putFixture("source/file.xml", "payload", "etag", time.Time{})
			stream := strings.Join([]string{workLine, markedA1, markedA2, markedB, markedSummary}, "\n")
			stdout, stderr, err := runTransferReflowWithProviderFactory(t, src, dst, stream, args...)
			require.NoError(t, err, stderr)
			require.NotContains(t, stdout, sentinel)
			require.NotContains(t, stderr, sentinel)
			if pool {
				require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
			} else {
				require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
			}
			require.Equal(t, "complete", requireReflowData(t, stdout, "complete").Status)
			requireNoRecordType(t, stdout, "gonimbus.error.v1")

			warnings := recordsByType(t, stdout, reflowpkg.WarningRecordType)
			var skippedTypes []string
			for _, wRec := range warnings {
				var w reflowpkg.Warning
				require.NoError(t, json.Unmarshal(wRec.Data, &w))
				if w.Code == reflowpkg.AdvisoryRecordSkippedWarningCode {
					rt, _ := w.Details["record_type"].(string)
					skippedTypes = append(skippedTypes, rt)
				}
			}
			require.Equal(t, []string{"gonimbus.future.audit.v1", "gonimbus.future.telemetry.v2"}, skippedTypes)

			// Unmarked unknown at later position fails closed on both engine and CLI pool without echoing payload.
			src2, dst2 := newReflowMemoryProvider(), newReflowMemoryProvider()
			src2.putFixture("source/file.xml", "payload", "etag", time.Time{})
			badStream := strings.Join([]string{workLine, unmarkedUnknown}, "\n")
			badStdout, badStderr, badErr := runTransferReflowWithProviderFactory(t, src2, dst2, badStream, args...)
			require.Error(t, badErr)
			require.NotContains(t, badStdout, sentinel)
			require.NotContains(t, badStderr, sentinel)
			errData := requireErrorData(t, badStdout)
			require.Contains(t, errData.Message, `"gonimbus.future.unmarked.v1"`)
			require.Contains(t, errData.Message, `producer_version="v0.5.0"`)
		}

		// Marked unknown at first position followed by work line succeeds and emits warning.
		srcFirst, dstFirst := newReflowMemoryProvider(), newReflowMemoryProvider()
		srcFirst.putFixture("source/file.xml", "payload", "etag", time.Time{})
		firstStream := strings.Join([]string{markedA1, markedA2, workLine}, "\n")
		firstStdout, firstStderr, firstErr := runTransferReflowWithProviderFactory(t, srcFirst, dstFirst, firstStream,
			"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1",
		)
		require.NoError(t, firstErr, firstStderr)
		require.NotContains(t, firstStdout, sentinel)
		require.Equal(t, "complete", requireReflowData(t, firstStdout, "complete").Status)
		firstWarnings := recordsByType(t, firstStdout, reflowpkg.WarningRecordType)
		skippedFirstCount := 0
		for _, wRec := range firstWarnings {
			var w reflowpkg.Warning
			require.NoError(t, json.Unmarshal(wRec.Data, &w))
			if w.Code == reflowpkg.AdvisoryRecordSkippedWarningCode {
				skippedFirstCount++
				require.Equal(t, "gonimbus.future.audit.v1", w.Details["record_type"])
			}
		}
		require.Equal(t, 1, skippedFirstCount)
	})

	t.Run("warning cap reached emits aggregated summary warning on both CLI engine and pool", func(t *testing.T) {
		var lines []string
		lines = append(lines, workLine)
		for i := 0; i < reflowpkg.MaxAdvisoryWarningTypes+5; i++ {
			lines = append(lines, fmt.Sprintf(`{"type":"gonimbus.future.captype%02d.v1","advisory":true,"data":{}}`, i))
		}
		stream := strings.Join(lines, "\n")
		for _, pool := range []bool{false, true} {
			args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"}
			if pool {
				args = append(args, poolRoute...)
			}
			src, dst := newReflowMemoryProvider(), newReflowMemoryProvider()
			src.putFixture("source/file.xml", "payload", "etag", time.Time{})
			stdout, stderr, err := runTransferReflowWithProviderFactory(t, src, dst, stream, args...)
			require.NoError(t, err, stderr)

			warnings := recordsByType(t, stdout, reflowpkg.WarningRecordType)
			skippedCount := 0
			var capWarnings []reflowpkg.Warning
			for _, wRec := range warnings {
				var w reflowpkg.Warning
				require.NoError(t, json.Unmarshal(wRec.Data, &w))
				switch w.Code {
				case reflowpkg.AdvisoryRecordSkippedWarningCode:
					skippedCount++
				case reflowpkg.AdvisoryWarningCapReachedCode:
					capWarnings = append(capWarnings, w)
				}
			}
			require.Equal(t, reflowpkg.MaxAdvisoryWarningTypes, skippedCount)
			require.Len(t, capWarnings, 1)
			require.EqualValues(t, reflowpkg.MaxAdvisoryWarningTypes+5, capWarnings[0].Details["skipped_records"])
			require.EqualValues(t, reflowpkg.MaxAdvisoryWarningTypes+5, capWarnings[0].Details["distinct_types"])
			require.EqualValues(t, 5, capWarnings[0].Details["unreported_records"])
			require.EqualValues(t, 5, capWarnings[0].Details["unreported_types"])
		}
	})

	t.Run("zero-work stream of only skipped advisory records succeeds with exit 0 on both CLI engine and pool", func(t *testing.T) {
		markedSummary := `{"type":"` + probe.SummaryRecordType + `","advisory":true,"data":` + string(validSummaryRaw) + `}`
		markedFuture := `{"type":"gonimbus.future.control.v1","advisory":true,"data":{"note":"zero-work"}}`
		stream := strings.Join([]string{markedFuture, markedSummary}, "\n")
		for _, pool := range []bool{false, true} {
			args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--rewrite-from", "{key}", "--rewrite-to", "{key}", "--parallel", "1"}
			if pool {
				args = append(args, poolRoute...)
			}
			src, dst := newReflowMemoryProvider(), newReflowMemoryProvider()
			stdout, stderr, err := runTransferReflowWithProviderFactory(t, src, dst, stream, args...)
			require.NoError(t, err, stderr)
			requireNoRecordType(t, stdout, reflowpkg.RecordType)
			requireNoRecordType(t, stdout, "gonimbus.error.v1")

			warnings := recordsByType(t, stdout, reflowpkg.WarningRecordType)
			skippedCount := 0
			for _, wRec := range warnings {
				var w reflowpkg.Warning
				require.NoError(t, json.Unmarshal(wRec.Data, &w))
				if w.Code == reflowpkg.AdvisoryRecordSkippedWarningCode {
					skippedCount++
					require.Equal(t, "gonimbus.future.control.v1", w.Details["record_type"])
				}
			}
			require.Equal(t, 1, skippedCount)
		}
	})
}
