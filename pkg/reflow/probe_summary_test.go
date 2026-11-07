package reflow

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func TestRunnerProbeSummaryOnlyIsNotAnObject(t *testing.T) {
	raw, err := json.Marshal(probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"})
	require.NoError(t, err)
	line := `{"type":"` + probe.SummaryRecordType + `","data":` + string(raw) + `}`
	sink := &collectSink{}
	runner, err := NewRunner(dryRunConfig(sink))
	require.NoError(t, err)
	_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(line), Resolve: func(context.Context, string) (provider.Provider, error) {
		t.Fatal("control record must not resolve a provider")
		return nil, nil
	}})
	require.NoError(t, err)
	require.Empty(t, sink.records)
	require.Empty(t, sink.errs)
	_, err = parseReflowInputLine(`{"type":"` + probe.SummaryRecordType + `","data":{}}`)
	require.Error(t, err)
	require.NotErrorIs(t, err, errProbeSummaryControl)
}

func TestRunnerRejectsMalformedProbeSummaryHistogram(t *testing.T) {
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
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(line), Resolve: func(context.Context, string) (provider.Provider, error) {
			t.Fatal("malformed control must not resolve provider")
			return nil, nil
		}})
		var invalid *InvalidInputsError
		require.ErrorAs(t, err, &invalid, histogram)
		require.Empty(t, sink.records)
		require.Len(t, sink.errs, 1)
	}
}

func TestRunnerAdvisoryEnvelopeContract(t *testing.T) {
	validSummaryRaw, err := json.Marshal(probe.Summary{ErrorsByCode: map[string]int64{}, Termination: "completed"})
	require.NoError(t, err)
	validWork := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/source/a.xml","source_key":"source/a.xml","dest_rel_key":"source/a.xml"}}`

	t.Run("known summary with advisory=true dispatches as known without skipped warning", func(t *testing.T) {
		line := `{"type":"` + probe.SummaryRecordType + `","advisory":true,"data":` + string(validSummaryRaw) + `}`
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(validWork + "\n" + line)})
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		require.Empty(t, sink.errs)
		for _, w := range sink.warnings {
			require.NotEqual(t, AdvisoryRecordSkippedWarningCode, w.Code)
		}
	})

	t.Run("known malformed + marked is refused", func(t *testing.T) {
		for _, line := range []string{
			`{"type":"` + probe.SummaryRecordType + `","advisory":true,"data":{}}`,
			`{"type":"gonimbus.reflow.input.v1","advisory":true,"data":{}}`,
			`{"type":"gonimbus.index.object.v1","advisory":true,"data":{}}`,
		} {
			_, err := parseReflowInputLine(line)
			require.Error(t, err, line)
			require.NotErrorIs(t, err, errProbeSummaryControl)
			require.NotErrorIs(t, err, errAdvisoryControl)

			sink := &collectSink{}
			runner, runErr := NewRunner(dryRunConfig(sink))
			require.NoError(t, runErr)
			_, runErr = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(line)})
			var invalid *InvalidInputsError
			require.ErrorAs(t, runErr, &invalid, line)
			require.Empty(t, sink.records)
			require.Len(t, sink.errs, 1)
		}
	})

	t.Run("unknown marked at first and later positions skips with one bounded sanitized warning per type", func(t *testing.T) {
		sentinel := "SENSITIVE_ADVISORY_PAYLOAD_DO_NOT_ECHO"
		unknownA1 := `{"type":"gonimbus.future.audit.v1","advisory":true,"data":{"token":"` + sentinel + `","seq":1}}`
		unknownA2 := `{"type":"gonimbus.future.audit.v1","advisory":true,"data":{"token":"` + sentinel + `","seq":2}}`
		unknownB := `{"type":"gonimbus.future.telemetry.v2","advisory":true,"data":{"secret":"` + sentinel + `"}}`

		stream := strings.Join([]string{unknownA1, validWork, unknownA2, unknownB}, "\n")
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(stream)})
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		require.Empty(t, sink.errs)

		var skipped []Warning
		for _, w := range sink.warnings {
			if w.Code == AdvisoryRecordSkippedWarningCode {
				skipped = append(skipped, w)
			}
			rawW, mErr := json.Marshal(w)
			require.NoError(t, mErr)
			require.NotContains(t, string(rawW), sentinel)
		}
		require.Len(t, skipped, 2, "must emit at most one warning per distinct unknown advisory type")
		require.Equal(t, "gonimbus.future.audit.v1", skipped[0].Details["record_type"])
		require.Equal(t, "gonimbus.future.telemetry.v2", skipped[1].Details["record_type"])
	})

	t.Run("advisory warning tracker caps distinct types per run and emits cap summary warning", func(t *testing.T) {
		var lines []string
		for i := 0; i < MaxAdvisoryWarningTypes+8; i++ {
			lines = append(lines, fmt.Sprintf(`{"type":"gonimbus.future.type%02d.v1","advisory":true,"data":{}}`, i))
		}
		lines = append(lines, validWork)
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		_, err = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(strings.Join(lines, "\n"))})
		require.NoError(t, err)
		require.Len(t, sink.records, 1)
		require.Empty(t, sink.errs)
		skippedCount := 0
		var capWarnings []Warning
		for _, w := range sink.warnings {
			switch w.Code {
			case AdvisoryRecordSkippedWarningCode:
				skippedCount++
			case AdvisoryWarningCapReachedCode:
				capWarnings = append(capWarnings, w)
			}
		}
		require.Equal(t, MaxAdvisoryWarningTypes, skippedCount)
		require.Len(t, capWarnings, 1)
		require.EqualValues(t, MaxAdvisoryWarningTypes+8, capWarnings[0].Details["skipped_records"])
		require.EqualValues(t, MaxAdvisoryWarningTypes+8, capWarnings[0].Details["distinct_types"])
		require.EqualValues(t, 8, capWarnings[0].Details["unreported_records"])
		require.EqualValues(t, 8, capWarnings[0].Details["unreported_types"])
		require.EqualValues(t, MaxAdvisoryWarningTypes, capWarnings[0].Details["warning_cap"])
	})

	t.Run("zero-work stream of only skipped advisory records succeeds cleanly", func(t *testing.T) {
		markedSummary := `{"type":"` + probe.SummaryRecordType + `","advisory":true,"data":` + string(validSummaryRaw) + `}`
		markedFuture := `{"type":"gonimbus.future.control.v1","advisory":true,"data":{"note":"noop"}}`
		sink := &collectSink{}
		runner, err := NewRunner(dryRunConfig(sink))
		require.NoError(t, err)
		sum, err := runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(markedFuture + "\n" + markedSummary)})
		require.NoError(t, err)
		require.Empty(t, sink.records)
		require.Empty(t, sink.errs)
		require.EqualValues(t, 0, sum.InvalidInputs)
		require.EqualValues(t, 0, sum.Errors)
		var skipped []Warning
		for _, w := range sink.warnings {
			if w.Code == AdvisoryRecordSkippedWarningCode {
				skipped = append(skipped, w)
			}
		}
		require.Len(t, skipped, 1)
		require.Equal(t, "gonimbus.future.control.v1", skipped[0].Details["record_type"])
	})

	t.Run("duplicate or case-variant envelope keys and uppercase record types are refused", func(t *testing.T) {
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
				want: `unsupported record type "GONIMBUS.FUTURE.METRIC.V1"`,
			},
			{
				name: "case_variant_advisory_key",
				line: `{"type":"gonimbus.future.metric.v1","Advisory":true,"data":{}}`,
				want: "advisory must be a boolean",
			},
			{
				name: "case_variant_type_key",
				line: `{"TYPE":"gonimbus.future.metric.v1","advisory":true,"data":{}}`,
				want: `unsupported record type "<empty>"`,
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
				_, err := parseReflowInputLine(tc.line)
				require.Error(t, err)
				require.NotErrorIs(t, err, errAdvisoryControl)
				require.Contains(t, err.Error(), tc.want)
			})
		}
	})

	t.Run("unknown unmarked or advisory=false at first and later positions fails closed", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			line string
			want string
		}{
			{
				name: "unmarked_unknown",
				line: `{"type":"gonimbus.future.control.v1","data":{"secret":"REDACT"}}`,
				want: `unsupported record type "gonimbus.future.control.v1"`,
			},
			{
				name: "advisory_false_unknown",
				line: `{"type":"gonimbus.future.control.v1","advisory":false,"data":{"secret":"REDACT"}}`,
				want: `unsupported record type "gonimbus.future.control.v1"`,
			},
			{
				name: "unmarked_with_producer_version",
				line: `{"type":"gonimbus.future.control.v1","producer_version":"v0.5.0","data":{}}`,
				want: `unsupported record type "gonimbus.future.control.v1" (producer_version="v0.5.0")`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := parseReflowInputLine(tc.line)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.want)
				require.NotContains(t, err.Error(), "REDACT")
				if !strings.Contains(tc.line, "producer_version") {
					require.NotContains(t, err.Error(), "producer_version")
				}

				for _, stream := range []string{tc.line + "\n" + validWork, validWork + "\n" + tc.line} {
					sink := &collectSink{}
					runner, runErr := NewRunner(dryRunConfig(sink))
					require.NoError(t, runErr)
					_, runErr = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(stream)})
					var invalid *InvalidInputsError
					require.ErrorAs(t, runErr, &invalid)
					require.Len(t, sink.errs, 1)
					require.Contains(t, sink.errs[0].Message, tc.want)
					require.NotContains(t, sink.errs[0].Message, "REDACT")
				}
			})
		}
	})

	t.Run("recognized work, integrity, and error families at unknown version + advisory=true are refused", func(t *testing.T) {
		for _, recType := range []string{
			"gonimbus.reflow.input.v0",
			"gonimbus.reflow.input.v2",
			"gonimbus.index.object.v2",
			"gonimbus.index.canonical_object.v1",
			"gonimbus.index.query_receipt.v1",
			"gonimbus.index.query_receipt.v2",
			"gonimbus.crawl.selection_summary.v1",
			"gonimbus.content.probe.v1",
			"gonimbus.content.probe.v2",
			"gonimbus.error.v1",
			"gonimbus.error.v2",
			"gonimbus.object.v1",
		} {
			line := `{"type":"` + recType + `","advisory":true,"data":{"source_uri":"s3://source-bucket/source/a.xml"}}`
			_, err := parseReflowInputLine(line)
			require.Error(t, err, recType)
			require.Contains(t, err.Error(), recType)
			require.Contains(t, err.Error(), "cannot be skipped as advisory")

			sink := &collectSink{}
			runner, runErr := NewRunner(dryRunConfig(sink))
			require.NoError(t, runErr)
			_, runErr = runner.Run(context.Background(), RecordStreamSource{Records: strings.NewReader(line)})
			var invalid *InvalidInputsError
			require.ErrorAs(t, runErr, &invalid, recType)
			require.Empty(t, sink.records)
			require.Len(t, sink.errs, 1)
		}
	})

	t.Run("malformed non-boolean advisory marker is refused", func(t *testing.T) {
		for _, marker := range []string{`"yes"`, `1`, `0`, `null`, `{}`, `[]`} {
			for _, recType := range []string{"gonimbus.future.control.v1", probe.SummaryRecordType, ReflowInputRecordType} {
				line := `{"type":"` + recType + `","advisory":` + marker + `,"data":` + string(validSummaryRaw) + `}`
				_, err := parseReflowInputLine(line)
				require.Error(t, err, "%s / %s", recType, marker)
				require.Contains(t, err.Error(), "advisory must be a boolean")
				require.Contains(t, err.Error(), recType)
			}
		}
	})
}

func TestFirstPartyRecordTypesCoveredByNonAdvisoryDenylist(t *testing.T) {
	versionedTypeRE := regexp.MustCompile(`^gonimbus\.[a-z0-9_.-]+\.v[0-9]+$`)
	fset := token.NewFileSet()
	discovered := make(map[string]struct{})

	err := filepath.WalkDir(filepath.Join("..", ".."), func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "vendor" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileAST, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		ast.Inspect(fileAST, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			val, unquoteErr := strconv.Unquote(lit.Value)
			if unquoteErr != nil {
				return true
			}
			if versionedTypeRE.MatchString(val) {
				discovered[val] = struct{}{}
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)

	var types []string
	for rt := range discovered {
		types = append(types, rt)
	}
	sort.Strings(types)
	require.GreaterOrEqual(t, len(types), 45, "expected AST walk to discover first-party gonimbus.*.vN record types")

	for _, mustInclude := range []string{
		"gonimbus.index.complete.v1",
		"gonimbus.index.complete.v2",
		"gonimbus.index.manifest.v1",
		"gonimbus.index.latest.v1",
		"gonimbus.index.journal_header.v1",
		"gonimbus.index.journal_footer.v1",
		"gonimbus.index.spill_run_footer.v1",
		"gonimbus.index.object_record.v1",
		"gonimbus.index.hub_marker.v1",
		"gonimbus.index.hub_marker.v2",
		"gonimbus.index.hub_marker.v3",
		"gonimbus.index.acquired_bundle.v1",
		"gonimbus.index.acquired_bundle.v2",
		"gonimbus.index.build_result.v1",
		"gonimbus.operation.error.v1",
		"gonimbus.warning.v1",
		"gonimbus.summary.v1",
		"gonimbus.selection.summary.v1",
		"gonimbus.content.head.v1",
		"gonimbus.cas.fail.v1",
	} {
		require.Contains(t, discovered, mustInclude)
	}

	for _, rt := range types {
		if output.IsAdvisoryProducerRecordType(rt) {
			require.Equal(t, probe.SummaryRecordType, rt)
			continue
		}
		require.True(t, IsNonAdvisoryFamily(rt), "first-party record type %q must be covered by IsNonAdvisoryFamily or explicitly allowlisted in output.IsAdvisoryProducerRecordType", rt)
		line := `{"type":"` + rt + `","advisory":true,"data":{}}`
		_, parseErr := parseReflowInputLine(line)
		require.Error(t, parseErr, "first-party non-advisory record type %q must be refused even when marked advisory=true", rt)
		require.NotErrorIs(t, parseErr, errAdvisoryControl, "first-party non-advisory record type %q must never be skipped as advisory", rt)
		require.NotErrorIs(t, parseErr, errProbeSummaryControl)
	}
}
