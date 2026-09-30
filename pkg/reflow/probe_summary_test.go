package reflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

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
