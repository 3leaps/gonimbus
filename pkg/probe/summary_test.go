package probe

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSummaryExactControlValidation(t *testing.T) {
	s := Summary{Termination: "completed", ErrorsByCode: map[string]int64{}}
	raw, err := json.Marshal(s)
	require.NoError(t, err)
	parsed, err := ParseSummary(raw)
	require.NoError(t, err)
	require.Equal(t, s, parsed)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for key := range fields {
		t.Run(key, func(t *testing.T) {
			copyFields := make(map[string]json.RawMessage)
			for k, v := range fields {
				copyFields[k] = v
			}
			delete(copyFields, key)
			missing, err := json.Marshal(copyFields)
			require.NoError(t, err)
			_, err = ParseSummary(missing)
			require.Error(t, err)
			copyFields[key] = json.RawMessage("null")
			null, err := json.Marshal(copyFields)
			require.NoError(t, err)
			_, err = ParseSummary(null)
			require.Error(t, err)
		})
	}
	for _, replacement := range []string{`"emitted":{}`, `"emitted":{"probe":0}`, `"routing":{}`, `"inputs":-1`, `"inputs":1.5`, `"termination":"unknown"`, `"exit_code":60`, `"errors_by_code":{"https://example.test?token=secret":1}`} {
		var change map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte("{"+replacement+"}"), &change))
		copyFields := make(map[string]json.RawMessage)
		for k, v := range fields {
			copyFields[k] = v
		}
		for k, v := range change {
			copyFields[k] = v
		}
		invalid, err := json.Marshal(copyFields)
		require.NoError(t, err)
		_, err = ParseSummary(invalid)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
	_, err = ParseSummary(json.RawMessage(strings.Replace(string(raw), `"inputs":0`, `"extra":0,"inputs":0`, 1)))
	require.Error(t, err)
}

func TestSummaryCounterAndTerminationInvariants(t *testing.T) {
	for _, s := range []Summary{
		{Inputs: 1, Enumerated: 2, Processed: 2, Routing: RoutingCounts{Normal: 1}, Errors: 1, ErrorsByCode: map[string]int64{"INTERNAL": 1}, Termination: "completed_with_errors", ExitCode: 60},
		{Enumerated: 2, Processed: 1, Errors: 1, ErrorsByCode: map[string]int64{"PROVIDER_UNAVAILABLE": 1}, Termination: "failed", ExitCode: 32},
		{Enumerated: 2, Processed: 1, ErrorsByCode: map[string]int64{}, Termination: "aborted", ExitCode: 130},
	} {
		require.NoError(t, s.Validate())
	}
	for _, s := range []Summary{
		{Processed: 1, ErrorsByCode: map[string]int64{}, Termination: "completed"},
		{Enumerated: 1, ErrorsByCode: map[string]int64{}, Termination: "completed"},
		{Errors: 1, ErrorsByCode: map[string]int64{}, Termination: "failed", ExitCode: 32},
		{ErrorsByCode: map[string]int64{"UNKNOWN": 0}, Termination: "completed"},
		{ErrorsByCode: map[string]int64{}, Termination: "failed", ExitCode: 0},
	} {
		require.Error(t, s.Validate())
	}
}
