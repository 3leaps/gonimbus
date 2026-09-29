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
