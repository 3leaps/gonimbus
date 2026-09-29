package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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
