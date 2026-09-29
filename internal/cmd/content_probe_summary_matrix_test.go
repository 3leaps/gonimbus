package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func probeSummaryTestCommand(t *testing.T, input string, prov contentProbeProvider) (*cobra.Command, *bytes.Buffer) {
	t.Helper()
	oldStdin, oldConcurrency, oldBytes, oldConfig, oldEmit, oldRewrite := contentProbeStdin, contentProbeConcurrency, contentProbeBytes, contentProbeConfigPath, contentProbeEmit, contentProbeRewriteFrom
	oldProvider := newContentProbeProvider
	t.Cleanup(func() {
		contentProbeStdin, contentProbeConcurrency, contentProbeBytes, contentProbeConfigPath, contentProbeEmit, contentProbeRewriteFrom = oldStdin, oldConcurrency, oldBytes, oldConfig, oldEmit, oldRewrite
		newContentProbeProvider = oldProvider
	})
	contentProbeStdin, contentProbeConcurrency, contentProbeBytes, contentProbeEmit, contentProbeRewriteFrom = true, 1, 4096, "both", ""
	contentProbeConfigPath = filepath.Join(t.TempDir(), "probe.yaml")
	require.NoError(t, os.WriteFile(contentProbeConfigPath, []byte("extract:\n  - name: value\n    type: regex\n    pattern: 'value=([a-z]+)'\n    group: 1\n    required: true\n    on_missing: fail\n"), 0600))
	newContentProbeProvider = func(context.Context, *uri.ObjectURI, int) (contentProbeProvider, error) { return prov, nil }
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetIn(strings.NewReader(input))
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	return cmd, buf
}

func terminalProbeSummary(t *testing.T, stdout string) probe.Summary {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	var last output.Record
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &last))
	require.Equal(t, probe.SummaryRecordType, last.Type)
	s, err := probe.ParseSummary(last.Data)
	require.NoError(t, err)
	count := 0
	for _, line := range lines {
		var rec output.Record
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		require.Equal(t, last.JobID, rec.JobID)
		if rec.Type == probe.SummaryRecordType {
			count++
		}
	}
	require.Equal(t, 1, count)
	return s
}

type probePageProvider struct {
	*rangeProbeProvider
	mu     sync.Mutex
	tokens []string
	calls  int
}

func (p *probePageProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.calls
	p.calls++
	if n >= len(p.tokens) {
		return nil, errors.New("unexpected extra LIST")
	}
	return &provider.ListResult{Objects: []provider.ObjectSummary{{Key: "key"}}, IsTruncated: true, ContinuationToken: p.tokens[n]}, nil
}

func TestProbeSummaryRefusesInvalidPaginationWithoutTokenEcho(t *testing.T) {
	for _, tokens := range [][]string{{""}, {"private-token", "private-token"}, {"private-token-a", "private-token-b", "private-token-a"}} {
		p := &probePageProvider{rangeProbeProvider: newRangeProbeProvider("key", []byte("value=valid")), tokens: tokens}
		cmd, buf := probeSummaryTestCommand(t, "s3://bucket/prefix/", p)
		err := runContentProbe(cmd, nil)
		var cause *probeExitCause
		require.ErrorAs(t, err, &cause)
		require.Equal(t, 32, cause.code)
		s := terminalProbeSummary(t, buf.String())
		require.Equal(t, "failed", s.Termination)
		require.Equal(t, int64(len(tokens)), s.Enumerated)
		require.Equal(t, s.Enumerated, s.Processed)
		require.Equal(t, len(tokens), p.calls)
		require.NotContains(t, buf.String(), "private-token")
	}
}

func TestProbeSummaryUpstreamErrorExitMatrixAndBoundedCodes(t *testing.T) {
	for _, tc := range []struct {
		code, termination string
		exit              int
		actual            string
	}{
		{"NOT_FOUND", "completed_with_errors", 60, "NOT_FOUND"},
		{"ACCESS_DENIED", "failed", 32, "ACCESS_DENIED"},
		{"TIMEOUT", "failed", 32, "TIMEOUT"},
		{"THROTTLED", "failed", 32, "THROTTLED"},
		{"TRANSIENT", "failed", 32, "TRANSIENT"},
		{"PROVIDER_UNAVAILABLE", "failed", 32, "PROVIDER_UNAVAILABLE"},
		{"INTERNAL", "failed", 32, "INTERNAL"},
		{"INVALID_INPUT", "failed", 40, "INVALID_INPUT"},
		{"https://example.test?token=secret", "failed", 40, "INVALID_INPUT"},
	} {
		t.Run(tc.actual+tc.code, func(t *testing.T) {
			raw, err := json.Marshal(output.ErrorRecord{Code: tc.code, Message: "upstream"})
			require.NoError(t, err)
			cmd, buf := probeSummaryTestCommand(t, `{"type":"gonimbus.error.v1","data":`+string(raw)+`}`, nil)
			err = runContentProbe(cmd, nil)
			var cause *probeExitCause
			require.ErrorAs(t, err, &cause)
			require.Equal(t, tc.exit, cause.code)
			s := terminalProbeSummary(t, buf.String())
			require.Equal(t, tc.termination, s.Termination)
			require.Equal(t, map[string]int64{tc.actual: 1}, s.ErrorsByCode)
			require.Zero(t, s.Enumerated)
			require.NotContains(t, buf.String(), "secret")
		})
	}
}

type probePartialErrorReader struct{ delivered bool }

func (r *probePartialErrorReader) Read(p []byte) (int, error) {
	if r.delivered {
		return 0, io.EOF
	}
	r.delivered = true
	return copy(p, []byte("partial")), errors.New("synthetic read failure")
}

type probePartialProvider struct{ *rangeProbeProvider }

func (p *probePartialProvider) GetRange(context.Context, string, int64, int64) (io.ReadCloser, int64, error) {
	return io.NopCloser(&probePartialErrorReader{}), 100, nil
}

func TestProbeSummaryCountsPartialReadBeforeInfrastructureError(t *testing.T) {
	p := &probePartialProvider{newRangeProbeProvider("key", []byte("value=valid"))}
	cmd, buf := probeSummaryTestCommand(t, "s3://bucket/key", p)
	require.Error(t, runContentProbe(cmd, nil))
	s := terminalProbeSummary(t, buf.String())
	require.Equal(t, "failed", s.Termination)
	require.Equal(t, int64(7), s.BytesRead)
	require.Equal(t, int64(1), s.Processed)
}

type probeCancelledProvider struct {
	*rangeProbeProvider
	started chan struct{}
}

func (p *probeCancelledProvider) Head(ctx context.Context, _ string) (*provider.ObjectMeta, error) {
	close(p.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestProbeSummaryCancellationDrainsWithHealthyFinalization(t *testing.T) {
	p := &probeCancelledProvider{rangeProbeProvider: newRangeProbeProvider("key", nil), started: make(chan struct{})}
	cmd, buf := probeSummaryTestCommand(t, "s3://bucket/key", p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd.SetContext(ctx)
	done := make(chan error, 1)
	go func() { done <- runContentProbe(cmd, nil) }()
	<-p.started
	cancel()
	err := <-done
	var cause *probeExitCause
	require.ErrorAs(t, err, &cause)
	require.Equal(t, 130, cause.code)
	s := terminalProbeSummary(t, buf.String())
	require.Equal(t, "aborted", s.Termination)
	require.Equal(t, int64(1), s.Enumerated)
	require.Zero(t, s.Processed)
}

type probeFailAtRecordWriter struct {
	bytes.Buffer
	calls, failAt int
}

func (w *probeFailAtRecordWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		n, _ := w.Buffer.Write(p[:len(p)/2])
		return n, errors.New("synthetic output failure")
	}
	return w.Buffer.Write(p)
}

func TestProbeSummaryOutputFailureOverridesDataAndSummaryOutcomes(t *testing.T) {
	for _, failAt := range []int{1, 3} {
		cmd, _ := probeSummaryTestCommand(t, "s3://bucket/key", newRangeProbeProvider("key", []byte("value=valid")))
		w := &probeFailAtRecordWriter{failAt: failAt}
		cmd.SetOut(w)
		err := runContentProbe(cmd, nil)
		var cause *probeExitCause
		require.ErrorAs(t, err, &cause)
		require.Equal(t, 1, cause.code)
		require.Equal(t, failAt, w.calls, "no write follows failed output, including summary failure")
		require.False(t, strings.HasSuffix(w.String(), "\n"), "failed partial line stays terminal rather than being followed by a success record")
	}
}

type probeMixedProvider struct {
	*rangeProbeProvider
	failure error
}

func (p *probeMixedProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	if key == "failure" && p.failure != nil {
		return nil, p.failure
	}
	return p.rangeProbeProvider.Head(ctx, key)
}
func (p *probeMixedProvider) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, int64, error) {
	if key == "failure" {
		return io.NopCloser(strings.NewReader("missing")), 7, nil
	}
	return p.rangeProbeProvider.GetRange(ctx, key, start, end)
}

func TestProbeSummaryMixedDataInfrastructureAndQuarantineModes(t *testing.T) {
	for _, mode := range []string{"probe", "reflow-input", "both"} {
		for _, tc := range []struct {
			name       string
			failure    error
			quarantine bool
			exit       int
		}{
			{"data", nil, false, 60}, {"not-found", provider.ErrNotFound, false, 60},
			{"access", provider.ErrAccessDenied, false, 32}, {"unavailable", provider.ErrProviderUnavailable, false, 32},
			{"quarantine", nil, true, 0},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				p := &probeMixedProvider{rangeProbeProvider: newRangeProbeProvider("key", []byte("value=valid")), failure: tc.failure}
				cmd, buf := probeSummaryTestCommand(t, "s3://bucket/key\ns3://bucket/failure", p)
				contentProbeEmit = mode
				if tc.quarantine {
					raw, err := os.ReadFile(contentProbeConfigPath)
					require.NoError(t, err)
					raw = []byte("quarantine_prefix: _quarantine\n" + strings.ReplaceAll(string(raw), "on_missing: fail", "on_missing: quarantine"))
					require.NoError(t, os.WriteFile(contentProbeConfigPath, raw, 0600))
				}
				err := runContentProbe(cmd, nil)
				if tc.exit == 0 {
					require.NoError(t, err)
				} else {
					var cause *probeExitCause
					require.ErrorAs(t, err, &cause)
					require.Equal(t, tc.exit, cause.code)
				}
				s := terminalProbeSummary(t, buf.String())
				require.Equal(t, int64(2), s.Processed)
				require.Equal(t, s.Processed, s.Enumerated)
				successes := int64(1)
				if tc.quarantine {
					successes = 2
					require.Equal(t, int64(1), s.Routing.Quarantine)
					require.Zero(t, s.Errors)
				}
				if mode == "probe" || mode == "both" {
					require.Equal(t, successes, s.Emitted.Probe)
				} else {
					require.Zero(t, s.Emitted.Probe)
				}
				if mode == "reflow-input" || mode == "both" {
					require.Equal(t, successes, s.Emitted.ReflowInput)
				} else {
					require.Zero(t, s.Emitted.ReflowInput)
				}
				require.Equal(t, int64(1), s.Routing.Normal)
			})
		}
	}
}
