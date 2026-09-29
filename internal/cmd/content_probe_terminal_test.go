package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type probeFailWriter struct {
	calls int
	err   error
}

func TestProbeTerminalHandledEmptySetupAndDataOutcomes(t *testing.T) {
	oldStdin, oldConcurrency, oldBytes, oldConfig, oldEmit, oldRewrite := contentProbeStdin, contentProbeConcurrency, contentProbeBytes, contentProbeConfigPath, contentProbeEmit, contentProbeRewriteFrom
	oldProvider := newContentProbeProvider
	t.Cleanup(func() {
		contentProbeStdin, contentProbeConcurrency, contentProbeBytes, contentProbeConfigPath, contentProbeEmit, contentProbeRewriteFrom = oldStdin, oldConcurrency, oldBytes, oldConfig, oldEmit, oldRewrite
		newContentProbeProvider = oldProvider
	})
	contentProbeStdin, contentProbeConcurrency, contentProbeBytes, contentProbeEmit, contentProbeRewriteFrom = true, 1, 4096, "both", ""
	contentProbeConfigPath = filepath.Join(t.TempDir(), "probe.yaml")
	require.NoError(t, os.WriteFile(contentProbeConfigPath, []byte("extract:\n  - name: value\n    type: regex\n    pattern: 'value=([a-z]+)'\n    group: 1\n    required: true\n    on_missing: fail\n"), 0600))
	for _, tc := range []struct {
		name, input, body, termination string
		exit                           int
		processed, lines               int64
	}{
		{"empty", "", "", "completed", 0, 0, 1},
		{"success", "s3://bucket/key", "value=valid", "completed", 0, 1, 3},
		{"data", "s3://bucket/key", "missing", "completed_with_errors", 60, 1, 2},
		{"invalid", "not-a-uri", "", "failed", 40, 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newContentProbeProvider = func(context.Context, *uri.ObjectURI, int) (contentProbeProvider, error) {
				return newRangeProbeProvider("key", []byte(tc.body)), nil
			}
			cmd := &cobra.Command{}
			cmd.SetContext(context.Background())
			cmd.SetIn(strings.NewReader(tc.input))
			var buf bytes.Buffer
			cmd.SetOut(&buf)
			err := runContentProbe(cmd, nil)
			if tc.exit == 0 {
				require.NoError(t, err)
			} else {
				var cause *probeExitCause
				require.ErrorAs(t, err, &cause)
				require.Equal(t, tc.exit, cause.code)
			}
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			require.Len(t, lines, int(tc.lines))
			var env output.Record
			require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &env))
			require.Equal(t, probe.SummaryRecordType, env.Type)
			summary, err := probe.ParseSummary(env.Data)
			require.NoError(t, err)
			require.Equal(t, tc.termination, summary.Termination)
			require.Equal(t, tc.exit, summary.ExitCode)
			require.Equal(t, tc.processed, summary.Processed)
			for _, line := range lines {
				var rec output.Record
				require.NoError(t, json.Unmarshal([]byte(line), &rec))
				require.Equal(t, env.JobID, rec.JobID)
			}
		})
	}
	contentProbeConfigPath = filepath.Join(t.TempDir(), "missing.yaml")
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	require.Error(t, runContentProbe(cmd, nil))
	require.Contains(t, buf.String(), probe.SummaryRecordType)
}

func (w *probeFailWriter) Write(p []byte) (int, error) { w.calls++; return len(p) / 2, w.err }

func TestProbeTerminalWriterLatchesBeforeConcurrentAndFinalWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("synthetic output failure")
	dst := &probeFailWriter{err: failure}
	w := newProbeTerminalWriter(output.NewJSONLWriter(dst, "job", "s3"), cancel)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.WriteAny(context.Background(), "gonimbus.content.probe.v1", map[string]int{"value": 1})
		}()
	}
	wg.Wait()
	require.ErrorIs(t, w.outputFailure(), failure)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, w.WriteError(context.Background(), &output.ErrorRecord{Code: output.ErrCodeInternal}), failure)
	require.ErrorIs(t, w.WriteAny(context.Background(), probe.SummaryRecordType, w.snapshot()), failure)
	require.Equal(t, 1, dst.calls, "no late record after the first partial write")
	require.Zero(t, w.snapshot().Emitted.Probe)
	require.Zero(t, w.snapshot().Errors)
}
