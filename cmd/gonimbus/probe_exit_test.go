package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/stretchr/testify/require"
)

// This builds and executes the actual CLI, not a helper that formats an exit
// code. Full producer/mover pipe integration is tested separately.
func TestProbeActualProcessExitContract(t *testing.T) {
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	dir := t.TempDir()
	binary := filepath.Join(dir, "gonimbus")
	build := exec.Command("go", "build", "-o", binary, ".") // #nosec G204 -- fixed Go build command and test-owned destination.
	build.Dir = "."
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, string(buildOutput))
	config := filepath.Join(dir, "probe.yaml")
	require.NoError(t, os.WriteFile(config, []byte("extract:\n  - name: value\n    type: regex\n    pattern: 'value=([a-z]+)'\n    group: 1\n    required: true\n    on_missing: fail\n"), 0600))
	object := filepath.Join(dir, "object.txt")
	require.NoError(t, os.WriteFile(object, []byte("missing"), 0600))
	newCommand := func(ctx context.Context, extra ...string) *exec.Cmd {
		args := append([]string{"content", "probe", "--stdin", "--config", config, "--concurrency", "1"}, extra...)
		cmd := exec.CommandContext(ctx, binary, args...) // #nosec G204 -- test-built local binary and fixed CLI arguments.
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+dir, "AWS_ACCESS_KEY_ID=synthetic", "AWS_SECRET_ACCESS_KEY=synthetic", "AWS_EC2_METADATA_DISABLED=true")
		return cmd
	}
	checkExit := func(t *testing.T, err error, code int) {
		t.Helper()
		var exited *exec.ExitError
		require.ErrorAs(t, err, &exited)
		require.Equal(t, code, exited.ExitCode())
	}
	checkSummary := func(t *testing.T, stdout string, code int) {
		t.Helper()
		lines := strings.Split(strings.TrimSpace(stdout), "\n")
		var rec output.Record
		require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &rec))
		require.Equal(t, probe.SummaryRecordType, rec.Type)
		s, err := probe.ParseSummary(rec.Data)
		require.NoError(t, err)
		require.Equal(t, code, s.ExitCode)
	}
	for _, tc := range []struct {
		name, input string
		code        int
	}{
		{"data", "file://" + filepath.ToSlash(object), 60},
		{"invalid", "not-a-uri", 40},
		{"provider", `{"type":"gonimbus.error.v1","data":{"code":"ACCESS_DENIED","message":"upstream"}}`, 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			cmd := newCommand(ctx)
			cmd.Stdin = strings.NewReader(tc.input)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			checkExit(t, cmd.Run(), tc.code)
			checkSummary(t, stdout.String(), tc.code)
		})
	}
	t.Run("configuration-value", func(t *testing.T) {
		bad := filepath.Join(dir, "bad.yaml")
		require.NoError(t, os.WriteFile(bad, []byte("extract:\n  - name: value\n    type: regex\n    required: SYNTHETIC-PRIVATE-LITERAL\n"), 0600))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := newCommand(ctx, "--config", bad)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		checkExit(t, cmd.Run(), 40)
		checkSummary(t, stdout.String(), 40)
		require.NotContains(t, stdout.String()+stderr.String(), "SYNTHETIC-PRIVATE-LITERAL")
	})
	t.Run("abort", func(t *testing.T) {
		started := make(chan struct{}, 1)
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-r.Context().Done()
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := newCommand(ctx, "--endpoint", server.URL, "--region", "us-east-1")
		cmd.Stdin = strings.NewReader("s3://bucket/key")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		require.NoError(t, cmd.Start())
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("probe did not reach provider")
		}
		require.NoError(t, cmd.Process.Signal(os.Interrupt))
		checkExit(t, cmd.Wait(), 130)
		checkSummary(t, stdout.String(), 130)
	})
	t.Run("output", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := newCommand(ctx)
		cmd.Stdin = strings.NewReader("")
		reader, writer, err := os.Pipe()
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		defer func() { _ = writer.Close() }()
		cmd.Stdout = writer
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		checkExit(t, cmd.Run(), 1)
	})
}
