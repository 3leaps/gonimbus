package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/stretchr/testify/require"
)

func TestProbeBuiltReflowPipelines(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "gonimbus")
	build := exec.Command("go", "build", "-o", binary, ".") // #nosec G204 -- fixed build command, test-owned destination.
	built, err := build.CombinedOutput()
	require.NoError(t, err, string(built))
	config := filepath.Join(dir, "probe.yaml")
	require.NoError(t, os.WriteFile(config, []byte("extract:\n  - name: value\n    type: regex\n    pattern: 'value=([a-z]+)'\n    group: 1\n    required: true\n    on_missing: fail\n"), 0600))
	for _, pool := range []bool{false, true} {
		for _, tc := range []struct {
			name, input string
			producer    int
			writes      int
			dropErrors  bool
		}{
			{"success", "s3://source/good", 0, 1, false},
			{"empty", "", 0, 0, false},
			{"data-errors", "s3://source/bad", 60, 0, false},
			{"mixed", "s3://source/good\ns3://source/bad", 60, 1, false},
			{"invalid", "not-a-uri", 40, 0, false},
			{"provider-error", `{"type":"gonimbus.error.v1","data":{"code":"ACCESS_DENIED","message":"upstream"}}`, 32, 0, false},
			{"summary-first-failed", "s3://source/bad", 60, 0, true},
		} {
			t.Run(fmt.Sprintf("pool=%t/%s", pool, tc.name), func(t *testing.T) {
				var mu sync.Mutex
				writes := map[string][]byte{}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasPrefix(r.URL.Path, "/source/") {
						body := "value=valid"
						if strings.HasSuffix(r.URL.Path, "/bad") {
							body = "missing"
						}
						w.Header().Set("ETag", `"source-etag"`)
						w.Header().Set("Last-Modified", "Tue, 01 Sep 2026 00:00:00 GMT")
						w.Header().Set("Content-Length", strconv.Itoa(len(body)))
						if r.Method != http.MethodHead {
							_, _ = io.WriteString(w, body)
						}
						return
					}
					if r.Method == http.MethodPut {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						mu.Lock()
						writes[r.URL.Path] = body
						mu.Unlock()
						w.Header().Set("ETag", `"destination-etag"`)
						return
					}
					w.WriteHeader(http.StatusNotFound)
				}))
				defer server.Close()
				work := t.TempDir()
				input := filepath.Join(work, "input.txt")
				producerOutput := filepath.Join(work, "probe.jsonl")
				statuses := filepath.Join(work, "statuses.txt")
				wrapperReport := filepath.Join(work, "wrapper-report.txt")
				require.NoError(t, os.WriteFile(input, []byte(tc.input), 0600))
				// Positional shell arguments keep all test paths/URLs out of shell
				// syntax. PIPESTATUS proves the producer result independently of
				// the mover. Filtering is an explicit compatibility policy here.
				script := `set -o pipefail
pool=()
if [ "$7" = yes ]; then pool=(--on-source-failure fail); fi
"$1" content probe --stdin --config "$2" --emit reflow-input --concurrency 1 --endpoint "$3" --region us-east-1 < "$4" |
tee "$5" |
awk -v drop="$8" 'drop != "yes" || $0 !~ /"type":"gonimbus.error.v1"/' |
"$1" transfer reflow --stdin --dest s3://destination/data/ --rewrite-from '{key}' --rewrite-to '{key}' --parallel 1 --on-collision overwrite --overwrite --src-endpoint "$3" --dest-endpoint "$3" --src-region us-east-1 --dest-region us-east-1 "${pool[@]}"
statuses=("${PIPESTATUS[@]}")
printf '%s\n' "${statuses[@]}" > "$6"
printf 'probe=%s reflow=%s\n' "${statuses[0]}" "${statuses[3]}" > "$9"
result=0
for status in "${statuses[@]}"; do if [ "$status" -ne 0 ]; then result="$status"; fi; done
exit "$result"
`
				poolArg, dropArg := "no", "no"
				if pool {
					poolArg = "yes"
				}
				if tc.dropErrors {
					dropArg = "yes"
				}
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "/bin/bash", "-c", script, "pipeline", binary, config, server.URL, input, producerOutput, statuses, poolArg, dropArg, wrapperReport) // #nosec G204 -- fixed shell script with positional test-owned arguments.
				cmd.Env = append(os.Environ(), "GONIMBUS_DATA_DIR="+work, "XDG_CONFIG_HOME="+work, "AWS_ACCESS_KEY_ID=synthetic", "AWS_SECRET_ACCESS_KEY=synthetic", "AWS_EC2_METADATA_DISABLED=true")
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				runErr := cmd.Run()
				rawStatuses, err := os.ReadFile(statuses)
				require.NoError(t, err, stderr.String())
				rawWrapperReport, err := os.ReadFile(wrapperReport)
				require.NoError(t, err, stderr.String())
				parts := strings.Fields(string(rawStatuses))
				require.Len(t, parts, 4)
				require.Equal(t, strconv.Itoa(tc.producer), parts[0], stderr.String())
				require.Contains(t, string(rawWrapperReport), fmt.Sprintf("probe=%d ", tc.producer), "wrapper must report producer exit code distinctly")
				if tc.producer == 60 {
					require.Contains(t, string(rawWrapperReport), "probe=60 ", "exit-60 wrapper must report probe=60 distinctly even when downstream reflow exits non-zero")
				}
				require.Equal(t, "0", parts[1], stderr.String())
				require.Equal(t, "0", parts[2], stderr.String())
				if tc.producer == 0 {
					require.NoError(t, runErr, stderr.String())
					require.Equal(t, "0", parts[3])
				} else {
					require.Error(t, runErr)
					var exited *exec.ExitError
					require.ErrorAs(t, runErr, &exited)
					wantExit := tc.producer
					if parts[3] != "0" {
						wantExit, err = strconv.Atoi(parts[3])
						require.NoError(t, err)
					}
					require.Equal(t, wantExit, exited.ExitCode(), "shell process retains rightmost failure under pipefail")
				}
				if tc.dropErrors {
					require.Equal(t, "0", parts[3], "downstream zero must not erase producer60 under pipefail")
				}
				if tc.producer != 0 && !tc.dropErrors {
					require.NotEqual(t, "0", parts[3], "upstream error record is explicitly refused")
				}
				rawProbe, err := os.ReadFile(producerOutput)
				require.NoError(t, err)
				lines := strings.Split(strings.TrimSpace(string(rawProbe)), "\n")
				var terminal output.Record
				require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &terminal))
				require.Equal(t, probe.SummaryRecordType, terminal.Type)
				require.True(t, terminal.Advisory, "content probe summary envelope must carry advisory=true")
				summary, err := probe.ParseSummary(terminal.Data)
				require.NoError(t, err)
				require.Equal(t, tc.producer, summary.ExitCode)
				summaries := 0
				for _, line := range lines {
					var rec output.Record
					require.NoError(t, json.Unmarshal([]byte(line), &rec))
					require.Equal(t, terminal.JobID, rec.JobID)
					if rec.Type == probe.SummaryRecordType {
						summaries++
						require.True(t, rec.Advisory)
					} else {
						require.False(t, rec.Advisory, "non-summary producer record %q must not carry advisory=true", rec.Type)
					}
				}
				require.Equal(t, 1, summaries)
				mu.Lock()
				writeCount := len(writes)
				payload := writes["/destination/data/good"]
				mu.Unlock()
				require.Equal(t, tc.writes, writeCount)
				if tc.writes > 0 {
					require.Equal(t, []byte("value=valid"), payload)
				}
				if tc.name == "success" || tc.name == "mixed" {
					runSeen, completed := false, 0
					for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
						var rec output.Record
						require.NoError(t, json.Unmarshal([]byte(line), &rec))
						if rec.Type == reflow.RunRecordType {
							runSeen = true
							var run struct {
								ExecutionPath string `json:"execution_path"`
							}
							require.NoError(t, json.Unmarshal(rec.Data, &run))
							want := "engine"
							if pool {
								want = "cli-pool"
							}
							require.Equal(t, want, run.ExecutionPath)
						}
						if rec.Type == reflow.RecordType {
							var receipt struct {
								Status             string    `json:"status"`
								SHA256             string    `json:"dest_sha256"`
								ETag               string    `json:"dest_etag"`
								SourceLastModified time.Time `json:"source_last_modified"`
							}
							require.NoError(t, json.Unmarshal(rec.Data, &receipt))
							if receipt.Status == "complete" {
								completed++
								hash := sha256.Sum256([]byte("value=valid"))
								require.Equal(t, hex.EncodeToString(hash[:]), receipt.SHA256)
								require.Equal(t, "destination-etag", strings.Trim(receipt.ETag, `"`))
								require.Equal(t, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), receipt.SourceLastModified)
							}
						}
					}
					require.True(t, runSeen, "route assertion must not pass without a run record")
					require.Equal(t, tc.writes, completed)
				}
			})
		}
	}
}

func TestProbeV044ReflowConsumerCompatibility(t *testing.T) {
	if out, err := exec.Command("git", "rev-parse", "--verify", "v0.4.4^{commit}").CombinedOutput(); err != nil {
		t.Skipf("v0.4.4 tag not available in this git clone (%v: %s)", err, strings.TrimSpace(string(out)))
	}

	dir := t.TempDir()
	currentBinary := filepath.Join(dir, "gonimbus-current")
	builtCurrent, err := exec.Command("go", "build", "-o", currentBinary, ".").CombinedOutput() // #nosec G204 -- fixed build command, test-owned destination.
	require.NoError(t, err, string(builtCurrent))

	v044Src := filepath.Join(dir, "v044-src")
	require.NoError(t, os.MkdirAll(v044Src, 0o750))
	archiveCmd := exec.Command("/bin/bash", "-c", `git archive --format=tar v0.4.4 | tar -xf - -C "$1"`, "archive", v044Src) // #nosec G204 -- fixed command with positional test-owned dir.
	archiveCmd.Dir = filepath.Join("..", "..")
	archived, err := archiveCmd.CombinedOutput()
	require.NoError(t, err, string(archived))

	v044Binary := filepath.Join(dir, "gonimbus-v044")
	buildV044 := exec.Command("go", "build", "-o", v044Binary, "./cmd/gonimbus") // #nosec G204 -- fixed build command, test-owned destination.
	buildV044.Dir = v044Src
	builtV044, err := buildV044.CombinedOutput()
	require.NoError(t, err, string(builtV044))

	config := filepath.Join(dir, "probe.yaml")
	require.NoError(t, os.WriteFile(config, []byte("extract:\n  - name: value\n    type: regex\n    pattern: 'value=([a-z]+)'\n    group: 1\n    required: true\n    on_missing: fail\n"), 0o600))

	for _, pool := range []bool{false, true} {
		t.Run(fmt.Sprintf("v046_probe_to_v044_reflow/pool=%t", pool), func(t *testing.T) {
			var mu sync.Mutex
			writes := map[string][]byte{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/source/") {
					body := "value=valid"
					w.Header().Set("ETag", `"source-etag"`)
					w.Header().Set("Last-Modified", "Tue, 01 Sep 2026 00:00:00 GMT")
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					if r.Method != http.MethodHead {
						_, _ = io.WriteString(w, body)
					}
					return
				}
				if r.Method == http.MethodPut {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					writes[r.URL.Path] = body
					mu.Unlock()
					w.Header().Set("ETag", `"destination-etag"`)
					return
				}
				w.WriteHeader(http.StatusNotFound)
			}))
			defer server.Close()

			work := t.TempDir()
			env := append(os.Environ(), "GONIMBUS_DATA_DIR="+work, "XDG_CONFIG_HOME="+work, "AWS_ACCESS_KEY_ID=synthetic", "AWS_SECRET_ACCESS_KEY=synthetic", "AWS_EC2_METADATA_DISABLED=true")

			// Produce stream with current (v0.4.6) content probe, which marks summary v1 with top-level advisory=true.
			probeCmd := exec.Command(currentBinary, "content", "probe", "--stdin", "--config", config, "--emit", "reflow-input", "--concurrency", "1", "--endpoint", server.URL, "--region", "us-east-1") // #nosec G204 -- test binary.
			probeCmd.Env = env
			probeCmd.Stdin = strings.NewReader("s3://source/good\n")
			var probeStdout, probeStderr bytes.Buffer
			probeCmd.Stdout, probeCmd.Stderr = &probeStdout, &probeStderr
			require.NoError(t, probeCmd.Run(), probeStderr.String())
			require.Contains(t, probeStdout.String(), `"advisory":true`)

			// Pipe v0.4.6 probe output directly into real v0.4.4 transfer reflow --stdin.
			reflowArgs := []string{
				"transfer", "reflow", "--stdin",
				"--dest", "s3://destination/data/",
				"--rewrite-from", "{key}",
				"--rewrite-to", "{key}",
				"--parallel", "1",
				"--on-collision", "overwrite",
				"--overwrite",
				"--src-endpoint", server.URL,
				"--dest-endpoint", server.URL,
				"--src-region", "us-east-1",
				"--dest-region", "us-east-1",
			}
			if pool {
				reflowArgs = append(reflowArgs, "--on-source-failure", "fail")
			}
			reflowCmd := exec.Command(v044Binary, reflowArgs...) // #nosec G204 -- test binary.
			reflowCmd.Env = env
			reflowCmd.Stdin = bytes.NewReader(probeStdout.Bytes())
			var reflowStdout, reflowStderr bytes.Buffer
			reflowCmd.Stdout, reflowCmd.Stderr = &reflowStdout, &reflowStderr
			require.NoError(t, reflowCmd.Run(), reflowStderr.String())

			mu.Lock()
			gotBody := writes["/destination/data/good"]
			mu.Unlock()
			require.Equal(t, []byte("value=valid"), gotBody)

			// Verify the compatibility boundary: v0.4.4 rejects an unknown advisory record type,
			// whereas v0.4.6 skips it with a bounded warning.
			futureAdvisoryStream := probeStdout.String() + `{"type":"gonimbus.future.audit.v1","advisory":true,"data":{}}` + "\n"

			v044RejectCmd := exec.Command(v044Binary, reflowArgs...) // #nosec G204 -- test binary.
			v044RejectCmd.Env = env
			v044RejectCmd.Stdin = strings.NewReader(futureAdvisoryStream)
			require.Error(t, v044RejectCmd.Run(), "v0.4.4 does not implement advisory skipping for unknown types")

			v046AcceptCmd := exec.Command(currentBinary, reflowArgs...) // #nosec G204 -- test binary.
			v046AcceptCmd.Env = env
			v046AcceptCmd.Stdin = strings.NewReader(futureAdvisoryStream)
			var v046Stdout, v046Stderr bytes.Buffer
			v046AcceptCmd.Stdout, v046AcceptCmd.Stderr = &v046Stdout, &v046Stderr
			require.NoError(t, v046AcceptCmd.Run(), v046Stderr.String())
			require.Contains(t, v046Stdout.String(), reflow.AdvisoryRecordSkippedWarningCode)
		})
	}
}
