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
				cmd := exec.CommandContext(ctx, "/bin/bash", "-c", script, "pipeline", binary, config, server.URL, input, producerOutput, statuses, poolArg, dropArg) // #nosec G204 -- fixed shell script with positional test-owned arguments.
				cmd.Env = append(os.Environ(), "GONIMBUS_DATA_DIR="+work, "XDG_CONFIG_HOME="+work, "AWS_ACCESS_KEY_ID=synthetic", "AWS_SECRET_ACCESS_KEY=synthetic", "AWS_EC2_METADATA_DISABLED=true")
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				runErr := cmd.Run()
				rawStatuses, err := os.ReadFile(statuses)
				require.NoError(t, err, stderr.String())
				parts := strings.Fields(string(rawStatuses))
				require.Len(t, parts, 4)
				require.Equal(t, strconv.Itoa(tc.producer), parts[0], stderr.String())
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
