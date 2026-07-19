package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/gcs"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

// Flag-coverage parity matrix for `transfer reflow` (standing behavioral parity
// gate, item 3): every registered flag declares a disposition PER EXECUTION
// PATH — {engine, cli-pool} — and every applicable path cell carries an
// executable probe driving the real command with a representative non-default
// value:
//
//   - honored:          the cell's probe binds the value to an observable
//     behavior/configuration sentinel on THAT path (deleting
//     the flag's wiring on that path fails the probe — see
//     TestTransferReflowFlagMatrixSentinelSensitivity)
//   - routes-cli-pool:  (engine column only) setting the flag, or the probed
//     non-migrated value of it, makes dispatch select the
//     CLI pool — proven by execution_path
//   - rejected-loud:    validation refuses before any destination mutation
//   - not-applicable:   the flag is scoped to a shape that path cannot take
//     (companion of another flag, file-source/dest only,
//     provider-specific); the probe PINS the documented
//     no-effect so a behavior change must update the row
//
// "silently-ignored" is not a legal disposition anywhere in the table. The
// CLI-pool arm of shared flags is selected by genuine dispatch routing
// (--on-source-failure fail on a fixture where it changes nothing) or by a
// naturally-routed run shape (positional file sources, provenance, etc.).

const (
	flagHonored       = "honored"
	flagRoutesCLIPool = "routes-cli-pool"
	flagRejectedLoud  = "rejected-loud"
	flagNotApplicable = "not-applicable"
)

type flagPathCell struct {
	disposition string
	probe       func(t *testing.T)
}

type reflowFlagBehavior struct {
	engine  flagPathCell
	cliPool flagPathCell
	note    string
}

// poolRoute is the genuine dispatch-routing vehicle for CLI-pool arms of
// shared flags: a non-migrated policy value that changes nothing on fixtures
// without source failures.
var poolRoute = []string{"--on-source-failure", "fail"}

// flagProbeEnv is the shared one-object live-run environment for matrix probes.
type flagProbeEnv struct {
	src *reflowMemoryProvider
	dst *reflowMemoryProvider

	mu         sync.Mutex
	srcConfigs []s3.Config
	dstConfigs []s3.Config
	gcsConfigs []gcs.Config

	checkpointPath string
}

func newFlagProbeEnv(t *testing.T) *flagProbeEnv {
	t.Helper()
	withTransferReflowTestState(t)
	env := &flagProbeEnv{src: newReflowMemoryProvider(), dst: newReflowMemoryProvider()}
	env.src.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	env.checkpointPath = filepath.Join(t.TempDir(), "state.db")

	reflowResourceProbeForRun = reflowpkg.ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) {
			return transfer.DefaultRetryBufferMaxMemoryBytes * 64, "test_override", nil
		},
		FDSoftLimit: func() (int64, error) { return 100000, nil },
	}
	useTransferReflowProviderFactories(t, providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			env.mu.Lock()
			defer env.mu.Unlock()
			switch cfg.Bucket {
			case "source-bucket":
				env.srcConfigs = append(env.srcConfigs, cfg)
				return env.src, nil
			case "dest-bucket":
				env.dstConfigs = append(env.dstConfigs, cfg)
				return env.dst, nil
			default:
				return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
			}
		},
		GCS: func(_ context.Context, cfg gcs.Config) (provider.Provider, error) {
			env.mu.Lock()
			defer env.mu.Unlock()
			env.gcsConfigs = append(env.gcsConfigs, cfg)
			switch cfg.Bucket {
			case "source-bucket":
				return env.src, nil
			case "dest-bucket":
				return env.dst, nil
			default:
				return nil, fmt.Errorf("unexpected gcs bucket %q", cfg.Bucket)
			}
		},
	})
	return env
}

// run executes the standard one-object live stdin baseline plus extra args.
func (e *flagProbeEnv) run(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	return e.runInput(t, reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "")+"\n", extra...)
}

// runPool is run with the genuine CLI-pool routing vehicle appended.
func (e *flagProbeEnv) runPool(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	return e.run(t, append(append([]string{}, extra...), poolRoute...)...)
}

func (e *flagProbeEnv) runInput(t *testing.T, input string, extra ...string) (string, error) {
	t.Helper()
	args := append([]string{
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--parallel", "2",
		"--checkpoint", e.checkpointPath,
	}, extra...)
	return e.runRaw(t, strings.NewReader(input), args...)
}

// runRaw executes the command with fully caller-controlled input and args.
func (e *flagProbeEnv) runRaw(t *testing.T, in *strings.Reader, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(in)
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	execErr := cmd.Execute()
	return stdout.String(), execErr
}

// executionPathOf extracts execution_path from the run record.
func executionPathOf(t *testing.T, stdout string) string {
	t.Helper()
	run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
	var fields struct {
		ExecutionPath string `json:"execution_path"`
	}
	require.NoError(t, json.Unmarshal(run.Data, &fields))
	return fields.ExecutionPath
}

func requireProbeComplete(t *testing.T, stdout string, err error, path string) {
	t.Helper()
	require.NoError(t, err)
	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
	require.Equal(t, path, executionPathOf(t, stdout))
}

// runFilePositionalProbe runs a positional file->file transfer (a genuinely
// CLI-pool-routed shape: positional sources are not migrated) against real
// temporary directories using the production file provider.
func runFilePositionalProbe(t *testing.T, seed func(srcDir string), extra ...string) (stdout string, destDir string, err error) {
	t.Helper()
	withTransferReflowTestState(t)
	srcDir := t.TempDir()
	destDir = t.TempDir()
	seed(srcDir)
	var out, errBuf bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&errBuf)
	args := append([]string{
		fileURI(srcDir) + "/",
		"--dest", fileURI(destDir) + "/",
		"--rewrite-from", "{key}",
		"--rewrite-to", "{key}",
		"--parallel", "2",
	}, extra...)
	cmd.SetArgs(args)
	execErr := cmd.Execute()
	return out.String(), destDir, execErr
}

func writeProbeFile(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
}

// sidecarFailingDest fails writes whose key carries the provenance suffix.
type sidecarFailingDest struct {
	*reflowMemoryProvider
	suffix string
}

func (p *sidecarFailingDest) PutObjectWithOptions(ctx context.Context, key string, body io.Reader, contentLength int64, opts provider.PutOptions) error {
	if strings.HasSuffix(key, p.suffix) {
		return fmt.Errorf("injected sidecar write failure")
	}
	return p.reflowMemoryProvider.PutObjectWithOptions(ctx, key, body, contentLength, opts)
}

func (p *sidecarFailingDest) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	if strings.HasSuffix(key, p.suffix) {
		return fmt.Errorf("injected sidecar write failure")
	}
	return p.reflowMemoryProvider.PutObject(ctx, key, body, contentLength)
}

var reflowFlagMatrix = map[string]reflowFlagBehavior{
	"stdin": {
		note: "engine requires stdin record streams; positional sources are a pool shape (see file-scoped rows)",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t)
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t)
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
		}},
	},
	"dest": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t)
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.True(t, env.dst.hasObject("data/source/file.xml"))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t)
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.True(t, env.dst.hasObject("data/source/file.xml"))
		}},
	},
	"rewrite-from": {
		note: "applies to records without an explicit dest_rel_key",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			in := reflowInputLineNoDestRel("source/file.xml", "src-etag", int64(len("payload"))) + "\n"
			stdout, err := env.runInput(t, in, "--rewrite-from", "source/{name}", "--rewrite-to", "renamed/{name}")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.True(t, env.dst.hasObject("data/renamed/file.xml"))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			in := reflowInputLineNoDestRel("source/file.xml", "src-etag", int64(len("payload"))) + "\n"
			stdout, err := env.runInput(t, in, "--rewrite-from", "source/{name}", "--rewrite-to", "renamed/{name}", "--on-source-failure", "fail")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.True(t, env.dst.hasObject("data/renamed/file.xml"))
		}},
	},
	"rewrite-to": {
		note: "applies to records without an explicit dest_rel_key",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			in := reflowInputLineNoDestRel("source/file.xml", "src-etag", int64(len("payload"))) + "\n"
			stdout, err := env.runInput(t, in, "--rewrite-from", "{head}/file.xml", "--rewrite-to", "{head}/copy.xml")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.True(t, env.dst.hasObject("data/source/copy.xml"))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			in := reflowInputLineNoDestRel("source/file.xml", "src-etag", int64(len("payload"))) + "\n"
			stdout, err := env.runInput(t, in, "--rewrite-from", "{head}/file.xml", "--rewrite-to", "{head}/copy.xml", "--on-source-failure", "fail")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.True(t, env.dst.hasObject("data/source/copy.xml"))
		}},
	},
	"parallel": {
		note: "dual-path max-in-flight harness enforces the behavioral half on both paths",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--parallel", "3")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"parallel":3`)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--parallel", "3")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"parallel":3`)
		}},
	},
	"no-adaptive": {
		note: "concurrency resolved before dispatch, shared by both paths",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--no-adaptive")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"adaptive_enabled":false`)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--no-adaptive")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"adaptive_enabled":false`)
		}},
	},
	"memory-budget": {
		note: "operator budget replaces the fraction-derived memory budget; resolved before dispatch, shared by both paths; invalid values refuse before any provider work; values above the detected limit clamp to it with recorded source",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			// Rejected-value subcase: below the 64MiB floor refuses loudly
			// before any destination mutation.
			envRefuse := newFlagProbeEnv(t)
			_, refuseErr := envRefuse.run(t, "--memory-budget", "1MiB")
			require.Error(t, refuseErr, "--memory-budget below the floor must refuse")
			require.Contains(t, refuseErr.Error(), "64MiB floor")
			require.False(t, envRefuse.dst.hasObject("data/source/file.xml"),
				"refused memory budget must not mutate the destination")

			// Honored: 128MiB budget / 16MiB per-worker reservation = ceiling 8,
			// below the requested 32 — the budget visibly sizes the ceiling.
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--memory-budget", "128MiB", "--parallel", "32")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"concurrency_ceiling_effective":8`)
			require.Contains(t, string(run.Data), `"memory_budget_source":"operator"`)
			require.Contains(t, string(run.Data), `"memory_budget_effective_bytes":134217728`)
			require.Contains(t, string(run.Data), `"concurrency_ceiling_reason":"resource_capped:memory:operator_budget"`)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			// Honored on the pool path with the same resolved shape.
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--memory-budget", "128MiB", "--parallel", "32")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"concurrency_ceiling_effective":8`)
			require.Contains(t, string(run.Data), `"memory_budget_source":"operator"`)

			// Clamp subcase: a budget above the detected limit applies the
			// limit and records the clamp — never silently exceeds detection.
			envClamp := newFlagProbeEnv(t)
			stdoutClamp, errClamp := envClamp.runPool(t, "--memory-budget", "2TiB")
			requireProbeComplete(t, stdoutClamp, errClamp, reflowpkg.ExecutionPathCLIPool)
			runClamp := requireRecord(t, stdoutClamp, reflowpkg.RunRecordType, "")
			require.Contains(t, string(runClamp.Data), `"memory_budget_source":"operator_clamped_to_limit"`)
			require.Contains(t, string(runClamp.Data), `"memory_budget_effective_bytes":1073741824`)
		}},
	},
	"dry-run": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--dry-run")
			require.NoError(t, err)
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout), "planned", "", 1)
			require.False(t, env.dst.hasObject("data/source/file.xml"))
			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--dry-run")
			require.NoError(t, err)
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout), "planned", "", 1)
			require.False(t, env.dst.hasObject("data/source/file.xml"))
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
		}},
	},
	"resume": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout1, err := env.run(t)
			requireProbeComplete(t, stdout1, err, reflowpkg.ExecutionPathEngine)
			stdout2, err := env.run(t, "--resume")
			require.NoError(t, err)
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout2), "skipped", "resume.complete", 1)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout1, err := env.runPool(t)
			requireProbeComplete(t, stdout1, err, reflowpkg.ExecutionPathCLIPool)
			stdout2, err := env.runPool(t, "--resume")
			require.NoError(t, err)
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout2), "skipped", "resume.complete", 1)
		}},
	},
	"resume-run": {
		note: "run-id resume replays a checkpointed positional run (stdin runs are not checkpoint-eligible), so the resumed execution is a pool shape; unknown ids refuse loudly",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			probeResumeRunRoute(t)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			probeResumeRunRoute(t)
		}},
	},
	"checkpoint": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t)
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			_, statErr := os.Stat(env.checkpointPath)
			require.NoError(t, statErr)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t)
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			_, statErr := os.Stat(env.checkpointPath)
			require.NoError(t, statErr)
		}},
	},
	"overwrite": {
		note: "confirmation companion of --on-collision=overwrite (which refuses without it); overwrite semantics not migrated, so setting it routes dispatch",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--overwrite")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			// The confirmation gate: overwrite mode without the flag refuses.
			envRefuse := newFlagProbeEnv(t)
			_, err := envRefuse.run(t, "--on-collision", "overwrite")
			require.Error(t, err, "--on-collision=overwrite must refuse without --overwrite")
			require.Contains(t, err.Error(), "--overwrite")

			// With the flag, the stale destination is replaced on the pool.
			env := newFlagProbeEnv(t)
			env.dst.putFixture("data/source/file.xml", "stale-content", "other-etag", time.Time{})
			stdout, err := env.run(t, "--on-collision", "overwrite", "--overwrite")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "payload", string(env.dst.mustObject("data/source/file.xml")),
				"overwrite mode with confirmation must replace the stale destination object")
		}},
	},
	"on-collision": {
		note: "engine executes skip-if-duplicate|fail; other modes route to the pool (routing arm probed under collision-quarantine-prefix)",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.dst.putFixture("data/source/file.xml", "payload", "src-etag", time.Time{})
			stdout, err := env.run(t, "--on-collision", "fail")
			require.Error(t, err)
			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
			require.Equal(t, "collision.exists.duplicate", requireReflowData(t, stdout, "failed").Reason)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.dst.putFixture("data/source/file.xml", "payload", "src-etag", time.Time{})
			stdout, err := env.runPool(t, "--on-collision", "fail")
			require.Error(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
			require.Equal(t, "collision.exists.duplicate", requireReflowData(t, stdout, "failed").Reason)
		}},
	},
	"collision-quarantine-prefix": {
		note: "quarantine mode not migrated; prefix honored on the pool",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.dst.putFixture("data/source/file.xml", "different", "other-etag", time.Time{})
			stdout, err := env.run(t, "--on-collision", "quarantine", "--collision-quarantine-prefix", "quar/")
			require.NoError(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.dst.putFixture("data/source/file.xml", "different", "other-etag", time.Time{})
			stdout, err := env.run(t, "--on-collision", "quarantine", "--collision-quarantine-prefix", "quar/")
			require.NoError(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
			require.True(t, env.dst.hasObject("data/quar/source/file.xml"), "quarantine prefix applied on the pool")
		}},
	},

	"provenance": {
		note: "provenance sidecars not migrated",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance", "sidecar")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance", "sidecar")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.True(t, env.dst.hasObject("data/source/file.xml"+provenanceSuffix))
		}},
	},
	"provenance-sidecar-root": {
		note: "companion of --provenance: the valid combination routes with provenance and selects mirrored-root placement on the pool; the root without sidecar mode refuses loudly (rejected-value subcase)",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			// Valid combination: provenance routes, root rides along.
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance", "sidecar", "--provenance-sidecar-root", "s3://dest-bucket/prov/")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)

			// Rejected-value subcase: the root without sidecar mode refuses
			// loudly before any destination mutation.
			probeSidecarRootRejected(t)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance", "sidecar", "--provenance-sidecar-root", "s3://dest-bucket/prov/run-01/")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			// The root binds: the sidecar lands under the REQUESTED root (a
			// dropped binding would fall back to adjacent placement and fail
			// both assertions), and the run record reports mirrored-root.
			require.True(t, env.dst.hasObject("prov/run-01/source/file.xml"+provenanceSuffix),
				"sidecar must be written under the requested mirrored root")
			require.False(t, env.dst.hasObject("data/source/file.xml"+provenanceSuffix),
				"sidecar must not fall back to adjacent placement")
			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), `"placement":{"mode":"mirrored-root","sidecar_root":"s3://dest-bucket/prov/run-01/"}`)
		}},
	},
	"provenance-suffix": {
		note: "companion of --provenance; no effect while mode=none (pinned on engine); honored on the pool when provenance routes there",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance-suffix", ".custom.json")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.False(t, env.dst.hasObject("data/source/file.xml.custom.json"))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance", "sidecar", "--provenance-suffix", ".custom.json")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.True(t, env.dst.hasObject("data/source/file.xml.custom.json"))
		}},
	},
	"provenance-on-write-error": {
		note: "companion of --provenance; no effect while mode=none (pinned on engine); fail-vs-warn differential honored on the pool",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--provenance-on-write-error", "fail")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			// warn (default): sidecar write failure warns, object completes.
			envWarn := newFlagProbeEnv(t)
			failingWarn := &sidecarFailingDest{reflowMemoryProvider: envWarn.dst, suffix: provenanceSuffix}
			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return envWarn.src, nil
					}
					return failingWarn, nil
				},
			})
			stdoutWarn, err := envWarn.run(t, "--provenance", "sidecar")
			require.NoError(t, err, "on-write-error=warn must not fail the run")
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdoutWarn), "complete", "", 1)

			// fail: the same injected sidecar failure fails the object.
			envFail := newFlagProbeEnv(t)
			failingFail := &sidecarFailingDest{reflowMemoryProvider: envFail.dst, suffix: provenanceSuffix}
			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return envFail.src, nil
					}
					return failingFail, nil
				},
			})
			stdoutFail, err := envFail.run(t, "--provenance", "sidecar", "--provenance-on-write-error", "fail")
			require.Error(t, err, "on-write-error=fail must fail the run on sidecar write failure")
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdoutFail), "failed", "provenance.write_failed", 1)
		}},
	},
	"allow-unsafe-suffix": {
		note: "companion of --provenance-suffix; no effect while mode=none (pinned on engine); flips suffix validation on the pool",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--allow-unsafe-suffix")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			_, err := env.run(t, "--provenance", "sidecar", "--provenance-suffix", ".xml")
			require.Error(t, err, "unsafe suffix without the flag must refuse")

			env2 := newFlagProbeEnv(t)
			stdout, err := env2.run(t, "--provenance", "sidecar", "--provenance-suffix", ".xml", "--allow-unsafe-suffix")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.True(t, env2.dst.hasObject("data/source/file.xml.xml"), "unsafe suffix honored with the flag")
		}},
	},

	"metadata-policy": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"team": "alpha"}})
			stdout, err := env.run(t, "--metadata-policy", "preserve")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.Equal(t, "alpha", env.dst.metaSnapshot("data/source/file.xml").Metadata["team"])
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"team": "alpha"}})
			stdout, err := env.runPool(t, "--metadata-policy", "preserve")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "alpha", env.dst.metaSnapshot("data/source/file.xml").Metadata["team"])
		}},
	},
	"metadata-set": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--metadata-set", "owner=probe")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.Equal(t, "probe", env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--metadata-set", "owner=probe")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "probe", env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
		}},
	},
	"metadata-set-from-source-key": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"team": "alpha"}})
			stdout, err := env.run(t, "--metadata-set-from-source-key", "owner=team")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.Equal(t, "alpha", env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"team": "alpha"}})
			stdout, err := env.runPool(t, "--metadata-set-from-source-key", "owner=team")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "alpha", env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
		}},
	},
	"metadata-set-from-source-derived": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--metadata-set-from-source-derived", "origin-etag=system.etag")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.Equal(t, "src-etag", env.dst.metaSnapshot("data/source/file.xml").Metadata["origin-etag"])
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--metadata-set-from-source-derived", "origin-etag=system.etag")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "src-etag", env.dst.metaSnapshot("data/source/file.xml").Metadata["origin-etag"])
		}},
	},
	"metadata-on-missing-source": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--metadata-set-from-source-key", "owner=absent-key", "--metadata-on-missing-source", "fail")
			require.Error(t, err)
			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout), "complete", "", 0)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--metadata-set-from-source-key", "owner=absent-key", "--metadata-on-missing-source", "fail")
			require.Error(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdout), "complete", "", 0)
		}},
	},
	"preserve-content-type": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{ContentType: "text/xml"})
			stdout, err := env.run(t, "--preserve-content-type")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.Equal(t, "text/xml", env.dst.metaSnapshot("data/source/file.xml").ContentType)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{ContentType: "text/xml"})
			stdout, err := env.runPool(t, "--preserve-content-type")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "text/xml", env.dst.metaSnapshot("data/source/file.xml").ContentType)
		}},
	},
	"destination-storage-class": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--destination-storage-class", "STANDARD_IA")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.Equal(t, "STANDARD_IA", env.dst.metaSnapshot("data/source/file.xml").StorageClass)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--destination-storage-class", "STANDARD_IA")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.Equal(t, "STANDARD_IA", env.dst.metaSnapshot("data/source/file.xml").StorageClass)
		}},
	},
	"metadata-sidecar-suffix": {
		note: "file destinations only (a pool shape); no effect on object-store dests (pinned on engine)",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--metadata-sidecar-suffix", ".meta.json")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.False(t, env.dst.hasObject("data/source/file.xml.meta.json"))
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			stdout, destDir, err := runFilePositionalProbe(t, func(srcDir string) {
				writeProbeFile(t, srcDir, "file.txt", "payload")
			}, "--metadata-set", "owner=probe", "--metadata-sidecar-suffix", ".m.json")
			require.NoError(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
			_, statErr := os.Stat(filepath.Join(destDir, "file.txt.m.json"))
			require.NoError(t, statErr, "file destination metadata sidecar must use the custom suffix")
		}},
	},

	"symlinks": {
		note: "file-source policy; no effect on stdin record streams (pinned on engine)",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--symlinks", "follow")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			seed := func(srcDir string) {
				writeProbeFile(t, srcDir, "real.txt", "payload")
				require.NoError(t, os.Symlink(filepath.Join(srcDir, "real.txt"), filepath.Join(srcDir, "link.txt")))
			}
			// Default skip: the symlink is a source-failure skip.
			stdoutSkip, destSkip, err := runFilePositionalProbe(t, seed)
			require.NoError(t, err)
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdoutSkip), "skipped", "source.symlink.skipped", 1)
			_, statErr := os.Stat(filepath.Join(destSkip, "link.txt"))
			require.Error(t, statErr)

			// follow: the symlinked content lands.
			_, destFollow, err := runFilePositionalProbe(t, seed, "--symlinks", "follow")
			require.NoError(t, err)
			_, statErr = os.Stat(filepath.Join(destFollow, "link.txt"))
			require.NoError(t, statErr, "--symlinks follow must land the linked file")
		}},
	},
	"hidden": {
		note: "file-source policy; no effect on stdin record streams (pinned on engine)",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--hidden", "include")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			seed := func(srcDir string) {
				writeProbeFile(t, srcDir, "visible.txt", "payload")
				writeProbeFile(t, srcDir, ".hidden.txt", "secretless")
			}
			_, destSkip, err := runFilePositionalProbe(t, seed)
			require.NoError(t, err)
			_, statErr := os.Stat(filepath.Join(destSkip, ".hidden.txt"))
			require.Error(t, statErr, "default hidden=skip must not land dotfiles")

			_, destInc, err := runFilePositionalProbe(t, seed, "--hidden", "include")
			require.NoError(t, err)
			_, statErr = os.Stat(filepath.Join(destInc, ".hidden.txt"))
			require.NoError(t, statErr, "--hidden include must land dotfiles")
		}},
	},
	"exclude": {
		note: "file-source policy; no effect on stdin record streams (pinned on engine)",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--exclude", "*.tmp")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			stdout, destDir, err := runFilePositionalProbe(t, func(srcDir string) {
				writeProbeFile(t, srcDir, "keep.txt", "payload")
				writeProbeFile(t, srcDir, "drop.tmp", "payload")
			}, "--exclude", "*.tmp")
			require.NoError(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
			_, statErr := os.Stat(filepath.Join(destDir, "keep.txt"))
			require.NoError(t, statErr)
			_, statErr = os.Stat(filepath.Join(destDir, "drop.tmp"))
			require.Error(t, statErr, "--exclude must drop matching files")
		}},
	},
	"preserve-mode": {
		note: "preserve-mode not migrated; mode bits honored on file->file",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--preserve-mode")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			_, destDir, err := runFilePositionalProbe(t, func(srcDir string) {
				writeProbeFile(t, srcDir, "exec.sh", "#!/bin/sh\n")
				require.NoError(t, os.Chmod(filepath.Join(srcDir, "exec.sh"), 0o754))
			}, "--preserve-mode")
			require.NoError(t, err)
			info, statErr := os.Stat(filepath.Join(destDir, "exec.sh"))
			require.NoError(t, statErr)
			require.Equal(t, os.FileMode(0o754), info.Mode().Perm(), "--preserve-mode must carry Unix mode bits")
		}},
	},
	"on-source-failure": {
		note: "the default skip executes on the engine (every engine baseline probe carries it); the non-default fail value routes to the pool — this routing is also the matrix's pool vehicle, so its dispatch condition is exercised by every runPool probe; skip-vs-fail differential proven on a real source failure",
		engine: flagPathCell{disposition: flagRoutesCLIPool, probe: func(t *testing.T) {
			// The non-default value must route: this probe is the direct
			// negative assertion on the adapter's source-failure dispatch
			// condition — removing it leaves the run on the engine and fails.
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--on-source-failure", "fail")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)

			// Default-value contrast: without the flag the same fixture stays
			// on the engine.
			env2 := newFlagProbeEnv(t)
			stdout2, err := env2.run(t)
			requireProbeComplete(t, stdout2, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			seed := func(srcDir string) {
				writeProbeFile(t, srcDir, "real.txt", "payload")
				require.NoError(t, os.Symlink(filepath.Join(srcDir, "real.txt"), filepath.Join(srcDir, "link.txt")))
			}
			// skip (default): the source failure is a skipped record; run succeeds.
			stdoutSkip, _, err := runFilePositionalProbe(t, seed)
			require.NoError(t, err, "on-source-failure=skip must not fail the run")
			requireReflowStatusReasonCount(t, requireReflowRecords(t, stdoutSkip), "skipped", "source.symlink.skipped", 1)

			// fail: the same source failure fails the run.
			_, _, err = runFilePositionalProbe(t, seed, "--on-source-failure", "fail")
			require.Error(t, err, "on-source-failure=fail must fail the run on a source failure")
		}},
	},

	"src-region": {
		note: "source provider construction shared by both paths",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--src-region", "eu-probe-1")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.NotEmpty(t, env.srcConfigs)
			require.Equal(t, "eu-probe-1", env.srcConfigs[0].Region)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--src-region", "eu-probe-1")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.NotEmpty(t, env.srcConfigs)
			require.Equal(t, "eu-probe-1", env.srcConfigs[0].Region)
		}},
	},
	"src-profile": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--src-profile", "probe-profile")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.NotEmpty(t, env.srcConfigs)
			require.Equal(t, "probe-profile", env.srcConfigs[0].Profile)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--src-profile", "probe-profile")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.NotEmpty(t, env.srcConfigs)
			require.Equal(t, "probe-profile", env.srcConfigs[0].Profile)
		}},
	},
	"src-endpoint": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--src-endpoint", "https://s3.probe.example")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.NotEmpty(t, env.srcConfigs)
			require.Equal(t, "https://s3.probe.example", env.srcConfigs[0].Endpoint)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--src-endpoint", "https://s3.probe.example")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.NotEmpty(t, env.srcConfigs)
			require.Equal(t, "https://s3.probe.example", env.srcConfigs[0].Endpoint)
		}},
	},
	"src-gcp-project": {
		note: "no reachable transfer-reflow shape today: record streams accept s3:// only and positional gs:// sources refuse loudly (GCS-as-source is future work); pinned on both paths",
		engine: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--src-gcp-project", "probe-project")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
		}},
		cliPool: flagPathCell{disposition: flagNotApplicable, probe: func(t *testing.T) {
			// The only shape that could consume the hint — a gs:// positional
			// source — is refused loudly, so nothing is silently ignored.
			env := newFlagProbeEnv(t)
			env.src.putFixture("a.txt", "payload", "src-etag", time.Time{})
			_, err := env.runRaw(t, strings.NewReader(""),
				"gs://source-bucket/a.txt",
				"--dest", "s3://dest-bucket/data/",
				"--rewrite-from", "{key}",
				"--rewrite-to", "{key}",
				"--parallel", "2",
				"--src-gcp-project", "probe-project")
			require.Error(t, err, "gs:// positional sources refuse loudly")
			require.Contains(t, err.Error(), "unsupported provider")
			for _, key := range env.dst.conditionalPutCallsSnapshot() {
				require.True(t, isPreflightKey(key), "no object-level mutation before the refusal (saw %s)", key)
			}
		}},
	},
	"dest-region": {
		note: "destination provider constructed before dispatch",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--dest-region", "eu-probe-2")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.NotEmpty(t, env.dstConfigs)
			require.Equal(t, "eu-probe-2", env.dstConfigs[0].Region)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--dest-region", "eu-probe-2")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.NotEmpty(t, env.dstConfigs)
			require.Equal(t, "eu-probe-2", env.dstConfigs[0].Region)
		}},
	},
	"dest-profile": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--dest-profile", "probe-dest-profile")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.NotEmpty(t, env.dstConfigs)
			require.Equal(t, "probe-dest-profile", env.dstConfigs[0].Profile)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--dest-profile", "probe-dest-profile")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.NotEmpty(t, env.dstConfigs)
			require.Equal(t, "probe-dest-profile", env.dstConfigs[0].Profile)
		}},
	},
	"dest-endpoint": {
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, "--dest-endpoint", "https://s3.dest.example")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			require.NotEmpty(t, env.dstConfigs)
			require.Equal(t, "https://s3.dest.example", env.dstConfigs[0].Endpoint)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runPool(t, "--dest-endpoint", "https://s3.dest.example")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			require.NotEmpty(t, env.dstConfigs)
			require.Equal(t, "https://s3.dest.example", env.dstConfigs[0].Endpoint)
		}},
	},
	"dest-gcp-project": {
		note: "GCS destinations; gs:// dests are engine-eligible, so the hint binds on both paths",
		engine: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runRaw(t,
				strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "")+"\n"),
				"--stdin", "--dest", "gs://dest-bucket/data/", "--parallel", "2", "--checkpoint", env.checkpointPath, "--dest-gcp-project", "probe-project")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathEngine)
			env.mu.Lock()
			defer env.mu.Unlock()
			require.NotEmpty(t, env.gcsConfigs, "gs destination must construct through the GCS factory")
			require.Equal(t, "probe-project", env.gcsConfigs[0].Project)
		}},
		cliPool: flagPathCell{disposition: flagHonored, probe: func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.runRaw(t,
				strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "")+"\n"),
				"--stdin", "--dest", "gs://dest-bucket/data/", "--parallel", "2", "--checkpoint", env.checkpointPath, "--dest-gcp-project", "probe-project", "--on-source-failure", "fail")
			requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
			env.mu.Lock()
			defer env.mu.Unlock()
			require.NotEmpty(t, env.gcsConfigs)
			require.Equal(t, "probe-project", env.gcsConfigs[0].Project)
		}},
	},
}

// probeSidecarRootRejected proves --provenance-sidecar-root without sidecar
// mode is refused by validation before any destination mutation.
func probeSidecarRootRejected(t *testing.T) {
	t.Helper()
	env := newFlagProbeEnv(t)
	_, err := env.run(t, "--provenance-sidecar-root", "s3://dest-bucket/prov/")
	require.Error(t, err)
	require.Contains(t, err.Error(), "provenance")
	require.Empty(t, env.dst.conditionalPutCallsSnapshot(), "refusal must precede destination mutation")
}

// probeResumeRunRoute builds a genuine failed-resumable checkpoint for a
// positional run, resumes it by id, and observes the CLI-pool execution path —
// plus the loud refusal of an unknown id.
func probeResumeRunRoute(t *testing.T) {
	t.Helper()
	env := newFlagProbeEnv(t)
	env.src.putFixture("a.txt", "payload", "src-etag", time.Time{})

	ctx := context.Background()
	cfg := transferReflowCheckpointConfig{
		SourceURI:               "s3://source-bucket/a.txt",
		Dest:                    "s3://dest-bucket/data/",
		RewriteFrom:             "{key}",
		RewriteTo:               "{key}",
		Parallel:                2,
		CheckpointPath:          env.checkpointPath,
		OnCollision:             reflowCollisionSkip,
		Provenance:              provenanceModeNone,
		ProvenanceSuffix:        provenanceSuffix,
		ProvenanceOnWriteError:  provenanceErrorWarn,
		MetadataPolicy:          metadataPolicyClear,
		MetadataOnMissingSource: metadataMissingSkip,
		MetadataSidecarSuffix:   providerfile.DefaultMetadataSidecarSuffix,
		Symlinks:                reflowSymlinkSkip,
		Hidden:                  reflowHiddenSkip,
		OnSourceFailure:         reflowSourceFailSkip,
	}
	state, err := newReflowStateStore(ctx, reflowstate.Config{Path: env.checkpointPath})
	require.NoError(t, err)
	fingerprint, err := checkpointFingerprint(cfg)
	require.NoError(t, err)
	require.NoError(t, state.SetOperationCheckpointIdentity(ctx, operationTransferReflow, fingerprint))
	require.NoError(t, state.Close())
	payload, err := json.Marshal(transferReflowCheckpointPayload{Config: cfg})
	require.NoError(t, err)
	opStore, err := openDefaultOperationCheckpointStore(ctx)
	require.NoError(t, err)
	require.NoError(t, opStore.WriteCheckpoint(ctx, opcheckpoint.Envelope{
		SchemaVersion:     opcheckpoint.SchemaVersion,
		Operation:         operationTransferReflow,
		RunID:             "run_matrix_probe",
		ConfigFingerprint: fingerprint,
		Status:            opcheckpoint.StatusFailedResumable,
		CreatedAt:         time.Now().UTC(),
		Payload:           payload,
	}))

	stdout, err := env.runRaw(t, strings.NewReader(""), "--resume-run", "run_matrix_probe")
	require.NoError(t, err, "resuming a failed-resumable positional run must succeed")
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout),
		"a resumed positional run executes on the CLI pool")
	require.True(t, env.dst.hasObject("data/a.txt"))

	// Unknown id: loud refusal, no execution, no destination mutation.
	env2 := newFlagProbeEnv(t)
	stdout2, err := env2.runRaw(t, strings.NewReader(""), "--resume-run", "run_does_not_exist")
	require.Error(t, err, "unknown --resume-run id must refuse loudly")
	require.NotContains(t, stdout2, `"execution_path"`)
	require.Empty(t, env2.dst.conditionalPutCallsSnapshot())
}

// TestTransferReflowFlagCoverageParityMatrix enforces matrix completeness:
// every registered flag has a declared {engine, cli-pool} row with valid
// dispositions and executable probes, and no row goes stale.
func TestTransferReflowFlagCoverageParityMatrix(t *testing.T) {
	engineValid := map[string]bool{flagHonored: true, flagRoutesCLIPool: true, flagRejectedLoud: true, flagNotApplicable: true}
	poolValid := map[string]bool{flagHonored: true, flagRejectedLoud: true, flagNotApplicable: true}

	registered := map[string]bool{}
	transferReflowCmd.Flags().VisitAll(func(f *pflag.Flag) {
		registered[f.Name] = true
		row, ok := reflowFlagMatrix[f.Name]
		require.True(t, ok,
			"flag --%s has no parity-matrix row: declare its {engine, cli-pool} dispositions and probes (silently-ignored is not an option)", f.Name)
		require.True(t, engineValid[row.engine.disposition], "flag --%s engine disposition %q invalid", f.Name, row.engine.disposition)
		require.True(t, poolValid[row.cliPool.disposition], "flag --%s cli-pool disposition %q invalid", f.Name, row.cliPool.disposition)
		require.NotNil(t, row.engine.probe, "flag --%s engine cell must carry an executable probe", f.Name)
		require.NotNil(t, row.cliPool.probe, "flag --%s cli-pool cell must carry an executable probe", f.Name)
	})

	for name := range reflowFlagMatrix {
		require.True(t, registered[name], "parity-matrix row %q does not match any registered flag (stale row)", name)
	}
}

// TestTransferReflowFlagMatrixBehavioralProbes executes every path cell's
// probe. Package-global state: no t.Parallel.
func TestTransferReflowFlagMatrixBehavioralProbes(t *testing.T) {
	names := make([]string, 0, len(reflowFlagMatrix))
	for name := range reflowFlagMatrix {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		row := reflowFlagMatrix[name]
		t.Run(name+"/engine", func(t *testing.T) { row.engine.probe(t) })
		t.Run(name+"/cli-pool", func(t *testing.T) { row.cliPool.probe(t) })
	}
}

// TestTransferReflowFlagMatrixSentinelSensitivity is the mutation-sentinel
// negative control: the pool-arm honored sentinel for a shared flag is absent
// when the flag is not supplied, proving the probe's assertion is not
// vacuously true — a CLI-pool binding that silently dropped the value would
// fail the probe while the engine arm still passes.
func TestTransferReflowFlagMatrixSentinelSensitivity(t *testing.T) {
	env := newFlagProbeEnv(t)
	stdout, err := env.runPool(t) // no --metadata-set
	requireProbeComplete(t, stdout, err, reflowpkg.ExecutionPathCLIPool)
	require.Empty(t, env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"],
		"without --metadata-set the sentinel key must be absent on the pool path")

	env2 := newFlagProbeEnv(t)
	stdout2, err := env2.run(t) // engine arm, also without the flag
	requireProbeComplete(t, stdout2, err, reflowpkg.ExecutionPathEngine)
	require.Empty(t, env2.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
}

// TestTransferReflowDetectedSubLimitBudgetBothPaths pins the bounded-resource
// contract for detected hard limits below the 16MiB transfer default: the
// derived budget and the actual per-copy retry buffer share one resolved
// bound at or below the limit, an object above that bound spools and
// completes on both execution paths, and the records never admit more bytes
// than the detected limit.
func TestTransferReflowDetectedSubLimitBudgetBothPaths(t *testing.T) {
	const detectedLimit = int64(8) << 20 // derived budget 2MiB, retry cap 2MiB
	largeBody := strings.Repeat("x", 3<<20)

	cases := []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newFlagProbeEnv(t)
			env.src.putFixture("source/large.xml", largeBody, "large-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
			reflowResourceProbeForRun = reflowpkg.ResourceProbe{
				MemoryLimitBytes: func() (int64, string, error) { return detectedLimit, "cgroup_v2", nil },
				FDSoftLimit:      func() (int64, error) { return 100000, nil },
			}

			input := reflowInputLine("source/large.xml", "large-etag", int64(len(largeBody)), "", "") + "\n"
			stdout, err := env.runInput(t, input, tc.extra...)
			require.NoError(t, err)

			run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(run.Data), fmt.Sprintf(`"execution_path":%q`, tc.wantPath))
			require.Contains(t, string(run.Data), `"memory_limit_bytes":8388608`)
			require.Contains(t, string(run.Data), `"memory_limit_source":"cgroup_v2"`)
			require.Contains(t, string(run.Data), `"memory_budget_effective_bytes":2097152`)
			require.Contains(t, string(run.Data), `"retry_buffer_cap_bytes":2097152`)

			records := requireReflowRecords(t, stdout)
			requireReflowStatusReasonCount(t, records, "complete", "", 1)
			require.Equal(t, largeBody, string(env.dst.mustObject("data/source/large.xml")),
				"object above the resolved retry cap must spool and land byte-identical")
		})
	}
}
