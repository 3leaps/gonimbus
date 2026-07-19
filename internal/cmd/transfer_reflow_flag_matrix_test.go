package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

// Flag-coverage parity matrix for `transfer reflow` (standing behavioral parity
// gate, item 3). Every registered flag declares a disposition AND carries an
// executable probe that drives the real command with a representative
// non-default value and asserts the declared outcome:
//
//   - honored:          the probe proves an observable configuration/behavior
//     sentinel bound to the flag value (deleting the flag's
//     wiring fails the probe)
//   - routes-cli-pool:  the probe proves execution_path=cli-pool (the flag, or
//     its non-migrated value, selects the CLI pool where it
//     is honored)
//   - rejected-loud:    the probe proves the command refuses before any
//     destination mutation
//   - not-applicable:   the flag is scoped to a shape this baseline cannot
//     take (companion of another flag, file-source/file-dest
//     only, provider-specific); the probe PINS the documented
//     no-effect so a future behavior change must update the
//     declaration
//
// "silently-ignored" is not a legal category: a flag whose value a selected
// path would drop must either route dispatch, reject loudly, or carry an
// explicit pinned not-applicable disposition. The completeness test fails when
// a registered flag lacks a row (new flags added by later slices must declare
// and prove their disposition) or when a row goes stale.

const (
	flagHonored       = "honored"
	flagRoutesCLIPool = "routes-cli-pool"
	flagRejectedLoud  = "rejected-loud"
	flagNotApplicable = "not-applicable"
)

type reflowFlagBehavior struct {
	category string
	note     string
	probe    func(t *testing.T)
}

// flagProbeEnv is the shared one-object live-run environment for matrix probes.
type flagProbeEnv struct {
	src *reflowMemoryProvider
	dst *reflowMemoryProvider

	mu         sync.Mutex
	srcConfigs []s3.Config
	dstConfigs []s3.Config

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
	})
	return env
}

// run executes the standard one-object live stdin baseline plus extra args.
func (e *flagProbeEnv) run(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	args := append([]string{
		"--stdin",
		"--dest", "s3://dest-bucket/data/",
		"--parallel", "2",
		"--checkpoint", e.checkpointPath,
	}, extra...)
	return e.runRaw(t, strings.NewReader(reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "")+"\n"), args...)
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

func requireProbeComplete(t *testing.T, stdout string, err error) {
	t.Helper()
	require.NoError(t, err)
	records := requireReflowRecords(t, stdout)
	requireReflowStatusReasonCount(t, records, "complete", "", 1)
}

var reflowFlagMatrix = map[string]reflowFlagBehavior{
	"stdin": {category: flagHonored, note: "engine path requires stdin record streams; positional sources route to the pool", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t)
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"dest": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t)
		requireProbeComplete(t, stdout, err)
		require.True(t, env.dst.hasObject("data/source/file.xml"), "--dest base prefix applied")
	}},
	"rewrite-from": {category: flagHonored, note: "applies to records without an explicit dest_rel_key", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		in := strings.NewReader(reflowInputLineNoDestRel("source/file.xml", "src-etag", int64(len("payload"))) + "\n")
		stdout, err := env.runRaw(t, in, "--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2", "--checkpoint", env.checkpointPath, "--rewrite-from", "source/{name}", "--rewrite-to", "renamed/{name}")
		requireProbeComplete(t, stdout, err)
		require.True(t, env.dst.hasObject("data/renamed/file.xml"), "rewrite templates applied on the engine path")
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"rewrite-to": {category: flagHonored, note: "applies to records without an explicit dest_rel_key", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		in := strings.NewReader(reflowInputLineNoDestRel("source/file.xml", "src-etag", int64(len("payload"))) + "\n")
		stdout, err := env.runRaw(t, in, "--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2", "--checkpoint", env.checkpointPath, "--rewrite-from", "{head}/file.xml", "--rewrite-to", "{head}/copy.xml")
		requireProbeComplete(t, stdout, err)
		require.True(t, env.dst.hasObject("data/source/copy.xml"), "rewrite destination template applied")
	}},
	"parallel": {category: flagHonored, note: "dual-path max-in-flight harness enforces the behavioral half", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--parallel", "3")
		requireProbeComplete(t, stdout, err)
		run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
		require.Contains(t, string(run.Data), `"parallel":3`)
	}},
	"no-adaptive": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--no-adaptive")
		requireProbeComplete(t, stdout, err)
		run := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
		require.Contains(t, string(run.Data), `"adaptive_enabled":false`)
	}},
	"dry-run": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--dry-run")
		require.NoError(t, err)
		records := requireReflowRecords(t, stdout)
		requireReflowStatusReasonCount(t, records, "planned", "", 1)
		require.False(t, env.dst.hasObject("data/source/file.xml"), "dry-run must not land")
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"resume": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout1, err := env.run(t)
		requireProbeComplete(t, stdout1, err)
		stdout2, err := env.run(t, "--resume")
		require.NoError(t, err)
		records := requireReflowRecords(t, stdout2)
		requireReflowStatusReasonCount(t, records, "skipped", "resume.complete", 1)
	}},
	"resume-run": {category: flagRoutesCLIPool, note: "run-id resume is a positional-input shape; unknown ids refuse loudly", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.runRaw(t, strings.NewReader(""), "--resume-run", "run_does_not_exist", "--dest", "s3://dest-bucket/data/")
		require.Error(t, err, "unknown --resume-run id must refuse loudly")
		require.NotContains(t, stdout, `"execution_path":"engine"`)
		require.Empty(t, env.dst.putCallsSnapshot())
		require.Empty(t, env.dst.conditionalPutCallsSnapshot())
	}},
	"checkpoint": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t)
		requireProbeComplete(t, stdout, err)
		_, statErr := os.Stat(env.checkpointPath)
		require.NoError(t, statErr, "--checkpoint path must hold the state store")
	}},
	"overwrite": {category: flagRoutesCLIPool, note: "overwrite collision semantics not migrated", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--overwrite")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
	}},
	"on-collision": {category: flagHonored, note: "engine executes skip-if-duplicate|fail; other modes route to the pool", probe: func(t *testing.T) {
		// Migrated non-default value stays on the engine and is enforced.
		env := newFlagProbeEnv(t)
		env.dst.putFixture("data/source/file.xml", "payload", "src-etag", time.Time{})
		stdout, err := env.run(t, "--on-collision", "fail")
		require.Error(t, err, "--on-collision=fail must fail on a duplicate")
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		failed := requireReflowData(t, stdout, "failed")
		require.Equal(t, "collision.exists.duplicate", failed.Reason)

		// Non-migrated value routes to the pool where it is honored.
		env2 := newFlagProbeEnv(t)
		env2.dst.putFixture("data/source/file.xml", "different", "other-etag", time.Time{})
		stdout2, err := env2.run(t, "--on-collision", "quarantine", "--collision-quarantine-prefix", "quar/")
		require.NoError(t, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout2))
	}},
	"collision-quarantine-prefix": {category: flagRoutesCLIPool, note: "quarantine mode not migrated; prefix honored on the pool", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.dst.putFixture("data/source/file.xml", "different", "other-etag", time.Time{})
		stdout, err := env.run(t, "--on-collision", "quarantine", "--collision-quarantine-prefix", "quar/")
		require.NoError(t, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
		require.True(t, env.dst.hasObject("data/quar/source/file.xml"), "quarantine prefix applied on the pool")
	}},

	"provenance": {category: flagRoutesCLIPool, note: "provenance sidecars not migrated", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--provenance", "sidecar")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
		require.True(t, env.dst.hasObject("data/source/file.xml"+provenanceSuffix), "sidecar written on the pool")
	}},
	"provenance-sidecar-root": {category: flagRejectedLoud, note: "companion of --provenance; validation refuses it without --provenance sidecar", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		_, err := env.run(t, "--provenance-sidecar-root", "s3://dest-bucket/prov/")
		require.Error(t, err, "sidecar root without sidecar mode must refuse loudly")
		require.Contains(t, err.Error(), "provenance")
		require.Empty(t, env.dst.conditionalPutCallsSnapshot(), "refusal must precede destination mutation")
	}},
	"provenance-suffix": {category: flagNotApplicable, note: "companion of --provenance; honored on the pool when provenance routes there", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--provenance-suffix", ".custom.json")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout), "suffix alone must not route")

		env2 := newFlagProbeEnv(t)
		stdout2, err := env2.run(t, "--provenance", "sidecar", "--provenance-suffix", ".custom.json")
		requireProbeComplete(t, stdout2, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout2))
		require.True(t, env2.dst.hasObject("data/source/file.xml.custom.json"), "custom suffix honored with provenance active")
	}},
	"provenance-on-write-error": {category: flagNotApplicable, note: "companion of --provenance; no effect while mode=none (pinned)", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--provenance-on-write-error", "fail")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"allow-unsafe-suffix": {category: flagNotApplicable, note: "companion of --provenance-suffix; no effect while mode=none (pinned)", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--allow-unsafe-suffix")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},

	"metadata-policy": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"team": "alpha"}})
		stdout, err := env.run(t, "--metadata-policy", "preserve")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, "alpha", env.dst.metaSnapshot("data/source/file.xml").Metadata["team"], "preserve policy copies source metadata")
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"metadata-set": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--metadata-set", "owner=probe")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, "probe", env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
	}},
	"metadata-set-from-source-key": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{Metadata: map[string]string{"team": "alpha"}})
		stdout, err := env.run(t, "--metadata-set-from-source-key", "owner=team")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, "alpha", env.dst.metaSnapshot("data/source/file.xml").Metadata["owner"])
	}},
	"metadata-set-from-source-derived": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--metadata-set-from-source-derived", "origin-etag=system.etag")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, "src-etag", env.dst.metaSnapshot("data/source/file.xml").Metadata["origin-etag"])
	}},
	"metadata-on-missing-source": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--metadata-set-from-source-key", "owner=absent-key", "--metadata-on-missing-source", "fail")
		require.Error(t, err, "missing source metadata with policy=fail must fail the object")
		records := requireReflowRecords(t, stdout)
		requireReflowStatusReasonCount(t, records, "complete", "", 0)
	}},
	"preserve-content-type": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.src.putFixtureWithMeta("source/file.xml", "payload", "src-etag", time.Time{}, provider.ObjectMeta{ContentType: "text/xml"})
		stdout, err := env.run(t, "--preserve-content-type")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, "text/xml", env.dst.metaSnapshot("data/source/file.xml").ContentType)
	}},
	"destination-storage-class": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--destination-storage-class", "STANDARD_IA")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, "STANDARD_IA", env.dst.metaSnapshot("data/source/file.xml").StorageClass)
	}},
	"metadata-sidecar-suffix": {category: flagNotApplicable, note: "file destinations only; file destinations route to the pool; no effect on object-store dests (pinned)", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--metadata-sidecar-suffix", ".meta.json")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		require.False(t, env.dst.hasObject("data/source/file.xml.meta.json"))
	}},

	"symlinks": {category: flagNotApplicable, note: "file-source policy; no effect on stdin record streams (pinned); positional file sources route to the pool", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--symlinks", "follow")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"hidden": {category: flagNotApplicable, note: "file-source policy; no effect on stdin record streams (pinned); positional file sources route to the pool", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--hidden", "include")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"exclude": {category: flagNotApplicable, note: "file-source policy; no effect on stdin record streams (pinned); positional file sources route to the pool", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--exclude", "*.tmp")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"preserve-mode": {category: flagRoutesCLIPool, note: "preserve-mode not migrated; warns and no-ops outside file->file", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--preserve-mode")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
	}},
	"on-source-failure": {category: flagHonored, note: "engine executes skip (default); fail routes to the pool where it is honored", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t)
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))

		env2 := newFlagProbeEnv(t)
		stdout2, err := env2.run(t, "--on-source-failure", "fail")
		requireProbeComplete(t, stdout2, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout2))
	}},

	"src-region": {category: flagHonored, note: "source provider construction shared by both paths", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--src-region", "eu-probe-1")
		requireProbeComplete(t, stdout, err)
		require.NotEmpty(t, env.srcConfigs)
		require.Equal(t, "eu-probe-1", env.srcConfigs[0].Region, "source construction must carry --src-region")
	}},
	"src-profile": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--src-profile", "probe-profile")
		requireProbeComplete(t, stdout, err)
		require.NotEmpty(t, env.srcConfigs)
		require.Equal(t, "probe-profile", env.srcConfigs[0].Profile)
	}},
	"src-endpoint": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--src-endpoint", "https://s3.probe.example")
		requireProbeComplete(t, stdout, err)
		require.NotEmpty(t, env.srcConfigs)
		require.Equal(t, "https://s3.probe.example", env.srcConfigs[0].Endpoint)
	}},
	"src-gcp-project": {category: flagNotApplicable, note: "GCS sources only; gs:// record streams are not migrated (sniff routes them to the pool); no effect on s3 sources (pinned)", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--src-gcp-project", "probe-project")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
	"dest-region": {category: flagHonored, note: "destination provider constructed before dispatch", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--dest-region", "eu-probe-2")
		requireProbeComplete(t, stdout, err)
		require.NotEmpty(t, env.dstConfigs)
		require.Equal(t, "eu-probe-2", env.dstConfigs[0].Region)
	}},
	"dest-profile": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--dest-profile", "probe-dest-profile")
		requireProbeComplete(t, stdout, err)
		require.NotEmpty(t, env.dstConfigs)
		require.Equal(t, "probe-dest-profile", env.dstConfigs[0].Profile)
	}},
	"dest-endpoint": {category: flagHonored, probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--dest-endpoint", "https://s3.dest.example")
		requireProbeComplete(t, stdout, err)
		require.NotEmpty(t, env.dstConfigs)
		require.Equal(t, "https://s3.dest.example", env.dstConfigs[0].Endpoint)
	}},
	"dest-gcp-project": {category: flagNotApplicable, note: "GCS destinations only; no effect on s3 destinations (pinned)", probe: func(t *testing.T) {
		env := newFlagProbeEnv(t)
		stdout, err := env.run(t, "--dest-gcp-project", "probe-project")
		requireProbeComplete(t, stdout, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	}},
}

// TestTransferReflowFlagCoverageParityMatrix enforces matrix completeness:
// every registered flag has a declared row with a valid category and a probe,
// and no row goes stale.
func TestTransferReflowFlagCoverageParityMatrix(t *testing.T) {
	valid := map[string]bool{flagHonored: true, flagRoutesCLIPool: true, flagRejectedLoud: true, flagNotApplicable: true}

	registered := map[string]bool{}
	transferReflowCmd.Flags().VisitAll(func(f *pflag.Flag) {
		registered[f.Name] = true
		row, ok := reflowFlagMatrix[f.Name]
		require.True(t, ok,
			"flag --%s has no parity-matrix row: declare its disposition and probe (silently-ignored is not an option)", f.Name)
		require.True(t, valid[row.category], "flag --%s disposition %q invalid", f.Name, row.category)
		require.NotNil(t, row.probe, "flag --%s must carry an executable probe", f.Name)
	})

	for name := range reflowFlagMatrix {
		require.True(t, registered[name], "parity-matrix row %q does not match any registered flag (stale row)", name)
	}
}

// TestTransferReflowFlagMatrixBehavioralProbes executes every row's probe:
// each flag's declared disposition is proven against the real command with a
// representative non-default value. Package-global state: no t.Parallel.
func TestTransferReflowFlagMatrixBehavioralProbes(t *testing.T) {
	names := make([]string, 0, len(reflowFlagMatrix))
	for name := range reflowFlagMatrix {
		names = append(names, name)
	}
	// Deterministic subtest order.
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	for _, name := range names {
		row := reflowFlagMatrix[name]
		t.Run(name, func(t *testing.T) {
			row.probe(t)
		})
	}
}
