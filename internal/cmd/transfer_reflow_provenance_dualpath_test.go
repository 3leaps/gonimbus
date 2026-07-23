package cmd

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

// Engine<->CLI-pool byte-identical provenance sidecar parity (plan test #2). Each
// scenario runs the SAME live stdin input through the real command on both
// execution paths — the engine (default routing) and the genuinely-routed CLI pool
// (poolRoute) — against identically-seeded providers, with both arms pinning one
// fixed run id via --run-id. It then asserts the written gonimbus.provenance.v1
// sidecar payload is byte-identical modulo only the time-varying run.ts (run_id,
// vars, and the full probe audit are compared). The terminal record's provenance
// ref (written, key, uri) is also compared across paths.

func decodeSidecarPayload(t *testing.T, raw []byte) reflowpkg.ProvenanceSidecarPayload {
	t.Helper()
	var p reflowpkg.ProvenanceSidecarPayload
	require.NoError(t, json.Unmarshal(raw, &p))
	return p
}

// parityRunID is the single fixed run id both parity arms pin via --run-id, so
// run.run_id is compared exactly (an audit anchor) and only the time-varying
// run.ts is normalized — the settled decision-4 comparison.
const parityRunID = "parity-run-0001"

// normalizeSidecarTS asserts run_id equals the pinned parity id and run.ts is a
// valid UTC RFC3339Nano stamp, then normalizes only run.ts. run_id is NOT blanked:
// both arms are driven with the same --run-id, so it participates in the equality.
func normalizeSidecarTS(t *testing.T, p *reflowpkg.ProvenanceSidecarPayload) {
	t.Helper()
	require.Equal(t, parityRunID, p.Run.RunID, "both arms pin the same run id")
	require.NotEmpty(t, p.Run.TS, "run.ts must be populated")
	parsed, err := time.Parse(time.RFC3339Nano, p.Run.TS)
	require.NoError(t, err, "run.ts must be RFC3339Nano")
	require.Equal(t, time.UTC, parsed.Location(), "run.ts must be UTC")
	p.Run.TS = ""
}

// requireSidecarContentParity reads the sidecar at key from both arms and asserts
// the full normalized serialized payload is byte-identical (only run.ts normalized;
// run_id, vars, and the full probe audit compared).
func requireSidecarContentParity(t *testing.T, engineDst, poolDst *reflowMemoryProvider, key string) reflowpkg.ProvenanceSidecarPayload {
	t.Helper()
	require.True(t, engineDst.hasObject(key), "engine arm must write the sidecar")
	require.True(t, poolDst.hasObject(key), "pool arm must write the sidecar")
	enginePayload := decodeSidecarPayload(t, engineDst.mustObject(key))
	poolPayload := decodeSidecarPayload(t, poolDst.mustObject(key))
	// Guard against a vacuous pass: the fixture carries vars and a probe audit, so
	// full-payload parity would fail if either arm dropped them.
	require.NotEmpty(t, enginePayload.Vars, "engine sidecar carries vars")
	require.NotNil(t, enginePayload.Probe, "engine sidecar carries the probe audit")
	normalizeSidecarTS(t, &enginePayload)
	normalizeSidecarTS(t, &poolPayload)
	engineBytes, err := json.Marshal(enginePayload)
	require.NoError(t, err)
	poolBytes, err := json.Marshal(poolPayload)
	require.NoError(t, err)
	require.Equal(t, string(poolBytes), string(engineBytes),
		"engine and pool sidecar payloads must be byte-identical modulo run.ts")
	return enginePayload
}

func requireProvenanceRefParity(t *testing.T, engineOut, poolOut string) {
	t.Helper()
	engineTerminal := soleTerminalReflowRecord(t, engineOut)
	poolTerminal := soleTerminalReflowRecord(t, poolOut)
	require.NotNil(t, engineTerminal.Provenance, "engine terminal record carries a provenance ref")
	require.NotNil(t, poolTerminal.Provenance, "pool terminal record carries a provenance ref")
	require.Equal(t, poolTerminal.Provenance.Written, engineTerminal.Provenance.Written)
	require.Equal(t, poolTerminal.Provenance.Key, engineTerminal.Provenance.Key)
	require.Equal(t, poolTerminal.Provenance.URI, engineTerminal.Provenance.URI)
	require.True(t, engineTerminal.Provenance.Written)
}

func TestTransferReflowDualPath_ProvenanceSidecarCompleteParity(t *testing.T) {
	engineEnv := newFlagProbeEnv(t)
	engineOut, err := engineEnv.run(t, "--provenance", "sidecar", "--run-id", parityRunID)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engineOut))

	poolEnv := newFlagProbeEnv(t)
	poolOut, err := poolEnv.runPool(t, "--provenance", "sidecar", "--run-id", parityRunID)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, poolOut))

	key := "data/source/file.xml" + provenanceSuffix
	payload := requireSidecarContentParity(t, engineEnv.dst, poolEnv.dst, key)
	require.Equal(t, "landed", payload.Action)
	require.Equal(t, "s3://source-bucket/source/file.xml", payload.Source.URI)
	require.Equal(t, "s3://dest-bucket/data/source/file.xml", payload.Destination.URI)
	requireProvenanceRefParity(t, engineOut, poolOut)
}

func TestTransferReflowDualPath_ProvenanceSidecarDuplicateSkipParity(t *testing.T) {
	seed := func(env *flagProbeEnv) {
		// Same bytes as the source fixture: the re-drive is proven duplicate by
		// body comparison and skips, so both paths emit the skipped.duplicate sidecar.
		env.dst.putFixture("data/source/file.xml", "payload", "dest-etag", time.Time{})
	}
	engineEnv := newFlagProbeEnv(t)
	seed(engineEnv)
	engineOut, err := engineEnv.run(t, "--provenance", "sidecar", "--run-id", parityRunID)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engineOut))

	poolEnv := newFlagProbeEnv(t)
	seed(poolEnv)
	poolOut, err := poolEnv.runPool(t, "--provenance", "sidecar", "--run-id", parityRunID)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, poolOut))

	key := "data/source/file.xml" + provenanceSuffix
	payload := requireSidecarContentParity(t, engineEnv.dst, poolEnv.dst, key)
	require.Equal(t, "skipped.duplicate", payload.Action)
	require.NotNil(t, payload.Collision)
	require.Equal(t, "duplicate", payload.Collision.Kind)
	require.Equal(t, "dest-etag", payload.Destination.ETag)
	requireProvenanceRefParity(t, engineOut, poolOut)
}

func TestTransferReflowDualPath_ProvenanceSidecarMirroredRootParity(t *testing.T) {
	root := "s3://dest-bucket/prov/run-01/"
	engineEnv := newFlagProbeEnv(t)
	engineOut, err := engineEnv.run(t, "--provenance", "sidecar", "--provenance-sidecar-root", root, "--run-id", parityRunID)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engineOut))

	poolEnv := newFlagProbeEnv(t)
	poolOut, err := poolEnv.runPool(t, "--provenance", "sidecar", "--provenance-sidecar-root", root, "--run-id", parityRunID)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, poolOut))

	key := "prov/run-01/source/file.xml" + provenanceSuffix
	payload := requireSidecarContentParity(t, engineEnv.dst, poolEnv.dst, key)
	require.Equal(t, "landed", payload.Action)
	require.False(t, engineEnv.dst.hasObject("data/source/file.xml"+provenanceSuffix),
		"mirrored placement must not also write the sibling key")
	requireProvenanceRefParity(t, engineOut, poolOut)
}

// TestValidateReflowRunID pins the --run-id safe-segment validator.
func TestValidateReflowRunID(t *testing.T) {
	for _, ok := range []string{"", "run-01", "550e8400-e29b-41d4-a716-446655440000", "a.b_c-1"} {
		require.NoError(t, validateReflowRunID(ok), "accept %q", ok)
	}
	for _, bad := range []string{"../../outside", "/abs", "a/b", `a\b`, "..", ".", "-flag", "a b", "a\x00b", strings.Repeat("x", 129)} {
		require.Error(t, validateReflowRunID(bad), "reject %q", bad)
	}
}

// TestTransferReflowRunIDRejectsUnsafeSegmentZeroEffect pins that an unsafe
// --run-id is refused before any provider/checkpoint effect: nothing lands.
func TestTransferReflowRunIDRejectsUnsafeSegmentZeroEffect(t *testing.T) {
	for _, bad := range []string{"../../outside", "/abs/path", "a/b"} {
		t.Run(bad, func(t *testing.T) {
			env := newFlagProbeEnv(t)
			_, err := env.run(t, "--run-id", bad)
			require.Error(t, err)
			require.Contains(t, err.Error(), "run-id")
			require.False(t, env.dst.hasObject("data/source/file.xml"), "no destination mutation on a refused run id")
			require.Empty(t, env.dstConfigs, "no destination provider constructed on a refused run id")
		})
	}
}

// soleSidecarKey returns the single provenance sidecar object key on dst.
func soleSidecarKey(t *testing.T, dst *reflowMemoryProvider) string {
	t.Helper()
	dst.mu.Lock()
	defer dst.mu.Unlock()
	found := ""
	for k := range dst.objects {
		if strings.HasSuffix(k, provenanceSuffix) {
			require.Empty(t, found, "exactly one sidecar object")
			found = k
		}
	}
	require.NotEmpty(t, found, "a sidecar object must exist")
	return found
}

// TestTransferReflowProvenanceQuarantineGolden pins the still-pool quarantine
// delegation's sidecar content (relocation golden): quarantine remains pool-owned,
// but it emits through the same relocated shared writer, so its payload must be
// byte-identical to the complete expected object. requireQuarantineGolden compares
// the FULL normalized serialized payload against a golden built through the shared
// reflow builder (normalizing only the time-varying run.ts), so dropping any
// carried field — source size/last-modified, destination, routing, run identity
// including the exact tool version, vars, or any probe-audit field — fails.
func requireQuarantineGolden(t *testing.T, raw []byte, wantDestObjectURI string) {
	t.Helper()
	actual := decodeSidecarPayload(t, raw)
	require.NotEmpty(t, actual.Run.TS, "run.ts must be populated")
	require.Equal(t, time.UTC, mustParseRFC3339Nano(t, actual.Run.TS).Location(), "run.ts is UTC")
	// tool_version is build-info sourced (a sibling test injects a version into the
	// shared global), so assert it EXACTLY against the version accessor — not the
	// payload builder — then normalize it alongside run.ts so the literal golden
	// stays deterministic.
	require.Equal(t, reflowToolVersion(), actual.Run.ToolVersion, "sidecar records the build tool version")
	actual.Run.TS = ""
	actual.Run.ToolVersion = ""

	// Independent LITERAL golden — deliberately NOT built with the production
	// BuildProvenanceSidecar, so a builder defect cannot be masked by using the
	// builder as its own oracle. The quarantined object is PUT before its sidecar,
	// so the sidecar records the object's post-write etag/size (the mem provider
	// stamps "dest-"+destKey).
	destKey := strings.TrimPrefix(wantDestObjectURI, "s3://dest-bucket/")
	wantJSON := `{"schema":"gonimbus.provenance.v1","schema_version":"1.0.0",` +
		`"source":{"uri":"s3://source-bucket/source/file.xml","etag":"src-etag","size":7,"last_modified":"2026-01-15T20:53:44Z"},` +
		`"destination":{"uri":"` + wantDestObjectURI + `","etag":"dest-` + destKey + `","size":7},` +
		`"run":{"run_id":"parity-run-0001","ts":"","tool_version":""},` +
		`"routing":{"routing_class":"quarantine","quarantine_prefix":"quarantine"},` +
		`"vars":{"key":"source/file.xml"},` +
		`"probe":{"extractors":[{"name":"key","type":"regex","resolved":true,"required":true,"bytes_at_resolution":64}],"bytes_read":64,"termination_reason":"all_required_resolved"},` +
		`"action":"quarantined"}`
	actualBytes, err := json.Marshal(actual)
	require.NoError(t, err)
	require.Equal(t, wantJSON, string(actualBytes),
		"quarantine sidecar payload must match the independent literal golden (run.ts + tool_version normalized)")
}

func mustParseRFC3339Nano(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	require.NoError(t, err)
	return ts
}

// requireQuarantineTerminalRef pins the exact sidecar key/uri echoed on the pool
// terminal record's provenance ref, so the relocated quarantine writer's placement
// is proven against the terminal a caller consumes (not only the written object).
func requireQuarantineTerminalRef(t *testing.T, out, wantKey, wantURI string) {
	t.Helper()
	term := soleTerminalReflowRecord(t, out)
	require.NotNil(t, term.Provenance, "quarantine terminal carries a provenance ref")
	require.True(t, term.Provenance.Written)
	require.Equal(t, wantKey, term.Provenance.Key)
	require.Equal(t, wantURI, term.Provenance.URI)
}

func TestTransferReflowProvenanceQuarantineGolden(t *testing.T) {
	line := reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "quarantine", "quarantine")

	t.Run("sibling", func(t *testing.T) {
		env := newFlagProbeEnv(t)
		// The record is pre-classified for quarantine; poolRoute forces the still-pool
		// path so the relocated shared writer emits the quarantine sidecar.
		args := append([]string{"--provenance", "sidecar", "--run-id", parityRunID}, poolRoute...)
		out, err := env.runInput(t, line+"\n", args...)
		require.NoError(t, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, out), "quarantine stays pool-routed")
		key := soleSidecarKey(t, env.dst)
		require.Equal(t, "data/quarantine/source/file.xml"+provenanceSuffix, key, "sibling quarantine sidecar key")
		requireQuarantineGolden(t, env.dst.mustObject(key), "s3://dest-bucket/data/quarantine/source/file.xml")
		requireQuarantineTerminalRef(t, out, key, "s3://dest-bucket/"+key)
	})

	t.Run("mirrored", func(t *testing.T) {
		env := newFlagProbeEnv(t)
		args := append([]string{"--provenance", "sidecar", "--provenance-sidecar-root", "s3://dest-bucket/prov/", "--run-id", parityRunID}, poolRoute...)
		out, err := env.runInput(t, line+"\n", args...)
		require.NoError(t, err)
		require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, out))
		key := soleSidecarKey(t, env.dst)
		require.Equal(t, "prov/quarantine/source/file.xml"+provenanceSuffix, key, "mirrored quarantine sidecar key under the root")
		requireQuarantineGolden(t, env.dst.mustObject(key), "s3://dest-bucket/data/quarantine/source/file.xml")
		requireQuarantineTerminalRef(t, out, key, "s3://dest-bucket/"+key)
	})
}
