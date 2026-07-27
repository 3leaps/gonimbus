package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

const positionalProbeObject = "s3://source-bucket/source/file.xml"

// runPositionalDualPath runs the same exact-object positional argument on both
// execution paths through the real command, using the genuine routing vehicle
// for the pool arm rather than a test-only switch.
func runPositionalDualPath(t *testing.T, extra ...string) (engineOut, poolOut string) {
	t.Helper()

	baseArgs := func(env *flagProbeEnv, more ...string) []string {
		args := []string{
			positionalProbeObject,
			"--dest", "s3://dest-bucket/data/",
			"--rewrite-from", "{dir}/{file}",
			"--rewrite-to", "{dir}/{file}",
			"--parallel", "2",
			"--checkpoint", env.checkpointPath,
		}
		return append(append(args, extra...), more...)
	}

	envEngine := newFlagProbeEnv(t)
	engineOut, err := envEngine.runRaw(t, strings.NewReader(""), baseArgs(envEngine)...)
	require.NoError(t, err, "engine arm must succeed")
	require.True(t, envEngine.dst.hasObject("data/source/file.xml"), "engine arm must land the object")

	envPool := newFlagProbeEnv(t)
	poolOut, err = envPool.runRaw(t, strings.NewReader(""), baseArgs(envPool, poolRoute...)...)
	require.NoError(t, err, "pool arm must succeed")
	require.True(t, envPool.dst.hasObject("data/source/file.xml"), "pool arm must land the object")

	return engineOut, poolOut
}

// recordTypeSequence returns the ordered JSONL record types emitted on a run, so
// a test can pin an event's POSITION rather than only its presence.
func recordTypeSequence(t *testing.T, stdout string) []string {
	t.Helper()
	var types []string
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		types = append(types, rec.Type)
	}
	return types
}

func sourceRunRecordOf(t *testing.T, stdout string) reflowpkg.SourceRunRecord {
	t.Helper()
	rec := requireRecord(t, stdout, reflowpkg.SourceRecordType, "")
	var out reflowpkg.SourceRunRecord
	require.NoError(t, json.Unmarshal(rec.Data, &out))
	return out
}

// TestTransferReflowDualPath_ExactObjectPositional pins the migrated
// exact-object positional cell through the real command adapter: the engine arm
// reports execution_path=engine, and the source record's PAYLOAD and its
// POSITION relative to the run record are identical on both paths.
func TestTransferReflowDualPath_ExactObjectPositional(t *testing.T) {
	engineOut, poolOut := runPositionalDualPath(t)

	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engineOut))
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, poolOut))

	// Payload parity.
	engineSource := sourceRunRecordOf(t, engineOut)
	poolSource := sourceRunRecordOf(t, poolOut)
	require.Equal(t, poolSource, engineSource, "source record payload must match across paths")
	require.Equal(t, positionalProbeObject, engineSource.URI)
	require.Equal(t, "s3", engineSource.Provider)
	require.Equal(t, "source-bucket", engineSource.Bucket)
	require.Empty(t, engineSource.Root)
	require.False(t, engineSource.OutputOnly)

	// Position parity: the source record's index relative to the run record is
	// the same on both paths, so migrating the cell did not reorder the run
	// preamble a consumer reads.
	enginePos := recordTypePositions(t, engineOut)
	poolPos := recordTypePositions(t, poolOut)
	require.Equal(t,
		poolPos[reflowpkg.SourceRecordType]-poolPos[reflowpkg.RunRecordType],
		enginePos[reflowpkg.SourceRecordType]-enginePos[reflowpkg.RunRecordType],
		"source record must hold the same position relative to the run record on both paths")
	require.Greater(t, enginePos[reflowpkg.SourceRecordType], enginePos[reflowpkg.RunRecordType],
		"the source record follows the run record")

	// Terminal and summary parity.
	requireReflowTerminalEqual(t, soleTerminalReflowRecord(t, engineOut), soleTerminalReflowRecord(t, poolOut))
	requireReflowSummaryParity(t, engineOut, poolOut)
}

// recordTypePositions maps each record type to the index of its FIRST emission.
func recordTypePositions(t *testing.T, stdout string) map[string]int {
	t.Helper()
	positions := map[string]int{}
	for i, typ := range recordTypeSequence(t, stdout) {
		if _, seen := positions[typ]; !seen {
			positions[typ] = i
		}
	}
	return positions
}

// TestTransferReflowPositionalUnmigratedShapesFallThrough pins EVERY shape the
// handoff claims is still pool-owned, so the guard cannot silently broaden past
// its declared boundary.
//
// Prefix and pattern left this table in 8b — they now execute on the engine, and
// TestTransferReflowDualPath_PrefixPositional pins that they do. A local file
// tree is the one positional shape still pool-owned.
func TestTransferReflowPositionalUnmigratedShapesFallThrough(t *testing.T) {
	for name, source := range map[string]string{
		"file tree": "", // filled per-case below; a local root has no bucket
	} {
		t.Run(name, func(t *testing.T) {
			env := newFlagProbeEnv(t)
			if source == "" {
				source = "file://" + t.TempDir()
			}
			stdout, err := env.runRaw(t, strings.NewReader(""),
				source,
				"--dest", "s3://dest-bucket/data/",
				"--rewrite-from", "{dir}/{file}",
				"--rewrite-to", "{dir}/{file}",
				"--checkpoint", env.checkpointPath,
			)
			require.NoError(t, err)
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout),
				"%s positional sources are not migrated in this slice", name)
		})
	}
}

// TestTransferReflowDualPath_EscapedLiteralPositional pins that an escaped
// literal metacharacter — a supported exact-object spelling — copies identically
// on both paths. The engine parses the source as spelled, so the escape survives
// the adapter handoff; canonicalizing it first would strip the escape and refuse
// the object as a pattern on the engine arm only.
func TestTransferReflowDualPath_EscapedLiteralPositional(t *testing.T) {
	for name, tc := range map[string]struct{ spelled, literal string }{
		"escaped asterisk":      {`s3://source-bucket/source/file\*.xml`, "source/file*.xml"},
		"escaped question mark": {`s3://source-bucket/source/file\?.xml`, "source/file?.xml"},
		"escaped brackets":      {`s3://source-bucket/source/\[b\].xml`, "source/[b].xml"},
	} {
		t.Run(name, func(t *testing.T) {
			run := func(t *testing.T, extra ...string) (string, *flagProbeEnv) {
				t.Helper()
				env := newFlagProbeEnv(t)
				env.src.putFixture(tc.literal, "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
				args := append([]string{
					tc.spelled,
					"--dest", "s3://dest-bucket/data/",
					"--rewrite-from", "{dir}/{file}",
					"--rewrite-to", "{dir}/{file}",
					"--checkpoint", env.checkpointPath,
				}, extra...)
				stdout, err := env.runRaw(t, strings.NewReader(""), args...)
				require.NoError(t, err)
				return stdout, env
			}

			engineOut, engineEnv := run(t)
			poolOut, poolEnv := run(t, poolRoute...)

			require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engineOut))
			require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, poolOut))

			require.True(t, engineEnv.dst.hasObject("data/"+tc.literal),
				"engine arm must land the literal object")
			require.True(t, poolEnv.dst.hasObject("data/"+tc.literal),
				"pool arm must land the literal object")

			require.Equal(t, sourceRunRecordOf(t, poolOut), sourceRunRecordOf(t, engineOut),
				"source record payload must match across paths")
			requireReflowTerminalEqual(t, soleTerminalReflowRecord(t, engineOut), soleTerminalReflowRecord(t, poolOut))
			requireReflowSummaryParity(t, engineOut, poolOut)
		})
	}
}

// TestTransferReflowResumeRunDispositionMatchesObservedRoute is the matrix
// contract assertion for the cell 8a changed: the disposition each column
// DECLARES must agree with the execution path a real resumed run OBSERVES.
//
// The completeness gate only validates that a disposition string is legal, so
// without this a row could claim routes-cli-pool while its probe proves engine
// dispatch and nothing would fail. Flipping the engine cell back to
// flagRoutesCLIPool now fails here.
func TestTransferReflowResumeRunDispositionMatchesObservedRoute(t *testing.T) {
	behavior, ok := reflowFlagMatrix["resume-run"]
	require.True(t, ok, "resume-run must be declared in the matrix")

	for _, tc := range []struct {
		column      string
		disposition string
		scenario    resumeRunScenario
	}{
		{columnEngine, behavior.engine.disposition, resumeExactObjectSource},
		{columnCLIPool, behavior.cliPool.disposition, resumeFileTreeSource},
	} {
		t.Run(tc.column, func(t *testing.T) {
			want := declaredExecutionPath(tc.disposition, tc.column)
			require.NotEmpty(t, want,
				"resume-run/%s declares %q, which makes no dispatch claim; this cell must declare one",
				tc.column, tc.disposition)
			// The scenario is fixed per column and does NOT derive from want, so a
			// declaration that disagrees with observed dispatch fails here instead
			// of selecting a scenario that satisfies its own claim.
			probeResumeRunRoute(t, tc.scenario, want)
		})
	}
}

// TestTransferReflowPositionalGlobSpellingSelectsTheSourceForm is the
// counterpart to the escaped-literal control: the same characters, escaped and
// unescaped, must still take DIFFERENT source forms.
//
// Both forms now execute on the engine, so the execution path no longer
// distinguishes them — this control observes the distinction that actually
// matters instead. An exact object is planned from its URI and issues NO List;
// a pattern is planned BY listing. That difference is the whole reason the
// adapter must hand over the source as spelled.
func TestTransferReflowPositionalGlobSpellingSelectsTheSourceForm(t *testing.T) {
	t.Run("escaped literal is an exact object and never lists", func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.src.enableListing(2) // available, and must go unused
		env.src.putFixture("source/file*.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))

		stdout, err := env.runRaw(t, strings.NewReader(""),
			`s3://source-bucket/source/file\*.xml`,
			"--dest", "s3://dest-bucket/data/",
			"--rewrite-from", "{dir}/{file}",
			"--rewrite-to", "{dir}/{file}",
			"--checkpoint", env.checkpointPath,
		)
		require.NoError(t, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		require.Equal(t, 0, env.src.listCallCount(),
			"an exact object is planned from its URI, so no enumeration may happen")
		require.Equal(t, "s3://source-bucket/source/file*.xml", sourceRunRecordOf(t, stdout).URI,
			"the selector is the literal object")
		require.True(t, env.dst.hasObject("data/source/file*.xml"))
	})

	t.Run("unescaped glob is a pattern and lists", func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.src.enableListing(2)

		stdout, err := env.runRaw(t, strings.NewReader(""),
			"s3://source-bucket/source/file*.xml",
			"--dest", "s3://dest-bucket/data/",
			"--rewrite-from", "{dir}/{file}",
			"--rewrite-to", "{dir}/{file}",
			"--checkpoint", env.checkpointPath,
		)
		require.NoError(t, err)
		require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
		require.Positive(t, env.src.listCallCount(),
			"a pattern is planned by listing, so enumeration must happen")
		require.Equal(t, "s3://source-bucket/source/file*.xml", sourceRunRecordOf(t, stdout).URI,
			"the selector is the pattern as spelled")
		require.True(t, env.dst.hasObject("data/source/file.xml"),
			"the glob must select the object it matches")
	})
}

// storedSourceRunMetadata reads the run-level source columns the checkpoint
// store persists, using the product's own opener rather than a re-implementation.
func storedSourceRunMetadata(t *testing.T, checkpointPath string) (provider, bucket, root, sourceURI string) {
	t.Helper()
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: checkpointPath})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	var p, b, r, u sql.NullString
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT source_provider, source_bucket, source_root, source_uri FROM reflow_meta WHERE id = 1`,
	).Scan(&p, &b, &r, &u))
	return p.String, b.String, r.String, u.String
}

// TestTransferReflowPositionalEngineWritesSourceRunMetadata pins the real
// command-to-store composition: after a migrated exact-object run, the run-level
// selector is actually PERSISTED in the checkpoint store through the engine's
// optional capability, not merely echoed in the emitted source record.
//
// Reading the store is the point. Asserting the stdout record alone would stay
// green if the capability write were removed entirely.
func TestTransferReflowPositionalEngineWritesSourceRunMetadata(t *testing.T) {
	env := newFlagProbeEnv(t)
	stdout, err := env.runRaw(t, strings.NewReader(""),
		positionalProbeObject,
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{dir}/{file}",
		"--rewrite-to", "{dir}/{file}",
		"--checkpoint", env.checkpointPath,
	)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))

	storedProvider, storedBucket, storedRoot, storedURI := storedSourceRunMetadata(t, env.checkpointPath)
	require.Equal(t, "s3", storedProvider)
	require.Equal(t, "source-bucket", storedBucket)
	require.Empty(t, storedRoot, "an object-store selector carries no local root")
	require.Equal(t, positionalProbeObject, storedURI, "the persisted run-level selector")

	// The emitted record agrees with what was persisted.
	require.Equal(t, storedURI, sourceRunRecordOf(t, stdout).URI)
}

// TestTransferReflowPositionalPoolWritesSameSourceRunMetadata pins that the
// migration did not change what lands in the store: the pool arm persists the
// same run-level selector the engine arm does.
func TestTransferReflowPositionalPoolWritesSameSourceRunMetadata(t *testing.T) {
	env := newFlagProbeEnv(t)
	args := append([]string{
		positionalProbeObject,
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{dir}/{file}",
		"--rewrite-to", "{dir}/{file}",
		"--checkpoint", env.checkpointPath,
	}, poolRoute...)
	stdout, err := env.runRaw(t, strings.NewReader(""), args...)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))

	storedProvider, storedBucket, storedRoot, storedURI := storedSourceRunMetadata(t, env.checkpointPath)
	require.Equal(t, "s3", storedProvider)
	require.Equal(t, "source-bucket", storedBucket)
	require.Empty(t, storedRoot)
	require.Equal(t, positionalProbeObject, storedURI)
}
