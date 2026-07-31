package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

const (
	prefixProbeSelector  = "s3://source-bucket/source/"
	patternProbeSelector = "s3://source-bucket/source/*.xml"
	// A separate prefix, so the question-mark identity controls enumerate exactly
	// the keys they describe and nothing the shared probe env happens to seed.
	questionMarkProbeSelector = "s3://source-bucket/q/"
)

// prefixProbeKeys are the objects every prefix control enumerates. Two match the
// pattern and one does not, so the pattern arm is a strict subset rather than a
// relabelled whole.
var prefixProbeKeys = []string{"source/b.xml", "source/c.json", "source/file.xml"}

// newPrefixProbeEnv seeds the shared probe environment with three objects under
// one prefix and turns on real paginated listing.
//
// pageSize is deliberately smaller than the object count: a single-page listing
// would let an implementation that ignores continuation tokens pass.
func newPrefixProbeEnv(t *testing.T, pageSize int) *flagProbeEnv {
	t.Helper()
	env := newFlagProbeEnv(t) // already carries source/file.xml
	env.src.putFixture("source/b.xml", "payload-b", "src-etag-b", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	env.src.putFixture("source/c.json", "payload-c", "src-etag-c", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	env.src.enableListing(pageSize)
	return env
}

func prefixProbeArgs(env *flagProbeEnv, selector string, extra ...string) []string {
	args := []string{
		selector,
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{dir}/{file}",
		"--rewrite-to", "{dir}/{file}",
		"--parallel", "2",
		"--checkpoint", env.checkpointPath,
	}
	return append(args, extra...)
}

// terminalReflowRecordsByKey returns every terminal (non-in_progress) record
// indexed by source key — the multi-object counterpart of
// soleTerminalReflowRecord, which a prefix run outgrows.
//
// It tolerates a run with no object records, unlike requireReflowRecords: a
// selector that matches nothing is a legitimate outcome here, and asserting
// non-emptiness inside the collector would make the zero-match control
// unwritable.
func terminalReflowRecordsByKey(t *testing.T, stdout string) map[string]testReflowData {
	t.Helper()
	byKey := map[string]testReflowData{}
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record testRecordEnvelope
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.Type != reflowpkg.RecordType {
			continue
		}
		var rec testReflowData
		require.NoError(t, json.Unmarshal(record.Data, &rec))
		if rec.Status == "in_progress" {
			continue
		}
		_, dup := byKey[rec.SourceKey]
		require.False(t, dup, "exactly one terminal record per object; %q repeated", rec.SourceKey)
		byKey[rec.SourceKey] = rec
	}
	return byKey
}

func sortedTerminalKeys(byKey map[string]testReflowData) []string {
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// runPrefixDualPath runs the same prefix or pattern selector on both execution
// paths through the real command, using the genuine routing vehicle for the pool
// arm rather than a test-only switch.
func runPrefixDualPath(t *testing.T, selector string, pageSize int) (engineOut, poolOut string, engineEnv, poolEnv *flagProbeEnv) {
	t.Helper()

	engineEnv = newPrefixProbeEnv(t, pageSize)
	engineOut, err := engineEnv.runRaw(t, strings.NewReader(""), prefixProbeArgs(engineEnv, selector)...)
	require.NoError(t, err, "engine arm must succeed")

	poolEnv = newPrefixProbeEnv(t, pageSize)
	poolOut, err = poolEnv.runRaw(t, strings.NewReader(""), prefixProbeArgs(poolEnv, selector, poolRoute...)...)
	require.NoError(t, err, "pool arm must succeed")

	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, engineOut))
	require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, poolOut))
	return engineOut, poolOut, engineEnv, poolEnv
}

// TestTransferReflowDualPath_PrefixPositional pins the migrated prefix cell
// through the real command adapter: both paths enumerate the same objects, land
// the same destinations, and report the same source record, terminals, and
// summary. Only execution_path differs.
func TestTransferReflowDualPath_PrefixPositional(t *testing.T) {
	engineOut, poolOut, engineEnv, poolEnv := runPrefixDualPath(t, prefixProbeSelector, 2)

	// The engine arm genuinely paginated: three objects at two per page.
	require.GreaterOrEqual(t, engineEnv.src.listCallCount(), 2,
		"the control is only meaningful if the selector spanned more than one page")

	for _, key := range prefixProbeKeys {
		require.True(t, engineEnv.dst.hasObject("data/"+key), "engine arm must land %s", key)
		require.True(t, poolEnv.dst.hasObject("data/"+key), "pool arm must land %s", key)
	}

	// Source record parity: one selector, same payload, same position relative to
	// the run record.
	engineSource := sourceRunRecordOf(t, engineOut)
	require.Equal(t, sourceRunRecordOf(t, poolOut), engineSource,
		"source record payload must match across paths")
	require.Equal(t, prefixProbeSelector, engineSource.URI, "the selector, not an enumerated object")
	require.Equal(t, "s3", engineSource.Provider)
	require.Equal(t, "source-bucket", engineSource.Bucket)
	require.Empty(t, engineSource.Root)

	enginePos := recordTypePositions(t, engineOut)
	poolPos := recordTypePositions(t, poolOut)
	require.Equal(t,
		poolPos[reflowpkg.SourceRecordType]-poolPos[reflowpkg.RunRecordType],
		enginePos[reflowpkg.SourceRecordType]-enginePos[reflowpkg.RunRecordType],
		"source record must hold the same position relative to the run record on both paths")

	// Terminal parity, per object.
	engineTerminals := terminalReflowRecordsByKey(t, engineOut)
	poolTerminals := terminalReflowRecordsByKey(t, poolOut)
	require.Equal(t, prefixProbeKeys, sortedTerminalKeys(engineTerminals))
	require.Equal(t, sortedTerminalKeys(poolTerminals), sortedTerminalKeys(engineTerminals))
	for _, key := range prefixProbeKeys {
		requireReflowTerminalEqual(t, engineTerminals[key], poolTerminals[key])
	}

	requireReflowSummaryParity(t, engineOut, poolOut)

	// The run-level selector persists as the selector on the migrated path too.
	storedProvider, storedBucket, storedRoot, storedURI := storedSourceRunMetadata(t, engineEnv.checkpointPath)
	require.Equal(t, "s3", storedProvider)
	require.Equal(t, "source-bucket", storedBucket)
	require.Empty(t, storedRoot)
	require.Equal(t, prefixProbeSelector, storedURI)
}

func TestTransferReflowWholeScopePositionalRunsOnSerialPath(t *testing.T) {
	env := newPrefixProbeEnv(t, 2)
	stdout, err := env.runRaw(t, strings.NewReader(""),
		prefixProbeArgs(env, "s3://source-bucket/")...,
	)
	require.NoError(t, err)
	require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
	require.GreaterOrEqual(t, env.src.listCallCount(), 2,
		"the whole-scope compatibility control must genuinely paginate")
	for _, key := range prefixProbeKeys {
		require.True(t, env.dst.hasObject("data/"+key), "whole-scope run must land %s", key)
	}
}

// TestTransferReflowDualPath_PatternPositional pins the pattern cell: the glob
// selects the same strict subset on both paths, and the source record carries
// the pattern as spelled rather than the prefix it was derived from.
func TestTransferReflowDualPath_PatternPositional(t *testing.T) {
	engineOut, poolOut, engineEnv, poolEnv := runPrefixDualPath(t, patternProbeSelector, 2)

	wantKeys := []string{"source/b.xml", "source/file.xml"}
	for _, key := range wantKeys {
		require.True(t, engineEnv.dst.hasObject("data/"+key), "engine arm must land %s", key)
		require.True(t, poolEnv.dst.hasObject("data/"+key), "pool arm must land %s", key)
	}
	require.False(t, engineEnv.dst.hasObject("data/source/c.json"),
		"engine arm must not copy an object the pattern excludes")
	require.False(t, poolEnv.dst.hasObject("data/source/c.json"),
		"pool arm must not copy an object the pattern excludes")

	engineSource := sourceRunRecordOf(t, engineOut)
	require.Equal(t, sourceRunRecordOf(t, poolOut), engineSource)
	require.Equal(t, patternProbeSelector, engineSource.URI,
		"the selector is the pattern as spelled, not the derived listing prefix")

	engineTerminals := terminalReflowRecordsByKey(t, engineOut)
	poolTerminals := terminalReflowRecordsByKey(t, poolOut)
	require.Equal(t, wantKeys, sortedTerminalKeys(engineTerminals))
	require.Equal(t, sortedTerminalKeys(poolTerminals), sortedTerminalKeys(engineTerminals))
	for _, key := range wantKeys {
		requireReflowTerminalEqual(t, engineTerminals[key], poolTerminals[key])
	}

	requireReflowSummaryParity(t, engineOut, poolOut)
}

// TestTransferReflowDualPath_EscapedLiteralPrefix pins the 8a C1 defect class at
// the prefix boundary of the ADAPTER, which the engine-level control cannot
// reach: the adapter must hand the engine the selector as SPELLED.
//
// The spelling is load-bearing here in a way the plain prefix and pattern
// controls cannot expose, because for those two the canonical form and the
// spelling are the same string. An escaped metacharacter is where they diverge:
// as spelled, `dir\*lit/` is a literal directory listed under `dir*lit/`;
// canonicalized, it re-parses as a GLOB whose derived listing prefix is `dir`
// and whose matcher selects nothing. One object copied versus none.
func TestTransferReflowDualPath_EscapedLiteralPrefix(t *testing.T) {
	const literalKey = "dir*lit/x.xml"

	run := func(t *testing.T, extra ...string) (string, *flagProbeEnv) {
		t.Helper()
		env := newFlagProbeEnv(t)
		env.src.putFixture(literalKey, "payload-lit", "src-etag-lit", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		env.src.enableListing(2)
		args := append([]string{
			`s3://source-bucket/dir\*lit/`,
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

	require.True(t, engineEnv.dst.hasObject("data/"+literalKey),
		"engine arm must copy the object under the literal directory")
	require.True(t, poolEnv.dst.hasObject("data/"+literalKey),
		"pool arm must copy the object under the literal directory")

	require.Equal(t, "dir*lit/", engineEnv.src.listOptsSnapshot()[0].Prefix,
		"listing must use the unescaped literal, not a glob-derived prefix")

	require.Equal(t, sourceRunRecordOf(t, poolOut), sourceRunRecordOf(t, engineOut),
		"source record payload must match across paths")

	engineTerminals := terminalReflowRecordsByKey(t, engineOut)
	poolTerminals := terminalReflowRecordsByKey(t, poolOut)
	require.Equal(t, []string{literalKey}, sortedTerminalKeys(engineTerminals))
	requireReflowTerminalEqual(t, engineTerminals[literalKey], poolTerminals[literalKey])
	requireReflowSummaryParity(t, engineOut, poolOut)
}

// storedItemSourceURIs reads the per-item resume authorities the real checkpoint
// store persisted, using the product's own opener rather than a
// re-implementation. This is the value ItemDone is later asked about, so it is
// the authority under test — not a rendering of it.
func storedItemSourceURIs(t *testing.T, checkpointPath string) []string {
	t.Helper()
	db, err := indexstore.Open(context.Background(), indexstore.Config{Path: checkpointPath})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	rows, err := db.QueryContext(context.Background(), `SELECT source_uri FROM reflow_items ORDER BY source_uri`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var out []string
	for rows.Next() {
		var uri string
		require.NoError(t, rows.Scan(&uri))
		out = append(out, uri)
	}
	require.NoError(t, rows.Err())
	return out
}

// TestTransferReflowDualPath_QuestionMarkKeyIdentity pins object identity end to
// end through the real command and the real checkpoint store, for keys that a
// generic URL rewrite would collapse.
//
// A `?` is a key character under the gonimbus URI grammar, so two objects
// differing only after it are two objects. The checkpoint primary key is
// (source_uri, dest_uri): if the stored authority were rewritten to a common
// value, these two would contend for one row, and on resume one object's
// terminal would answer for the other. The dual-path arm additionally pins that
// the engine and the pool agree on that identity, which the terminal comparator
// previously did not check.
func TestTransferReflowDualPath_QuestionMarkKeyIdentity(t *testing.T) {
	keys := []string{"q/file?version=one", "q/file?version=two"}

	run := func(t *testing.T, extra ...string) (string, *flagProbeEnv) {
		t.Helper()
		env := newFlagProbeEnv(t)
		for i, key := range keys {
			env.src.putFixture(key, "payload-"+key, fmt.Sprintf("etag-%d", i), time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		}
		env.src.enableListing(2)
		args := append([]string{
			questionMarkProbeSelector,
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

	for _, key := range keys {
		require.True(t, engineEnv.dst.hasObject("data/"+key), "engine arm must land %s", key)
		require.True(t, poolEnv.dst.hasObject("data/"+key), "pool arm must land %s", key)
	}

	// Emitted identity: distinct per object, faithful to the listed key, and equal
	// across paths (the last assertion runs inside requireReflowTerminalEqual).
	engineTerminals := terminalReflowRecordsByKey(t, engineOut)
	poolTerminals := terminalReflowRecordsByKey(t, poolOut)
	require.Equal(t, keys, sortedTerminalKeys(engineTerminals))
	for _, key := range keys {
		require.Equal(t, "s3://source-bucket/"+key, engineTerminals[key].SourceURI,
			"the emitted source URI is the listed key, not a query-blanked rewrite")
		requireReflowTerminalEqual(t, engineTerminals[key], poolTerminals[key])
	}

	// Persisted identity: two distinct authorities in the real store, on both paths.
	wantStored := []string{"s3://source-bucket/" + keys[0], "s3://source-bucket/" + keys[1]}
	require.Equal(t, wantStored, storedItemSourceURIs(t, engineEnv.checkpointPath),
		"each listed object must hold its own resume authority")
	require.Equal(t, wantStored, storedItemSourceURIs(t, poolEnv.checkpointPath))
}

// TestTransferReflowPrefixResumeSkipsOnlyTheCompletedQuestionMarkKey is the
// SELECTIVE-resume proof through the real command and the real store: one
// completed question-mark key must not answer for a sibling that differs from it
// only after the `?`.
//
// The scenario is what makes "only" observable. The first run enumerates ONE
// object and completes it. A second object is then added to the source and the
// run is resumed over the same selector, so the resumed run sees one key that is
// already recorded and one that is not. A control that completed both objects up
// front could only ever show all-skip, which is ordinary resume and says nothing
// about whether the two keys are distinguishable.
//
// If the two shared one authority, the resumed run could not tell them apart:
// the recorded key would stop being recognized as itself, or the new key would
// be answered for by a row that was never about it.
func TestTransferReflowPrefixResumeSkipsOnlyTheCompletedQuestionMarkKey(t *testing.T) {
	const (
		completedKey = "q/file?version=one"
		addedKey     = "q/file?version=two"
	)
	modified := time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC)

	env := newFlagProbeEnv(t)
	env.src.putFixture(completedKey, "payload-one", "etag-1", modified)
	env.src.enableListing(2)

	args := []string{
		questionMarkProbeSelector,
		"--dest", "s3://dest-bucket/data/",
		"--rewrite-from", "{dir}/{file}",
		"--rewrite-to", "{dir}/{file}",
		"--checkpoint", env.checkpointPath,
	}

	// Run 1: exactly one object exists, and it completes.
	_, err := env.runRaw(t, strings.NewReader(""), args...)
	require.NoError(t, err)
	require.Equal(t, 1, env.dst.writeCountFor("data/"+completedKey))
	require.Equal(t, []string{"s3://source-bucket/" + completedKey},
		storedItemSourceURIs(t, env.checkpointPath),
		"the first run records exactly the object it copied")

	// A sibling appears under the same selector, differing only after the `?`.
	env.src.putFixture(addedKey, "payload-two", "etag-2", modified)

	// Run 2 with resume: one key is recorded, one is new.
	_, err = env.runRaw(t, strings.NewReader(""), append(append([]string{}, args...), "--resume")...)
	require.NoError(t, err)

	require.Equal(t, 1, env.dst.writeCountFor("data/"+completedKey),
		"the recorded key must still be recognized as itself and skipped")
	require.Equal(t, 1, env.dst.writeCountFor("data/"+addedKey),
		"the new key must be copied; a shared authority would let the recorded row answer for it")

	require.Equal(t, []string{
		"s3://source-bucket/" + completedKey,
		"s3://source-bucket/" + addedKey,
	}, storedItemSourceURIs(t, env.checkpointPath),
		"the store ends with one exact authority per object")
}

// TestTransferReflowPrefixEnumerationFailureIsExternalServiceUnavailable pins
// the command-level half of the enumeration-failure disposition: a List failure
// partway through exits ExitExternalServiceUnavailable, keeps the engine's typed
// identity and the provider's cause intact through the exit error, drains the
// objects already admitted, and emits NO terminal summary.
//
// The pool arm is deliberately NOT asserted for parity here. It writes a summary
// after the same drain and classifies a generic listing failure as an input
// error; that divergence is intended and confined to this failure path. Success
// and zero-match parity remain pinned by the controls above.
func TestTransferReflowPrefixEnumerationFailureIsExternalServiceUnavailable(t *testing.T) {
	listErr := errors.New("listing refused after the first page")
	env := newPrefixProbeEnv(t, 2)
	env.src.failListAfter(2, listErr)

	stdout, err := env.runRaw(t, strings.NewReader(""), prefixProbeArgs(env, prefixProbeSelector)...)
	require.Error(t, err, "a source that could not be enumerated must fail the run")

	var enumErr *reflowpkg.SourceEnumerationError
	require.True(t, errors.As(err, &enumErr),
		fmt.Sprintf("the engine's typed identity must survive to the command boundary, got %v", err))
	require.Equal(t, prefixProbeSelector, enumErr.Selector)
	require.ErrorIs(t, err, listErr, "the provider's own cause must stay unwrap-visible")
	require.Contains(t, err.Error(), "reflow could not enumerate the source")
	require.Contains(t, err.Error(), fmt.Sprintf("(exit code %d)", foundry.ExitExternalServiceUnavailable))

	require.Equal(t, 2, env.src.listCallCount(), "no page may be requested after the failure")

	// The first page drained to real terminals and real destination objects.
	terminals := terminalReflowRecordsByKey(t, stdout)
	require.Len(t, terminals, 2, "admitted work must drain to its own terminals")
	for _, key := range []string{"source/b.xml", "source/c.json"} {
		require.True(t, env.dst.hasObject("data/"+key), "admitted work must land: %s", key)
	}

	require.NotContains(t, recordTypeSequence(t, stdout), reflowpkg.SummaryRecordType,
		"a partial enumeration has no whole-selector accounting to publish")
}

// TestTransferReflowPrefixCancellationKeepsTheCancellationDisposition is the
// command half of the cancellation precedence.
//
// A List interrupted by cancellation must reach the command as a cancellation,
// not as a source that could not be enumerated. The two differ in exit code and
// in what an operator does next: a cancellation is self-inflicted and resumable,
// while an enumeration failure points at the provider. Typing every List error
// as an enumeration failure converts one into the other whenever a run is
// interrupted mid-listing.
//
// The claim is scoped to the classification. How the engine path renders a
// cancellation — currently the bare context error, shared with the record-stream
// path — is pre-existing behavior this slice does not change, so it is not
// asserted here.
func TestTransferReflowPrefixCancellationKeepsTheCancellationDisposition(t *testing.T) {
	env := newPrefixProbeEnv(t, 2)
	env.src.listBlocked = make(chan struct{}, 1)
	env.src.listBlockAfter = 2 // page 1 serves; page 2 blocks until cancelled

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type result struct {
		out string
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := env.runRawContext(t, ctx, strings.NewReader(""), prefixProbeArgs(env, prefixProbeSelector)...)
		done <- result{out: out, err: err}
	}()

	select {
	case <-env.src.listBlocked:
	case r := <-done:
		t.Fatalf("run finished before the listing blocked: err=%v", r.err)
	case <-time.After(10 * time.Second):
		t.Fatal("listing never blocked")
	}
	cancel()

	var got result
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled run did not return")
	}

	require.Error(t, got.err)
	require.ErrorIs(t, got.err, context.Canceled,
		"the cancellation must survive to the command as a cancellation")
	var enumErr *reflowpkg.SourceEnumerationError
	require.False(t, errors.As(got.err, &enumErr),
		"a cancelled listing must not carry the enumeration-failure type")
	require.NotContains(t, got.err.Error(), "could not enumerate the source")
	require.NotContains(t, got.err.Error(),
		fmt.Sprintf("(exit code %d)", foundry.ExitExternalServiceUnavailable),
		"a cancelled run must not take the unlistable-source exit code")
	require.NotContains(t, recordTypeSequence(t, got.out), reflowpkg.SummaryRecordType,
		"an interrupted run publishes no terminal summary")
}

// TestTransferReflowPrefixMatchingNothingSucceeds pins the zero-match case on
// both paths: a pattern that selects no object is an empty, successful run
// rather than a failure.
func TestTransferReflowPrefixMatchingNothingSucceeds(t *testing.T) {
	engineOut, poolOut, engineEnv, poolEnv := runPrefixDualPath(t, "s3://source-bucket/source/*.parquet", 2)

	require.Empty(t, terminalReflowRecordsByKey(t, engineOut))
	require.Empty(t, terminalReflowRecordsByKey(t, poolOut))
	require.False(t, engineEnv.dst.hasObject("data/source/file.xml"))
	require.False(t, poolEnv.dst.hasObject("data/source/file.xml"))
	requireReflowSummaryParity(t, engineOut, poolOut)
}
