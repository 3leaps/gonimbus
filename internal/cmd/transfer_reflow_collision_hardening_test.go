package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

// dumpReflowItemErrorMessages reads every persisted reflow_items.error_message
// from a checkpoint database, for redaction assertions on the durable cause.
func dumpReflowItemErrorMessages(t *testing.T, checkpointPath string) []string {
	t.Helper()
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(checkpointPath)}).String()
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(), "SELECT COALESCE(error_message, '') FROM reflow_items")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var msg string
		require.NoError(t, rows.Scan(&msg))
		out = append(out, msg)
	}
	require.NoError(t, rows.Err())
	return out
}

func transferDefaultBudget() int64 { return transfer.DefaultRetryBufferMaxMemoryBytes * 64 }

// reflowInputLineFull builds a gonimbus.reflow.input.v1 line with explicit
// control over declared size (nil omits the field) and LastModified, plus a
// destination rel-key — the knobs the size-knownness and fan-in hardening tests
// need and the fixed-timestamp helpers do not offer.
func reflowInputLineFull(sourceKey, destRel, etag string, size *int64, lastModified string) string {
	data := map[string]any{
		"source_uri":   "s3://source-bucket/" + sourceKey,
		"source_key":   sourceKey,
		"source_etag":  etag,
		"dest_rel_key": destRel,
		"vars":         map[string]string{"key": sourceKey},
	}
	if size != nil {
		data["source_size_bytes"] = *size
	}
	if lastModified != "" {
		data["source_last_modified"] = lastModified
	}
	line, err := json.Marshal(map[string]any{"type": "gonimbus.reflow.input.v1", "data": data})
	if err != nil {
		panic(err)
	}
	return string(line)
}

// TestTransferReflowSourceSizeKnownness pins the fail-closed size guard on the
// source-newer equal-timestamp tie-breaker (SR-CVG-S2-F1 / E-CVG-S2-P2): an
// unknown or negative source size can never be read as "a different size" and
// authorize a destructive overwrite; a known size difference still resolves.
func TestTransferReflowSourceSizeKnownness(t *testing.T) {
	const equalTime = "2026-01-15T20:53:44Z"
	equalTimeStamp := time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC)

	t.Run("negative_size_is_invalid_input_no_mutation", func(t *testing.T) {
		env := newFlagProbeEnv(t)
		env.dst.putFixture("data/source/file.xml", "old payload", "dest-etag", equalTimeStamp)
		line := reflowInputLineFull("source/file.xml", "source/file.xml", "src-etag", int64Ptr(-1), equalTime)
		stdout, err := env.runInput(t, line+"\n", "--on-collision", "overwrite-if-source-newer")
		require.Error(t, err, "a negative declared size must fail the run")
		require.Positive(t, reflowSummaryTallyOf(t, stdout).InvalidInputs, "negative size is rejected as invalid input")
		require.Equal(t, "old payload", string(env.dst.mustObject("data/source/file.xml")), "no destination mutation on invalid input")
	})

	t.Run("equal_time_unknown_size_refuses_fail_closed", func(t *testing.T) {
		// Size omitted (unknown) + a source HEAD that fails (optional, LastModified
		// present) leaves the size unknown at an equal-timestamp conflict.
		withTransferReflowTestState(t)
		srcBase := newReflowMemoryProvider()
		srcBase.putFixture("source/file.xml", "payload", "src-etag", equalTimeStamp)
		dst := newReflowMemoryProvider()
		dst.putFixture("data/source/file.xml", "old pay", "dest-etag", equalTimeStamp) // different body
		reflowResourceProbeForRun = reflowpkg.ResourceProbe{
			MemoryLimitBytes: func() (int64, string, error) { return int64(8) << 20, "cgroup_v2", nil },
			FDSoftLimit:      func() (int64, error) { return 100000, nil },
		}
		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				switch cfg.Bucket {
				case "source-bucket":
					return headFailingSourceProvider{srcBase}, nil
				case "dest-bucket":
					return dst, nil
				default:
					return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
				}
			},
		})
		line := reflowInputLineFull("source/file.xml", "source/file.xml", "src-etag", nil, equalTime)
		stdout, err := runTransferReflowRawStdin(t, line+"\n", "--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2", "--on-collision", "overwrite-if-source-newer")
		require.Error(t, err)
		rec := soleTerminalReflowRecord(t, stdout)
		require.Equal(t, "failed", rec.Status)
		require.Equal(t, "collision.source_size_unavailable", rec.Reason)
		require.Equal(t, "old pay", string(dst.mustObject("data/source/file.xml")), "an undecidable tie must never mutate the destination")
	})

	t.Run("equal_time_known_different_size_overwrites", func(t *testing.T) {
		engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
			// Same timestamp, a known different (shorter) size -> overwrite.
			dst.putFixture("data/source/file.xml", "old", "dest-etag", equalTimeStamp)
		}, "--on-collision", "overwrite-if-source-newer")
		require.NoError(t, engine.err)
		require.NoError(t, pool.err)
		engineRec := soleTerminalReflowRecord(t, engine.stdout)
		require.Equal(t, "complete", engineRec.Status)
		requireSourceNewerCollisionEqual(t, engineRec, reasonEqualSizeDiffers, equalTime, equalTime)
		requireReflowTerminalEqual(t, engineRec, soleTerminalReflowRecord(t, pool.stdout))
		require.Equal(t, "payload", string(engine.dst.mustObject("data/source/file.xml")))
	})

	t.Run("equal_time_known_equal_size_preserves", func(t *testing.T) {
		engine, pool := runReflowCollisionDualPath(t, func(dst *reflowMemoryProvider) {
			// Same timestamp, a known equal size (7) but different content -> preserve.
			dst.putFixture("data/source/file.xml", "oldpayl", "dest-etag", equalTimeStamp)
		}, "--on-collision", "overwrite-if-source-newer")
		require.NoError(t, engine.err)
		require.NoError(t, pool.err)
		engineRec := soleTerminalReflowRecord(t, engine.stdout)
		require.Equal(t, "skipped", engineRec.Status)
		require.Equal(t, "collision.skipped_src_older", engineRec.Reason)
		requireReflowTerminalEqual(t, engineRec, soleTerminalReflowRecord(t, pool.stdout))
		require.Equal(t, "oldpayl", string(engine.dst.mustObject("data/source/file.xml")), "equal known sizes preserve the destination")
	})
}

// TestTransferReflowMissingDestLastModifiedRedactsKey pins SR-CVG-S2-F2: the
// missing-destination-LastModified guard must not interpolate the rewritten
// destination key into the free-text error message or the persisted checkpoint
// cause; identity lives only in the record's structured fields.
func TestTransferReflowMissingDestLastModifiedRedactsKey(t *testing.T) {
	const keySentinel = "secretsite/2026/private-report.xml"

	withTransferReflowTestState(t)
	srcBase := newReflowMemoryProvider()
	srcBase.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
	dst := newReflowMemoryProvider()
	// Destination present with a ZERO LastModified triggers the guard.
	dst.putFixtureWithMeta("data/"+keySentinel, "old", "dest-etag", time.Time{}, provider.ObjectMeta{})
	reflowResourceProbeForRun = reflowpkg.ResourceProbe{
		MemoryLimitBytes: func() (int64, string, error) { return transferDefaultBudget(), "test_override", nil },
		FDSoftLimit:      func() (int64, error) { return 100000, nil },
	}
	useTransferReflowProviderFactories(t, providerdispatch.Factories{
		S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
			switch cfg.Bucket {
			case "source-bucket":
				return srcBase, nil
			case "dest-bucket":
				return dst, nil
			default:
				return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
			}
		},
	})

	checkpointPath := filepath.Join(t.TempDir(), "state.db")
	line := reflowInputLineFull("source/file.xml", keySentinel, "src-etag", int64Ptr(int64(len("payload"))), "2026-01-15T20:53:44Z")
	stdout, err := runTransferReflowRawStdin(t, line+"\n", "--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "1", "--checkpoint", checkpointPath, "--on-collision", "overwrite-if-source-newer")
	require.Error(t, err)

	rec := soleTerminalReflowRecord(t, stdout)
	require.Equal(t, "failed", rec.Status)
	require.Equal(t, "collision.missing_dest_last_modified", rec.Reason)

	// The rewritten key sentinel must not appear in the free-text error message.
	errEvent := requireRecord(t, stdout, reflowpkg.ErrorEventType, "")
	var ev struct {
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(errEvent.Data, &ev))
	require.NotContains(t, ev.Message, keySentinel, "the rewritten dest key must not be echoed into the error message")

	// Nor into the persisted checkpoint cause.
	for _, msg := range dumpReflowItemErrorMessages(t, checkpointPath) {
		require.NotContains(t, msg, keySentinel, "the rewritten dest key must not be persisted in the checkpoint cause")
	}
}

// fanInGateProbeDest instruments the destination so a controlled barrier forces
// the pre-fix stale-read interleave the per-key gate must prevent. It counts
// heads and successful If-Match lands on the contended key, and snapshots the
// landed count at the moment the SECOND head occurs — the direct gate-integrity
// witness: under the fix the second worker is held out of the critical section
// until the first commit lands (landed == 1 at the second head); if the gate
// release regresses to before the head, the second head races the stale original
// (landed == 0 at the second head) and a spurious concurrent_mutation results.
type fanInGateProbeDest struct {
	*reflowMemoryProvider
	mergedKey          string
	barrierT           time.Duration
	headCount          int32
	landed             int32
	landedAtSecondHead int32
	firstIfMatch       sync.Once
}

func (p *fanInGateProbeDest) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	meta, err := p.reflowMemoryProvider.Head(ctx, key)
	if key == p.mergedKey {
		if atomic.AddInt32(&p.headCount, 1) == 2 {
			atomic.StoreInt32(&p.landedAtSecondHead, atomic.LoadInt32(&p.landed))
		}
	}
	return meta, err
}

func (p *fanInGateProbeDest) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	if key == p.mergedKey && precond.IfMatchETag != nil {
		isFirst := false
		p.firstIfMatch.Do(func() { isFirst = true })
		if isFirst {
			// The first committer waits for a concurrent second head. Under the
			// per-key gate the second worker cannot head until this commit lands and
			// the gate releases, so headCount never reaches 2 here and the wait times
			// out harmlessly (a one-time barrierT delay on the happy path). If the
			// gate release regresses to before the head, the second worker heads the
			// stale original concurrently, headCount reaches 2, the wait ends early,
			// and the two contenders race their If-Match against the same original —
			// stranding one as a spurious concurrent_mutation.
			deadline := time.After(p.barrierT)
		waitLoop:
			for atomic.LoadInt32(&p.headCount) < 2 {
				select {
				case <-deadline:
					break waitLoop
				case <-time.After(time.Millisecond):
				}
			}
		}
	}
	res, err := p.reflowMemoryProvider.PutObjectConditional(ctx, key, body, contentLength, precond)
	if err == nil && key == p.mergedKey && precond.IfMatchETag != nil {
		atomic.AddInt32(&p.landed, 1)
	}
	return res, err
}

// TestTransferReflowSourceNewerFanInSerialized pins DR-CVG-S2-F1 with a
// controlled gate-integrity barrier. Two same-run sources — both newer than the
// original destination — contend for one destination key. The per-key gate holds
// the whole source-newer resolution (head + LastModified decision + If-Match PUT)
// so the SECOND contender re-evaluates against the FIRST's committed copy instead
// of racing its If-Match against the stale original.
//
// The barrier makes the failure deterministic rather than scheduling-dependent:
// the first committer parks until a concurrent second head is observed, which can
// only happen if the gate releases before the head. Under the fix the wait times
// out and the second head is taken only after the commit lands, so
// landedAtSecondHead == 1 and no contender is stranded as concurrent_mutation.
// Restoring the release to its pre-fix position (before the head) makes this test
// fail on both landedAtSecondHead and the concurrent_mutation assertion.
//
// Contract note (narrowed, behavior-preserving): the FIRST gate winner becomes
// the terminal authority. The destination LastModified is copy time, so the loser
// is a legitimate src_older skip — newest-business-date-wins is NOT claimed and no
// "no update is lost" guarantee is made; a future increment would need persisted
// logical source time. Both the IfAbsent-honored and head-compare fallback
// resolution arms are exercised.
func TestTransferReflowSourceNewerFanInSerialized(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC)

	setup := func(t *testing.T, ifAbsentHonored bool) *fanInGateProbeDest {
		withTransferReflowTestState(t)
		srcBase := newReflowMemoryProvider()
		srcBase.putFixture("src/older.xml", "OLDER-BODY", "older-etag", t1)
		srcBase.putFixture("src/newer.xml", "NEWER-BODY", "newer-etag", t2)
		dst := &fanInGateProbeDest{
			reflowMemoryProvider: newReflowMemoryProvider(),
			mergedKey:            "data/merged.xml",
			barrierT:             200 * time.Millisecond,
		}
		if !ifAbsentHonored {
			// An IfAbsent-ignoring destination fails the write probe's honored check,
			// activating the head-compare fallback resolution arm.
			dst.ignoreIfAbsent = true
		}
		dst.putFixture("data/merged.xml", "T0-BODY-ORIG", "e0", t0)
		reflowResourceProbeForRun = reflowpkg.ResourceProbe{
			MemoryLimitBytes: func() (int64, string, error) { return transferDefaultBudget(), "test_override", nil },
			FDSoftLimit:      func() (int64, error) { return 100000, nil },
		}
		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				switch cfg.Bucket {
				case "source-bucket":
					return srcBase, nil
				case "dest-bucket":
					return dst, nil
				default:
					return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
				}
			},
		})
		return dst
	}
	run := func(t *testing.T, checkpointPath string, resume bool) (string, error) {
		lines := []string{
			reflowInputLineFull("src/older.xml", "merged.xml", "older-etag", int64Ptr(int64(len("OLDER-BODY"))), t1.Format(time.RFC3339)),
			reflowInputLineFull("src/newer.xml", "merged.xml", "newer-etag", int64Ptr(int64(len("NEWER-BODY"))), t2.Format(time.RFC3339)),
		}
		args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "2", "--checkpoint", checkpointPath, "--on-collision", "overwrite-if-source-newer"}
		if resume {
			args = append(args, "--resume")
		}
		return runTransferReflowRawStdin(t, strings.Join(lines, "\n")+"\n", args...)
	}

	for _, arm := range []struct {
		name            string
		ifAbsentHonored bool
	}{
		{"ifabsent_honored", true},
		{"head_compare_fallback", false},
	} {
		t.Run(arm.name, func(t *testing.T) {
			dst := setup(t, arm.ifAbsentHonored)
			checkpointPath := filepath.Join(t.TempDir(), "state.db")
			stdout, err := run(t, checkpointPath, false)
			require.NoError(t, err, stdout)

			// Gate-integrity witnesses: the second head observed the winner's
			// committed copy (landed already 1), and exactly one contender landed.
			require.Equal(t, int32(1), atomic.LoadInt32(&dst.landedAtSecondHead),
				"the second destination head must occur only after the first commit lands")
			require.Equal(t, int32(1), atomic.LoadInt32(&dst.landed), "exactly one contender lands the overwrite")

			terminals := map[string]int{}
			for _, rec := range requireReflowRecords(t, stdout) {
				if rec.Status == "in_progress" {
					continue
				}
				require.NotEqual(t, "collision.skipped_concurrent_mutation", rec.Reason,
					"serialized contenders must never be stranded as a spurious concurrent-mutation skip")
				terminals[rec.Status+"/"+rec.Reason]++
			}
			require.Equal(t, 1, terminals["complete/"], "exactly one contender lands the overwrite")
			require.Equal(t, 1, terminals["skipped/collision.skipped_src_older"], "the other re-evaluates against the committed copy")
			final := string(dst.mustObject("data/merged.xml"))
			require.Contains(t, []string{"OLDER-BODY", "NEWER-BODY"}, final, "the gate winner's committed copy stands, not the untouched original")

			// Resume converges without a second land; the settled result is preserved.
			stdout2, err := run(t, checkpointPath, true)
			require.NoError(t, err)
			for _, rec := range requireReflowRecords(t, stdout2) {
				if rec.Status != "in_progress" {
					require.True(t, strings.HasPrefix(rec.Reason, "resume."), "resume must re-skip settled items, got reason %q", rec.Reason)
				}
			}
			require.Equal(t, int32(1), atomic.LoadInt32(&dst.landed), "resume must not land the overwrite a second time")
			require.Equal(t, final, string(dst.mustObject("data/merged.xml")), "resume must not change the settled destination")
		})
	}
}
