package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
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

// TestTransferReflowSourceNewerFanInSerialized pins DR-CVG-S2-F1: two same-run
// sources contending for one destination key resolve inside the per-key critical
// section (head + LastModified decision + If-Match PUT all under the gate), so
// the second contender re-evaluates against the FIRST's committed copy instead
// of racing its If-Match against the stale original. The exact winner is the
// gate-acquisition order (both sources are newer than the original T0, so the
// first to acquire overwrites; the destination LastModified is then the copy
// time, making the second a legitimate src_older skip). The load-bearing
// property is that no contender is ever stranded as a spurious
// concurrent_mutation skip and no update is lost to the race.
func TestTransferReflowSourceNewerFanInSerialized(t *testing.T) {
	t0 := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC)

	setup := func(t *testing.T) *reflowMemoryProvider {
		withTransferReflowTestState(t)
		srcBase := newReflowMemoryProvider()
		srcBase.putFixture("src/older.xml", "OLDER-BODY", "older-etag", t1)
		srcBase.putFixture("src/newer.xml", "NEWER-BODY", "newer-etag", t2)
		dst := newReflowMemoryProvider()
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

	dst := setup(t)
	checkpointPath := filepath.Join(t.TempDir(), "state.db")
	stdout, err := run(t, checkpointPath, false)
	require.NoError(t, err, stdout)

	terminals := map[string]int{}
	for _, rec := range requireReflowRecords(t, stdout) {
		if rec.Status == "in_progress" {
			continue
		}
		require.NotEqual(t, "collision.skipped_concurrent_mutation", rec.Reason,
			"serialized contenders must never be stranded as a spurious concurrent-mutation skip")
		terminals[rec.Status+"/"+rec.Reason]++
	}
	// The gate winner lands; the loser compares against the committed copy and
	// skips as src_older. Exactly one of each — no lost update, no race skip.
	require.Equal(t, 1, terminals["complete/"], "exactly one contender lands the overwrite")
	require.Equal(t, 1, terminals["skipped/collision.skipped_src_older"], "the other re-evaluates against the committed copy")
	final := string(dst.mustObject("data/merged.xml"))
	require.Contains(t, []string{"OLDER-BODY", "NEWER-BODY"}, final, "the destination holds a committed copy, not the untouched original")

	// Resume converges without a second land; the settled result is preserved.
	stdout2, err := run(t, checkpointPath, true)
	require.NoError(t, err)
	for _, rec := range requireReflowRecords(t, stdout2) {
		if rec.Status != "in_progress" {
			require.True(t, strings.HasPrefix(rec.Reason, "resume."), "resume must re-skip settled items, got reason %q", rec.Reason)
		}
	}
	require.Equal(t, final, string(dst.mustObject("data/merged.xml")), "resume must not change the settled destination")
}
