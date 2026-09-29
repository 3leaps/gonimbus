package reflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/stretchr/testify/require"
)

func TestRecordReceiptStatusOmissionAndTimestampFallback(t *testing.T) {
	observed := time.Date(2026, 1, 1, 12, 0, 0, 123, time.FixedZone("offset", 3600))
	for _, status := range []string{"complete", "quarantined", "skipped", "failed", "planned", "in_progress"} {
		rec := (reflowInput{SourceLastMod: observed}).record("s3://bucket/key", "key", status)
		receipt := transfer.CopyReceipt{UploadResult: transfer.UploadResult{SHA256: strings.Repeat("a", 64), ETag: "write-etag", Version: "write-version"}}
		rec = RecordWithReceipt(rec, receipt, "s3")
		require.Equal(t, "2026-01-01T11:00:00.000000123Z", rec.SourceLastModified)
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(data, &fields))
		if status == "complete" || status == "quarantined" {
			require.Equal(t, receipt.SHA256, fields["dest_sha256"])
			require.Equal(t, receipt.Version, fields["dest_version_id"])
			require.Equal(t, receipt.ETag, fields["dest_etag"])
		} else {
			for _, key := range []string{"dest_sha256", "dest_version_id", "dest_etag"} {
				require.NotContains(t, fields, key)
			}
		}
	}
	readTime := observed.Add(time.Hour)
	rec := RecordWithReceipt(Record{Status: "complete", SourceLastModified: receiptTimestamp(observed)}, transfer.CopyReceipt{SourceLastModified: readTime}, "s3")
	require.Equal(t, receiptTimestamp(readTime), rec.SourceLastModified)
	data, err := json.Marshal(Record{Status: "failed"})
	require.NoError(t, err)
	require.NotContains(t, string(data), "source_last_modified")
}

func TestRecordReceiptOmitUnsafeAndNullHandles(t *testing.T) {
	for _, unsafe := range []string{"https://example.test/path?X-Amz-Signature=secret", "Bearer secret", "token=secret", "line\nvalue", "\x00", " padded ", string([]byte{0xff})} {
		rec := RecordWithReceipt(Record{Status: "complete"}, transfer.CopyReceipt{UploadResult: transfer.UploadResult{ETag: unsafe, Version: unsafe}}, "s3")
		require.Empty(t, rec.DestETag)
		require.Empty(t, rec.DestVersionID)
	}
	for _, tc := range []struct{ provider, version string }{{"s3", "null"}, {"s3", ""}, {"file", "local-inode"}} {
		rec := RecordWithReceipt(Record{Status: "complete"}, transfer.CopyReceipt{UploadResult: transfer.UploadResult{ETag: "opaque", Version: tc.version}}, tc.provider)
		require.Empty(t, rec.DestVersionID)
		require.Equal(t, "opaque", rec.DestETag)
	}
	rec := RecordWithReceipt(Record{Status: "complete"}, transfer.CopyReceipt{UploadResult: transfer.UploadResult{Version: "9007199254740993"}}, "gs")
	data, err := json.Marshal(rec)
	require.NoError(t, err)
	require.Contains(t, string(data), `"dest_version_id":"9007199254740993"`)
}

func TestRunnerReceiptHashesLandedBytesAndOmitsNonWrites(t *testing.T) {
	for _, mode := range []string{CollisionSkipIfDuplicate, CollisionOverwrite} {
		for _, fail := range []bool{false, true} {
			src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
			src.putFixture("a/b.xml", "payload", "etag-a")
			if fail {
				dst.putErrByKey["data/a/b.xml"] = errors.New("synthetic write failure")
				dst.putObjectErrByKey["data/a/b.xml"] = errors.New("synthetic write failure")
			}
			sink := &collectSink{}
			cfg := copyConfig(dst, sink)
			cfg.Collision.Mode = mode
			runner, err := NewRunner(cfg)
			require.NoError(t, err)
			line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/b.xml","source_key":"a/b.xml","source_etag":"etag-a","source_size_bytes":7,"source_last_modified":"2026-01-01T00:00:00Z","dest_rel_key":"a/b.xml"}}`
			_, err = runner.Run(context.Background(), copySource(src, line))
			if fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, sink.records, 2)
			require.Empty(t, sink.records[0].DestSHA256)
			terminal := sink.records[1]
			require.Equal(t, "2026-01-01T00:00:00Z", terminal.SourceLastModified)
			if fail {
				require.Equal(t, "failed", terminal.Status)
				require.Empty(t, terminal.DestSHA256)
				require.Empty(t, terminal.DestETag)
				require.Empty(t, terminal.DestVersionID)
			} else {
				digest := sha256.Sum256(dst.body(terminal.DestKey))
				require.Equal(t, hex.EncodeToString(digest[:]), terminal.DestSHA256)
				require.Equal(t, "complete", terminal.Status)
				// A second skip-if-duplicate run makes no new destination claim.
				if mode == CollisionSkipIfDuplicate {
					sink.records = nil
					_, err = runner.Run(context.Background(), copySource(src, line))
					require.NoError(t, err)
					terminal = sink.records[len(sink.records)-1]
					require.Equal(t, "skipped", terminal.Status)
					require.Empty(t, terminal.DestSHA256)
					require.Empty(t, terminal.DestETag)
					require.Empty(t, terminal.DestVersionID)
				}
			}
		}
	}
}
