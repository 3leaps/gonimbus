package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/stretchr/testify/require"
)

func TestTransferReflowReceiptQuarantineNamesActualWrite(t *testing.T) {
	src, dst := newReflowMemoryProvider(), newReflowMemoryProvider()
	src.putFixture("source/file.xml", "new payload", "src-etag", time.Time{})
	dst.putFixture("source/file.xml", "old payload", "dest-etag", time.Time{})
	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "src-etag", 11, "", ""), "--on-collision", "quarantine", "--collision-quarantine-prefix", "_conflict")
	require.NoError(t, err)
	var rec reflowpkg.Record
	require.NoError(t, json.Unmarshal(requireRecord(t, stdout, reflowpkg.RecordType, "quarantined").Data, &rec))
	require.Equal(t, "_conflict/source/file.xml", rec.DestKey)
	digest := sha256.Sum256(dst.mustObject(rec.DestKey))
	require.Equal(t, hex.EncodeToString(digest[:]), rec.DestSHA256)
	require.Empty(t, rec.DestVersionID)
	require.NotEqual(t, "dest-etag", rec.DestETag)
	require.Equal(t, "old payload", string(dst.mustObject("source/file.xml")))
}

func TestTransferReflowReceiptDuplicateHasNoWriteClaims(t *testing.T) {
	src, dst := newReflowMemoryProvider(), newReflowMemoryProvider()
	src.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	dst.putFixture("source/file.xml", "payload", "same-etag", time.Time{})
	stdout, err := runTransferReflowWithProviders(t, src, dst, reflowInputLine("source/file.xml", "same-etag", 7, "", ""))
	require.NoError(t, err)
	var rec reflowpkg.Record
	require.NoError(t, json.Unmarshal(requireRecord(t, stdout, reflowpkg.RecordType, "skipped").Data, &rec))
	require.Empty(t, rec.DestSHA256)
	require.Empty(t, rec.DestETag)
	require.Empty(t, rec.DestVersionID)
}
