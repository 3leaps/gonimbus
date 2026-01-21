package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/stretchr/testify/require"
)

func TestWriter_RoundTrip(t *testing.T) {
	payload := []byte("abc123")

	var buf bytes.Buffer
	sw := NewWriter(&buf, "job-1", "s3")

	now := time.Now().UTC()
	size := int64(len(payload))
	open := &Open{StreamID: "s1", URI: "s3://b/k", ETag: "etag", Size: &size, LastModified: &now}
	require.NoError(t, sw.WriteOpen(context.Background(), open))

	hdr := &Chunk{StreamID: "s1", Seq: 0, NBytes: int64(len(payload))}
	require.NoError(t, sw.WriteChunk(context.Background(), hdr, bytes.NewReader(payload)))

	closeRec := &Close{StreamID: "s1", Status: "success", Chunks: 1, Bytes: int64(len(payload))}
	require.NoError(t, sw.WriteClose(context.Background(), closeRec))
	require.NoError(t, sw.Close())

	d := NewDecoder(&buf)

	ev, err := d.Next()
	require.NoError(t, err)
	require.Equal(t, EventRecord, ev.Kind)
	require.Equal(t, TypeStreamOpen, ev.Record.Type)
	var gotOpen Open
	require.NoError(t, json.Unmarshal(ev.Record.Data, &gotOpen))
	require.Equal(t, open.StreamID, gotOpen.StreamID)
	require.Equal(t, open.URI, gotOpen.URI)

	ev, err = d.Next()
	require.NoError(t, err)
	require.Equal(t, EventChunk, ev.Kind)
	gotBytes, err := io.ReadAll(ev.Chunk.Body)
	require.NoError(t, err)
	require.NoError(t, ev.Chunk.Body.Close())
	require.Equal(t, payload, gotBytes)

	ev, err = d.Next()
	require.NoError(t, err)
	require.Equal(t, EventRecord, ev.Kind)
	require.Equal(t, TypeStreamClose, ev.Record.Type)
	var gotClose Close
	require.NoError(t, json.Unmarshal(ev.Record.Data, &gotClose))
	require.Equal(t, closeRec.Status, gotClose.Status)
	require.Equal(t, closeRec.Bytes, gotClose.Bytes)

	_, err = d.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestWriter_WriteChunk_UnexpectedEOF(t *testing.T) {
	var buf bytes.Buffer
	sw := NewWriter(&buf, "job-1", "s3")

	hdr := &Chunk{StreamID: "s1", Seq: 0, NBytes: 10}
	err := sw.WriteChunk(context.Background(), hdr, bytes.NewReader([]byte("short")))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	// The header should still be a valid JSONL record.
	d := NewDecoder(&buf)
	ev, err := d.Next()
	require.NoError(t, err)
	require.Equal(t, EventChunk, ev.Kind)
	_ = ev

	// Decoder expects 10 raw bytes, but only 5 were written.
	_, err = io.ReadAll(ev.Chunk.Body)
	require.Error(t, err)
	require.ErrorIs(t, ev.Chunk.Body.Close(), io.ErrUnexpectedEOF)

	// No further records.
	_, err = d.Next()
	require.Error(t, err)
}

func TestWriter_ClosePreventsWrites(t *testing.T) {
	var buf bytes.Buffer
	sw := NewWriter(&buf, "job-1", "s3")
	require.NoError(t, sw.Close())
	require.ErrorIs(t, sw.WriteOpen(context.Background(), &Open{StreamID: "s1", URI: "s3://b/k"}), output.ErrWriterClosed)
}
