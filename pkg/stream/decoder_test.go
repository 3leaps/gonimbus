package stream

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/stretchr/testify/require"
)

func TestDecoder_MixedFraming(t *testing.T) {
	payload := []byte("hello world")

	open := output.Record{
		Type:     TypeStreamOpen,
		TS:       time.Now().UTC(),
		JobID:    "job-1",
		Provider: "s3",
		Data:     mustJSON(t, Open{StreamID: "s1", URI: "s3://b/k", ETag: "abc"}),
	}
	chunk := output.Record{
		Type:     TypeStreamChunk,
		TS:       time.Now().UTC(),
		JobID:    "job-1",
		Provider: "s3",
		Data:     mustJSON(t, Chunk{StreamID: "s1", Seq: 0, NBytes: int64(len(payload))}),
	}
	closeRec := output.Record{
		Type:     TypeStreamClose,
		TS:       time.Now().UTC(),
		JobID:    "job-1",
		Provider: "s3",
		Data:     mustJSON(t, Close{StreamID: "s1", Status: "success", Chunks: 1, Bytes: int64(len(payload))}),
	}

	var buf bytes.Buffer
	buf.Write(mustJSON(t, open))
	buf.WriteByte('\n')
	buf.Write(mustJSON(t, chunk))
	buf.WriteByte('\n')
	buf.Write(payload)
	buf.Write(mustJSON(t, closeRec))
	buf.WriteByte('\n')

	d := NewDecoder(&buf)

	ev, err := d.Next()
	require.NoError(t, err)
	require.Equal(t, EventRecord, ev.Kind)
	require.Equal(t, TypeStreamOpen, ev.Record.Type)

	ev, err = d.Next()
	require.NoError(t, err)
	require.Equal(t, EventChunk, ev.Kind)
	require.NotNil(t, ev.Chunk)
	require.Equal(t, int64(len(payload)), ev.Chunk.Header.NBytes)

	got, err := io.ReadAll(ev.Chunk.Body)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, ev.Chunk.Body.Close())

	ev, err = d.Next()
	require.NoError(t, err)
	require.Equal(t, EventRecord, ev.Kind)
	require.Equal(t, TypeStreamClose, ev.Record.Type)

	_, err = d.Next()
	require.ErrorIs(t, err, io.EOF)
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
