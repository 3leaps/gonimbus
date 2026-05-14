package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/stream"
)

func TestStreamPutCommand_RawStdinWritesFileDestination(t *testing.T) {
	resetStreamPutTestState(t)

	destPath := filepath.Join(t.TempDir(), "nested", "object.bin")
	stdout, stderr, err := runStreamPutTestCommand(t, []string{"file://" + destPath}, "hello from stdin")
	require.NoError(t, err, "stderr: %s", stderr)
	require.Empty(t, stderr)

	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, "hello from stdin", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, streamPutRecordType, rec.Type)
	require.Equal(t, "file", rec.Provider)

	var put streamPutRecord
	require.NoError(t, json.Unmarshal(rec.Data, &put))
	require.Equal(t, "file://"+destPath, put.DestURI)
	require.Equal(t, "object.bin", put.DestKey)
	require.Equal(t, int64(len("hello from stdin")), put.Bytes)
	require.Equal(t, "success", put.Status)
}

func TestStreamPutCommand_RejectsExistingDestinationByDefault(t *testing.T) {
	resetStreamPutTestState(t)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "object.bin")
	require.NoError(t, os.WriteFile(destPath, []byte("original"), 0o644))

	stdout, _, err := runStreamPutTestCommand(t, []string{"file://" + destPath}, "replacement")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Destination exists")

	got, readErr := os.ReadFile(destPath)
	require.NoError(t, readErr)
	require.Equal(t, "original", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, output.TypeError, rec.Type)

	var errRec output.ErrorRecord
	require.NoError(t, json.Unmarshal(rec.Data, &errRec))
	require.Equal(t, output.ErrCodeAlreadyExists, errRec.Code)
	require.Equal(t, "object.bin", errRec.Key)
	require.Contains(t, errRec.Message, "destination object already exists")
}

func TestStreamPutCommand_OverwriteReplacesExistingDestination(t *testing.T) {
	resetStreamPutTestState(t)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "object.bin")
	require.NoError(t, os.WriteFile(destPath, []byte("original"), 0o644))

	stdout, stderr, err := runStreamPutTestCommand(t, []string{"--overwrite", "file://" + destPath}, "replacement")
	require.NoError(t, err, "stderr: %s", stderr)

	got, readErr := os.ReadFile(destPath)
	require.NoError(t, readErr)
	require.Equal(t, "replacement", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, streamPutRecordType, rec.Type)

	var put streamPutRecord
	require.NoError(t, json.Unmarshal(rec.Data, &put))
	require.Equal(t, int64(len("replacement")), put.Bytes)
	require.Equal(t, "success", put.Status)
}

func TestStreamPutCommand_JSONLFramingWritesFileDestination(t *testing.T) {
	resetStreamPutTestState(t)

	destPath := filepath.Join(t.TempDir(), "object.bin")
	stdout, stderr, err := runStreamPutTestCommandBytes(t,
		[]string{"--framing", "jsonl", "file://" + destPath},
		buildStreamPutFramedInput(t, "stream-1", "s3://source/object.bin", [][]byte{[]byte("hello "), []byte("framed")}),
	)
	require.NoError(t, err, "stderr: %s", stderr)
	require.Empty(t, stderr)

	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, "hello framed", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, streamPutRecordType, rec.Type)

	var put streamPutRecord
	require.NoError(t, json.Unmarshal(rec.Data, &put))
	require.Equal(t, "file://"+destPath, put.DestURI)
	require.Equal(t, "object.bin", put.DestKey)
	require.Equal(t, int64(len("hello framed")), put.Bytes)
	require.Equal(t, "success", put.Status)
	require.Equal(t, "s3://source/object.bin", put.SourceURI)
	require.Equal(t, "stream-1", put.SourceStreamID)
}

func TestStreamPutCommand_JSONLFramingRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name    string
		input   func(t *testing.T) []byte
		wantErr string
	}{
		{
			name: "missing open",
			input: func(t *testing.T) []byte {
				return nil
			},
			wantErr: "missing stream open record",
		},
		{
			name: "duplicate open",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin"})
				writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin"})
				return buf.Bytes()
			},
			wantErr: "duplicate stream open record",
		},
		{
			name: "chunk before open",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				sw := stream.NewWriter(&buf, "job-test", "s3")
				require.NoError(t, sw.WriteChunk(context.Background(), &stream.Chunk{StreamID: "stream-1", Seq: 0, NBytes: 3}, strings.NewReader("abc")))
				return buf.Bytes()
			},
			wantErr: "stream chunk before open record",
		},
		{
			name: "missing close",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin"})
				return buf.Bytes()
			},
			wantErr: "missing stream close record",
		},
		{
			name: "non success close",
			input: func(t *testing.T) []byte {
				return buildStreamPutFramedInputWithClose(t, "stream-1", "s3://source/a.bin", [][]byte{[]byte("abc")}, stream.Close{StreamID: "stream-1", Status: "error", Chunks: 1, Bytes: 3})
			},
			wantErr: "stream close status must be success",
		},
		{
			name: "close byte mismatch",
			input: func(t *testing.T) []byte {
				return buildStreamPutFramedInputWithClose(t, "stream-1", "s3://source/a.bin", [][]byte{[]byte("abc")}, stream.Close{StreamID: "stream-1", Status: "success", Chunks: 1, Bytes: 4})
			},
			wantErr: "stream close bytes=4, want 3",
		},
		{
			name: "close chunk mismatch",
			input: func(t *testing.T) []byte {
				return buildStreamPutFramedInputWithClose(t, "stream-1", "s3://source/a.bin", [][]byte{[]byte("abc")}, stream.Close{StreamID: "stream-1", Status: "success", Chunks: 2, Bytes: 3})
			},
			wantErr: "stream close chunks=2, want 1",
		},
		{
			name: "stream id mismatch",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin"})
				sw := stream.NewWriter(&buf, "job-test", "s3")
				require.NoError(t, sw.WriteChunk(context.Background(), &stream.Chunk{StreamID: "stream-2", Seq: 0, NBytes: 3}, strings.NewReader("abc")))
				return buf.Bytes()
			},
			wantErr: "stream chunk uses stream_id",
		},
		{
			name: "sequence gap",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin"})
				sw := stream.NewWriter(&buf, "job-test", "s3")
				require.NoError(t, sw.WriteChunk(context.Background(), &stream.Chunk{StreamID: "stream-1", Seq: 1, NBytes: 3}, strings.NewReader("abc")))
				return buf.Bytes()
			},
			wantErr: "stream chunk seq=1, want 0",
		},
		{
			name: "truncated chunk body",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin"})
				writeStreamRecord(t, &buf, stream.TypeStreamChunk, &stream.Chunk{StreamID: "stream-1", Seq: 0, NBytes: 5})
				_, _ = buf.WriteString("abc")
				return buf.Bytes()
			},
			wantErr: "unexpected EOF",
		},
		{
			name: "trailing record",
			input: func(t *testing.T) []byte {
				var buf bytes.Buffer
				_, _ = buf.Write(buildStreamPutFramedInput(t, "stream-1", "s3://source/a.bin", [][]byte{[]byte("abc")}))
				writeStreamRecord(t, &buf, stream.TypeStreamClose, &stream.Close{StreamID: "stream-1", Status: "success"})
				return buf.Bytes()
			},
			wantErr: "trailing stream data after close record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetStreamPutTestState(t)

			destPath := filepath.Join(t.TempDir(), "object.bin")
			stdout, _, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + destPath}, tt.input(t))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
			require.NoFileExists(t, destPath)

			rec := decodeSingleOutputRecord(t, stdout)
			require.Equal(t, output.TypeError, rec.Type)

			var errRec output.ErrorRecord
			require.NoError(t, json.Unmarshal(rec.Data, &errRec))
			require.Equal(t, output.ErrCodeInvalidInput, errRec.Code)
			require.Contains(t, errRec.Message, tt.wantErr)
		})
	}
}

func TestStreamPutCommand_JSONLFramingRejectsOpenSizeMismatch(t *testing.T) {
	resetStreamPutTestState(t)

	var buf bytes.Buffer
	size := int64(4)
	writeStreamRecord(t, &buf, stream.TypeStreamOpen, &stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin", Size: &size})
	sw := stream.NewWriter(&buf, "job-test", "s3")
	require.NoError(t, sw.WriteChunk(context.Background(), &stream.Chunk{StreamID: "stream-1", Seq: 0, NBytes: 3}, strings.NewReader("abc")))
	writeStreamRecord(t, &buf, stream.TypeStreamClose, &stream.Close{StreamID: "stream-1", Status: "success", Chunks: 1, Bytes: 3})

	destPath := filepath.Join(t.TempDir(), "object.bin")
	stdout, _, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + destPath}, buf.Bytes())
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream open size=4, want 3")

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, output.TypeError, rec.Type)
}

func TestStreamPutCommand_JSONLFramingCollisionBehavior(t *testing.T) {
	resetStreamPutTestState(t)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "object.bin")
	require.NoError(t, os.WriteFile(destPath, []byte("original"), 0o644))
	input := buildStreamPutFramedInput(t, "stream-1", "s3://source/object.bin", [][]byte{[]byte("replacement")})

	stdout, _, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + destPath}, input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Destination exists")

	got, readErr := os.ReadFile(destPath)
	require.NoError(t, readErr)
	require.Equal(t, "original", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, output.TypeError, rec.Type)

	stdout, stderr, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "--overwrite", "file://" + destPath}, input)
	require.NoError(t, err, "stderr: %s", stderr)
	require.Empty(t, stderr)

	got, readErr = os.ReadFile(destPath)
	require.NoError(t, readErr)
	require.Equal(t, "replacement", string(got))

	rec = decodeSingleOutputRecord(t, stdout)
	require.Equal(t, streamPutRecordType, rec.Type)
}

func TestStreamPutCommand_JSONLFramingValidatesInputBeforeCollision(t *testing.T) {
	resetStreamPutTestState(t)

	dir := t.TempDir()
	destPath := filepath.Join(dir, "object.bin")
	require.NoError(t, os.WriteFile(destPath, []byte("original"), 0o644))

	stdout, _, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + destPath}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing stream open record")

	got, readErr := os.ReadFile(destPath)
	require.NoError(t, readErr)
	require.Equal(t, "original", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, output.TypeError, rec.Type)

	var errRec output.ErrorRecord
	require.NoError(t, json.Unmarshal(rec.Data, &errRec))
	require.Equal(t, output.ErrCodeInvalidInput, errRec.Code)
	require.Contains(t, errRec.Message, "missing stream open record")
}

func TestStreamPutCommand_RejectsNonExactDestination(t *testing.T) {
	resetStreamPutTestState(t)

	stdout, _, err := runStreamPutTestCommand(t, []string{"file://" + t.TempDir() + "/"}, "payload")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid destination URI")
	require.Empty(t, stdout)
}

func TestStreamPutCommand_RejectsUnsupportedFraming(t *testing.T) {
	resetStreamPutTestState(t)

	destPath := filepath.Join(t.TempDir(), "object.bin")
	stdout, _, err := runStreamPutTestCommand(t, []string{"--framing", "xml", "file://" + destPath}, "payload")
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream put supports raw or jsonl framing")
	require.Empty(t, stdout)
}

func runStreamPutTestCommand(t *testing.T, args []string, stdin string) (stdout string, stderr string, err error) {
	t.Helper()

	cmd := newStreamPutCommand()
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)

	err = cmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

func runStreamPutTestCommandBytes(t *testing.T, args []string, stdin []byte) (stdout string, stderr string, err error) {
	t.Helper()

	cmd := newStreamPutCommand()
	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	cmd.SetIn(bytes.NewReader(stdin))
	cmd.SetArgs(args)

	err = cmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

func buildStreamPutFramedInput(t *testing.T, streamID string, uri string, chunks [][]byte) []byte {
	t.Helper()
	var total int64
	for _, chunk := range chunks {
		total += int64(len(chunk))
	}
	return buildStreamPutFramedInputWithClose(t, streamID, uri, chunks, stream.Close{
		StreamID: streamID,
		Status:   "success",
		Chunks:   int64(len(chunks)),
		Bytes:    total,
	})
}

func buildStreamPutFramedInputWithClose(t *testing.T, streamID string, uri string, chunks [][]byte, closeRec stream.Close) []byte {
	t.Helper()

	var buf bytes.Buffer
	var total int64
	for _, chunk := range chunks {
		total += int64(len(chunk))
	}
	size := total

	ctx := context.Background()
	sw := stream.NewWriter(&buf, "job-test", "s3")
	require.NoError(t, sw.WriteOpen(ctx, &stream.Open{StreamID: streamID, URI: uri, Size: &size}))
	for seq, chunk := range chunks {
		offset := totalBytesBefore(chunks, seq)
		require.NoError(t, sw.WriteChunk(ctx, &stream.Chunk{
			StreamID: streamID,
			Seq:      int64(seq),
			NBytes:   int64(len(chunk)),
			Offset:   &offset,
		}, bytes.NewReader(chunk)))
	}
	require.NoError(t, sw.WriteClose(ctx, &closeRec))
	return buf.Bytes()
}

func totalBytesBefore(chunks [][]byte, index int) int64 {
	var total int64
	for i := 0; i < index; i++ {
		total += int64(len(chunks[i]))
	}
	return total
}

func writeStreamRecord(t *testing.T, buf *bytes.Buffer, recordType string, data any) {
	t.Helper()
	w := output.NewJSONLWriter(buf, "job-test", "s3")
	require.NoError(t, w.WriteAny(context.Background(), recordType, data))
}

func decodeSingleOutputRecord(t *testing.T, stdout string) output.Record {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	require.Len(t, lines, 1, "stdout: %s", stdout)

	var rec output.Record
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &rec))
	return rec
}

func resetStreamPutTestState(t *testing.T) {
	t.Helper()

	oldRegion := streamPutRegion
	oldProfile := streamPutProfile
	oldEndpoint := streamPutEndpoint
	oldFraming := streamPutFraming
	oldOverwrite := streamPutOverwrite

	streamPutRegion = ""
	streamPutProfile = ""
	streamPutEndpoint = ""
	streamPutFraming = streamPutFramingRaw
	streamPutOverwrite = false

	t.Cleanup(func() {
		streamPutRegion = oldRegion
		streamPutProfile = oldProfile
		streamPutEndpoint = oldEndpoint
		streamPutFraming = oldFraming
		streamPutOverwrite = oldOverwrite
	})
}
