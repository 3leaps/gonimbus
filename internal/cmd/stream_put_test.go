package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/stream"
)

func TestStreamPutParsesGCSDestinationRoot(t *testing.T) {
	root, err := parseStreamPutDestRoot("gs://bucket/root/")
	require.NoError(t, err)
	require.Equal(t, string(provider.ProviderGCS), root.Provider)
	require.Equal(t, "bucket", root.Bucket)
	require.Equal(t, "root/", root.Prefix)

	spec := root.objectSpec("object.bin")
	require.Equal(t, string(provider.ProviderGCS), spec.Provider)
	require.Equal(t, "bucket", spec.Bucket)
	require.Equal(t, "root/object.bin", spec.Key)
	require.Equal(t, "gs://bucket/root/object.bin", outputDestURI(spec))
	require.Equal(t, string(provider.ProviderGCS), streamPutOutputProviderName("gs://bucket/root/object.bin"))
}

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

func TestStreamPutCommand_RawFileDestinationIgnoresMultipartThreshold(t *testing.T) {
	resetStreamPutTestState(t)

	destPath := filepath.Join(t.TempDir(), "object.bin")
	stdout, stderr, err := runStreamPutTestCommand(t, []string{"--multipart-threshold", "2", "--part-size", "2", "file://" + destPath}, "abc")
	require.NoError(t, err, "stderr: %s", stderr)
	require.Empty(t, stderr)

	got, err := os.ReadFile(destPath)
	require.NoError(t, err)
	require.Equal(t, "abc", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, streamPutRecordType, rec.Type)

	var put streamPutRecord
	require.NoError(t, json.Unmarshal(rec.Data, &put))
	require.Equal(t, "single", put.UploadMode)
	require.Equal(t, int64(3), put.Bytes)
}

func TestStreamPutCommand_JSONLFramingWritesFileDestination(t *testing.T) {
	resetStreamPutTestState(t)

	destPath := filepath.Join(t.TempDir(), "object.bin")
	stdout, stderr, err := runStreamPutTestCommandBytes(t,
		[]string{"--framing", "jsonl", "file://" + destPath},
		buildStreamPutFramedInput(t, "stream-1", "s3://source/original.bin", [][]byte{[]byte("hello "), []byte("framed")}),
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
	require.Equal(t, "s3://source/original.bin", put.SourceURI)
	require.Equal(t, "stream-1", put.SourceStreamID)
}

func TestStreamPutCommand_JSONLFramingWritesMultipleObjectsUnderRoot(t *testing.T) {
	resetStreamPutTestState(t)

	root := t.TempDir()
	var input bytes.Buffer
	input.Write(buildStreamPutFramedInput(t, "stream-1", "s3://source/a.bin", [][]byte{[]byte("alpha")}))
	input.Write(buildStreamPutFramedInput(t, "stream-2", "s3://source/nested/b.bin", [][]byte{[]byte("bravo")}))

	stdout, stderr, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + root + "/"}, input.Bytes())
	require.NoError(t, err, "stderr: %s", stderr)
	require.Empty(t, stderr)

	gotA, err := os.ReadFile(filepath.Join(root, "a.bin"))
	require.NoError(t, err)
	require.Equal(t, "alpha", string(gotA))
	gotB, err := os.ReadFile(filepath.Join(root, "nested", "b.bin"))
	require.NoError(t, err)
	require.Equal(t, "bravo", string(gotB))

	records := decodeOutputRecords(t, stdout)
	require.Len(t, records, 2)
	for _, rec := range records {
		require.Equal(t, streamPutRecordType, rec.Type)
	}
}

func TestStreamPutFramedObjectUsesResolvedProviderKey(t *testing.T) {
	resetStreamPutTestState(t)

	input := buildStreamPutFramedInput(t, "stream-1", "s3://source/original.bin", [][]byte{[]byte("payload")})
	root := streamPutDestRoot{
		Provider: string(provider.ProviderS3),
		Bucket:   "bucket",
		Prefix:   "dest/",
		ExactKey: "data-copy.bin",
	}
	mock := &streamPutKeyCaptureMock{}
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-test", "s3")

	rec, err := readStreamPutFramedObject(context.Background(), stream.NewDecoder(bytes.NewReader(input)), root, mock, streamPutUploadOptions{}, w, 0)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, "dest/data-copy.bin", mock.key)
	require.Equal(t, "dest/data-copy.bin", rec.DestKey)
	require.Equal(t, "s3://bucket/dest/data-copy.bin", rec.DestURI)
	require.Equal(t, []byte("payload"), mock.body)
}

func TestStreamPutCommand_JSONLFramingRejectsMultipleObjectsForExactDestination(t *testing.T) {
	resetStreamPutTestState(t)

	destPath := filepath.Join(t.TempDir(), "copy.bin")
	var input bytes.Buffer
	input.Write(buildStreamPutFramedInput(t, "stream-1", "s3://source/original.bin", [][]byte{[]byte("alpha")}))
	input.Write(buildStreamPutFramedInput(t, "stream-2", "s3://source/second.bin", [][]byte{[]byte("bravo")}))

	stdout, _, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + destPath}, input.Bytes())
	require.Error(t, err)
	require.Contains(t, err.Error(), "exact destination accepts only one framed object")

	got, readErr := os.ReadFile(destPath)
	require.NoError(t, readErr)
	require.Equal(t, "alpha", string(got))

	records := decodeOutputRecords(t, stdout)
	require.Len(t, records, 2)
	require.Equal(t, streamPutRecordType, records[0].Type)
	require.Equal(t, output.TypeError, records[1].Type)
}

func TestStreamPutCommand_JSONLFramingDestKeyRequiresOptInAndStaysUnderRoot(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		destKey string
		wantErr string
	}{
		{name: "requires opt in", args: nil, destKey: "chosen.bin", wantErr: "frame dest_key requires --dest-from-frame"},
		{name: "rejects absolute uri", args: []string{"--dest-from-frame"}, destKey: "s3://other/key", wantErr: "dest_key must be relative"},
		{name: "rejects traversal", args: []string{"--dest-from-frame"}, destKey: "../escape.bin", wantErr: "dest_key escapes destination root"},
		{name: "rejects scheme prefix", args: []string{"--dest-from-frame"}, destKey: "s3:escape.bin", wantErr: "dest_key must not start with a URI scheme"},
		{name: "rejects file scheme path", args: []string{"--dest-from-frame"}, destKey: "file:/tmp/escape.bin", wantErr: "dest_key must not start with a URI scheme"},
		{name: "rejects windows drive path", args: []string{"--dest-from-frame"}, destKey: "C:/escape.bin", wantErr: "dest_key must not start with a URI scheme"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetStreamPutTestState(t)
			root := t.TempDir()
			input := buildStreamPutFramedInputWithOpen(t, stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin", DestKey: tt.destKey}, [][]byte{[]byte("payload")})
			args := append([]string{"--framing", "jsonl"}, tt.args...)
			args = append(args, "file://"+root+"/")
			stdout, _, err := runStreamPutTestCommandBytes(t, args, input)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
			rec := decodeSingleOutputRecord(t, stdout)
			require.Equal(t, output.TypeError, rec.Type)
		})
	}
}

func TestStreamPutCommand_JSONLFramingDestKeyOptInWritesRelativeKey(t *testing.T) {
	resetStreamPutTestState(t)
	root := t.TempDir()
	input := buildStreamPutFramedInputWithOpen(t, stream.Open{StreamID: "stream-1", URI: "s3://source/a.bin", DestKey: "chosen/out.bin"}, [][]byte{[]byte("payload")})

	stdout, stderr, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "--dest-from-frame", "file://" + root + "/"}, input)
	require.NoError(t, err, "stderr: %s", stderr)
	got, err := os.ReadFile(filepath.Join(root, "chosen", "out.bin"))
	require.NoError(t, err)
	require.Equal(t, "payload", string(got))

	rec := decodeSingleOutputRecord(t, stdout)
	require.Equal(t, streamPutRecordType, rec.Type)
}

func TestStreamPutUploadReaderUsesMultipartAfterThreshold(t *testing.T) {
	resetStreamPutTestState(t)
	mock := &streamPutMultipartMock{}
	var stdout bytes.Buffer
	w := output.NewJSONLWriter(&stdout, "job-test", "s3")

	result, err := uploadStreamPutReader(context.Background(), mock, "s3://dst/object.bin", "object.bin", strings.NewReader("abcdefghijkl"), streamPutUploadOptions{
		PartSize:           5,
		MultipartThreshold: 6,
		Overwrite:          false,
	}, w)
	require.NoError(t, err)
	require.Equal(t, int64(12), result.Bytes)
	require.Equal(t, "multipart", result.Mode)
	require.False(t, mock.putCalled)
	require.Equal(t, [][]byte{[]byte("abcde"), []byte("fghij"), []byte("kl")}, mock.parts)
	require.True(t, mock.completeConditional)

	records := decodeOutputRecords(t, stdout.String())
	require.Len(t, records, 3)
	for _, rec := range records {
		require.Equal(t, streamPutProgressType, rec.Type)
	}
}

func TestStreamPutCommand_JSONLFramingRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name           string
		input          func(t *testing.T) []byte
		wantErr        string
		wantDestExists bool
		skipDestCheck  bool
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
			wantErr:       "trailing stream data after close record",
			skipDestCheck: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetStreamPutTestState(t)

			destPath := filepath.Join(t.TempDir(), "object.bin")
			stdout, _, err := runStreamPutTestCommandBytes(t, []string{"--framing", "jsonl", "file://" + destPath}, tt.input(t))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
			if tt.skipDestCheck {
				// Multi-object framed mode commits a completed object before
				// detecting a trailing record that starts the next object.
			} else if tt.wantDestExists {
				require.FileExists(t, destPath)
			} else {
				require.NoFileExists(t, destPath)
			}

			records := decodeOutputRecords(t, stdout)
			rec := records[len(records)-1]
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
	return buildStreamPutFramedInputWithOpen(t, stream.Open{StreamID: streamID, URI: uri}, chunks)
}

func buildStreamPutFramedInputWithOpen(t *testing.T, open stream.Open, chunks [][]byte) []byte {
	t.Helper()
	var total int64
	for _, chunk := range chunks {
		total += int64(len(chunk))
	}
	return buildStreamPutFramedInputWithCloseAndOpen(t, open.StreamID, open.URI, chunks, stream.Close{
		StreamID: open.StreamID,
		Status:   "success",
		Chunks:   int64(len(chunks)),
		Bytes:    total,
	}, open)
}

func buildStreamPutFramedInputWithClose(t *testing.T, streamID string, uri string, chunks [][]byte, closeRec stream.Close) []byte {
	return buildStreamPutFramedInputWithCloseAndOpen(t, streamID, uri, chunks, closeRec, stream.Open{StreamID: streamID, URI: uri})
}

func buildStreamPutFramedInputWithCloseAndOpen(t *testing.T, streamID string, uri string, chunks [][]byte, closeRec stream.Close, open stream.Open) []byte {
	t.Helper()

	var buf bytes.Buffer
	var total int64
	for _, chunk := range chunks {
		total += int64(len(chunk))
	}
	size := total

	ctx := context.Background()
	sw := stream.NewWriter(&buf, "job-test", "s3")
	open.Size = &size
	require.NoError(t, sw.WriteOpen(ctx, &open))
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

	records := decodeOutputRecords(t, stdout)
	require.Len(t, records, 1, "stdout: %s", stdout)
	return records[0]
}

func decodeOutputRecords(t *testing.T, stdout string) []output.Record {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	records := make([]output.Record, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec output.Record
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		records = append(records, rec)
	}
	require.NotEmpty(t, records, "stdout: %s", stdout)
	return records
}

func resetStreamPutTestState(t *testing.T) {
	t.Helper()

	oldRegion := streamPutRegion
	oldProfile := streamPutProfile
	oldEndpoint := streamPutEndpoint
	oldFraming := streamPutFraming
	oldOverwrite := streamPutOverwrite
	oldDestFrame := streamPutDestFrame
	oldFailFast := streamPutFailFast
	oldPartSize := streamPutPartSize
	oldThreshold := streamPutThreshold

	streamPutRegion = ""
	streamPutProfile = ""
	streamPutEndpoint = ""
	streamPutFraming = streamPutFramingRaw
	streamPutOverwrite = false
	streamPutDestFrame = false
	streamPutFailFast = false
	streamPutPartSize = "8MiB"
	streamPutThreshold = "64MiB"

	t.Cleanup(func() {
		streamPutRegion = oldRegion
		streamPutProfile = oldProfile
		streamPutEndpoint = oldEndpoint
		streamPutFraming = oldFraming
		streamPutOverwrite = oldOverwrite
		streamPutDestFrame = oldDestFrame
		streamPutFailFast = oldFailFast
		streamPutPartSize = oldPartSize
		streamPutThreshold = oldThreshold
	})
}

type streamPutMultipartMock struct {
	putCalled           bool
	completeConditional bool
	parts               [][]byte
}

func (m *streamPutMultipartMock) PutObject(context.Context, string, io.Reader, int64) error {
	m.putCalled = true
	return nil
}

func (m *streamPutMultipartMock) PutObjectConditional(context.Context, string, io.Reader, int64, provider.PutPrecondition) (provider.PutResult, error) {
	m.putCalled = true
	return provider.PutResult{}, nil
}

func (m *streamPutMultipartMock) CreateMultipartUpload(context.Context, string) (string, error) {
	return "upload-1", nil
}

func (m *streamPutMultipartMock) UploadPart(_ context.Context, _ string, _ string, partNumber int32, body io.Reader, size int64) (provider.PartETag, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return provider.PartETag{}, err
	}
	m.parts = append(m.parts, b)
	return provider.PartETag{PartNumber: partNumber, ETag: "etag"}, nil
}

func (m *streamPutMultipartMock) CompleteMultipartUpload(context.Context, string, string, []provider.PartETag) (provider.PutResult, error) {
	return provider.PutResult{ETag: "complete"}, nil
}

func (m *streamPutMultipartMock) CompleteMultipartUploadConditional(context.Context, string, string, []provider.PartETag, provider.PutPrecondition) (provider.PutResult, error) {
	m.completeConditional = true
	return provider.PutResult{ETag: "complete"}, nil
}

func (m *streamPutMultipartMock) AbortMultipartUpload(context.Context, string, string) error {
	return nil
}

type streamPutKeyCaptureMock struct {
	key  string
	body []byte
}

func (m *streamPutKeyCaptureMock) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	result, err := m.PutObjectConditional(ctx, key, body, size, provider.PutPrecondition{})
	if err != nil {
		return err
	}
	_ = result
	return nil
}

func (m *streamPutKeyCaptureMock) PutObjectConditional(_ context.Context, key string, body io.Reader, size int64, _ provider.PutPrecondition) (provider.PutResult, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return provider.PutResult{}, err
	}
	m.key = key
	m.body = b
	return provider.PutResult{}, nil
}
