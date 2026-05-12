package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/output"
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
	stdout, _, err := runStreamPutTestCommand(t, []string{"--framing", "jsonl", "file://" + destPath}, "payload")
	require.Error(t, err)
	require.Contains(t, err.Error(), "raw framing only")
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
