package output

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJSONLWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	assert.NotNil(t, w)
	assert.Equal(t, "job-123", w.jobID)
	assert.Equal(t, "s3", w.provider)
}

func TestJSONLWriter_WriteObject(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	obj := &ObjectRecord{
		Key:          "data/2024/file.parquet",
		Size:         1048576,
		ETag:         "abc123",
		LastModified: time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
	}

	err := w.WriteObject(context.Background(), obj)
	require.NoError(t, err)

	// Parse the output
	var record Record
	err = json.Unmarshal(buf.Bytes(), &record)
	require.NoError(t, err)

	assert.Equal(t, TypeObject, record.Type)
	assert.Equal(t, "job-123", record.JobID)
	assert.Equal(t, "s3", record.Provider)
	assert.False(t, record.TS.IsZero())

	// Parse the data payload
	var objData ObjectRecord
	err = json.Unmarshal(record.Data, &objData)
	require.NoError(t, err)

	assert.Equal(t, "data/2024/file.parquet", objData.Key)
	assert.Equal(t, int64(1048576), objData.Size)
	assert.Equal(t, "abc123", objData.ETag)
	assert.Equal(t, time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC), objData.LastModified)
}

func TestJSONLWriter_WriteObject_WithMetadata(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-456", "s3")

	obj := &ObjectRecord{
		Key:          "data/file.csv",
		Size:         512,
		ETag:         "def456",
		LastModified: time.Now().UTC(),
		ContentType:  "text/csv",
		Metadata: map[string]string{
			"x-custom": "value",
		},
	}

	err := w.WriteObject(context.Background(), obj)
	require.NoError(t, err)

	// Verify metadata is included
	var record Record
	err = json.Unmarshal(buf.Bytes(), &record)
	require.NoError(t, err)

	var objData ObjectRecord
	err = json.Unmarshal(record.Data, &objData)
	require.NoError(t, err)

	assert.Equal(t, "text/csv", objData.ContentType)
	assert.Equal(t, "value", objData.Metadata["x-custom"])
}

func TestJSONLWriter_WriteError(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	errRec := &ErrorRecord{
		Code:    ErrCodeAccessDenied,
		Message: "Access denied to bucket",
		Prefix:  "secret/",
	}

	err := w.WriteError(context.Background(), errRec)
	require.NoError(t, err)

	var record Record
	err = json.Unmarshal(buf.Bytes(), &record)
	require.NoError(t, err)

	assert.Equal(t, TypeError, record.Type)

	var errData ErrorRecord
	err = json.Unmarshal(record.Data, &errData)
	require.NoError(t, err)

	assert.Equal(t, ErrCodeAccessDenied, errData.Code)
	assert.Equal(t, "Access denied to bucket", errData.Message)
	assert.Equal(t, "secret/", errData.Prefix)
}

func TestJSONLWriter_WriteProgress(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	prog := &ProgressRecord{
		Phase:          PhaseListing,
		ObjectsFound:   1000,
		ObjectsMatched: 500,
		BytesTotal:     52428800,
		Prefix:         "data/2024/",
	}

	err := w.WriteProgress(context.Background(), prog)
	require.NoError(t, err)

	var record Record
	err = json.Unmarshal(buf.Bytes(), &record)
	require.NoError(t, err)

	assert.Equal(t, TypeProgress, record.Type)

	var progData ProgressRecord
	err = json.Unmarshal(record.Data, &progData)
	require.NoError(t, err)

	assert.Equal(t, PhaseListing, progData.Phase)
	assert.Equal(t, int64(1000), progData.ObjectsFound)
	assert.Equal(t, int64(500), progData.ObjectsMatched)
	assert.Equal(t, int64(52428800), progData.BytesTotal)
	assert.Equal(t, "data/2024/", progData.Prefix)
}

func TestJSONLWriter_WriteSummary(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	sum := &SummaryRecord{
		ObjectsFound:   5000,
		ObjectsMatched: 2500,
		BytesTotal:     10737418240,
		Duration:       30 * time.Second,
		DurationHuman:  "30s",
		Errors:         2,
		Prefixes:       []string{"data/2024/", "data/2025/"},
	}

	err := w.WriteSummary(context.Background(), sum)
	require.NoError(t, err)

	var record Record
	err = json.Unmarshal(buf.Bytes(), &record)
	require.NoError(t, err)

	assert.Equal(t, TypeSummary, record.Type)

	var sumData SummaryRecord
	err = json.Unmarshal(record.Data, &sumData)
	require.NoError(t, err)

	assert.Equal(t, int64(5000), sumData.ObjectsFound)
	assert.Equal(t, int64(2500), sumData.ObjectsMatched)
	assert.Equal(t, int64(10737418240), sumData.BytesTotal)
	assert.Equal(t, 30*time.Second, sumData.Duration)
	assert.Equal(t, "30s", sumData.DurationHuman)
	assert.Equal(t, int64(2), sumData.Errors)
	assert.Equal(t, []string{"data/2024/", "data/2025/"}, sumData.Prefixes)
}

func TestJSONLWriter_NewlineTerminated(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	err := w.WriteObject(context.Background(), &ObjectRecord{Key: "file1.txt"})
	require.NoError(t, err)

	err = w.WriteObject(context.Background(), &ObjectRecord{Key: "file2.txt"})
	require.NoError(t, err)

	// Output should be two lines
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 2)

	// Each line should be valid JSON
	for _, line := range lines {
		var record Record
		err := json.Unmarshal([]byte(line), &record)
		assert.NoError(t, err)
	}
}

func TestJSONLWriter_Close(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	err := w.Close()
	require.NoError(t, err)

	// Writing after close should fail
	err = w.WriteObject(context.Background(), &ObjectRecord{Key: "file.txt"})
	assert.ErrorIs(t, err, ErrWriterClosed)
}

func TestJSONLWriter_ConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	const numWriters = 10
	const writesPerWriter = 100

	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				obj := &ObjectRecord{
					Key:  "file.txt",
					Size: int64(writerID*writesPerWriter + j),
				}
				_ = w.WriteObject(context.Background(), obj)
			}
		}(i)
	}

	wg.Wait()

	// Verify all lines are complete JSON objects (no interleaving)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, numWriters*writesPerWriter)

	for i, line := range lines {
		var record Record
		err := json.Unmarshal([]byte(line), &record)
		assert.NoError(t, err, "line %d should be valid JSON: %s", i, line)
	}
}

func TestJSONLWriter_ContextCancellation(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf, "job-123", "s3")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := w.WriteObject(ctx, &ObjectRecord{Key: "file.txt"})
	assert.ErrorIs(t, err, context.Canceled)

	// Buffer should be empty (nothing written)
	assert.Empty(t, buf.String())
}

func TestJSONLWriter_WriteFailure(t *testing.T) {
	// Create a writer that always fails
	failWriter := &failingWriter{err: errors.New("disk full")}
	w := NewJSONLWriter(failWriter, "job-123", "s3")

	err := w.WriteObject(context.Background(), &ObjectRecord{Key: "file.txt"})
	require.Error(t, err)

	var writeErr *WriteError
	assert.True(t, errors.As(err, &writeErr))
	assert.Equal(t, "write", writeErr.Op)
}

// failingWriter is an io.Writer that always returns an error.
type failingWriter struct {
	err error
}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	return 0, f.err
}

func TestJSONLWriter_ShortWrite(t *testing.T) {
	// Create a writer that simulates short writes (returns n < len(p) with nil error)
	shortWriter := &shortWriteWriter{bytesPerWrite: 10}
	w := NewJSONLWriter(shortWriter, "job-123", "s3")

	obj := &ObjectRecord{
		Key:  "data/2024/file.parquet",
		Size: 1048576,
		ETag: "abc123",
	}

	err := w.WriteObject(context.Background(), obj)
	require.NoError(t, err)

	// Verify complete output despite short writes
	lines := strings.Split(strings.TrimSpace(shortWriter.buf.String()), "\n")
	assert.Len(t, lines, 1)

	var record Record
	err = json.Unmarshal([]byte(lines[0]), &record)
	assert.NoError(t, err, "output should be valid JSON despite short writes")
	assert.Equal(t, TypeObject, record.Type)
}

func TestJSONLWriter_ZeroWrite(t *testing.T) {
	// Create a writer that returns 0 bytes written with nil error (pathological case)
	zeroWriter := &zeroWriteWriter{}
	w := NewJSONLWriter(zeroWriter, "job-123", "s3")

	err := w.WriteObject(context.Background(), &ObjectRecord{Key: "file.txt"})
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrShortWrite)
}

// shortWriteWriter simulates an io.Writer that performs short writes.
// It writes at most bytesPerWrite bytes per call, returning nil error.
type shortWriteWriter struct {
	buf           bytes.Buffer
	bytesPerWrite int
}

func (sw *shortWriteWriter) Write(p []byte) (n int, err error) {
	toWrite := len(p)
	if toWrite > sw.bytesPerWrite {
		toWrite = sw.bytesPerWrite
	}
	return sw.buf.Write(p[:toWrite])
}

// zeroWriteWriter always returns 0 bytes written with nil error.
type zeroWriteWriter struct{}

func (zw *zeroWriteWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

func TestWriteError(t *testing.T) {
	underlying := errors.New("underlying error")
	err := &WriteError{Op: "marshal", Err: underlying}

	assert.Equal(t, "output: marshal: underlying error", err.Error())
	assert.ErrorIs(t, err, underlying)
}

func TestRecord_JSONSerialization(t *testing.T) {
	// Test that records serialize correctly
	record := Record{
		Type:     TypeObject,
		TS:       time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		JobID:    "abc123",
		Provider: "s3",
		Data:     json.RawMessage(`{"key":"test.txt","size":100}`),
	}

	data, err := json.Marshal(record)
	require.NoError(t, err)

	// Verify JSON structure
	var parsed map[string]any
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, TypeObject, parsed["type"])
	assert.Equal(t, "abc123", parsed["job_id"])
	assert.Equal(t, "s3", parsed["provider"])
	assert.NotNil(t, parsed["ts"])
	assert.NotNil(t, parsed["data"])
}

func TestObjectRecord_OmitEmpty(t *testing.T) {
	// ContentType and Metadata should be omitted when empty
	obj := ObjectRecord{
		Key:          "file.txt",
		Size:         100,
		ETag:         "abc",
		LastModified: time.Now().UTC(),
	}

	data, err := json.Marshal(obj)
	require.NoError(t, err)

	// Should not contain content_type or metadata keys
	assert.NotContains(t, string(data), "content_type")
	assert.NotContains(t, string(data), "metadata")
}

func TestErrorRecord_OmitEmpty(t *testing.T) {
	// Key, Prefix, Details should be omitted when empty
	errRec := ErrorRecord{
		Code:    ErrCodeInternal,
		Message: "Something went wrong",
	}

	data, err := json.Marshal(errRec)
	require.NoError(t, err)

	assert.NotContains(t, string(data), "key")
	assert.NotContains(t, string(data), "prefix")
	assert.NotContains(t, string(data), "details")
}

func TestProgressRecord_OmitEmpty(t *testing.T) {
	// Prefix should be omitted when empty
	prog := ProgressRecord{
		Phase:          PhaseComplete,
		ObjectsFound:   100,
		ObjectsMatched: 50,
		BytesTotal:     1024,
	}

	data, err := json.Marshal(prog)
	require.NoError(t, err)

	assert.NotContains(t, string(data), "prefix")
}

// Benchmark for write performance
func BenchmarkJSONLWriter_WriteObject(b *testing.B) {
	w := NewJSONLWriter(io.Discard, "job-123", "s3")
	obj := &ObjectRecord{
		Key:          "data/2024/01/15/file.parquet",
		Size:         1048576,
		ETag:         "abc123def456",
		LastModified: time.Now().UTC(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.WriteObject(ctx, obj)
	}
}

func BenchmarkJSONLWriter_WriteObject_WithMetadata(b *testing.B) {
	w := NewJSONLWriter(io.Discard, "job-123", "s3")
	obj := &ObjectRecord{
		Key:          "data/2024/01/15/file.parquet",
		Size:         1048576,
		ETag:         "abc123def456",
		LastModified: time.Now().UTC(),
		ContentType:  "application/octet-stream",
		Metadata: map[string]string{
			"x-amz-meta-custom1": "value1",
			"x-amz-meta-custom2": "value2",
		},
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.WriteObject(ctx, obj)
	}
}
