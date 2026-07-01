package cmd

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/transfer"
	"github.com/stretchr/testify/require"
)

func TestParseOutputDest(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		wantErr  bool
		provider string
		bucket   string
		key      string
		baseDir  string
	}{
		{
			name:     "s3 simple key",
			uri:      "s3://bucket/key.jsonl",
			provider: string(provider.ProviderS3),
			bucket:   "bucket",
			key:      "key.jsonl",
		},
		{
			name:     "s3 deep path",
			uri:      "s3://bucket/deep/path/file.jsonl",
			provider: string(provider.ProviderS3),
			bucket:   "bucket",
			key:      "deep/path/file.jsonl",
		},
		{
			name:     "gcs deep path",
			uri:      "gs://bucket/deep/path/file.jsonl",
			provider: string(provider.ProviderGCS),
			bucket:   "bucket",
			key:      "deep/path/file.jsonl",
		},
		{
			name:     "file absolute path",
			uri:      "file:///tmp/out.jsonl",
			provider: string(provider.ProviderFile),
			key:      "out.jsonl",
			baseDir:  "/tmp",
		},
		{
			name:     "file nested path",
			uri:      "file:///home/user/data/output.jsonl",
			provider: string(provider.ProviderFile),
			key:      "output.jsonl",
			baseDir:  "/home/user/data",
		},
		{
			name:    "empty uri",
			uri:     "",
			wantErr: true,
		},
		{
			name:    "unsupported noncanonical gcs scheme",
			uri:     "gcs://bucket/key.jsonl",
			wantErr: true,
		},
		{
			name:    "s3 missing bucket",
			uri:     "s3://",
			wantErr: true,
		},
		{
			name:    "s3 missing key (bucket only)",
			uri:     "s3://bucket",
			wantErr: true,
		},
		{
			name:    "s3 trailing slash (prefix not key)",
			uri:     "s3://bucket/prefix/",
			wantErr: true,
		},
		{
			name:    "s3 bucket with trailing slash only",
			uri:     "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "file empty path",
			uri:     "file://",
			wantErr: true,
		},
		{
			name:    "file directory only",
			uri:     "file:///tmp/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOutputDest(tt.uri)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.provider, got.Provider)
			require.Equal(t, tt.bucket, got.Bucket)
			require.Equal(t, tt.key, got.Key)
			require.Equal(t, tt.baseDir, got.BaseDir)
		})
	}
}

func TestUploadToOutputDestUsesMultipartAboveSharedThreshold(t *testing.T) {
	path := sparseTempFile(t, transfer.DefaultMultipartThresholdBytes+1)
	mock := &outputMultipartMock{}

	err := uploadToOutputDest(context.Background(), mock, "index.db", path)

	require.NoError(t, err)
	require.False(t, mock.putCalled)
	require.Greater(t, len(mock.partSizes), 1)
}

func TestUploadConditionallyToOutputDestUsesConditionalMultipartComplete(t *testing.T) {
	path := sparseTempFile(t, transfer.DefaultMultipartThresholdBytes+1)
	mock := &outputMultipartMock{}

	err := uploadConditionallyToOutputDest(context.Background(), mock, "index.db", path, provider.PutPrecondition{IfAbsent: true})

	require.NoError(t, err)
	require.False(t, mock.putCalled)
	require.True(t, mock.completeConditional)
	require.Greater(t, len(mock.partSizes), 1)
}

func sparseTempFile(t *testing.T, size int64) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "gonimbus-output-upload-*")
	require.NoError(t, err)
	require.NoError(t, f.Truncate(size))
	require.NoError(t, f.Close())
	return f.Name()
}

type outputMultipartMock struct {
	putCalled           bool
	completeConditional bool
	partSizes           []int64
}

func (m *outputMultipartMock) PutObject(context.Context, string, io.Reader, int64) error {
	m.putCalled = true
	return nil
}

func (m *outputMultipartMock) PutObjectConditional(context.Context, string, io.Reader, int64, provider.PutPrecondition) (provider.PutResult, error) {
	m.putCalled = true
	return provider.PutResult{}, nil
}

func (m *outputMultipartMock) CreateMultipartUpload(context.Context, string) (string, error) {
	return "upload-1", nil
}

func (m *outputMultipartMock) UploadPart(_ context.Context, _ string, _ string, partNumber int32, body io.Reader, size int64) (provider.PartETag, error) {
	_, err := io.Copy(io.Discard, body)
	if err != nil {
		return provider.PartETag{}, err
	}
	m.partSizes = append(m.partSizes, size)
	return provider.PartETag{PartNumber: partNumber, ETag: "part-etag"}, nil
}

func (m *outputMultipartMock) CompleteMultipartUpload(context.Context, string, string, []provider.PartETag) (provider.PutResult, error) {
	return provider.PutResult{ETag: "complete-etag"}, nil
}

func (m *outputMultipartMock) CompleteMultipartUploadConditional(context.Context, string, string, []provider.PartETag, provider.PutPrecondition) (provider.PutResult, error) {
	m.completeConditional = true
	return provider.PutResult{ETag: "complete-etag"}, nil
}

func (m *outputMultipartMock) AbortMultipartUpload(context.Context, string, string) error {
	return nil
}
