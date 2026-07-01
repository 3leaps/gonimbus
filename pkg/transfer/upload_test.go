package transfer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func TestUploadReaderUsesMultipartAfterThreshold(t *testing.T) {
	mock := &uploadMock{}
	var progress []UploadProgress

	result, err := UploadReader(context.Background(), mock, "object.bin", strings.NewReader("abcdefghijkl"), UploadOptions{
		PartSizeBytes:         5,
		MultipartThreshold:    6,
		MultipartThresholdSet: true,
		Precondition:          provider.PutPrecondition{IfAbsent: true},
		Progress: func(_ context.Context, p UploadProgress) error {
			progress = append(progress, p)
			return nil
		},
	})

	require.NoError(t, err)
	require.Equal(t, int64(12), result.Bytes)
	require.Equal(t, "complete-etag", result.ETag)
	require.Equal(t, "multipart", result.Mode)
	require.False(t, mock.putCalled)
	require.True(t, mock.completeConditional)
	require.Equal(t, [][]byte{[]byte("abcde"), []byte("fghij"), []byte("kl")}, mock.parts)
	require.Len(t, progress, 3)
}

func TestUploadFileUsesSharedMultipartPath(t *testing.T) {
	path := writeUploadFixture(t, "abcdefghijkl")
	mock := &uploadMock{}

	result, err := UploadFile(context.Background(), mock, "object.bin", path, UploadOptions{
		PartSizeBytes:         5,
		MultipartThreshold:    6,
		MultipartThresholdSet: true,
	})

	require.NoError(t, err)
	require.Equal(t, int64(12), result.Bytes)
	require.Equal(t, "multipart", result.Mode)
	require.False(t, mock.putCalled)
	require.Equal(t, [][]byte{[]byte("abcde"), []byte("fghij"), []byte("kl")}, mock.parts)
}

func TestUploadReaderAbortsMultipartAndSurfacesAbortFailure(t *testing.T) {
	mock := &uploadMock{completeErr: errors.New("complete failed"), abortErr: errors.New("abort denied")}

	_, err := UploadReader(context.Background(), mock, "object.bin", strings.NewReader("abcdefghijkl"), UploadOptions{
		PartSizeBytes:         5,
		MultipartThreshold:    6,
		MultipartThresholdSet: true,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "complete multipart upload")
	require.Contains(t, err.Error(), "abort multipart upload")
	require.Contains(t, err.Error(), "abort denied")
	require.True(t, mock.abortCalled)
}

func TestUploadReaderWithDefaultOptionsUsesSinglePutBelowDefaultThreshold(t *testing.T) {
	mock := &uploadMock{}

	result, err := UploadReaderWithSize(context.Background(), mock, "object.bin", strings.NewReader("small"), int64(len("small")), UploadOptions{})

	require.NoError(t, err)
	require.Equal(t, "single", result.Mode)
	require.True(t, mock.putCalled)
	require.Empty(t, mock.parts)
}

func TestUploadReaderWithExplicitZeroThresholdUsesMultipart(t *testing.T) {
	mock := &uploadMock{}

	result, err := UploadReaderWithSize(context.Background(), mock, "object.bin", strings.NewReader("small"), int64(len("small")), UploadOptions{
		MultipartThreshold:    0,
		MultipartThresholdSet: true,
	})

	require.NoError(t, err)
	require.Equal(t, "multipart", result.Mode)
	require.False(t, mock.putCalled)
	require.NotEmpty(t, mock.parts)
}

func TestUploadSessionMultipartCloseSurfacesCleanupFailure(t *testing.T) {
	mock := &uploadMock{}
	session, err := NewUploadSession(context.Background(), mock, "object.bin", UploadOptions{})
	require.NoError(t, err)
	session.multipart = true
	session.uploadID = "upload-1"
	session.parts = []provider.PartETag{{PartNumber: 1, ETag: "part-etag"}}
	session.spoolCleanup = func() error { return errors.New("cleanup failed") }

	_, err = session.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "cleanup failed")
}

func TestUploadReaderRejectsRepoLocalTempDirFromPackageSubdir(t *testing.T) {
	repoRoot, ok := findRepoRoot(".")
	require.True(t, ok)
	dir := filepath.Join(repoRoot, "tmp-upload-spool")
	err := os.MkdirAll(dir, 0o700)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	oldWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(filepath.Join(repoRoot, "pkg", "transfer")))
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	_, err = UploadReader(context.Background(), &uploadMock{}, "object.bin", strings.NewReader("abc"), UploadOptions{
		MultipartThreshold: MultipartDisabledThreshold,
		TempDir:            dir,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "outside the repository working tree")
}

func TestUploadReaderRejectsRepoLocalTempDirWhenCwdOutsideRepo(t *testing.T) {
	repoRoot, ok := findRepoRoot(".")
	require.True(t, ok)
	dir := filepath.Join(repoRoot, "tmp-upload-spool")
	err := os.MkdirAll(dir, 0o700)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	oldWD, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(t.TempDir()))
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	_, err = UploadReader(context.Background(), &uploadMock{}, "object.bin", strings.NewReader("abc"), UploadOptions{
		MultipartThreshold: MultipartDisabledThreshold,
		TempDir:            dir,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "outside the repository working tree")
}

func TestStrongETagEqualRejectsMultipartETags(t *testing.T) {
	require.True(t, strongETagEqual(`"abc123"`, `"abc123"`))
	require.False(t, strongETagEqual(`"abc123-2"`, `"abc123-2"`))
}

func TestTransferRunUsesSharedMultipartUploaderAboveDefaultThreshold(t *testing.T) {
	src := &transferMultipartProvider{objects: map[string]string{"big.bin": ""}, size: DefaultMultipartThresholdBytes + 1}
	dst := &transferMultipartProvider{objects: map[string]string{}}
	matcher, err := match.New(match.Config{Includes: []string{"**"}})
	require.NoError(t, err)
	w := output.NewJSONLWriter(io.Discard, "job-test", "s3")
	tfer := New(src, dst, matcher, w, "job-test", Config{Concurrency: 1})

	_, err = tfer.Run(context.Background())

	require.NoError(t, err)
	require.False(t, dst.putCalled)
	require.Greater(t, len(dst.partSizes), 1)
}

func writeUploadFixture(t *testing.T, body string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "upload-fixture-*")
	require.NoError(t, err)
	_, err = f.WriteString(body)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

type uploadMock struct {
	putCalled           bool
	completeConditional bool
	abortCalled         bool
	completeErr         error
	abortErr            error
	parts               [][]byte
}

func (m *uploadMock) PutObject(context.Context, string, io.Reader, int64) error {
	m.putCalled = true
	return nil
}

func (m *uploadMock) PutObjectConditional(context.Context, string, io.Reader, int64, provider.PutPrecondition) (provider.PutResult, error) {
	m.putCalled = true
	return provider.PutResult{}, nil
}

func (m *uploadMock) CreateMultipartUpload(context.Context, string) (string, error) {
	return "upload-1", nil
}

func (m *uploadMock) UploadPart(_ context.Context, _ string, _ string, partNumber int32, body io.Reader, _ int64) (provider.PartETag, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return provider.PartETag{}, err
	}
	m.parts = append(m.parts, b)
	return provider.PartETag{PartNumber: partNumber, ETag: "part-etag"}, nil
}

func (m *uploadMock) CompleteMultipartUpload(context.Context, string, string, []provider.PartETag) (provider.PutResult, error) {
	if m.completeErr != nil {
		return provider.PutResult{}, m.completeErr
	}
	return provider.PutResult{ETag: "complete-etag"}, nil
}

func (m *uploadMock) CompleteMultipartUploadConditional(context.Context, string, string, []provider.PartETag, provider.PutPrecondition) (provider.PutResult, error) {
	m.completeConditional = true
	if m.completeErr != nil {
		return provider.PutResult{}, m.completeErr
	}
	return provider.PutResult{ETag: "complete-etag"}, nil
}

func (m *uploadMock) AbortMultipartUpload(context.Context, string, string) error {
	m.abortCalled = true
	return m.abortErr
}

type transferMultipartProvider struct {
	objects   map[string]string
	size      int64
	payload   io.Reader
	putCalled bool
	partSizes []int64
}

func (p *transferMultipartProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	out := make([]provider.ObjectSummary, 0, len(p.objects))
	for key := range p.objects {
		out = append(out, provider.ObjectSummary{Key: key, Size: p.size})
	}
	return &provider.ListResult{Objects: out}, nil
}

func (p *transferMultipartProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (p *transferMultipartProvider) Close() error { return nil }

func (p *transferMultipartProvider) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	if p.payload != nil {
		return io.NopCloser(p.payload), p.size, nil
	}
	return io.NopCloser(io.LimitReader(zeroReader{}, p.size)), p.size, nil
}

func (p *transferMultipartProvider) PutObject(context.Context, string, io.Reader, int64) error {
	p.putCalled = true
	return nil
}

func (p *transferMultipartProvider) CreateMultipartUpload(context.Context, string) (string, error) {
	return "upload-1", nil
}

func (p *transferMultipartProvider) UploadPart(_ context.Context, _ string, _ string, partNumber int32, body io.Reader, size int64) (provider.PartETag, error) {
	_, err := io.Copy(io.Discard, body)
	if err != nil {
		return provider.PartETag{}, err
	}
	p.partSizes = append(p.partSizes, size)
	return provider.PartETag{PartNumber: partNumber, ETag: "part-etag"}, nil
}

func (p *transferMultipartProvider) CompleteMultipartUpload(context.Context, string, string, []provider.PartETag) (provider.PutResult, error) {
	return provider.PutResult{ETag: "complete-etag"}, nil
}

func (p *transferMultipartProvider) AbortMultipartUpload(context.Context, string, string) error {
	return nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
