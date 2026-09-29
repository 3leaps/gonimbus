package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

type receiptPutter struct {
	payload []byte
	opts    provider.PutOptions
	err     error
}

func (p *receiptPutter) PutObject(ctx context.Context, key string, body io.Reader, size int64) error {
	_, err := p.PutObjectResult(ctx, key, body, size)
	return err
}

func (p *receiptPutter) PutObjectResult(ctx context.Context, key string, body io.Reader, size int64) (provider.PutResult, error) {
	return p.PutObjectResultWithOptions(ctx, key, body, size, provider.PutOptions{})
}

func (p *receiptPutter) PutObjectResultWithOptions(_ context.Context, _ string, body io.Reader, size int64, opts provider.PutOptions) (provider.PutResult, error) {
	p.opts = opts
	// Simulate an SDK partially consuming and then replaying a seekable PUT.
	if _, err := io.CopyN(io.Discard, body, 2); err != nil && !errors.Is(err, io.EOF) {
		return provider.PutResult{}, err
	}
	if _, err := body.(io.Seeker).Seek(0, io.SeekStart); err != nil {
		return provider.PutResult{}, err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return provider.PutResult{}, err
	}
	if int64(len(data)) != size {
		return provider.PutResult{}, errors.New("content length mismatch")
	}
	p.payload = data
	if p.err != nil {
		return provider.PutResult{}, p.err
	}
	return provider.PutResult{ETag: "returned-etag", Version: "returned-version"}, nil
}

func payloadSHA256(payload string) string {
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func TestUploadReceiptSingleReplay(t *testing.T) {
	for _, payload := range []string{"", "abcdef"} {
		for _, size := range []int64{-1, 0, 1, int64(len(payload)), int64(len(payload) + 3)} {
			for _, memory := range []int64{1, 32} {
				p := &receiptPutter{}
				result, err := UploadReaderWithSize(context.Background(), p, "object", strings.NewReader(payload), size, UploadOptions{RetryBufferBytes: memory})
				require.NoError(t, err)
				require.Equal(t, []byte(payload), p.payload)
				require.Equal(t, int64(len(payload)), result.Bytes)
				require.Equal(t, payloadSHA256(payload), result.SHA256)
				require.Equal(t, "returned-etag", result.ETag)
				require.Equal(t, "returned-version", result.Version)
			}
		}
	}
}

func TestUploadReceiptSessionAndMetadata(t *testing.T) {
	p := &receiptPutter{}
	opts := UploadOptions{PutOptions: provider.PutOptions{ContentType: "text/plain"}}
	result, err := UploadReader(context.Background(), p, "object", strings.NewReader("abcdef"), opts)
	require.NoError(t, err)
	require.Equal(t, payloadSHA256("abcdef"), result.SHA256)
	require.Equal(t, opts.PutOptions, p.opts)
	require.Equal(t, "returned-version", result.Version)
}

func TestUploadReceiptMultipartAndFailure(t *testing.T) {
	for _, knownSize := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			p := &uploadMock{}
			if fail {
				p.completeErr = errors.New("failed completion")
			}
			opts := UploadOptions{PartSizeBytes: 3, MultipartThreshold: 4, MultipartThresholdSet: true}
			var result UploadResult
			var err error
			if knownSize {
				result, err = UploadReaderWithSize(context.Background(), p, "object", strings.NewReader("abcdefgh"), 10, opts)
			} else {
				result, err = UploadReader(context.Background(), p, "object", strings.NewReader("abcdefgh"), opts)
			}
			if fail {
				require.Error(t, err)
				require.Equal(t, UploadResult{}, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(8), result.Bytes)
				require.Equal(t, payloadSHA256("abcdefgh"), result.SHA256)
			}
		}
	}
}

func TestUploadReceiptFailureAndLegacyFallback(t *testing.T) {
	p := &receiptPutter{err: errors.New("write failed")}
	result, err := UploadReaderWithSize(context.Background(), p, "object", strings.NewReader("abc"), 3, UploadOptions{})
	require.Error(t, err)
	require.Equal(t, UploadResult{}, result)
	result, err = UploadReaderWithSize(context.Background(), &uploadMock{}, "object", strings.NewReader("abc"), 3, UploadOptions{})
	require.NoError(t, err)
	require.Equal(t, payloadSHA256("abc"), result.SHA256)
	require.Empty(t, result.ETag)
	require.Empty(t, result.Version)
}

type replayMultipartPutter struct{ uploadMock }

func (p *replayMultipartPutter) UploadPart(ctx context.Context, key, uploadID string, number int32, body io.Reader, size int64) (provider.PartETag, error) {
	_, err := io.Copy(io.Discard, body)
	if err != nil {
		return provider.PartETag{}, err
	}
	_, err = body.(io.Seeker).Seek(0, io.SeekStart)
	if err != nil {
		return provider.PartETag{}, err
	}
	return p.uploadMock.UploadPart(ctx, key, uploadID, number, body, size)
}

func TestUploadReceiptPartReplayAndIncrementalThreshold(t *testing.T) {
	for _, known := range []bool{true, false} {
		p := &replayMultipartPutter{}
		opts := UploadOptions{PartSizeBytes: 3, MultipartThreshold: 4, MultipartThresholdSet: true}
		var result UploadResult
		var err error
		if known {
			result, err = UploadReaderWithSize(context.Background(), p, "object", strings.NewReader("abcdefgh"), 8, opts)
		} else {
			s, openErr := NewUploadSession(context.Background(), p, "object", opts)
			require.NoError(t, openErr)
			for _, chunk := range []string{"ab", "cde", "fgh"} {
				_, err = s.Write([]byte(chunk))
				require.NoError(t, err)
			}
			result, err = s.Close(context.Background())
		}
		require.NoError(t, err)
		require.Equal(t, payloadSHA256("abcdefgh"), result.SHA256)
		require.Equal(t, [][]byte{[]byte("abc"), []byte("def"), []byte("gh")}, p.parts)
	}
}
