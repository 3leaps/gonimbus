package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	DefaultMultipartPartSizeBytes  int64 = 8 << 20  // 8 MiB
	DefaultMultipartThresholdBytes int64 = 64 << 20 // 64 MiB
	MultipartDisabledThreshold     int64 = 1<<63 - 1
	MaxMultipartParts                    = 10000
)

type UploadOptions struct {
	PartSizeBytes         int64
	MultipartThreshold    int64
	MultipartThresholdSet bool
	RetryBufferBytes      int64
	TempDir               string
	Precondition          provider.PutPrecondition
	PutOptions            provider.PutOptions
	Progress              func(context.Context, UploadProgress) error
}

type UploadProgress struct {
	Key        string
	UploadID   string
	PartNumber int32
	PartBytes  int64
	Bytes      int64
	Status     string
}

type UploadResult struct {
	Bytes   int64
	ETag    string
	Version string
	Mode    string
}

func UploadReader(ctx context.Context, putter provider.ObjectPutter, key string, r io.Reader, opts UploadOptions) (UploadResult, error) {
	opts = normalizeUploadOptions(putter, opts)
	session, err := NewUploadSession(ctx, putter, key, opts)
	if err != nil {
		return UploadResult{}, err
	}
	if _, err := io.Copy(session, r); err != nil {
		return UploadResult{}, joinAbortErr(err, session.Abort(ctx))
	}
	result, err := session.Close(ctx)
	if err != nil {
		return UploadResult{}, joinAbortErr(err, session.Abort(ctx))
	}
	return result, nil
}

func UploadReaderWithSize(ctx context.Context, putter provider.ObjectPutter, key string, r io.Reader, size int64, opts UploadOptions) (UploadResult, error) {
	opts = normalizeUploadOptions(putter, opts)
	if size >= 0 && shouldUseMultipart(size, opts.MultipartThreshold, putter) {
		return uploadMultipartKnownSize(ctx, putter, key, r, size, opts)
	}
	body, err := newRetryableBodyWithTempDir(ctx, io.NopCloser(r), size, opts.RetryBufferBytes, opts.TempDir)
	if err != nil {
		return UploadResult{}, err
	}
	defer func() { _ = body.Close() }()
	result, err := putSingle(ctx, putter, key, body.Reader(), size, opts)
	if err != nil {
		return UploadResult{}, err
	}
	return UploadResult{Bytes: size, ETag: result.ETag, Version: result.Version, Mode: "single"}, nil
}

func UploadFile(ctx context.Context, putter provider.ObjectPutter, key string, path string, opts UploadOptions) (UploadResult, error) {
	f, err := os.Open(path) // #nosec G304 -- callers pass internal spool paths or explicit operator-selected files.
	if err != nil {
		return UploadResult{}, fmt.Errorf("open upload file: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return UploadResult{}, fmt.Errorf("stat upload file: %w", err)
	}
	return UploadReaderWithSize(ctx, putter, key, f, info.Size(), opts)
}

type UploadSession struct {
	ctx    context.Context
	putter provider.ObjectPutter
	key    string
	opts   UploadOptions

	spoolPath    string
	spool        *os.File
	spoolCleanup func() error

	multipart bool
	uploadID  string
	parts     []provider.PartETag
	partNum   int32
	partBuf   bytes.Buffer
	bytes     int64
	closed    bool
}

func NewUploadSession(ctx context.Context, putter provider.ObjectPutter, key string, opts UploadOptions) (*UploadSession, error) {
	opts = normalizeUploadOptions(putter, opts)
	spool, spoolCleanup, err := createSecureTempFile(opts.TempDir, "gonimbus-upload-spool-*")
	if err != nil {
		return nil, fmt.Errorf("create temp spool: %w", err)
	}
	return &UploadSession{
		ctx:          ctx,
		putter:       putter,
		key:          key,
		opts:         opts,
		spool:        spool,
		spoolPath:    spool.Name(),
		spoolCleanup: spoolCleanup,
		partNum:      1,
	}, nil
}

func (s *UploadSession) Write(p []byte) (int, error) {
	if s.closed {
		return 0, ErrUploadSessionClosed
	}
	written := 0
	for len(p) > 0 {
		if !s.multipart {
			n, err := s.spool.Write(p)
			if n > 0 {
				s.bytes += int64(n)
				written += n
				p = p[n:]
			}
			if err != nil {
				return written, err
			}
			if s.bytes > s.opts.MultipartThreshold {
				if err := s.startMultipart(s.ctx); err != nil {
					return written, err
				}
			}
			continue
		}
		space := int(s.opts.PartSizeBytes) - s.partBuf.Len()
		if space <= 0 {
			if err := s.flushPart(s.ctx, false); err != nil {
				return written, err
			}
			continue
		}
		if space > len(p) {
			space = len(p)
		}
		n, err := s.partBuf.Write(p[:space])
		s.bytes += int64(n)
		written += n
		p = p[n:]
		if err != nil {
			return written, err
		}
		if int64(s.partBuf.Len()) == s.opts.PartSizeBytes {
			if err := s.flushPart(s.ctx, false); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (s *UploadSession) Close(ctx context.Context) (UploadResult, error) {
	if s.closed {
		return UploadResult{}, ErrUploadSessionClosed
	}
	s.closed = true
	if !s.multipart {
		if err := s.spool.Close(); err != nil {
			return UploadResult{}, joinAbortErr(fmt.Errorf("close temp spool: %w", err), s.cleanupSpool())
		}
		result, err := UploadFile(ctx, s.putter, s.key, s.spoolPath, s.opts)
		cleanupErr := s.cleanupSpool()
		if err != nil {
			return UploadResult{}, joinAbortErr(err, cleanupErr)
		}
		if cleanupErr != nil {
			return UploadResult{}, cleanupErr
		}
		return result, nil
	}
	if s.partBuf.Len() > 0 || len(s.parts) == 0 {
		if err := s.flushPart(ctx, true); err != nil {
			return UploadResult{}, err
		}
	}
	result, err := completeMultipart(ctx, s.putter, s.key, s.uploadID, s.parts, s.opts)
	if err != nil {
		return UploadResult{}, fmt.Errorf("complete multipart upload: %w", err)
	}
	if err := s.cleanupSpool(); err != nil {
		return UploadResult{}, err
	}
	return UploadResult{Bytes: s.bytes, ETag: result.ETag, Version: result.Version, Mode: "multipart"}, nil
}

func (s *UploadSession) Abort(ctx context.Context) error {
	err := s.cleanupSpool()
	if s.multipart && s.uploadID != "" {
		if abortErr := s.multipartUploader().AbortMultipartUpload(ctx, s.key, s.uploadID); abortErr != nil {
			err = errors.Join(err, fmt.Errorf("abort multipart upload: %w", abortErr))
		}
	}
	return err
}

func (s *UploadSession) startMultipart(ctx context.Context) error {
	mu, ok := s.putter.(provider.MultipartUploader)
	if !ok {
		return fmt.Errorf("destination provider does not support multipart uploads")
	}
	uploadID, err := createMultipart(ctx, s.putter, s.key, s.opts.PutOptions)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	s.multipart = true
	s.uploadID = uploadID

	if _, err := s.spool.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek temp spool: %w", err)
	}
	buf := make([]byte, s.opts.PartSizeBytes)
	for {
		n, readErr := io.ReadFull(s.spool, buf)
		if n > 0 {
			if readErr == io.ErrUnexpectedEOF {
				_, _ = s.partBuf.Write(buf[:n])
			} else {
				if err := s.checkPartLimit(); err != nil {
					return err
				}
				part, err := mu.UploadPart(ctx, s.key, s.uploadID, s.partNum, bytes.NewReader(buf[:n]), int64(n))
				if err != nil {
					return fmt.Errorf("upload multipart part %d: %w", s.partNum, err)
				}
				s.parts = append(s.parts, part)
				if err := s.writeProgress(ctx, part.PartNumber, int64(n), "uploaded"); err != nil {
					return err
				}
				s.partNum++
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read temp spool: %w", readErr)
		}
	}
	if err := s.spool.Close(); err != nil {
		return fmt.Errorf("close temp spool: %w", err)
	}
	if err := s.cleanupSpool(); err != nil {
		return err
	}
	return nil
}

func (s *UploadSession) flushPart(ctx context.Context, final bool) error {
	if s.partBuf.Len() == 0 && !final {
		return nil
	}
	if err := s.checkPartLimit(); err != nil {
		return err
	}
	body := append([]byte(nil), s.partBuf.Bytes()...)
	s.partBuf.Reset()
	part, err := s.multipartUploader().UploadPart(ctx, s.key, s.uploadID, s.partNum, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return fmt.Errorf("upload multipart part %d: %w", s.partNum, err)
	}
	s.parts = append(s.parts, part)
	if err := s.writeProgress(ctx, part.PartNumber, int64(len(body)), "uploaded"); err != nil {
		return err
	}
	s.partNum++
	return nil
}

func (s *UploadSession) checkPartLimit() error {
	if len(s.parts) >= MaxMultipartParts {
		return fmt.Errorf("multipart upload exceeds %d part limit", MaxMultipartParts)
	}
	return nil
}

func (s *UploadSession) multipartUploader() provider.MultipartUploader {
	return s.putter.(provider.MultipartUploader)
}

func (s *UploadSession) writeProgress(ctx context.Context, partNumber int32, partBytes int64, status string) error {
	if s.opts.Progress == nil {
		return nil
	}
	return s.opts.Progress(ctx, UploadProgress{
		Key:        s.key,
		UploadID:   s.uploadID,
		PartNumber: partNumber,
		PartBytes:  partBytes,
		Bytes:      s.bytes,
		Status:     status,
	})
}

func (s *UploadSession) cleanupSpool() error {
	if s.spoolCleanup == nil {
		return nil
	}
	cleanup := s.spoolCleanup
	s.spoolCleanup = nil
	return cleanup()
}

var ErrUploadSessionClosed = errors.New("transfer upload session is closed")

func uploadMultipartKnownSize(ctx context.Context, putter provider.ObjectPutter, key string, r io.Reader, size int64, opts UploadOptions) (UploadResult, error) {
	opts.PartSizeBytes = partSizeForKnownSize(size, opts.PartSizeBytes)
	uploadID, err := createMultipart(ctx, putter, key, opts.PutOptions)
	if err != nil {
		return UploadResult{}, fmt.Errorf("create multipart upload: %w", err)
	}
	var (
		parts       []provider.PartETag
		partNumber  int32 = 1
		transferred int64
	)
	mu := putter.(provider.MultipartUploader)
	buf := make([]byte, opts.PartSizeBytes)
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			if len(parts) >= MaxMultipartParts {
				err := fmt.Errorf("multipart upload exceeds %d part limit", MaxMultipartParts)
				return UploadResult{}, joinAbortErr(err, mu.AbortMultipartUpload(ctx, key, uploadID))
			}
			part, err := mu.UploadPart(ctx, key, uploadID, partNumber, bytes.NewReader(buf[:n]), int64(n))
			if err != nil {
				return UploadResult{}, joinAbortErr(fmt.Errorf("upload multipart part %d: %w", partNumber, err), mu.AbortMultipartUpload(ctx, key, uploadID))
			}
			parts = append(parts, part)
			transferred += int64(n)
			if opts.Progress != nil {
				if err := opts.Progress(ctx, UploadProgress{Key: key, UploadID: uploadID, PartNumber: part.PartNumber, PartBytes: int64(n), Bytes: transferred, Status: "uploaded"}); err != nil {
					return UploadResult{}, joinAbortErr(err, mu.AbortMultipartUpload(ctx, key, uploadID))
				}
			}
			partNumber++
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return UploadResult{}, joinAbortErr(fmt.Errorf("read multipart source: %w", readErr), mu.AbortMultipartUpload(ctx, key, uploadID))
		}
	}
	if len(parts) == 0 {
		part, err := mu.UploadPart(ctx, key, uploadID, 1, bytes.NewReader(nil), 0)
		if err != nil {
			return UploadResult{}, joinAbortErr(fmt.Errorf("upload multipart part 1: %w", err), mu.AbortMultipartUpload(ctx, key, uploadID))
		}
		parts = append(parts, part)
	}
	result, err := completeMultipart(ctx, putter, key, uploadID, parts, opts)
	if err != nil {
		return UploadResult{}, joinAbortErr(fmt.Errorf("complete multipart upload: %w", err), mu.AbortMultipartUpload(ctx, key, uploadID))
	}
	return UploadResult{Bytes: size, ETag: result.ETag, Version: result.Version, Mode: "multipart"}, nil
}

func createMultipart(ctx context.Context, putter provider.ObjectPutter, key string, opts provider.PutOptions) (string, error) {
	if opts.Empty() {
		mu, ok := putter.(provider.MultipartUploader)
		if !ok {
			return "", fmt.Errorf("destination provider does not support multipart uploads")
		}
		return mu.CreateMultipartUpload(ctx, key)
	}
	mu, ok := putter.(provider.MetadataAwareMultipartUploader)
	if !ok {
		return "", fmt.Errorf("destination provider does not support metadata-aware multipart uploads")
	}
	return mu.CreateMultipartUploadWithOptions(ctx, key, opts)
}

func completeMultipart(ctx context.Context, putter provider.ObjectPutter, key string, uploadID string, parts []provider.PartETag, opts UploadOptions) (provider.PutResult, error) {
	if hasPrecondition(opts.Precondition) {
		conditional, ok := putter.(provider.ConditionalMultipartCompleter)
		if !ok {
			return provider.PutResult{}, fmt.Errorf("destination provider does not support conditional multipart completion")
		}
		return conditional.CompleteMultipartUploadConditional(ctx, key, uploadID, parts, opts.Precondition)
	}
	mu, ok := putter.(provider.MultipartUploader)
	if !ok {
		return provider.PutResult{}, fmt.Errorf("destination provider does not support multipart uploads")
	}
	return mu.CompleteMultipartUpload(ctx, key, uploadID, parts)
}

func putSingle(ctx context.Context, putter provider.ObjectPutter, key string, r io.Reader, size int64, opts UploadOptions) (provider.PutResult, error) {
	if hasPrecondition(opts.Precondition) {
		if opts.PutOptions.Empty() {
			conditional, ok := putter.(provider.ConditionalPutter)
			if !ok {
				return provider.PutResult{}, fmt.Errorf("destination provider does not support conditional writes")
			}
			return conditional.PutObjectConditional(ctx, key, r, size, opts.Precondition)
		}
		optioned, ok := putter.(provider.MetadataAwarePutter)
		if !ok {
			return provider.PutResult{}, fmt.Errorf("destination provider does not support metadata-aware conditional writes")
		}
		return optioned.PutObjectConditionalWithOptions(ctx, key, r, size, opts.Precondition, opts.PutOptions)
	}
	if opts.PutOptions.Empty() {
		if err := putter.PutObject(ctx, key, r, size); err != nil {
			return provider.PutResult{}, err
		}
		return provider.PutResult{}, nil
	}
	optioned, ok := putter.(provider.MetadataAwarePutter)
	if !ok {
		return provider.PutResult{}, fmt.Errorf("destination provider does not support metadata-aware writes")
	}
	if err := optioned.PutObjectWithOptions(ctx, key, r, size, opts.PutOptions); err != nil {
		return provider.PutResult{}, err
	}
	return provider.PutResult{}, nil
}

func normalizeUploadOptions(putter provider.ObjectPutter, opts UploadOptions) UploadOptions {
	if opts.PartSizeBytes <= 0 {
		opts.PartSizeBytes = DefaultMultipartPartSizeBytes
	}
	if !opts.MultipartThresholdSet {
		opts.MultipartThreshold = DefaultMultipartThresholdBytes
	} else if opts.MultipartThreshold < 0 {
		opts.MultipartThreshold = DefaultMultipartThresholdBytes
	}
	if _, ok := putter.(provider.MultipartUploader); !ok {
		opts.MultipartThreshold = MultipartDisabledThreshold
	}
	if opts.RetryBufferBytes == 0 {
		opts.RetryBufferBytes = DefaultRetryBufferMaxMemoryBytes
	}
	return opts
}

func shouldUseMultipart(size int64, threshold int64, putter provider.ObjectPutter) bool {
	if size < 0 || size <= threshold {
		return false
	}
	_, ok := putter.(provider.MultipartUploader)
	return ok
}

func partSizeForKnownSize(size int64, requested int64) int64 {
	if requested <= 0 {
		requested = DefaultMultipartPartSizeBytes
	}
	if size <= 0 {
		return requested
	}
	minPartSize := (size + MaxMultipartParts - 1) / MaxMultipartParts
	if minPartSize > requested {
		return minPartSize
	}
	return requested
}

func hasPrecondition(precond provider.PutPrecondition) bool {
	return precond.IfAbsent || precond.IfMatchETag != nil
}

func joinAbortErr(err error, abortErr error) error {
	if abortErr == nil {
		return err
	}
	return errors.Join(err, abortErr)
}

func createSecureTempFile(root string, pattern string) (*os.File, func() error, error) {
	if err := rejectRepoLocalTempRoot(root); err != nil {
		return nil, nil, err
	}
	dir, err := os.MkdirTemp(root, "gonimbus-transfer-*")
	if err != nil {
		return nil, nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil { // #nosec G302 -- owner-only temp directory permissions are intentional.
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	if err := os.Chmod(f.Name(), 0o600); err != nil {
		_ = f.Close()
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	cleanup := func() error {
		closeErr := f.Close()
		rmErr := os.RemoveAll(dir)
		if closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			return fmt.Errorf("close temp file: %w", closeErr)
		}
		if rmErr != nil {
			return fmt.Errorf("remove temp spool: %w", rmErr)
		}
		return nil
	}
	return f, cleanup, nil
}

func rejectRepoLocalTempRoot(root string) error {
	if root == "" {
		root = os.TempDir()
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if _, ok := findRepoRoot(absRoot); ok {
		return fmt.Errorf("temp spool directory must be outside the repository working tree")
	}
	return nil
}

func findRepoRoot(start string) (string, bool) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", false
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}
