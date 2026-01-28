package file

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Provider implements provider.Provider for local filesystem paths.
//
// Keys are treated as relative paths under BaseDir.
//
// This provider is intended for bucket-to-local transfer workflows.
type Provider struct {
	baseDir string
}

// Ensure Provider implements provider capability interfaces.
var (
	_ provider.Provider      = (*Provider)(nil)
	_ provider.ObjectGetter  = (*Provider)(nil)
	_ provider.ObjectRanger  = (*Provider)(nil)
	_ provider.ObjectPutter  = (*Provider)(nil)
	_ provider.ObjectDeleter = (*Provider)(nil)
)

type Config struct {
	BaseDir string
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.BaseDir) == "" {
		return fmt.Errorf("base dir is required")
	}
	return nil
}

func New(cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base := filepath.Clean(cfg.BaseDir)
	return &Provider{baseDir: base}, nil
}

func (p *Provider) Close() error { return nil }

func (p *Provider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	_ = ctx
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	prefix := strings.TrimPrefix(opts.Prefix, "/")
	keys, err := p.collectKeys(prefix)
	if err != nil {
		return nil, p.wrapError("List", opts.Prefix, err)
	}
	sort.Strings(keys)

	start := 0
	if opts.ContinuationToken != "" {
		// Start strictly after the last returned key.
		idx := sort.SearchStrings(keys, opts.ContinuationToken)
		for idx < len(keys) && keys[idx] <= opts.ContinuationToken {
			idx++
		}
		start = idx
	}

	end := start + maxKeys
	if end > len(keys) {
		end = len(keys)
	}

	objects := make([]provider.ObjectSummary, 0, end-start)
	for _, k := range keys[start:end] {
		full, err := p.fullPath(k)
		if err != nil {
			continue
		}
		st, err := os.Stat(full)
		if err != nil || st.IsDir() {
			continue
		}
		objects = append(objects, provider.ObjectSummary{Key: k, Size: st.Size(), LastModified: st.ModTime()})
	}

	res := &provider.ListResult{Objects: objects}
	if end < len(keys) {
		res.IsTruncated = true
		res.ContinuationToken = keys[end-1]
	}
	return res, nil
}

func (p *Provider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	_ = ctx
	full, err := p.fullPath(key)
	if err != nil {
		return nil, p.wrapError("Head", key, err)
	}
	st, err := os.Stat(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderFile, Key: key, Err: provider.ErrNotFound}
		}
		return nil, p.wrapError("Head", key, err)
	}
	if st.IsDir() {
		return nil, &provider.ProviderError{Op: "Head", Provider: provider.ProviderFile, Key: key, Err: provider.ErrNotFound}
	}

	return &provider.ObjectMeta{
		ObjectSummary: provider.ObjectSummary{Key: strings.TrimPrefix(key, "/"), Size: st.Size(), LastModified: st.ModTime()},
		ContentType:   "",
		Metadata:      nil,
	}, nil
}

func (p *Provider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	_ = ctx
	full, err := p.fullPath(key)
	if err != nil {
		return nil, 0, p.wrapError("GetObject", key, err)
	}
	f, err := os.Open(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, &provider.ProviderError{Op: "GetObject", Provider: provider.ProviderFile, Key: key, Err: provider.ErrNotFound}
		}
		return nil, 0, p.wrapError("GetObject", key, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, p.wrapError("GetObject", key, err)
	}
	return f, st.Size(), nil
}

func (p *Provider) GetRange(ctx context.Context, key string, start, endInclusive int64) (io.ReadCloser, int64, error) {
	_ = ctx
	full, err := p.fullPath(key)
	if err != nil {
		return nil, 0, p.wrapError("GetRange", key, err)
	}
	f, err := os.Open(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, &provider.ProviderError{Op: "GetRange", Provider: provider.ProviderFile, Key: key, Err: provider.ErrNotFound}
		}
		return nil, 0, p.wrapError("GetRange", key, err)
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, p.wrapError("GetRange", key, err)
	}

	if start < 0 {
		_ = f.Close()
		return nil, 0, p.wrapError("GetRange", key, fmt.Errorf("start must be >= 0"))
	}
	if endInclusive < start {
		_ = f.Close()
		return nil, 0, p.wrapError("GetRange", key, fmt.Errorf("end must be >= start"))
	}
	length := (endInclusive - start) + 1
	if start >= st.Size() {
		_ = f.Close()
		return io.NopCloser(strings.NewReader("")), 0, nil
	}
	if start+length > st.Size() {
		length = st.Size() - start
	}

	// Wrap with a closer that closes the file.
	r := io.NewSectionReader(f, start, length)
	return &sectionReadCloser{r: r, c: f}, length, nil
}

type sectionReadCloser struct {
	r io.Reader
	c io.Closer
}

func (s *sectionReadCloser) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *sectionReadCloser) Close() error               { return s.c.Close() }

func (p *Provider) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	_ = ctx
	_ = contentLength
	full, err := p.fullPath(key)
	if err != nil {
		return p.wrapError("PutObject", key, err)
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return p.wrapError("PutObject", key, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(full), "gonimbus-put-*")
	if err != nil {
		return p.wrapError("PutObject", key, err)
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err := io.Copy(tmp, body); err != nil {
		return p.wrapError("PutObject", key, err)
	}
	if err := tmp.Close(); err != nil {
		return p.wrapError("PutObject", key, err)
	}

	if err := os.Rename(tmpName, full); err != nil {
		return p.wrapError("PutObject", key, err)
	}
	return nil
}

func (p *Provider) DeleteObject(ctx context.Context, key string) error {
	_ = ctx
	full, err := p.fullPath(key)
	if err != nil {
		return p.wrapError("DeleteObject", key, err)
	}
	if err := os.Remove(full); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return p.wrapError("DeleteObject", key, err)
	}
	return nil
}

func (p *Provider) fullPath(key string) (string, error) {
	key = strings.TrimSpace(key)
	key = strings.TrimPrefix(key, "/")
	// Prevent path traversal.
	clean := filepath.Clean("/" + key)
	clean = strings.TrimPrefix(clean, "/")
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("invalid key path")
	}
	return filepath.Join(p.baseDir, filepath.FromSlash(clean)), nil
}

func (p *Provider) collectKeys(prefix string) ([]string, error) {
	root, err := p.fullPath(prefix)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var keys []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(p.baseDir, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		keys = append(keys, rel)
		return nil
	})
	return keys, nil
}

func (p *Provider) wrapError(op, key string, err error) error {
	wrapped := &provider.ProviderError{Op: op, Provider: provider.ProviderFile, Key: key, Err: err}
	if err == nil {
		wrapped.Err = fmt.Errorf("unknown error")
	}
	// Normalize common filesystem errors to provider sentinels.
	if os.IsNotExist(err) {
		wrapped.Err = provider.ErrNotFound
	}
	if os.IsPermission(err) {
		wrapped.Err = provider.ErrAccessDenied
	}
	return wrapped
}

// File providers don't use ETag; provide a consistent "modified" time for records.
