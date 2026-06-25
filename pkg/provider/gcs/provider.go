package gcs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Provider implements read operations for Google Cloud Storage.
type Provider struct {
	client  *storage.Client
	bucket  string
	maxKeys int
}

var (
	_ provider.Provider        = (*Provider)(nil)
	_ provider.ObjectGetter    = (*Provider)(nil)
	_ provider.VersionedGetter = (*Provider)(nil)
	_ provider.ObjectRanger    = (*Provider)(nil)
)

// New creates a new GCS provider with the given configuration.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	clientOpts := clientOptionsForConfig(ctx, cfg)
	clientOpts = append(clientOpts, storage.WithJSONReads())

	client, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, wrapGCSError("New", cfg.Bucket, "", err)
	}

	maxKeys := cfg.MaxKeys
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}
	if maxKeys > MaxAllowedKeys {
		maxKeys = MaxAllowedKeys
	}

	return &Provider{client: client, bucket: cfg.Bucket, maxKeys: maxKeys}, nil
}

func clientOptionsForConfig(ctx context.Context, cfg Config) []option.ClientOption {
	httpClient := transportTunedHTTPClient(cfg)
	if httpClient == nil {
		return cfg.AuthClientOptions()
	}

	switch {
	case cfg.Anonymous:
		return []option.ClientOption{option.WithHTTPClient(httpClient)}
	case cfg.TokenSource != nil:
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
		return []option.ClientOption{option.WithHTTPClient(oauth2.NewClient(ctx, cfg.TokenSource))}
	default:
		// WithHTTPClient takes precedence over ADC-related options. Keep ambient
		// ADC on the SDK default transport until authenticated transport tuning
		// can be added without changing credential resolution.
		return nil
	}
}

func transportTunedHTTPClient(cfg Config) *http.Client {
	if cfg.MaxIdleConnsPerHost <= 0 && cfg.MaxConnsPerHost <= 0 {
		return nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.MaxIdleConnsPerHost > 0 {
		tr.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
		if tr.MaxIdleConns < cfg.MaxIdleConnsPerHost {
			tr.MaxIdleConns = cfg.MaxIdleConnsPerHost
		}
	}
	if cfg.MaxConnsPerHost > 0 {
		tr.MaxConnsPerHost = cfg.MaxConnsPerHost
	}
	return &http.Client{Transport: tr}
}

// List returns a page of objects with the given prefix.
func (p *Provider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	iter := p.client.Bucket(p.bucket).Objects(ctx, &storage.Query{Prefix: opts.Prefix})
	pager := iterator.NewPager(iter, clampMaxKeys(opts.MaxKeys, p.maxKeys), opts.ContinuationToken)

	var attrs []*storage.ObjectAttrs
	nextToken, err := pager.NextPage(&attrs)
	if err != nil {
		return nil, p.wrapError("List", "", err)
	}

	objects := make([]provider.ObjectSummary, 0, len(attrs))
	for _, attr := range attrs {
		objects = append(objects, objectSummaryFromAttrs(attr))
	}

	return &provider.ListResult{
		Objects:           objects,
		ContinuationToken: nextToken,
		IsTruncated:       nextToken != "",
	}, nil
}

// Head returns metadata for a single object.
func (p *Provider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	attrs, err := p.client.Bucket(p.bucket).Object(key).Attrs(ctx)
	if err != nil {
		return nil, p.wrapError("Head", key, err)
	}
	return objectMetaFromAttrs(attrs), nil
}

// GetObject downloads an object as a stream.
func (p *Provider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	reader, err := p.client.Bucket(p.bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, 0, p.wrapError("GetObject", key, err)
	}
	return reader, reader.Attrs.Size, nil
}

// GetObjectVersioned downloads an object and returns the generation observed
// from the same read handle.
func (p *Provider) GetObjectVersioned(ctx context.Context, key string) (io.ReadCloser, provider.ObjectMeta, error) {
	obj := p.client.Bucket(p.bucket).Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, provider.ObjectMeta{}, p.wrapError("GetObjectVersioned", key, err)
	}

	reader, err := obj.Generation(attrs.Generation).NewReader(ctx)
	if err != nil {
		return nil, provider.ObjectMeta{}, p.wrapError("GetObjectVersioned", key, err)
	}
	return reader, *objectMetaFromAttrs(attrs), nil
}

// GetRange downloads a byte range as a stream.
func (p *Provider) GetRange(ctx context.Context, key string, start, endInclusive int64) (io.ReadCloser, int64, error) {
	if start < 0 {
		return nil, 0, p.wrapError("GetRange", key, errors.New("start must be >= 0"))
	}
	if endInclusive < start {
		return nil, 0, p.wrapError("GetRange", key, errors.New("end must be >= start"))
	}
	length := endInclusive - start + 1
	reader, err := p.client.Bucket(p.bucket).Object(key).NewRangeReader(ctx, start, length)
	if err != nil {
		return nil, 0, p.wrapError("GetRange", key, err)
	}
	return reader, length, nil
}

// Close releases resources held by the SDK client.
func (p *Provider) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Close()
}

func (p *Provider) wrapError(op, key string, err error) error {
	return wrapGCSError(op, p.bucket, key, err)
}

func wrapGCSError(op, bucket, key string, err error) error {
	wrapped := &provider.ProviderError{
		Op:       op,
		Provider: provider.ProviderGCS,
		Bucket:   bucket,
		Key:      key,
		Err:      err,
	}

	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		wrapped.Err = provider.ErrNotFound
		return wrapped
	case errors.Is(err, storage.ErrBucketNotExist):
		wrapped.Err = provider.ErrBucketNotFound
		return wrapped
	case isGCSCredentialRefreshFailure(err):
		wrapped.Err = errors.Join(provider.ErrCredentialsRefreshFailed, err)
		return wrapped
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case http.StatusUnauthorized:
			wrapped.Err = provider.ErrInvalidCredentials
		case http.StatusForbidden:
			wrapped.Err = provider.ErrAccessDenied
		case http.StatusNotFound:
			if key == "" {
				wrapped.Err = provider.ErrBucketNotFound
			} else {
				wrapped.Err = provider.ErrNotFound
			}
		case http.StatusPreconditionFailed:
			wrapped.Err = provider.ErrPreconditionFailed
		case http.StatusTooManyRequests:
			wrapped.Err = provider.ErrThrottled
		case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			wrapped.Err = provider.ErrProviderUnavailable
		}
		return wrapped
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "credential") && (strings.Contains(msg, "refresh") || strings.Contains(msg, "token")):
		wrapped.Err = errors.Join(provider.ErrCredentialsRefreshFailed, err)
	case strings.Contains(msg, "access denied") || strings.Contains(msg, "forbidden") || strings.Contains(msg, "permission") || strings.Contains(msg, " 403"):
		wrapped.Err = provider.ErrAccessDenied
	case strings.Contains(msg, "unauthorized") || strings.Contains(msg, " 401"):
		wrapped.Err = provider.ErrInvalidCredentials
	case strings.Contains(msg, "not found") || strings.Contains(msg, " 404"):
		if key == "" {
			wrapped.Err = provider.ErrBucketNotFound
		} else {
			wrapped.Err = provider.ErrNotFound
		}
	case strings.Contains(msg, "rate") || strings.Contains(msg, "quota") || strings.Contains(msg, " 429"):
		wrapped.Err = provider.ErrThrottled
	case strings.Contains(msg, "unavailable") || strings.Contains(msg, " 503"):
		wrapped.Err = provider.ErrProviderUnavailable
	}

	return wrapped
}

func isGCSCredentialRefreshFailure(err error) bool {
	if err == nil {
		return false
	}
	if provider.IsCredentialsRefreshFailed(err) {
		return true
	}
	for err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "failed to refresh") && strings.Contains(msg, "credential") {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func objectSummaryFromAttrs(attrs *storage.ObjectAttrs) provider.ObjectSummary {
	if attrs == nil {
		return provider.ObjectSummary{}
	}
	return provider.ObjectSummary{
		Key:          attrs.Name,
		Size:         attrs.Size,
		ETag:         attrs.Etag,
		LastModified: attrs.Updated,
		StorageClass: attrs.StorageClass,
	}
}

func objectMetaFromAttrs(attrs *storage.ObjectAttrs) *provider.ObjectMeta {
	if attrs == nil {
		return &provider.ObjectMeta{}
	}
	return &provider.ObjectMeta{
		ObjectSummary: objectSummaryFromAttrs(attrs),
		Version:       generationString(attrs.Generation),
		ContentType:   attrs.ContentType,
		Metadata:      cloneStringMap(attrs.Metadata),
		StorageClass:  attrs.StorageClass,
	}
}

func generationString(generation int64) string {
	if generation == 0 {
		return ""
	}
	return strconv.FormatInt(generation, 10)
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clampMaxKeys(requested int, providerDefault int) int {
	maxKeys := requested
	if maxKeys <= 0 {
		maxKeys = providerDefault
	}
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeys
	}
	if maxKeys > MaxAllowedKeys {
		maxKeys = MaxAllowedKeys
	}
	return maxKeys
}
