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
	googleoauth "golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// Provider implements read operations for Google Cloud Storage.
type Provider struct {
	client               *storage.Client
	bucket               string
	maxKeys              int
	anonymous            bool
	writerChunkSizeBytes int
}

var (
	_ provider.Provider            = (*Provider)(nil)
	_ provider.PrefixLister        = (*Provider)(nil)
	_ provider.DelimiterLister     = (*Provider)(nil)
	_ provider.ObjectGetter        = (*Provider)(nil)
	_ provider.VersionedGetter     = (*Provider)(nil)
	_ provider.ObjectRanger        = (*Provider)(nil)
	_ provider.ObjectPutter        = (*Provider)(nil)
	_ provider.ConditionalPutter   = (*Provider)(nil)
	_ provider.MetadataAwarePutter = (*Provider)(nil)
	_ provider.ObjectDeleter       = (*Provider)(nil)
)

var adcTokenSource = googleoauth.DefaultTokenSource

// New creates a new GCS provider with the given configuration.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	clientOpts, err := clientOptionsForConfig(ctx, cfg)
	if err != nil {
		return nil, wrapGCSError("New", cfg.Bucket, "", err)
	}
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

	return &Provider{
		client:               client,
		bucket:               cfg.Bucket,
		maxKeys:              maxKeys,
		anonymous:            cfg.Anonymous,
		writerChunkSizeBytes: cfg.WriterChunkSizeBytes,
	}, nil
}

func clientOptionsForConfig(ctx context.Context, cfg Config) ([]option.ClientOption, error) {
	httpClient := transportTunedHTTPClient(cfg)
	if httpClient == nil {
		return cfg.AuthClientOptions(), nil
	}

	switch {
	case cfg.Anonymous:
		return []option.ClientOption{option.WithHTTPClient(httpClient)}, nil
	case cfg.TokenSource != nil:
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
		return []option.ClientOption{option.WithHTTPClient(oauth2.NewClient(ctx, cfg.TokenSource))}, nil
	default:
		// Keep ADC ambient: callers can request transport sizing, but cannot
		// provide credential file paths or endpoint overrides through Config.
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
		tokenSource, err := adcTokenSource(ctx, DefaultScopes()...)
		if err != nil {
			return nil, err
		}
		return []option.ClientOption{option.WithHTTPClient(oauth2.NewClient(ctx, tokenSource))}, nil
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

// ListCommonPrefixes returns immediate child prefixes for prefix discovery.
func (p *Provider) ListCommonPrefixes(ctx context.Context, opts provider.ListCommonPrefixesOptions) (*provider.ListCommonPrefixesResult, error) {
	delimiter := opts.Delimiter
	if delimiter == "" {
		delimiter = "/"
	}
	iter := p.client.Bucket(p.bucket).Objects(ctx, &storage.Query{Prefix: opts.Prefix, Delimiter: delimiter})
	pager := iterator.NewPager(iter, clampMaxKeys(opts.MaxKeys, p.maxKeys), opts.ContinuationToken)

	var attrs []*storage.ObjectAttrs
	nextToken, err := pager.NextPage(&attrs)
	if err != nil {
		return nil, p.wrapError("ListCommonPrefixes", "", err)
	}

	prefixes := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		if attr != nil && attr.Prefix != "" {
			prefixes = append(prefixes, attr.Prefix)
		}
	}
	return &provider.ListCommonPrefixesResult{
		Prefixes:          prefixes,
		ContinuationToken: nextToken,
		IsTruncated:       nextToken != "",
	}, nil
}

// ListWithDelimiter returns direct objects and immediate child prefixes.
func (p *Provider) ListWithDelimiter(ctx context.Context, opts provider.ListWithDelimiterOptions) (*provider.ListWithDelimiterResult, error) {
	delimiter := opts.Delimiter
	if delimiter == "" {
		delimiter = "/"
	}
	iter := p.client.Bucket(p.bucket).Objects(ctx, &storage.Query{Prefix: opts.Prefix, Delimiter: delimiter})
	pager := iterator.NewPager(iter, clampMaxKeys(opts.MaxKeys, p.maxKeys), opts.ContinuationToken)

	var attrs []*storage.ObjectAttrs
	nextToken, err := pager.NextPage(&attrs)
	if err != nil {
		return nil, p.wrapError("ListWithDelimiter", "", err)
	}

	objects := make([]provider.ObjectSummary, 0, len(attrs))
	prefixes := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		if attr == nil {
			continue
		}
		if attr.Prefix != "" {
			prefixes = append(prefixes, attr.Prefix)
			continue
		}
		objects = append(objects, objectSummaryFromAttrs(attr))
	}
	return &provider.ListWithDelimiterResult{
		Objects:           objects,
		CommonPrefixes:    prefixes,
		ContinuationToken: nextToken,
		IsTruncated:       nextToken != "",
	}, nil
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

// PutObject uploads an object.
func (p *Provider) PutObject(ctx context.Context, key string, body io.Reader, contentLength int64) error {
	return p.PutObjectWithOptions(ctx, key, body, contentLength, provider.PutOptions{})
}

func (p *Provider) PutObjectWithOptions(ctx context.Context, key string, body io.Reader, _ int64, opts provider.PutOptions) error {
	if err := p.guardWrite("PutObject", key); err != nil {
		return err
	}
	writer := p.newWriter(ctx, p.client.Bucket(p.bucket).Object(key), opts)
	if _, err := io.Copy(writer, body); err != nil {
		_ = writer.Close()
		return p.wrapError("PutObject", key, err)
	}
	if err := writer.Close(); err != nil {
		return p.wrapError("PutObject", key, err)
	}
	return nil
}

func (p *Provider) PutObjectConditional(ctx context.Context, key string, body io.Reader, contentLength int64, precond provider.PutPrecondition) (provider.PutResult, error) {
	return p.PutObjectConditionalWithOptions(ctx, key, body, contentLength, precond, provider.PutOptions{})
}

func (p *Provider) PutObjectConditionalWithOptions(ctx context.Context, key string, body io.Reader, _ int64, precond provider.PutPrecondition, opts provider.PutOptions) (provider.PutResult, error) {
	if err := p.guardWrite("PutObjectConditional", key); err != nil {
		return provider.PutResult{}, err
	}
	if err := precond.Validate(); err != nil {
		return provider.PutResult{}, p.wrapError("PutObjectConditional", key, err)
	}
	if precond.IfMatchETag != nil {
		return provider.PutResult{}, &provider.ProviderError{
			Op:       "PutObjectConditional",
			Provider: provider.ProviderGCS,
			Bucket:   p.bucket,
			Key:      key,
			Err:      provider.ErrUnsupportedPrecondition,
		}
	}

	obj := p.client.Bucket(p.bucket).Object(key)
	if precond.IfAbsent {
		obj = obj.If(storage.Conditions{DoesNotExist: true})
	}
	writer := p.newWriter(ctx, obj, opts)
	if _, err := io.Copy(writer, body); err != nil {
		_ = writer.Close()
		return provider.PutResult{}, p.wrapConditionalPutError(key, precond, err)
	}
	if err := writer.Close(); err != nil {
		return provider.PutResult{}, p.wrapConditionalPutError(key, precond, err)
	}
	return putResultFromAttrs(writer.Attrs()), nil
}

// DeleteObject deletes an object.
func (p *Provider) DeleteObject(ctx context.Context, key string) error {
	if err := p.guardWrite("DeleteObject", key); err != nil {
		return err
	}
	if err := p.client.Bucket(p.bucket).Object(key).Delete(ctx); err != nil {
		return p.wrapError("DeleteObject", key, err)
	}
	return nil
}

// Close releases resources held by the SDK client.
func (p *Provider) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Close()
}

func (p *Provider) newWriter(ctx context.Context, obj *storage.ObjectHandle, opts provider.PutOptions) *storage.Writer {
	writer := obj.NewWriter(ctx)
	if p.writerChunkSizeBytes > 0 {
		writer.ChunkSize = p.writerChunkSizeBytes
	}
	if opts.ContentType != "" {
		writer.ContentType = opts.ContentType
	}
	if len(opts.UserMetadata) > 0 {
		writer.Metadata = opts.UserMetadata
	}
	if opts.StorageClass != "" {
		writer.StorageClass = opts.StorageClass
	}
	return writer
}

// guardWrite is the fail-closed chokepoint for anonymous providers. Every GCS
// mutating method must call it before constructing or issuing a request.
func (p *Provider) guardWrite(op, key string) error {
	if !p.anonymous {
		return nil
	}
	return &provider.ProviderError{
		Op:       op,
		Provider: provider.ProviderGCS,
		Bucket:   p.bucket,
		Key:      key,
		Err:      errors.Join(provider.ErrAccessDenied, provider.ErrAnonymousReadOnly),
	}
}

func (p *Provider) wrapError(op, key string, err error) error {
	return wrapGCSError(op, p.bucket, key, err)
}

func (p *Provider) wrapConditionalPutError(key string, precond provider.PutPrecondition, err error) error {
	wrapped := p.wrapError("PutObjectConditional", key, err)
	if !isGCSPreconditionFailure(err) {
		return wrapped
	}
	if providerErr, ok := wrapped.(*provider.ProviderError); ok {
		if precond.IfAbsent {
			providerErr.Err = provider.ErrAlreadyExists
			return providerErr
		}
		if precond.IfMatchETag != nil {
			providerErr.Err = provider.ErrPreconditionFailed
			return providerErr
		}
	}
	return wrapped
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
		if isGCSThrottleError(apiErr) {
			wrapped.Err = provider.ErrThrottled
			return wrapped
		}
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

func isGCSThrottleError(err *googleapi.Error) bool {
	if err == nil {
		return false
	}
	for _, item := range err.Errors {
		if isGCSThrottleReason(item.Reason) || isGCSThrottleReason(item.Message) {
			return true
		}
	}
	return isGCSThrottleReason(err.Message) || isGCSThrottleReason(err.Body)
}

func isGCSThrottleReason(value string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "_", ""))
	switch normalized {
	case "ratelimitexceeded", "userratelimitexceeded", "quotaexceeded", "resourceexhausted":
		return true
	default:
		return strings.Contains(normalized, "resourceexhausted")
	}
}

func isGCSPreconditionFailure(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "Precondition") || strings.Contains(errMsg, "conditionNotMet") || strings.Contains(errMsg, " 412")
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

func putResultFromAttrs(attrs *storage.ObjectAttrs) provider.PutResult {
	if attrs == nil {
		return provider.PutResult{}
	}
	return provider.PutResult{
		ETag:    attrs.Etag,
		Version: generationString(attrs.Generation),
	}
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
