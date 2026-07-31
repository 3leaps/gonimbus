// Package providerdispatch centralizes CLI provider construction from parsed
// provider schemes.
package providerdispatch

import (
	"context"
	"fmt"
	"os"

	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/gcs"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/runbudget"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const quotaDomainVersion = 1

// S3Options contains command-supplied S3 construction options.
type S3Options struct {
	Region              string
	Profile             string
	Endpoint            string
	ForcePathStyle      bool
	MaxIdleConnsPerHost int
	MaxConnsPerHost     int
}

// GCSOptions contains command-supplied GCS construction options.
type GCSOptions struct {
	Project              string
	Anonymous            bool
	MaxIdleConnsPerHost  int
	MaxConnsPerHost      int
	WriterChunkSizeBytes int
}

// SourceOptions contains source-provider construction policy.
type SourceOptions struct {
	Command               string
	S3                    S3Options
	GCS                   GCSOptions
	FileMetadataSidecar   string
	FileBaseDir           string
	FileSymlinkPolicy     string
	FileRootlessReadMode  string
	FileSelectionMode     string
	FileSelectionManifest string
}

// SourceBinding pairs a provider handle with the quota domains its requests
// consume. Keeping the values side-by-side preserves the provider's concrete
// dynamic type, including optional capabilities; wrapping the provider behind a
// new type would hide those capabilities from type assertions.
//
// QuotaDomains are conservative service-wide domains. They intentionally do not
// include bucket, endpoint, profile, project, or credential-derived values:
// provider endpoint resolution may be ambient, and splitting an unresolved
// service into narrower identities could let one run multiply load against the
// same real quota. Conservative sharing can reduce utilization but cannot
// exceed the configured run ceiling.
type SourceBinding struct {
	Provider     provider.Provider
	QuotaDomains []runbudget.Domain
}

// DestinationOptions contains destination-provider construction policy.
type DestinationOptions struct {
	Command  string
	Provider string

	S3Bucket string
	// S3Prefix is carried for command-owned destination policy. Provider
	// construction uses the bucket; key prefixing remains with the caller.
	S3Prefix    string
	S3          S3Options
	GCSBucket   string
	GCSPrefix   string
	GCS         GCSOptions
	FileBaseDir string

	FileMetadataSidecar string
}

// Factories holds provider constructors. It is primarily used by command tests
// to preserve their existing no-network provider stubs.
type Factories struct {
	S3   func(context.Context, s3.Config) (provider.Provider, error)
	GCS  func(context.Context, gcs.Config) (provider.Provider, error)
	File func(providerfile.Config) (provider.Provider, error)
}

var factories = Factories{
	S3: func(ctx context.Context, cfg s3.Config) (provider.Provider, error) {
		return s3.New(ctx, cfg)
	},
	GCS: func(ctx context.Context, cfg gcs.Config) (provider.Provider, error) {
		return gcs.New(ctx, cfg)
	},
	File: func(cfg providerfile.Config) (provider.Provider, error) {
		return providerfile.New(cfg)
	},
}

// UseFactoriesForTest installs temporary constructors and returns a restore
// function. Tests should call the returned function from t.Cleanup.
func UseFactoriesForTest(next Factories) func() {
	old := factories
	if next.S3 != nil {
		factories.S3 = next.S3
	}
	if next.GCS != nil {
		factories.GCS = next.GCS
	}
	if next.File != nil {
		factories.File = next.File
	}
	return func() {
		factories = old
	}
}

// NewSource constructs a provider for a parsed source URI.
func NewSource(ctx context.Context, src *uri.ObjectURI, opts SourceOptions) (provider.Provider, error) {
	binding, err := NewSourceBinding(ctx, src, opts)
	if err != nil {
		return nil, err
	}
	return binding.Provider, nil
}

// NewSourceBinding constructs a source provider and its provider-supplied quota
// identity for consumers that enforce a shared run budget.
func NewSourceBinding(ctx context.Context, src *uri.ObjectURI, opts SourceOptions) (*SourceBinding, error) {
	if src == nil {
		return nil, fmt.Errorf("%s source URI is nil", commandName(opts.Command))
	}
	var (
		p   provider.Provider
		err error
	)
	switch src.Provider {
	case string(provider.ProviderS3):
		p, err = factories.S3(ctx, s3.Config{
			Bucket:              src.Bucket,
			Region:              opts.S3.Region,
			Endpoint:            opts.S3.Endpoint,
			Profile:             opts.S3.Profile,
			ForcePathStyle:      opts.S3.ForcePathStyle,
			MaxIdleConnsPerHost: opts.S3.MaxIdleConnsPerHost,
			MaxConnsPerHost:     opts.S3.MaxConnsPerHost,
		})
	case string(provider.ProviderGCS):
		p, err = factories.GCS(ctx, gcs.Config{
			Bucket:               src.Bucket,
			Project:              opts.GCS.Project,
			Anonymous:            opts.GCS.Anonymous,
			MaxIdleConnsPerHost:  opts.GCS.MaxIdleConnsPerHost,
			MaxConnsPerHost:      opts.GCS.MaxConnsPerHost,
			WriterChunkSizeBytes: opts.GCS.WriterChunkSizeBytes,
		})
	case string(provider.ProviderFile):
		baseDir := opts.FileBaseDir
		if baseDir == "" {
			baseDir = src.Key
		}
		p, err = factories.File(providerfile.Config{
			BaseDir:               baseDir,
			MetadataSidecarSuffix: opts.FileMetadataSidecar,
			SymlinkPolicy:         opts.FileSymlinkPolicy,
		})
	default:
		return nil, unsupportedProviderError(opts.Command, "source", src.Provider)
	}
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("%s source provider %q constructor returned no provider", commandName(opts.Command), src.Provider)
	}
	domain, ok := sourceQuotaDomain(src.Provider)
	if !ok {
		// The provider switch and domain catalog must advance together. Refuse if
		// they drift rather than constructing a provider whose requests bypass
		// shared admission.
		_ = p.Close()
		return nil, fmt.Errorf("%s source provider %q has no quota-domain adapter", commandName(opts.Command), src.Provider)
	}
	return &SourceBinding{
		Provider:     p,
		QuotaDomains: []runbudget.Domain{domain},
	}, nil
}

func sourceQuotaDomain(providerName string) (runbudget.Domain, bool) {
	switch providerName {
	case string(provider.ProviderS3):
		return runbudget.Domain{Version: quotaDomainVersion, ID: "s3-service"}, true
	case string(provider.ProviderGCS):
		return runbudget.Domain{Version: quotaDomainVersion, ID: "gcs-service"}, true
	case string(provider.ProviderFile):
		return runbudget.Domain{Version: quotaDomainVersion, ID: "file-service"}, true
	default:
		return runbudget.Domain{}, false
	}
}

// NewDestination constructs a provider for destination options.
func NewDestination(ctx context.Context, opts DestinationOptions) (provider.Provider, error) {
	switch opts.Provider {
	case string(provider.ProviderS3):
		return factories.S3(ctx, s3.Config{
			Bucket:              opts.S3Bucket,
			Region:              opts.S3.Region,
			Endpoint:            opts.S3.Endpoint,
			Profile:             opts.S3.Profile,
			ForcePathStyle:      opts.S3.ForcePathStyle,
			MaxIdleConnsPerHost: opts.S3.MaxIdleConnsPerHost,
			MaxConnsPerHost:     opts.S3.MaxConnsPerHost,
		})
	case string(provider.ProviderGCS):
		bucket := opts.GCSBucket
		if bucket == "" {
			bucket = opts.S3Bucket
		}
		return factories.GCS(ctx, gcs.Config{
			Bucket:               bucket,
			Project:              opts.GCS.Project,
			Anonymous:            opts.GCS.Anonymous,
			MaxIdleConnsPerHost:  opts.GCS.MaxIdleConnsPerHost,
			MaxConnsPerHost:      opts.GCS.MaxConnsPerHost,
			WriterChunkSizeBytes: opts.GCS.WriterChunkSizeBytes,
		})
	case string(provider.ProviderFile):
		if opts.FileBaseDir == "" {
			return nil, fmt.Errorf("%s destination file base dir is required", commandName(opts.Command))
		}
		if err := os.MkdirAll(opts.FileBaseDir, 0o750); err != nil {
			return nil, err
		}
		return factories.File(providerfile.Config{
			BaseDir:               opts.FileBaseDir,
			MetadataSidecarSuffix: opts.FileMetadataSidecar,
		})
	default:
		return nil, unsupportedProviderError(opts.Command, "destination", opts.Provider)
	}
}

// CapabilityError reports a missing optional provider capability.
type CapabilityError struct {
	Command    string
	Provider   string
	Capability string
}

func (e *CapabilityError) Error() string {
	return fmt.Sprintf("%s provider %q does not support %s", commandName(e.Command), e.Provider, e.Capability)
}

// RequireCapability returns a typed optional capability or a fail-closed error.
func RequireCapability[T any](p provider.Provider, command string, providerName string, capability string) (T, error) {
	var zero T
	capabilityValue, ok := any(p).(T)
	if ok {
		return capabilityValue, nil
	}
	if providerName == "" {
		providerName = "unknown"
	}
	return zero, &CapabilityError{Command: command, Provider: providerName, Capability: capability}
}

func unsupportedProviderError(command string, role string, providerName string) error {
	return fmt.Errorf("%s unsupported %s provider %q", commandName(command), role, providerName)
}

func commandName(command string) string {
	if command == "" {
		return "provider dispatch"
	}
	return command
}
