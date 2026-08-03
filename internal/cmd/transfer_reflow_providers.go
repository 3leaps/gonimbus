package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/producer"
	"github.com/3leaps/gonimbus/pkg/provider"
	providergcs "github.com/3leaps/gonimbus/pkg/provider/gcs"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/runbudget"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func newDestProvider(ctx context.Context, dest *reflowDestSpec, metaCfg reflowMetadataConfig, concurrency reflowpkg.ConcurrencyConfig) (provider.Provider, error) {
	if dest == nil {
		return nil, fmt.Errorf("destination is nil")
	}
	pool, err := provider.ResolveConnectionPool(concurrency.EffectiveCeiling)
	if err != nil {
		return nil, fmt.Errorf("resolve destination connection pool: %w", err)
	}
	return providerdispatch.NewDestination(ctx, providerdispatch.DestinationOptions{
		Command:             operationTransferReflow,
		Provider:            dest.Provider,
		S3Bucket:            dest.Bucket,
		S3Prefix:            dest.Prefix,
		GCSBucket:           dest.Bucket,
		GCSPrefix:           dest.Prefix,
		FileBaseDir:         dest.BaseDir,
		FileMetadataSidecar: metaCfg.MetadataSidecarSuffix,
		S3: providerdispatch.S3Options{
			Region:              dest.Region,
			Endpoint:            dest.Endpoint,
			Profile:             dest.Profile,
			ForcePathStyle:      dest.ForcePathStyle,
			MaxIdleConnsPerHost: pool.MaxIdleConnsPerHost,
			MaxConnsPerHost:     pool.MaxConnsPerHost,
		},
		GCS: providerdispatch.GCSOptions{
			Project:             strings.TrimSpace(dest.GCPProject),
			MaxIdleConnsPerHost: pool.MaxIdleConnsPerHost,
			MaxConnsPerHost:     pool.MaxConnsPerHost,
			// Keep destination writer memory explicit and bounded under the
			// source-side retry-buffer budget that drives the concurrency cap.
			WriterChunkSizeBytes: providergcs.MinWriterChunkSizeBytes,
		},
	})
}

func newSourceProvider(ctx context.Context, src *uri.ObjectURI, concurrency reflowpkg.ConcurrencyConfig) (provider.Provider, error) {
	binding, err := newSourceBinding(ctx, src, concurrency)
	if err != nil {
		return nil, err
	}
	return binding.Provider, nil
}

func newSourceBinding(ctx context.Context, src *uri.ObjectURI, concurrency reflowpkg.ConcurrencyConfig) (*providerdispatch.SourceBinding, error) {
	if src == nil {
		return nil, fmt.Errorf("source URI is nil")
	}
	pool, err := provider.ResolveConnectionPool(concurrency.EffectiveCeiling)
	if err != nil {
		return nil, fmt.Errorf("resolve source connection pool: %w", err)
	}
	return providerdispatch.NewSourceBinding(ctx, src, providerdispatch.SourceOptions{
		Command:             operationTransferReflow,
		FileMetadataSidecar: reflowMetaSuffix,
		FileSymlinkPolicy:   reflowSymlinks,
		S3: providerdispatch.S3Options{
			Region:              reflowSrcRegion,
			Endpoint:            reflowSrcEndpoint,
			Profile:             reflowSrcProfile,
			ForcePathStyle:      reflowSrcEndpoint != "",
			MaxIdleConnsPerHost: pool.MaxIdleConnsPerHost,
			MaxConnsPerHost:     pool.MaxConnsPerHost,
		},
		GCS: providerdispatch.GCSOptions{
			Project:             strings.TrimSpace(reflowSrcGCPProject),
			MaxIdleConnsPerHost: pool.MaxIdleConnsPerHost,
			MaxConnsPerHost:     pool.MaxConnsPerHost,
		},
	})
}

func newPrefixLaneEnumerator(src *uri.ObjectURI, selector string, binding *providerdispatch.SourceBinding, concurrency reflowpkg.ConcurrencyConfig) (producer.LaneEnumerator, *partition.Authority, error) {
	if src == nil {
		return nil, nil, fmt.Errorf("source URI is nil")
	}
	if binding == nil || binding.Provider == nil {
		return nil, nil, fmt.Errorf("source provider binding is nil")
	}
	// The partition contract refuses an empty prefix because it names a whole
	// scope rather than an explicit partition. Preserve the standing
	// unpartitioned whole-bucket path until a scope compiler supplies explicit
	// partitions for it.
	if src.Key == "" {
		return nil, nil, nil
	}

	ceiling := concurrency.EffectiveCeiling
	if ceiling < 1 {
		ceiling = 1
	}
	budget, err := runbudget.New(runbudget.Limits{
		InFlight: map[runbudget.OpClass]int{runbudget.OpList: ceiling},
	})
	if err != nil {
		return nil, nil, err
	}
	fingerprint := sha256.Sum256([]byte("transfer-reflow-prefix-enumeration-v1\x00" + selector))
	authority, err := partition.CompileAuthority(partition.PlanRequest{
		Prefixes:          []string{src.Key},
		Coverage:          partition.CoverageComplete,
		BaseIdentity:      reflowSourceIdentity(src),
		ConfigFingerprint: fmt.Sprintf("%x", fingerprint),
		MaxLanes:          1,
	})
	if err != nil {
		return nil, nil, err
	}
	executor, err := producer.NewLaneExecutor(producer.LaneExecutorConfig{
		Authority:     authority,
		Provider:      binding.Provider,
		Budget:        budget,
		Domains:       binding.QuotaDomains,
		QueueCapacity: ceiling * 2,
	})
	if err != nil {
		return nil, nil, err
	}
	return executor, authority, nil
}

func reflowSourceIdentity(src *uri.ObjectURI) string {
	if src == nil {
		return ""
	}
	switch src.Provider {
	case string(provider.ProviderFile):
		return "file:" + filepath.Clean(src.Key)
	case string(provider.ProviderS3):
		return "s3:" + src.Bucket
	default:
		return src.Provider + ":" + src.Bucket + ":" + src.Key
	}
}

func emitPreserveModeWarning(w io.Writer, srcProvider string, destProvider string) {
	if srcProvider == string(provider.ProviderFile) && destProvider == string(provider.ProviderFile) {
		return
	}
	switch {
	case srcProvider != string(provider.ProviderFile) && destProvider != string(provider.ProviderFile):
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect unless both source and destination are file:// (S3 has no Unix mode bits to read or preserve).")
	case srcProvider != string(provider.ProviderFile):
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect unless the source is file:// (S3 has no Unix mode bits to preserve).")
	case destProvider != string(provider.ProviderFile):
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect unless the destination is file:// (S3 has no Unix mode-bits concept).")
	default:
		_, _ = fmt.Fprintln(w, "warning: --preserve-mode has no effect for this provider combination.")
	}
}
