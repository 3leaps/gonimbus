package cmd

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	providergcs "github.com/3leaps/gonimbus/pkg/provider/gcs"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func newDestProvider(ctx context.Context, dest *reflowDestSpec, metaCfg reflowMetadataConfig, concurrency reflowpkg.ConcurrencyConfig) (provider.Provider, error) {
	if dest == nil {
		return nil, fmt.Errorf("destination is nil")
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
			MaxIdleConnsPerHost: concurrency.EffectiveCeiling,
			MaxConnsPerHost:     concurrency.EffectiveCeiling,
		},
		GCS: providerdispatch.GCSOptions{
			Project:             strings.TrimSpace(dest.GCPProject),
			MaxIdleConnsPerHost: concurrency.EffectiveCeiling,
			MaxConnsPerHost:     concurrency.EffectiveCeiling,
			// Keep destination writer memory explicit and bounded under the
			// source-side retry-buffer budget that drives the concurrency cap.
			WriterChunkSizeBytes: providergcs.MinWriterChunkSizeBytes,
		},
	})
}

func newSourceProvider(ctx context.Context, src *uri.ObjectURI, concurrency reflowpkg.ConcurrencyConfig) (provider.Provider, error) {
	if src == nil {
		return nil, fmt.Errorf("source URI is nil")
	}
	return providerdispatch.NewSource(ctx, src, providerdispatch.SourceOptions{
		Command:             operationTransferReflow,
		FileMetadataSidecar: reflowMetaSuffix,
		FileSymlinkPolicy:   reflowSymlinks,
		S3: providerdispatch.S3Options{
			Region:              reflowSrcRegion,
			Endpoint:            reflowSrcEndpoint,
			Profile:             reflowSrcProfile,
			ForcePathStyle:      reflowSrcEndpoint != "",
			MaxIdleConnsPerHost: concurrency.EffectiveCeiling,
			MaxConnsPerHost:     concurrency.EffectiveCeiling,
		},
		GCS: providerdispatch.GCSOptions{
			Project:             strings.TrimSpace(reflowSrcGCPProject),
			MaxIdleConnsPerHost: concurrency.EffectiveCeiling,
			MaxConnsPerHost:     concurrency.EffectiveCeiling,
		},
	})
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
