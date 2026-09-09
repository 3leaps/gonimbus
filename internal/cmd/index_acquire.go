package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var indexAcquireCmd = &cobra.Command{
	Use:   "acquire",
	Short: "Acquire an exact durable hub run as an immutable query bundle",
	Long: `Acquire an exact durable-v2 hub run through a configured read-only
hub handle. The command never selects latest and never installs into the
canonical index/cache namespace. It stages and verifies every current-run
artifact, writes acquired.json last, and atomically publishes the final bundle.

The selected hub_read_handle is a logical name under hub_read_handles in the
Gonimbus configuration. The handle owns the hub URI and read credentials.

Examples:
  gonimbus index acquire --hub-read-handle archive-read \
    --index-set idx_<full-sha256> --run-id run_1783087200000000000 \
    --dest /var/lib/example/acquired/run-1783087200000000000

  gonimbus index acquire --hub-read-handle archive-read \
    --index-set idx_<full-sha256> --run-id run_1783173600000000000 \
    --proof-through-run run_1783087200000000000 \
    --dest /var/lib/example/acquired/delta-proof`,
	RunE: runIndexAcquire,
}

func init() {
	indexCmd.AddCommand(indexAcquireCmd)
	indexAcquireCmd.Flags().String("hub-read-handle", "", "Configured hub_read_handle logical name (required)")
	indexAcquireCmd.Flags().String("index-set", "", "Full lowercase index set ID (required)")
	indexAcquireCmd.Flags().String("run-id", "", "Exact durable run ID (required)")
	indexAcquireCmd.Flags().String("proof-through-run", "", "Acquire verified lineage through this exact baseline run")
	indexAcquireCmd.Flags().String("dest", "", "Final acquired-bundle directory (required; parent must exist)")
	_ = indexAcquireCmd.MarkFlagRequired("hub-read-handle")
	_ = indexAcquireCmd.MarkFlagRequired("index-set")
	_ = indexAcquireCmd.MarkFlagRequired("run-id")
	_ = indexAcquireCmd.MarkFlagRequired("dest")
}

type hubReadHandleResolution struct {
	reader indexreader.HubExactObjectReader
	closer io.Closer
}

var configuredHubReadHandleResolver = resolveConfiguredHubReadHandle

func runIndexAcquire(cmd *cobra.Command, _ []string) error {
	handle, _ := cmd.Flags().GetString("hub-read-handle")
	indexSetID, _ := cmd.Flags().GetString("index-set")
	runID, _ := cmd.Flags().GetString("run-id")
	proofThrough, _ := cmd.Flags().GetString("proof-through-run")
	dest, _ := cmd.Flags().GetString("dest")

	resolved, err := configuredHubReadHandleResolver(cmd.Context(), strings.TrimSpace(handle))
	if err != nil {
		return err
	}
	if resolved.closer != nil {
		defer func() { _ = resolved.closer.Close() }()
	}
	marker, err := indexreader.AcquireBundle(cmd.Context(), resolved.reader, indexreader.AcquireBundleOptions{
		IndexSetID: strings.TrimSpace(indexSetID), RunID: strings.TrimSpace(runID),
		ProofThroughRunID: strings.TrimSpace(proofThrough), Destination: strings.TrimSpace(dest),
		Warning: func(message string) {
			_, _ = fmt.Fprintf(os.Stderr, "warning: %s\n", message)
		},
	})
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stderr, "Acquired durable bundle: index_set=%s run=%s\n", marker.IndexSetID, marker.RunID)
	return nil
}

var hubReadHandleNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func resolveConfiguredHubReadHandle(ctx context.Context, name string) (hubReadHandleResolution, error) {
	return resolveConfiguredHubReadHandleFrom(ctx, viper.GetViper(), name)
}

func resolveConfiguredHubReadHandleFrom(ctx context.Context, config *viper.Viper, name string) (hubReadHandleResolution, error) {
	if !hubReadHandleNameRE.MatchString(name) {
		return hubReadHandleResolution{}, fmt.Errorf("hub_read_handle name is invalid")
	}
	if config == nil {
		return hubReadHandleResolution{}, fmt.Errorf("hub_read_handle configuration is invalid")
	}
	cfg := config.Sub("hub_read_handles." + name)
	if cfg == nil {
		return hubReadHandleResolution{}, fmt.Errorf("hub_read_handle is not configured")
	}
	hubURI := strings.TrimSpace(cfg.GetString("uri"))
	hub, err := parseHubURI(hubURI)
	if err != nil {
		return hubReadHandleResolution{}, fmt.Errorf("hub_read_handle configuration is invalid")
	}
	hub.Profile = strings.TrimSpace(cfg.GetString("profile"))
	hub.Region = strings.TrimSpace(cfg.GetString("region"))
	hub.Endpoint = strings.TrimSpace(cfg.GetString("endpoint"))
	hub.GCPProject = strings.TrimSpace(cfg.GetString("gcp_project"))
	if hub.Endpoint != "" {
		hub.ForcePathStyle = true
	}
	src := &uri.ObjectURI{Provider: hub.Provider, Bucket: hub.Bucket, Key: hub.Prefix}
	if hub.Provider == string(provider.ProviderFile) {
		src.Bucket = "local"
		src.Key = filepath.ToSlash(hub.BaseDir)
	}
	p, err := providerdispatch.NewSource(ctx, src, providerdispatch.SourceOptions{
		Command:     "index acquire",
		FileBaseDir: hub.BaseDir,
		S3: providerdispatch.S3Options{
			Region: hub.Region, Profile: hub.Profile, Endpoint: hub.Endpoint,
			ForcePathStyle: hub.ForcePathStyle,
		},
		GCS: providerdispatch.GCSOptions{Project: hub.GCPProject},
	})
	if err != nil {
		return hubReadHandleResolution{}, fmt.Errorf("open hub_read_handle: provider unavailable")
	}
	getter, err := providerdispatch.RequireCapability[provider.ObjectGetter](p, "index acquire", hub.Provider, "ObjectGetter")
	if err != nil {
		_ = p.Close()
		return hubReadHandleResolution{}, fmt.Errorf("open hub_read_handle: exact object reads unavailable")
	}
	return hubReadHandleResolution{
		reader: &hubExactProviderAdapter{getter: getter, prefix: hub.Prefix},
		closer: p,
	}, nil
}

type hubExactProviderAdapter struct {
	getter provider.ObjectGetter
	prefix string
}

func (a *hubExactProviderAdapter) OpenExact(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if a == nil || a.getter == nil {
		return nil, 0, indexreader.ErrHubExactRead
	}
	if strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || filepath.Clean(filepath.FromSlash(key)) != filepath.FromSlash(key) {
		return nil, 0, indexreader.ErrHubExactRead
	}
	return a.getter.GetObject(ctx, a.prefix+key)
}
