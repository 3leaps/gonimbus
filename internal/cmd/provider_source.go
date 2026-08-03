package cmd

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type commandSourceTarget struct {
	ProviderURI *uri.ObjectURI
	QueryURI    *uri.ObjectURI
	ProviderID  string
}

func commandSourceTargetForRead(in *uri.ObjectURI) commandSourceTarget {
	if in == nil || in.Provider != string(provider.ProviderFile) {
		return commandSourceTarget{ProviderURI: in, QueryURI: cloneObjectURI(in), ProviderID: commandSourceProviderID(in)}
	}

	clean := filepath.Clean(in.Key)
	if in.IsPrefix() {
		providerURI := &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: "local", Key: clean}
		queryURI := cloneObjectURI(in)
		queryURI.Key = ""
		return commandSourceTarget{ProviderURI: providerURI, QueryURI: queryURI, ProviderID: commandSourceProviderID(providerURI)}
	}

	providerURI := &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: "local", Key: filepath.Dir(clean)}
	queryURI := cloneObjectURI(in)
	queryURI.Key = filepath.Base(clean)
	return commandSourceTarget{ProviderURI: providerURI, QueryURI: queryURI, ProviderID: commandSourceProviderID(providerURI)}
}

func newCommandSourceProviderWithGCSProject(ctx context.Context, src *uri.ObjectURI, command, region, profile, endpoint, gcpProject string) (provider.Provider, error) {
	// sdk-default construction: no admitted-N pool exact-assign (doctor/inspect/stream).
	return providerdispatch.NewSource(ctx, src, commandSourceOptions(command, region, profile, endpoint, gcpProject))
}

// newCommandSourceProviderWithPool exact-assigns connection pool knobs from
// ResolveConnectionPool(admittedN). Callers must resolve admitted N first.
func newCommandSourceProviderWithPool(ctx context.Context, src *uri.ObjectURI, command, region, profile, endpoint, gcpProject string, admittedN int) (provider.Provider, error) {
	opts := commandSourceOptions(command, region, profile, endpoint, gcpProject)
	if err := applySourceConnectionPool(&opts, admittedN); err != nil {
		return nil, err
	}
	return providerdispatch.NewSource(ctx, src, opts)
}

func commandSourceOptions(command, region, profile, endpoint, gcpProject string) providerdispatch.SourceOptions {
	return providerdispatch.SourceOptions{
		Command: command,
		S3: providerdispatch.S3Options{
			Region:         region,
			Endpoint:       endpoint,
			Profile:        profile,
			ForcePathStyle: endpoint != "",
		},
		GCS: providerdispatch.GCSOptions{
			Project: strings.TrimSpace(gcpProject),
		},
	}
}

func commandSourceProviderID(in *uri.ObjectURI) string {
	if in == nil {
		return ""
	}
	switch in.Provider {
	case string(provider.ProviderFile):
		return "file:" + filepath.Clean(in.Key)
	case string(provider.ProviderS3):
		return "s3:" + in.Bucket
	case string(provider.ProviderGCS):
		return "gcs:" + in.Bucket
	default:
		return in.Provider + ":" + in.Bucket + ":" + in.Key
	}
}

func cloneObjectURI(in *uri.ObjectURI) *uri.ObjectURI {
	if in == nil {
		return nil
	}
	out := *in
	if out.Provider == string(provider.ProviderFile) {
		out.Key = filepath.ToSlash(out.Key)
	} else {
		out.Key = strings.TrimPrefix(out.Key, "/")
	}
	return &out
}

func commandOutputProviderForInputs(inputs []string, fallback string) string {
	for _, raw := range inputs {
		parsed, err := uri.ParseURI(strings.TrimSpace(raw))
		if err == nil && parsed.Provider != "" {
			return parsed.Provider
		}
	}
	return fallback
}
