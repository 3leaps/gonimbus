package cmd

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/stretchr/testify/require"
)

type receiptProvenanceDest struct {
	*reflowMemoryProvider
	writeETag   string
	resultCalls int
}

func (p *receiptProvenanceDest) PutObjectResult(ctx context.Context, key string, body io.Reader, size int64) (provider.PutResult, error) {
	return p.PutObjectResultWithOptions(ctx, key, body, size, provider.PutOptions{})
}

func (p *receiptProvenanceDest) PutObjectResultWithOptions(ctx context.Context, key string, body io.Reader, size int64, opts provider.PutOptions) (provider.PutResult, error) {
	if err := p.PutObjectWithOptions(ctx, key, body, size, opts); err != nil {
		return provider.PutResult{}, err
	}
	p.resultCalls++
	return provider.PutResult{ETag: p.writeETag, Version: "write-version"}, nil
}

func TestReceiptProvenanceUnconditionalSidecarParity(t *testing.T) {
	for _, mode := range []string{"overwrite", "head-fallback"} {
		for _, etag := range []string{"write-etag", "https://example.test/path?token=synthetic-secret"} {
			t.Run(mode+"/"+etag, func(t *testing.T) {
				var sidecars []reflowpkg.ProvenanceSidecarPayload
				for _, pool := range []bool{false, true} {
					env := newFlagProbeEnv(t)
					dst := &receiptProvenanceDest{reflowMemoryProvider: env.dst, writeETag: etag}
					if mode == "head-fallback" {
						dst.ignoreIfAbsent = true
					}
					useTransferReflowProviderFactories(t, providerdispatch.Factories{S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
						if cfg.Bucket == "source-bucket" {
							return env.src, nil
						}
						return dst, nil
					}})
					args := []string{"--provenance", "sidecar", "--run-id", parityRunID}
					if mode == "overwrite" {
						args = append(args, "--on-collision", "overwrite", "--overwrite")
					}
					var stdout string
					var err error
					if pool {
						stdout, err = env.runPool(t, args...)
					} else {
						stdout, err = env.run(t, args...)
					}
					require.NoError(t, err)
					if pool {
						require.Equal(t, reflowpkg.ExecutionPathCLIPool, executionPathOf(t, stdout))
					} else {
						require.Equal(t, reflowpkg.ExecutionPathEngine, executionPathOf(t, stdout))
					}
					require.Equal(t, 1, dst.resultCalls, "data write must use the result-bearing capability")
					if mode == "head-fallback" {
						require.False(t, *requireReflowSummaryData(t, stdout).DestIfAbsentHonored)
					}
					var terminal reflowpkg.Record
					require.NoError(t, json.Unmarshal(requireRecord(t, stdout, reflowpkg.RecordType, "complete").Data, &terminal))
					require.NotEmpty(t, terminal.DestSHA256)
					if etag == "write-etag" {
						require.Equal(t, etag, terminal.DestETag)
					} else {
						require.Empty(t, terminal.DestETag)
						require.NotContains(t, stdout, "synthetic-secret")
					}
					raw := env.dst.mustObject("data/source/file.xml" + provenanceSuffix)
					require.NotContains(t, string(raw), "synthetic-secret")
					payload := decodeSidecarPayload(t, raw)
					require.Empty(t, payload.Destination.ETag, "legacy unconditional sidecar must not acquire a write-response ETag")
					normalizeSidecarTS(t, &payload)
					sidecars = append(sidecars, payload)
				}
				require.Equal(t, sidecars[0], sidecars[1], "engine/pool sidecar publication must remain equivalent")
			})
		}
	}
}
