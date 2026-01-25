package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/content"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

var contentHeadCmd = &cobra.Command{
	Use:   "head <uri>",
	Short: "Read the first N bytes (JSONL)",
	Long: `Read the first N bytes of a single object.

Output is JSONL on stdout.
Errors are emitted on stdout as gonimbus.error.v1 records.
`,
	Args: cobra.ExactArgs(1),
	RunE: runContentHead,
}

var (
	contentHeadBytes int64
)

func init() {
	contentCmd.AddCommand(contentHeadCmd)
	contentHeadCmd.Flags().Int64Var(&contentHeadBytes, "bytes", 4096, "Number of bytes to read")
	contentHeadCmd.Flags().StringVarP(&streamGetRegion, "region", "r", "", "AWS region")
	contentHeadCmd.Flags().StringVarP(&streamGetProfile, "profile", "p", "", "AWS profile")
	contentHeadCmd.Flags().StringVar(&streamGetEndpoint, "endpoint", "", "Custom S3 endpoint")
}

func runContentHead(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	uri := args[0]

	parsed, err := ParseURI(uri)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid URI", err)
	}
	if parsed.Provider != string(provider.ProviderS3) {
		return exitError(foundry.ExitInvalidArgument, "Unsupported provider", fmt.Errorf("provider %q is not supported", parsed.Provider))
	}
	if parsed.IsPattern() || parsed.IsPrefix() {
		return exitError(foundry.ExitInvalidArgument, "content head requires an exact object key", fmt.Errorf("provide an exact object URI (no glob, no trailing '/'): %s", uri))
	}

	prov, err := s3.New(ctx, s3.Config{
		Bucket:         parsed.Bucket,
		Region:         streamGetRegion,
		Endpoint:       streamGetEndpoint,
		Profile:        streamGetProfile,
		ForcePathStyle: streamGetEndpoint != "",
	})
	if err != nil {
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	defer func() { _ = prov.Close() }()

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, parsed.Provider)
	defer func() { _ = w.Close() }()

	b, meta, err := content.HeadBytes(ctx, prov, parsed.Key, contentHeadBytes)
	if err != nil {
		_ = emitStreamError(context.Background(), w, parsed.Key, err)
		return exitError(foundry.ExitExternalServiceUnavailable, "content head failed", err)
	}

	payload := map[string]any{
		"uri":             parsed.String(),
		"key":             parsed.Key,
		"bytes_requested": contentHeadBytes,
		"bytes_returned":  int64(len(b)),
		"content_b64":     base64.StdEncoding.EncodeToString(b),
		"etag":            meta.ETag,
		"size":            meta.Size,
		"last_modified":   meta.LastModified,
		"content_type":    meta.ContentType,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		_ = emitStreamError(context.Background(), w, parsed.Key, err)
		return exitError(foundry.ExitParseError, "Failed to marshal output", err)
	}

	rec := output.Record{Type: "gonimbus.content.head.v1", TS: time.Now().UTC(), JobID: jobID, Provider: parsed.Provider, Data: data}
	recBytes, err := json.Marshal(rec)
	if err != nil {
		_ = emitStreamError(context.Background(), w, parsed.Key, err)
		return exitError(foundry.ExitParseError, "Failed to marshal record", err)
	}
	recBytes = append(recBytes, '\n')
	if _, err := cmd.OutOrStdout().Write(recBytes); err != nil {
		observability.CLILogger.Error("Failed to write record", zap.Error(err))
		return exitError(foundry.ExitFileWriteError, "Failed to write output", err)
	}
	return nil
}
