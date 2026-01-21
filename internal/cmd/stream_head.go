package cmd

import (
	"context"
	"fmt"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
)

var streamHeadCmd = &cobra.Command{
	Use:   "head <uri>",
	Short: "Fetch object metadata (JSONL only)",
	Long: `Fetch object metadata using a HEAD request.

Streaming mode semantics:
- Output is JSONL records on stdout.
- Operational errors are emitted to stdout as gonimbus.error.v1 records.

This is a "probe" command (like stream get) intended for dogfooding and helper validation.
`,
	Args: cobra.ExactArgs(1),
	RunE: runStreamHead,
}

func init() {
	streamCmd.AddCommand(streamHeadCmd)

	streamHeadCmd.Flags().StringVarP(&streamGetRegion, "region", "r", "", "AWS region")
	streamHeadCmd.Flags().StringVarP(&streamGetProfile, "profile", "p", "", "AWS profile")
	streamHeadCmd.Flags().StringVar(&streamGetEndpoint, "endpoint", "", "Custom S3 endpoint")
}

func runStreamHead(cmd *cobra.Command, args []string) error {
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
		return exitError(foundry.ExitInvalidArgument, "stream head requires an exact object key", fmt.Errorf("provide an exact object URI (no glob, no trailing '/'): %s", uri))
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

	meta, err := prov.Head(ctx, parsed.Key)
	if err != nil {
		_ = emitStreamError(context.Background(), w, parsed.Key, err)
		return exitError(foundry.ExitExternalServiceUnavailable, "Head failed", err)
	}

	payload := &output.ObjectRecord{
		Key:          meta.Key,
		Size:         meta.Size,
		LastModified: meta.LastModified,
		ETag:         meta.ETag,
		ContentType:  meta.ContentType,
		Metadata:     meta.Metadata,
	}

	// Streaming mode: emit JSONL on stdout.
	// We reuse the standard object record envelope.
	return w.WriteObject(ctx, payload)
}
