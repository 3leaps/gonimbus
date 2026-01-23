package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/stream"
)

var streamGetCmd = &cobra.Command{
	Use:   "get <uri>",
	Short: "Stream a single object (JSONL headers + raw bytes)",
	Long: `Stream a single object as a mixed-framing stream.

Behavior:
- Performs a HEAD first to capture metadata (etag/last_modified/size) for the open record.
- Streams content in fixed-size chunks.
- Emits errors to stdout as gonimbus.error.v1 records (streaming mode contract).
`,
	Args: cobra.ExactArgs(1),
	RunE: runStreamGet,
}

var (
	streamGetRegion   string
	streamGetProfile  string
	streamGetEndpoint string
	streamGetChunk    int
)

func init() {
	streamCmd.AddCommand(streamGetCmd)

	streamGetCmd.Flags().StringVarP(&streamGetRegion, "region", "r", "", "AWS region")
	streamGetCmd.Flags().StringVarP(&streamGetProfile, "profile", "p", "", "AWS profile")
	streamGetCmd.Flags().StringVar(&streamGetEndpoint, "endpoint", "", "Custom S3 endpoint")
	streamGetCmd.Flags().IntVar(&streamGetChunk, "chunk-bytes", 64*1024, "Chunk size in bytes")
}

func runStreamGet(cmd *cobra.Command, args []string) error {
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
		return exitError(foundry.ExitInvalidArgument, "stream get requires an exact object key", fmt.Errorf("provide an exact object URI (no glob, no trailing '/'): %s", uri))
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
	recordWriter := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, parsed.Provider)
	defer func() { _ = recordWriter.Close() }()

	// HEAD first: probing command, so extra roundtrip is acceptable.
	meta, err := prov.Head(ctx, parsed.Key)
	if err != nil {
		_ = emitStreamError(ctx, recordWriter, parsed.Key, err)
		return exitError(foundry.ExitExternalServiceUnavailable, "Head failed", err)
	}

	streamID := uuid.New().String()
	size := meta.Size
	open := &stream.Open{
		StreamID:     streamID,
		URI:          parsed.String(),
		ETag:         meta.ETag,
		Size:         &size,
		LastModified: &meta.LastModified,
		ContentType:  meta.ContentType,
		Encoding:     "",
		Range:        nil,
	}

	streamWriter := stream.NewWriter(cmd.OutOrStdout(), jobID, parsed.Provider)
	defer func() { _ = streamWriter.Close() }()

	if err := streamWriter.WriteOpen(ctx, open); err != nil {
		_ = emitStreamError(ctx, recordWriter, parsed.Key, err)
		return exitError(foundry.ExitFileWriteError, "Failed to write stream open", err)
	}

	body, gotSize, err := prov.GetObject(ctx, parsed.Key)
	if err != nil {
		_ = emitStreamError(ctx, recordWriter, parsed.Key, err)
		_ = streamWriter.WriteClose(ctx, &stream.Close{StreamID: streamID, Status: "error", Chunks: 0, Bytes: 0})
		return exitError(foundry.ExitExternalServiceUnavailable, "GetObject failed", err)
	}
	defer func() { _ = body.Close() }()

	// validate=size for probe: ensure GetObject size matches Head size.
	if meta.Size > 0 && gotSize >= 0 && meta.Size != gotSize {
		err := &streamSizeMismatchError{Key: parsed.Key, Expected: meta.Size, Got: gotSize}
		_ = emitStreamError(ctx, recordWriter, parsed.Key, err)
		_ = streamWriter.WriteClose(ctx, &stream.Close{StreamID: streamID, Status: "error", Chunks: 0, Bytes: 0})
		return exitError(foundry.ExitExternalServiceUnavailable, "Size mismatch", err)
	}

	chunkBytes := streamGetChunk
	if chunkBytes <= 0 {
		chunkBytes = 64 * 1024
	}
	buf := make([]byte, chunkBytes)

	var seq int64
	var total int64
	for {
		n, rerr := body.Read(buf)
		if n > 0 {
			hdr := &stream.Chunk{StreamID: streamID, Seq: seq, NBytes: int64(n)}
			if err := streamWriter.WriteChunk(ctx, hdr, bytes.NewReader(buf[:n])); err != nil {
				_ = emitStreamError(ctx, recordWriter, parsed.Key, err)
				_ = streamWriter.WriteClose(ctx, &stream.Close{StreamID: streamID, Status: "error", Chunks: seq, Bytes: total})
				return exitError(foundry.ExitFileWriteError, "Failed to write stream chunk", err)
			}
			seq++
			total += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			_ = emitStreamError(ctx, recordWriter, parsed.Key, rerr)
			_ = streamWriter.WriteClose(ctx, &stream.Close{StreamID: streamID, Status: "error", Chunks: seq, Bytes: total})
			return exitError(foundry.ExitExternalServiceUnavailable, "Read failed", rerr)
		}
	}

	if err := streamWriter.WriteClose(ctx, &stream.Close{StreamID: streamID, Status: "success", Chunks: seq, Bytes: total}); err != nil {
		_ = emitStreamError(ctx, recordWriter, parsed.Key, err)
		return exitError(foundry.ExitFileWriteError, "Failed to write stream close", err)
	}

	return nil
}

type streamSizeMismatchError struct {
	Key      string
	Expected int64
	Got      int64
}

func (e *streamSizeMismatchError) Error() string {
	return fmt.Sprintf("source size mismatch for %s: expected=%d got=%d", e.Key, e.Expected, e.Got)
}

func emitStreamError(ctx context.Context, w output.Writer, key string, err error) error {
	code := output.ErrCodeInternal
	if provider.IsNotFound(err) {
		code = output.ErrCodeNotFound
	} else if provider.IsAccessDenied(err) {
		code = output.ErrCodeAccessDenied
	} else if provider.IsThrottled(err) {
		code = output.ErrCodeThrottled
	} else if provider.IsProviderUnavailable(err) {
		code = output.ErrCodeProviderUnavailable
	} else if _, ok := err.(*streamSizeMismatchError); ok {
		code = output.ErrCodeNotFound
	}

	details := map[string]any{
		"mode": "streaming",
	}
	if err := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: err.Error(), Key: key, Details: details}); err != nil {
		observability.CLILogger.Debug("Failed to emit streaming error record", zap.Error(err))
	}
	return nil
}
