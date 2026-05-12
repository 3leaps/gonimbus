package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	streamPutRecordType = "gonimbus.stream.put.v1"
	streamPutFramingRaw = "raw"
)

var streamPutCmd = newStreamPutCommand()

var (
	streamPutRegion    string
	streamPutProfile   string
	streamPutEndpoint  string
	streamPutFraming   string
	streamPutOverwrite bool
)

func init() {
	streamCmd.AddCommand(streamPutCmd)
}

func newStreamPutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put <dest-uri>",
		Short: "Write raw stdin to one destination object",
		Long: `Write raw stdin to a single destination object.

Streaming mode semantics:
- stdin carries the object byte payload.
- stdout emits JSONL records only.
- Existing destination objects are refused unless --overwrite is set.

This first slice supports raw stdin only. JSONL stream framing is reserved for a
follow-on implementation.`,
		Args:         cobra.ExactArgs(1),
		RunE:         runStreamPut,
		SilenceUsage: true,
	}

	cmd.Flags().StringVarP(&streamPutRegion, "region", "r", "", "AWS region")
	cmd.Flags().StringVarP(&streamPutProfile, "profile", "p", "", "AWS profile")
	cmd.Flags().StringVar(&streamPutEndpoint, "endpoint", "", "Custom S3 endpoint")
	cmd.Flags().StringVar(&streamPutFraming, "framing", streamPutFramingRaw, "Input framing (raw)")
	cmd.Flags().BoolVar(&streamPutOverwrite, "overwrite", false, "Allow overwriting an existing destination object")

	return cmd
}

type streamPutRecord struct {
	DestURI string `json:"dest_uri"`
	DestKey string `json:"dest_key"`
	Bytes   int64  `json:"bytes"`
	Status  string `json:"status"`
}

type streamPutHeadPutter interface {
	provider.ObjectPutter
	Head(ctx context.Context, key string) (*provider.ObjectMeta, error)
}

func runStreamPut(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	framing := strings.ToLower(strings.TrimSpace(streamPutFraming))
	if framing == "" {
		framing = streamPutFramingRaw
	}
	if framing != streamPutFramingRaw {
		return exitError(foundry.ExitInvalidArgument, "Invalid --framing value", fmt.Errorf("stream put currently supports raw framing only"))
	}

	dest, err := parseOutputDest(args[0])
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid destination URI", err)
	}
	dest.Region = streamPutRegion
	dest.Profile = streamPutProfile
	dest.Endpoint = streamPutEndpoint
	dest.ForcePathStyle = streamPutEndpoint != ""

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, dest.Provider)
	defer func() { _ = w.Close() }()

	putter, err := newOutputProvider(ctx, dest)
	if err != nil {
		_ = emitStreamPutError(ctx, w, dest.Key, output.ErrCodeProviderUnavailable, "Failed to connect to storage provider", err, nil)
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	if closer, ok := putter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	headPutter, ok := putter.(streamPutHeadPutter)
	if !ok {
		err := fmt.Errorf("destination provider %q does not support Head", dest.Provider)
		_ = emitStreamPutError(ctx, w, dest.Key, output.ErrCodeProviderUnavailable, "Destination provider cannot check existing object", err, nil)
		return exitError(foundry.ExitExternalServiceUnavailable, "Destination provider cannot check existing object", err)
	}

	if !streamPutOverwrite {
		if _, err := headPutter.Head(ctx, dest.Key); err == nil {
			existsErr := fmt.Errorf("destination object already exists: %s", outputDestURI(dest))
			_ = emitStreamPutError(ctx, w, dest.Key, output.ErrCodeAlreadyExists, "Destination exists", existsErr, map[string]any{"dest_uri": outputDestURI(dest)})
			return exitError(foundry.ExitFileWriteError, "Destination exists", existsErr)
		} else if !provider.IsNotFound(err) {
			_ = emitStreamPutError(ctx, w, dest.Key, classifyStreamPutErrCode(err), "Failed to check destination", err, nil)
			return exitError(foundry.ExitExternalServiceUnavailable, "Failed to check destination", err)
		}
	}

	tempPath, bytesWritten, err := spoolStreamPutInput(cmd.InOrStdin())
	if err != nil {
		_ = emitStreamPutError(ctx, w, dest.Key, output.ErrCodeInternal, "Failed to read stdin", err, nil)
		return exitError(foundry.ExitFileReadError, "Failed to read stdin", err)
	}
	defer func() { _ = os.Remove(tempPath) }()

	if err := uploadToOutputDest(ctx, putter, dest.Key, tempPath); err != nil {
		_ = emitStreamPutError(ctx, w, dest.Key, classifyStreamPutErrCode(err), "PutObject failed", err, nil)
		return exitError(foundry.ExitExternalServiceUnavailable, "PutObject failed", err)
	}

	return w.WriteAny(ctx, streamPutRecordType, &streamPutRecord{
		DestURI: outputDestURI(dest),
		DestKey: dest.Key,
		Bytes:   bytesWritten,
		Status:  "success",
	})
}

func spoolStreamPutInput(r io.Reader) (string, int64, error) {
	tmp, err := os.CreateTemp("", "gonimbus-stream-put-*")
	if err != nil {
		return "", 0, fmt.Errorf("create temp spool: %w", err)
	}
	path := tmp.Name()

	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(path)
		}
	}()

	n, err := io.Copy(tmp, r)
	if err != nil {
		return "", n, fmt.Errorf("copy stdin to temp spool: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", n, fmt.Errorf("close temp spool: %w", err)
	}

	cleanup = false
	return path, n, nil
}

func outputDestURI(dest *outputDestSpec) string {
	if dest == nil {
		return ""
	}
	switch dest.Provider {
	case string(provider.ProviderS3):
		return fmt.Sprintf("s3://%s/%s", dest.Bucket, dest.Key)
	case string(provider.ProviderFile):
		return fileURI(filepath.Join(dest.BaseDir, dest.Key))
	default:
		return ""
	}
}

func emitStreamPutError(ctx context.Context, w output.Writer, key, code, msg string, err error, details map[string]any) error {
	if details == nil {
		details = map[string]any{"mode": "stream_put"}
	} else {
		details["mode"] = "stream_put"
	}
	if err == nil {
		err = errors.New(msg)
	}
	return w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Key: key, Details: details})
}

func classifyStreamPutErrCode(err error) string {
	switch {
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeProviderUnavailable
	default:
		return output.ErrCodeInternal
	}
}
