package cmd

import (
	"context"
	"encoding/json"
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
	"github.com/3leaps/gonimbus/pkg/stream"
)

const (
	streamPutRecordType  = "gonimbus.stream.put.v1"
	streamPutFramingRaw  = "raw"
	streamPutFramingJSON = "jsonl"
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
		Short: "Write stdin to one destination object",
		Long: `Write stdin to a single destination object.

Streaming mode semantics:
- stdin carries the object byte payload.
- stdout emits JSONL records only.
- Existing destination objects are refused unless --overwrite is set.

Framing modes:
- raw: stdin is the destination object payload.
- jsonl: stdin is one gonimbus stream open/chunk*/close sequence.`,
		Args:         cobra.ExactArgs(1),
		RunE:         runStreamPut,
		SilenceUsage: true,
	}

	cmd.Flags().StringVarP(&streamPutRegion, "region", "r", "", "AWS region")
	cmd.Flags().StringVarP(&streamPutProfile, "profile", "p", "", "AWS profile")
	cmd.Flags().StringVar(&streamPutEndpoint, "endpoint", "", "Custom S3 endpoint")
	cmd.Flags().StringVar(&streamPutFraming, "framing", streamPutFramingRaw, "Input framing (raw or jsonl)")
	cmd.Flags().BoolVar(&streamPutOverwrite, "overwrite", false, "Allow overwriting an existing destination object")

	return cmd
}

type streamPutRecord struct {
	DestURI        string `json:"dest_uri"`
	DestKey        string `json:"dest_key"`
	Bytes          int64  `json:"bytes"`
	Status         string `json:"status"`
	SourceURI      string `json:"source_uri,omitempty"`
	SourceStreamID string `json:"source_stream_id,omitempty"`
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
	if framing != streamPutFramingRaw && framing != streamPutFramingJSON {
		return exitError(foundry.ExitInvalidArgument, "Invalid --framing value", fmt.Errorf("stream put supports raw or jsonl framing"))
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

	var (
		tempPath       string
		bytesWritten   int64
		sourceURI      string
		sourceStreamID string
	)
	switch framing {
	case streamPutFramingRaw:
		tempPath, bytesWritten, err = spoolStreamPutInput(cmd.InOrStdin())
	case streamPutFramingJSON:
		tempPath, bytesWritten, sourceURI, sourceStreamID, err = spoolStreamPutFramedInput(cmd.InOrStdin())
	default:
		err = fmt.Errorf("unsupported framing %q", framing)
	}
	if err != nil {
		code := output.ErrCodeInternal
		msg := "Failed to read stdin"
		exitCode := foundry.ExitFileReadError
		if framing == streamPutFramingJSON {
			code = output.ErrCodeInvalidInput
			msg = "Invalid stream input"
			exitCode = foundry.ExitInvalidArgument
		}
		_ = emitStreamPutError(ctx, w, dest.Key, code, msg, err, map[string]any{"framing": framing})
		return exitError(exitCode, msg, err)
	}
	defer func() { _ = os.Remove(tempPath) }()

	if err := uploadToOutputDest(ctx, putter, dest.Key, tempPath); err != nil {
		_ = emitStreamPutError(ctx, w, dest.Key, classifyStreamPutErrCode(err), "PutObject failed", err, nil)
		return exitError(foundry.ExitExternalServiceUnavailable, "PutObject failed", err)
	}

	return w.WriteAny(ctx, streamPutRecordType, &streamPutRecord{
		DestURI:        outputDestURI(dest),
		DestKey:        dest.Key,
		Bytes:          bytesWritten,
		Status:         "success",
		SourceURI:      sourceURI,
		SourceStreamID: sourceStreamID,
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

func spoolStreamPutFramedInput(r io.Reader) (path string, bytesWritten int64, sourceURI string, sourceStreamID string, err error) {
	tmp, err := os.CreateTemp("", "gonimbus-stream-put-*")
	if err != nil {
		return "", 0, "", "", fmt.Errorf("create temp spool: %w", err)
	}
	path = tmp.Name()

	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(path)
		}
	}()

	dec := stream.NewDecoder(r)
	var open stream.Open
	var openSeen bool
	var closeSeen bool
	var expectedSeq int64

	for {
		ev, nextErr := dec.Next()
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				if !openSeen {
					return "", 0, "", "", errors.New("missing stream open record")
				}
				if !closeSeen {
					return "", 0, "", "", errors.New("missing stream close record")
				}
				break
			}
			return "", 0, "", "", fmt.Errorf("decode stream input: %w", nextErr)
		}

		if closeSeen {
			return "", 0, "", "", errors.New("trailing stream data after close record")
		}

		if ev.Kind == stream.EventChunk {
			if !openSeen {
				_ = ev.Chunk.Body.Close()
				return "", 0, "", "", errors.New("stream chunk before open record")
			}
			hdr := ev.Chunk.Header
			if hdr.StreamID != open.StreamID {
				_ = ev.Chunk.Body.Close()
				return "", 0, "", "", fmt.Errorf("stream chunk uses stream_id %q, want %q", hdr.StreamID, open.StreamID)
			}
			if hdr.Seq != expectedSeq {
				_ = ev.Chunk.Body.Close()
				return "", 0, "", "", fmt.Errorf("stream chunk seq=%d, want %d", hdr.Seq, expectedSeq)
			}
			if hdr.Offset != nil && *hdr.Offset != bytesWritten {
				_ = ev.Chunk.Body.Close()
				return "", 0, "", "", fmt.Errorf("stream chunk offset=%d, want %d", *hdr.Offset, bytesWritten)
			}

			n, copyErr := io.Copy(tmp, ev.Chunk.Body)
			closeErr := ev.Chunk.Body.Close()
			if copyErr != nil {
				return "", 0, "", "", fmt.Errorf("read stream chunk seq=%d: %w", hdr.Seq, copyErr)
			}
			if closeErr != nil {
				return "", 0, "", "", fmt.Errorf("close stream chunk seq=%d: %w", hdr.Seq, closeErr)
			}
			if n != hdr.NBytes {
				return "", 0, "", "", fmt.Errorf("stream chunk seq=%d read %d bytes, want %d", hdr.Seq, n, hdr.NBytes)
			}
			bytesWritten += n
			expectedSeq++
			continue
		}

		switch ev.Record.Type {
		case stream.TypeStreamOpen:
			if openSeen {
				return "", 0, "", "", errors.New("duplicate stream open record")
			}
			if err := json.Unmarshal(ev.Record.Data, &open); err != nil {
				return "", 0, "", "", fmt.Errorf("decode stream open record: %w", err)
			}
			if strings.TrimSpace(open.StreamID) == "" {
				return "", 0, "", "", errors.New("stream open record missing stream_id")
			}
			openSeen = true
			sourceURI = open.URI
			sourceStreamID = open.StreamID
		case stream.TypeStreamClose:
			if !openSeen {
				return "", 0, "", "", errors.New("stream close before open record")
			}
			var closeRec stream.Close
			if err := json.Unmarshal(ev.Record.Data, &closeRec); err != nil {
				return "", 0, "", "", fmt.Errorf("decode stream close record: %w", err)
			}
			if closeRec.StreamID != open.StreamID {
				return "", 0, "", "", fmt.Errorf("stream close uses stream_id %q, want %q", closeRec.StreamID, open.StreamID)
			}
			if closeRec.Status != "success" {
				return "", 0, "", "", fmt.Errorf("stream close status must be success, got %q", closeRec.Status)
			}
			if closeRec.Chunks != expectedSeq {
				return "", 0, "", "", fmt.Errorf("stream close chunks=%d, want %d", closeRec.Chunks, expectedSeq)
			}
			if closeRec.Bytes != bytesWritten {
				return "", 0, "", "", fmt.Errorf("stream close bytes=%d, want %d", closeRec.Bytes, bytesWritten)
			}
			if open.Size != nil && *open.Size != bytesWritten {
				return "", 0, "", "", fmt.Errorf("stream open size=%d, want %d", *open.Size, bytesWritten)
			}
			closeSeen = true
		default:
			if !openSeen {
				return "", 0, "", "", fmt.Errorf("expected stream open record, got %q", ev.Record.Type)
			}
			return "", 0, "", "", fmt.Errorf("unexpected stream record %q", ev.Record.Type)
		}
	}

	if err := tmp.Close(); err != nil {
		return "", 0, "", "", fmt.Errorf("close temp spool: %w", err)
	}

	cleanup = false
	return path, bytesWritten, sourceURI, sourceStreamID, nil
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
