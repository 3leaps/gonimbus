package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/stream"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

const (
	streamPutRecordType     = "gonimbus.stream.put.v1"
	streamPutProgressType   = "gonimbus.stream.progress.v1"
	streamPutFramingRaw     = "raw"
	streamPutFramingJSON    = "jsonl"
	streamPutDefaultPart    = int64(8 * 1024 * 1024)
	streamPutDefaultTrigger = int64(64 * 1024 * 1024)
)

var streamPutCmd = newStreamPutCommand()

var (
	streamPutRegion     string
	streamPutProfile    string
	streamPutEndpoint   string
	streamPutGCPProject string
	streamPutFraming    string
	streamPutOverwrite  bool
	streamPutDestFrame  bool
	streamPutFailFast   bool
	streamPutPartSize   string
	streamPutThreshold  string
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
	cmd.Flags().StringVar(&streamPutGCPProject, "gcp-project", "", "GCP project hint for GCS")
	cmd.Flags().StringVar(&streamPutFraming, "framing", streamPutFramingRaw, "Input framing (raw or jsonl)")
	cmd.Flags().BoolVar(&streamPutOverwrite, "overwrite", false, "Allow overwriting an existing destination object")
	cmd.Flags().BoolVar(&streamPutDestFrame, "dest-from-frame", false, "Allow framed dest_key values under the CLI destination root")
	cmd.Flags().BoolVar(&streamPutFailFast, "fail-fast", false, "Stop framed batch processing after the first per-object failure")
	cmd.Flags().StringVar(&streamPutPartSize, "part-size", "8MiB", "Multipart upload part size in bytes or KiB/MiB/GiB")
	cmd.Flags().StringVar(&streamPutThreshold, "multipart-threshold", "64MiB", "Size threshold for switching to multipart upload")

	return cmd
}

type streamPutRecord struct {
	DestURI        string `json:"dest_uri"`
	DestKey        string `json:"dest_key"`
	Bytes          int64  `json:"bytes"`
	Status         string `json:"status"`
	SourceURI      string `json:"source_uri,omitempty"`
	SourceStreamID string `json:"source_stream_id,omitempty"`
	ETag           string `json:"etag,omitempty"`
	UploadMode     string `json:"upload_mode,omitempty"`
	Error          string `json:"error,omitempty"`
}

type streamPutProgressRecord struct {
	DestURI    string `json:"dest_uri"`
	DestKey    string `json:"dest_key"`
	UploadID   string `json:"upload_id,omitempty"`
	PartNumber int32  `json:"part_number"`
	PartBytes  int64  `json:"part_bytes"`
	Bytes      int64  `json:"bytes"`
	Status     string `json:"status"`
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

	jobID := uuid.New().String()
	providerName := streamPutOutputProviderName(args[0])
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, providerName)
	defer func() { _ = w.Close() }()

	partSize, err := parseStreamPutSize(streamPutPartSize, streamPutDefaultPart)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --part-size value", err)
	}
	threshold, err := parseStreamPutSize(streamPutThreshold, streamPutDefaultTrigger)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --multipart-threshold value", err)
	}
	if partSize <= 0 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --part-size value", errors.New("part size must be > 0"))
	}
	if threshold < 0 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --multipart-threshold value", errors.New("multipart threshold must be >= 0"))
	}

	opts := streamPutUploadOptions{PartSize: partSize, MultipartThreshold: threshold, Overwrite: streamPutOverwrite}
	if framing == streamPutFramingRaw {
		return runStreamPutRaw(ctx, cmd, args[0], w, opts)
	}
	return runStreamPutFramed(ctx, cmd, args[0], w, opts)
}

func runStreamPutRaw(ctx context.Context, cmd *cobra.Command, rawDest string, w *output.JSONLWriter, opts streamPutUploadOptions) error {
	dest, err := parseOutputDest(rawDest)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid destination URI", err)
	}
	applyStreamPutProviderFlags(dest)
	putter, err := newOutputProvider(ctx, dest)
	if err != nil {
		_ = emitStreamPutError(ctx, w, dest.Key, output.ErrCodeProviderUnavailable, "Failed to connect to storage provider", err, nil)
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	if closer, ok := putter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	result, err := uploadStreamPutReader(ctx, putter, outputDestURI(dest), dest.Key, cmd.InOrStdin(), opts, w)
	if err != nil {
		return streamPutUploadExit(ctx, w, dest, err)
	}
	return w.WriteAny(ctx, streamPutRecordType, &streamPutRecord{
		DestURI:    outputDestURI(dest),
		DestKey:    dest.Key,
		Bytes:      result.Bytes,
		Status:     "success",
		ETag:       result.ETag,
		UploadMode: result.Mode,
	})
}

type streamPutUploadOptions struct {
	PartSize           int64
	MultipartThreshold int64
	Overwrite          bool
}

type streamPutUploadResult struct {
	Bytes int64
	ETag  string
	Mode  string
}

func uploadStreamPutReader(ctx context.Context, putter provider.ObjectPutter, destURI string, key string, r io.Reader, opts streamPutUploadOptions, w *output.JSONLWriter) (streamPutUploadResult, error) {
	result, err := transfer.UploadReader(ctx, putter, key, r, streamPutTransferOptions(destURI, opts, w))
	if err != nil {
		return streamPutUploadResult{}, err
	}
	return streamPutUploadResult{Bytes: result.Bytes, ETag: result.ETag, Mode: result.Mode}, nil
}

func streamPutTransferOptions(destURI string, opts streamPutUploadOptions, w *output.JSONLWriter) transfer.UploadOptions {
	uploadOpts := transfer.UploadOptions{
		PartSizeBytes:         opts.PartSize,
		MultipartThreshold:    opts.MultipartThreshold,
		MultipartThresholdSet: true,
		Progress: func(ctx context.Context, progress transfer.UploadProgress) error {
			return w.WriteAny(ctx, streamPutProgressType, &streamPutProgressRecord{
				DestURI:    destURI,
				DestKey:    progress.Key,
				UploadID:   progress.UploadID,
				PartNumber: progress.PartNumber,
				PartBytes:  progress.PartBytes,
				Bytes:      progress.Bytes,
				Status:     progress.Status,
			})
		},
	}
	if !opts.Overwrite {
		uploadOpts.Precondition = provider.PutPrecondition{IfAbsent: true}
	}
	return uploadOpts
}

func streamPutUploadExit(ctx context.Context, w output.Writer, dest *outputDestSpec, err error) error {
	if provider.IsAlreadyExists(err) {
		existsErr := fmt.Errorf("destination object already exists: %s", outputDestURI(dest))
		_ = emitStreamPutError(ctx, w, dest.Key, output.ErrCodeAlreadyExists, "Destination exists", existsErr, map[string]any{"dest_uri": outputDestURI(dest)})
		return exitError(foundry.ExitFileWriteError, "Destination exists", existsErr)
	}
	_ = emitStreamPutError(ctx, w, dest.Key, classifyStreamPutErrCode(err), "PutObject failed", err, nil)
	return exitError(foundry.ExitExternalServiceUnavailable, "PutObject failed", err)
}

type streamPutDestRoot struct {
	Provider       string
	Bucket         string
	Prefix         string
	ExactKey       string
	BaseDir        string
	Region         string
	Profile        string
	Endpoint       string
	ForcePathStyle bool
	GCPProject     string
}

func runStreamPutFramed(ctx context.Context, cmd *cobra.Command, rawDest string, w *output.JSONLWriter, opts streamPutUploadOptions) error {
	root, err := parseStreamPutDestRoot(rawDest)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid destination URI", err)
	}
	root.Region = streamPutRegion
	root.Profile = streamPutProfile
	root.Endpoint = streamPutEndpoint
	root.ForcePathStyle = streamPutEndpoint != ""
	root.GCPProject = streamPutGCPProject

	provSpec := root.providerSpec()
	putter, err := newOutputProvider(ctx, provSpec)
	if err != nil {
		_ = emitStreamPutError(ctx, w, root.Prefix, output.ErrCodeProviderUnavailable, "Failed to connect to storage provider", err, nil)
		return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to storage provider", err)
	}
	if closer, ok := putter.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	dec := stream.NewDecoder(cmd.InOrStdin())
	var failures int
	var objects int
	var firstErr error
	for {
		rec, err := readStreamPutFramedObject(ctx, dec, root, putter, opts, w, objects)
		if errors.Is(err, io.EOF) {
			if objects == 0 && failures == 0 {
				firstErr = errors.New("missing stream open record")
				failures++
				_ = emitStreamPutError(ctx, w, "", output.ErrCodeInvalidInput, "Invalid stream input", firstErr, map[string]any{"framing": streamPutFramingJSON})
			}
			break
		}
		if err != nil {
			failures++
			if firstErr == nil {
				firstErr = err
			}
			_ = emitStreamPutError(ctx, w, "", output.ErrCodeInvalidInput, "Invalid stream input", err, map[string]any{"framing": streamPutFramingJSON})
			if streamPutFailFast {
				return exitError(foundry.ExitInvalidArgument, "Invalid stream input", err)
			}
			break
		}
		if rec == nil {
			continue
		}
		objects++
		if rec.Status != "success" {
			failures++
			if firstErr == nil {
				firstErr = errors.New(rec.Error)
			}
			if streamPutFailFast {
				return exitError(foundry.ExitExternalServiceUnavailable, "PutObject failed", fmt.Errorf("stream %s failed", rec.SourceStreamID))
			}
		}
	}
	if failures > 0 {
		if firstErr == nil {
			firstErr = fmt.Errorf("failures=%d", failures)
		}
		return exitError(foundry.ExitExternalServiceUnavailable, "stream put completed with failures", firstErr)
	}
	return nil
}

func readStreamPutFramedObject(ctx context.Context, dec *stream.Decoder, root streamPutDestRoot, putter provider.ObjectPutter, opts streamPutUploadOptions, w *output.JSONLWriter, objectIndex int) (*streamPutRecord, error) {
	ev, err := dec.Next()
	if err != nil {
		return nil, err
	}
	if ev.Kind != stream.EventRecord || ev.Record.Type != stream.TypeStreamOpen {
		if ev.Kind == stream.EventChunk {
			_ = ev.Chunk.Body.Close()
			return nil, errors.New("stream chunk before open record")
		}
		if ev.Record.Type == stream.TypeStreamClose {
			return nil, errors.New("trailing stream data after close record")
		}
		return nil, fmt.Errorf("expected stream open record, got %q", ev.Record.Type)
	}

	var open stream.Open
	if err := json.Unmarshal(ev.Record.Data, &open); err != nil {
		return nil, fmt.Errorf("decode stream open record: %w", err)
	}
	if strings.TrimSpace(open.StreamID) == "" {
		return nil, errors.New("stream open record missing stream_id")
	}
	destKey, err := root.keyForOpen(open, streamPutDestFrame, objectIndex)
	if err != nil {
		return nil, err
	}
	dest := root.objectSpec(destKey)
	providerKey := dest.Key
	destURI := outputDestURI(dest)
	session, err := transfer.NewUploadSession(ctx, putter, providerKey, streamPutTransferOptions(destURI, opts, w))
	if err != nil {
		return nil, err
	}

	var bytesWritten int64
	var expectedSeq int64
	for {
		ev, err = dec.Next()
		if err != nil {
			_ = session.Abort(ctx)
			if errors.Is(err, io.EOF) {
				return nil, errors.New("missing stream close record")
			}
			return nil, fmt.Errorf("decode stream input: %w", err)
		}
		if ev.Kind == stream.EventChunk {
			hdr := ev.Chunk.Header
			if hdr.StreamID != open.StreamID {
				_ = ev.Chunk.Body.Close()
				_ = session.Abort(ctx)
				return nil, fmt.Errorf("stream chunk uses stream_id %q, want %q", hdr.StreamID, open.StreamID)
			}
			if hdr.Seq != expectedSeq {
				_ = ev.Chunk.Body.Close()
				_ = session.Abort(ctx)
				return nil, fmt.Errorf("stream chunk seq=%d, want %d", hdr.Seq, expectedSeq)
			}
			if hdr.Offset != nil && *hdr.Offset != bytesWritten {
				_ = ev.Chunk.Body.Close()
				_ = session.Abort(ctx)
				return nil, fmt.Errorf("stream chunk offset=%d, want %d", *hdr.Offset, bytesWritten)
			}
			n, copyErr := io.Copy(session, ev.Chunk.Body)
			closeErr := ev.Chunk.Body.Close()
			if copyErr != nil {
				_ = session.Abort(ctx)
				return nil, fmt.Errorf("read stream chunk seq=%d: %w", hdr.Seq, copyErr)
			}
			if closeErr != nil {
				_ = session.Abort(ctx)
				return nil, fmt.Errorf("close stream chunk seq=%d: %w", hdr.Seq, closeErr)
			}
			if n != hdr.NBytes {
				_ = session.Abort(ctx)
				return nil, fmt.Errorf("stream chunk seq=%d read %d bytes, want %d", hdr.Seq, n, hdr.NBytes)
			}
			bytesWritten += n
			expectedSeq++
			continue
		}
		if ev.Record.Type != stream.TypeStreamClose {
			_ = session.Abort(ctx)
			if ev.Record.Type == stream.TypeStreamOpen {
				return nil, errors.New("duplicate stream open record")
			}
			return nil, fmt.Errorf("unexpected stream record %q", ev.Record.Type)
		}
		var closeRec stream.Close
		if err := json.Unmarshal(ev.Record.Data, &closeRec); err != nil {
			_ = session.Abort(ctx)
			return nil, fmt.Errorf("decode stream close record: %w", err)
		}
		if closeRec.StreamID != open.StreamID {
			_ = session.Abort(ctx)
			return nil, fmt.Errorf("stream close uses stream_id %q, want %q", closeRec.StreamID, open.StreamID)
		}
		if closeRec.Status != "success" {
			_ = session.Abort(ctx)
			return nil, fmt.Errorf("stream close status must be success, got %q", closeRec.Status)
		}
		if closeRec.Chunks != expectedSeq {
			_ = session.Abort(ctx)
			return nil, fmt.Errorf("stream close chunks=%d, want %d", closeRec.Chunks, expectedSeq)
		}
		if closeRec.Bytes != bytesWritten {
			_ = session.Abort(ctx)
			return nil, fmt.Errorf("stream close bytes=%d, want %d", closeRec.Bytes, bytesWritten)
		}
		if open.Size != nil && *open.Size != bytesWritten {
			_ = session.Abort(ctx)
			return nil, fmt.Errorf("stream open size=%d, want %d", *open.Size, bytesWritten)
		}
		break
	}

	result, err := session.Close(ctx)
	if err != nil {
		_ = session.Abort(ctx)
		_ = emitStreamPutError(ctx, w, providerKey, classifyStreamPutErrCode(err), "PutObject failed", err, map[string]any{"dest_uri": destURI})
		errText := err.Error()
		if provider.IsAlreadyExists(err) {
			errText = "Destination exists"
		}
		return &streamPutRecord{DestURI: destURI, DestKey: providerKey, Bytes: bytesWritten, Status: "error", SourceURI: open.URI, SourceStreamID: open.StreamID, Error: errText}, nil
	}
	rec := &streamPutRecord{
		DestURI:        destURI,
		DestKey:        providerKey,
		Bytes:          result.Bytes,
		Status:         "success",
		SourceURI:      open.URI,
		SourceStreamID: open.StreamID,
		ETag:           result.ETag,
		UploadMode:     result.Mode,
	}
	return rec, w.WriteAny(ctx, streamPutRecordType, rec)
}

func parseStreamPutDestRoot(raw string) (streamPutDestRoot, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return streamPutDestRoot{}, fmt.Errorf("destination root is required")
	}
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "file://") {
		p := strings.TrimSpace(raw[len("file://"):])
		if p == "" {
			return streamPutDestRoot{}, fmt.Errorf("file destination root is empty")
		}
		isPrefix := strings.HasSuffix(p, "/")
		p = filepath.Clean(p)
		if !filepath.IsAbs(p) {
			return streamPutDestRoot{}, fmt.Errorf("file destination root must be absolute: %s", p)
		}
		exactKey := ""
		if !isPrefix {
			exactKey = filepath.ToSlash(filepath.Base(p))
			p = filepath.Dir(p)
		}
		return streamPutDestRoot{Provider: string(provider.ProviderFile), BaseDir: p, ExactKey: exactKey}, nil
	}
	if strings.HasPrefix(lower, "s3://") || strings.HasPrefix(lower, "gs://") {
		scheme := "s3"
		providerName := string(provider.ProviderS3)
		prefixLen := len("s3://")
		if strings.HasPrefix(lower, "gs://") {
			scheme = "gs"
			providerName = string(provider.ProviderGCS)
			prefixLen = len("gs://")
		}
		remainder := raw[prefixLen:]
		slashIdx := strings.Index(remainder, "/")
		if slashIdx == -1 {
			return streamPutDestRoot{}, fmt.Errorf("%s destination root must include a prefix or key: %s", scheme, raw)
		}
		bucket := remainder[:slashIdx]
		prefix := remainder[slashIdx+1:]
		if bucket == "" {
			return streamPutDestRoot{}, fmt.Errorf("%s destination root missing bucket: %s", scheme, raw)
		}
		exactKey := ""
		if prefix != "" && !strings.HasSuffix(prefix, "/") {
			exactKey = path.Base(prefix)
			parent := path.Dir(prefix)
			if parent == "." {
				prefix = ""
			} else {
				prefix = parent + "/"
			}
		}
		return streamPutDestRoot{Provider: providerName, Bucket: bucket, Prefix: prefix, ExactKey: exactKey}, nil
	}
	return streamPutDestRoot{}, fmt.Errorf("unsupported output scheme %q (supported: s3, gs, file)", raw)
}

func (r streamPutDestRoot) providerSpec() *outputDestSpec {
	return r.objectSpec("probe")
}

func (r streamPutDestRoot) objectSpec(key string) *outputDestSpec {
	spec := &outputDestSpec{Provider: r.Provider, Region: r.Region, Profile: r.Profile, Endpoint: r.Endpoint, ForcePathStyle: r.ForcePathStyle, GCPProject: r.GCPProject}
	switch r.Provider {
	case string(provider.ProviderFile):
		spec.BaseDir = r.BaseDir
		spec.Key = filepath.FromSlash(key)
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		spec.Bucket = r.Bucket
		spec.Key = r.Prefix + key
	}
	return spec
}

func (r streamPutDestRoot) keyForOpen(open stream.Open, allowDestKey bool, objectIndex int) (string, error) {
	if strings.TrimSpace(open.DestKey) != "" {
		if !allowDestKey {
			return "", errors.New("frame dest_key requires --dest-from-frame")
		}
		if r.ExactKey != "" {
			return "", errors.New("frame dest_key requires a destination root")
		}
		return sanitizeStreamPutRelativeKey(open.DestKey)
	}
	if r.ExactKey != "" {
		if objectIndex > 0 {
			return "", errors.New("exact destination accepts only one framed object; use a trailing-slash destination root for multiple objects")
		}
		return r.ExactKey, nil
	}
	return sanitizeStreamPutRelativeKey(defaultStreamPutSourceKey(open.URI))
}

func defaultStreamPutSourceKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if parsed, err := parseOutputLikeURI(raw); err == nil {
		return parsed
	}
	return path.Base(strings.Trim(raw, "/"))
}

func parseOutputLikeURI(raw string) (string, error) {
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "s3://") {
		remainder := raw[len("s3://"):]
		idx := strings.Index(remainder, "/")
		if idx == -1 || idx == len(remainder)-1 {
			return "", fmt.Errorf("source URI has no key")
		}
		return remainder[idx+1:], nil
	}
	if strings.HasPrefix(lower, "gs://") {
		remainder := raw[len("gs://"):]
		idx := strings.Index(remainder, "/")
		if idx == -1 || idx == len(remainder)-1 {
			return "", fmt.Errorf("source URI has no key")
		}
		return remainder[idx+1:], nil
	}
	if strings.HasPrefix(lower, "file://") {
		return filepath.Base(raw[len("file://"):]), nil
	}
	return "", fmt.Errorf("unsupported source URI")
}

func sanitizeStreamPutRelativeKey(raw string) (string, error) {
	key := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if key == "" {
		return "", errors.New("dest_key is empty")
	}
	if strings.Contains(key, "://") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("dest_key must be relative: %s", raw)
	}
	if first, _, _ := strings.Cut(key, "/"); isStreamPutSchemePrefix(first) {
		return "", fmt.Errorf("dest_key must not start with a URI scheme: %s", raw)
	}
	clean := path.Clean(key)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "/../") {
		return "", fmt.Errorf("dest_key escapes destination root: %s", raw)
	}
	return clean, nil
}

func isStreamPutSchemePrefix(segment string) bool {
	name, _, ok := strings.Cut(segment, ":")
	if !ok || name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case i == 0 && ((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')):
		case i > 0 && ((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '+' || r == '.' || r == '-'):
		default:
			return false
		}
	}
	return true
}

func streamPutOutputProviderName(raw string) string {
	lower := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.HasPrefix(lower, "s3://"):
		return string(provider.ProviderS3)
	case strings.HasPrefix(lower, "gs://"):
		return string(provider.ProviderGCS)
	case strings.HasPrefix(lower, "file://"):
		return string(provider.ProviderFile)
	default:
		return "stream"
	}
}

func applyStreamPutProviderFlags(dest *outputDestSpec) {
	dest.Region = streamPutRegion
	dest.Profile = streamPutProfile
	dest.Endpoint = streamPutEndpoint
	dest.ForcePathStyle = streamPutEndpoint != ""
	dest.GCPProject = streamPutGCPProject
}

func parseStreamPutSize(raw string, fallback int64) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	lower := strings.ToLower(raw)
	mult := int64(1)
	for _, suffix := range []struct {
		text string
		mult int64
	}{
		{"gib", 1024 * 1024 * 1024},
		{"gb", 1000 * 1000 * 1000},
		{"mib", 1024 * 1024},
		{"mb", 1000 * 1000},
		{"kib", 1024},
		{"kb", 1000},
		{"b", 1},
	} {
		if strings.HasSuffix(lower, suffix.text) {
			mult = suffix.mult
			raw = strings.TrimSpace(raw[:len(raw)-len(suffix.text)])
			break
		}
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}

func outputDestURI(dest *outputDestSpec) string {
	if dest == nil {
		return ""
	}
	switch dest.Provider {
	case string(provider.ProviderS3):
		return fmt.Sprintf("s3://%s/%s", dest.Bucket, dest.Key)
	case string(provider.ProviderGCS):
		return fmt.Sprintf("gs://%s/%s", dest.Bucket, dest.Key)
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
	case provider.IsAlreadyExists(err):
		return output.ErrCodeAlreadyExists
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
