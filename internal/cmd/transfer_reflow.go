package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

const (
	reflowRecordType    = "gonimbus.reflow.v1"
	reflowRunRecordType = "gonimbus.reflow.run.v1"
)

var transferReflowCmd = &cobra.Command{
	Use:   "reflow [source-uri]",
	Short: "Copy objects to a new key layout (JSONL)",
	Long: `Copy objects from a source location to a destination prefix while rewriting keys.

Input can be provided via --stdin (one item per line):
- Plain URIs: s3://bucket/key, s3://bucket/prefix/, s3://bucket/prefix/**/*.xml
- JSONL index objects: {"type":"gonimbus.index.object.v1", ...}

Notes:
- v0.1.7 supports a single source bucket per reflow run.

Output is JSONL on stdout.
Errors are emitted on stdout as gonimbus.error.v1 records.
`,
	Args: validateTransferReflowArgs,
	RunE: runTransferReflow,
}

var (
	reflowStdin       bool
	reflowDest        string
	reflowRewriteFrom string
	reflowRewriteTo   string
	reflowParallel    int
	reflowDryRun      bool
	reflowResume      bool
	reflowCheckpoint  string
	reflowOverwrite   bool
	reflowOnCollision string

	reflowSrcRegion   string
	reflowSrcProfile  string
	reflowSrcEndpoint string
	reflowDstRegion   string
	reflowDstProfile  string
	reflowDstEndpoint string
)

func init() {
	transferCmd.AddCommand(transferReflowCmd)

	transferReflowCmd.Flags().BoolVar(&reflowStdin, "stdin", false, "Read selection from stdin")
	transferReflowCmd.Flags().StringVar(&reflowDest, "dest", "", "Destination base URI (prefix), e.g. s3://bucket/base/")
	transferReflowCmd.Flags().StringVar(&reflowRewriteFrom, "rewrite-from", "", "Rewrite source template (segment captures)")
	transferReflowCmd.Flags().StringVar(&reflowRewriteTo, "rewrite-to", "", "Rewrite destination template (segment renders)")
	transferReflowCmd.Flags().IntVar(&reflowParallel, "parallel", 16, "Concurrent copy workers")
	transferReflowCmd.Flags().BoolVar(&reflowDryRun, "dry-run", false, "Emit planned mappings without writing")
	transferReflowCmd.Flags().BoolVar(&reflowResume, "resume", false, "Resume from checkpoint (requires --checkpoint)")
	transferReflowCmd.Flags().StringVar(&reflowCheckpoint, "checkpoint", "", "Checkpoint DB path (sqlite)")
	transferReflowCmd.Flags().BoolVar(&reflowOverwrite, "overwrite", false, "Allow overwriting destination objects")
	transferReflowCmd.Flags().StringVar(&reflowOnCollision, "on-collision", "log", "Collision policy: log|fail|overwrite")

	transferReflowCmd.Flags().StringVar(&reflowSrcRegion, "src-region", "", "Source AWS region")
	transferReflowCmd.Flags().StringVar(&reflowSrcProfile, "src-profile", "", "Source AWS profile")
	transferReflowCmd.Flags().StringVar(&reflowSrcEndpoint, "src-endpoint", "", "Source custom S3 endpoint")
	transferReflowCmd.Flags().StringVar(&reflowDstRegion, "dest-region", "", "Destination AWS region")
	transferReflowCmd.Flags().StringVar(&reflowDstProfile, "dest-profile", "", "Destination AWS profile")
	transferReflowCmd.Flags().StringVar(&reflowDstEndpoint, "dest-endpoint", "", "Destination custom S3 endpoint")

	_ = transferReflowCmd.MarkFlagRequired("dest")
	_ = transferReflowCmd.MarkFlagRequired("rewrite-from")
	_ = transferReflowCmd.MarkFlagRequired("rewrite-to")
}

func validateTransferReflowArgs(cmd *cobra.Command, args []string) error {
	stdin, _ := cmd.Flags().GetBool("stdin")
	if stdin {
		if len(args) != 0 {
			return fmt.Errorf("when using --stdin, do not provide positional source-uri")
		}
		return nil
	}
	if len(args) != 1 {
		return fmt.Errorf("requires exactly 1 argument: [source-uri] (or use --stdin)")
	}
	return nil
}

type reflowTask struct {
	SourceBucket string
	SourceURI    string
	SourceKey    string
	SourceETag   string
	SourceSize   int64
	Vars         map[string]string
	DestRelKey   string
}

type reflowRecord struct {
	SourceURI     string         `json:"source_uri"`
	SourceKey     string         `json:"source_key"`
	SourceETag    string         `json:"source_etag,omitempty"`
	SourceSize    int64          `json:"source_size_bytes,omitempty"`
	DestURI       string         `json:"dest_uri"`
	DestKey       string         `json:"dest_key"`
	Bytes         int64          `json:"bytes,omitempty"`
	Status        string         `json:"status"`
	Reason        string         `json:"reason,omitempty"`
	CollisionKind string         `json:"collision_kind,omitempty"`
	CollisionETag string         `json:"collision_etag,omitempty"`
	CollisionSize int64          `json:"collision_size_bytes,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

type reflowRunRecord struct {
	DestURI        string `json:"dest_uri"`
	CheckpointPath string `json:"checkpoint_path"`
	DryRun         bool   `json:"dry_run"`
	Resume         bool   `json:"resume"`
	Parallel       int    `json:"parallel"`
}

func runTransferReflow(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if reflowParallel < 1 {
		return exitError(foundry.ExitInvalidArgument, "Invalid --parallel value", fmt.Errorf("parallel must be >= 1"))
	}
	if reflowResume && strings.TrimSpace(reflowCheckpoint) == "" {
		return exitError(foundry.ExitInvalidArgument, "Invalid --resume usage", fmt.Errorf("--resume requires --checkpoint"))
	}
	switch reflowOnCollision {
	case "log", "fail", "overwrite":
		// ok
	default:
		return exitError(foundry.ExitInvalidArgument, "Invalid --on-collision value", fmt.Errorf("on-collision must be one of: log, fail, overwrite"))
	}
	if reflowOnCollision == "overwrite" && !reflowOverwrite {
		return exitError(foundry.ExitInvalidArgument, "Overwrite not enabled", fmt.Errorf("--on-collision=overwrite requires --overwrite"))
	}

	destParsed, err := ParseURI(reflowDest)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --dest URI", err)
	}
	if destParsed.Provider != string(provider.ProviderS3) {
		return exitError(foundry.ExitInvalidArgument, "Unsupported destination provider", fmt.Errorf("provider %q is not supported", destParsed.Provider))
	}
	if destParsed.IsPattern() {
		return exitError(foundry.ExitInvalidArgument, "Invalid --dest", fmt.Errorf("destination must be a prefix URI"))
	}
	if !destParsed.IsPrefix() {
		// Treat bucket/key without trailing slash as prefix base.
		destParsed.Key = strings.TrimSuffix(destParsed.Key, "/") + "/"
	}
	destBucket := destParsed.Bucket
	destPrefix := destParsed.Key
	destURI := fmt.Sprintf("%s://%s/%s", destParsed.Provider, destBucket, destPrefix)

	rewrite, err := transfer.CompileReflowRewrite(reflowRewriteFrom, reflowRewriteTo)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid rewrite templates", err)
	}

	jobID := uuid.New().String()
	w := output.NewJSONLWriter(cmd.OutOrStdout(), jobID, destParsed.Provider)
	defer func() { _ = w.Close() }()

	checkpointPath, err := resolveReflowCheckpointPath(jobID)
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to resolve checkpoint path", err)
	}
	if strings.TrimSpace(reflowCheckpoint) != "" {
		checkpointPath = reflowCheckpoint
	}

	state, err := reflowstate.Open(ctx, reflowstate.Config{Path: checkpointPath})
	if err != nil {
		return exitError(foundry.ExitFileWriteError, "Failed to open checkpoint", err)
	}
	defer func() { _ = state.Close() }()

	_ = w.WriteAny(ctx, reflowRunRecordType, reflowRunRecord{
		DestURI:        destURI,
		CheckpointPath: checkpointPath,
		DryRun:         reflowDryRun,
		Resume:         reflowResume,
		Parallel:       reflowParallel,
	})

	// Providers are created after we discover the source bucket.
	var (
		srcProv   *s3.Provider
		dstProv   *s3.Provider
		srcBucket string
		provMu    sync.Mutex
	)
	getProviders := func(bucket string) (*s3.Provider, *s3.Provider, error) {
		provMu.Lock()
		defer provMu.Unlock()

		if dstProv == nil {
			pNew, err := s3.New(ctx, s3.Config{
				Bucket:         destBucket,
				Region:         reflowDstRegion,
				Endpoint:       reflowDstEndpoint,
				Profile:        reflowDstProfile,
				ForcePathStyle: reflowDstEndpoint != "",
			})
			if err != nil {
				return nil, nil, err
			}
			dstProv = pNew
		}
		if srcProv == nil {
			pNew, err := s3.New(ctx, s3.Config{
				Bucket:         bucket,
				Region:         reflowSrcRegion,
				Endpoint:       reflowSrcEndpoint,
				Profile:        reflowSrcProfile,
				ForcePathStyle: reflowSrcEndpoint != "",
			})
			if err != nil {
				return nil, nil, err
			}
			srcProv = pNew
			srcBucket = bucket
		} else if srcBucket != "" && bucket != "" && srcBucket != bucket {
			return nil, nil, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", bucket, srcBucket)
		}
		return srcProv, dstProv, nil
	}
	defer func() {
		provMu.Lock()
		toCloseSrc := srcProv
		toCloseDst := dstProv
		provMu.Unlock()
		if toCloseSrc != nil {
			_ = toCloseSrc.Close()
		}
		if toCloseDst != nil {
			_ = toCloseDst.Close()
		}
	}()

	var (
		invalidCount atomic.Int64
		errorCount   atomic.Int64
	)

	tasks := make(chan reflowTask, reflowParallel*2)
	var wg sync.WaitGroup
	for i := 0; i < reflowParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if ctx.Err() != nil {
					return
				}

				src, dst, err := getProviders(task.SourceBucket)
				if err != nil {
					errorCount.Add(1)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "failed to connect to provider", err, map[string]any{"source_uri": task.SourceURI})
					continue
				}

				var destRel string
				if task.DestRelKey != "" {
					destRel = task.DestRelKey
				} else {
					mapped, _, err := rewrite.ApplyWithVars(task.SourceKey, task.Vars)
					if err != nil {
						invalidCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "rewrite failed", err, map[string]any{"source_uri": task.SourceURI})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: "", SourceKey: task.SourceKey, DestKey: "", SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "failed", ErrorCode: output.ErrCodeInternal, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
					destRel = mapped
				}

				dstKey := strings.TrimPrefix(destPrefix+destRel, "/")
				dstKey = strings.ReplaceAll(dstKey, "//", "/")
				dstURI := fmt.Sprintf("%s://%s/%s", destParsed.Provider, destBucket, dstKey)

				if reflowResume {
					done, status, err := state.ItemDone(ctx, task.SourceURI, dstURI)
					if err != nil {
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "checkpoint read failed", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
						continue
					}
					if done {
						_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, DestURI: dstURI, DestKey: dstKey, Status: "skipped", Reason: "resume." + status})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, Status: "skipped", Reason: "resume." + status}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						continue
					}
				}

				if reflowDryRun {
					_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, DestURI: dstURI, DestKey: dstKey, Status: "planned"})
					continue
				}

				_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: task.SourceETag, SourceSize: task.SourceSize, DestURI: dstURI, DestKey: dstKey, Status: "in_progress"})

				srcETag := task.SourceETag
				srcSize := task.SourceSize
				if srcETag == "" || srcSize == 0 {
					meta, err := src.Head(ctx, task.SourceKey)
					if err == nil {
						srcETag = meta.ETag
						srcSize = meta.Size
					}
				}

				dstMeta, headErr := dst.Head(ctx, dstKey)
				if headErr == nil {
					if werr := state.NoteDestKeySource(context.Background(), dstKey, task.SourceURI, srcETag, srcSize); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}

					dup := false
					if srcETag != "" && dstMeta.ETag != "" && srcETag == dstMeta.ETag {
						dup = true
						if srcSize > 0 && dstMeta.Size > 0 && srcSize != dstMeta.Size {
							dup = false
						}
					}

					if reflowOnCollision == "overwrite" {
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionOverwrite, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
					} else if dup {
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionDuplicate, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "skipped", Reason: "collision.duplicate"}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "skipped", Reason: "collision.duplicate", CollisionKind: "duplicate", CollisionETag: dstMeta.ETag, CollisionSize: dstMeta.Size})
						continue
					}

					// Conflict (or overwrite not enabled)
					if reflowOnCollision != "overwrite" {
						if werr := state.NoteCollision(context.Background(), dstKey, reflowstate.CollisionConflict, task.SourceURI, srcETag, srcSize, dstMeta.ETag, dstMeta.Size); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						err := fmt.Errorf("destination key exists with different content: %s", dstKey)
						code := output.ErrCodeInternal
						errorCount.Add(1)
						_ = emitReflowError(context.Background(), w, task.SourceKey, "collision", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI, "dest_etag": dstMeta.ETag, "dest_size": dstMeta.Size})
						if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: err.Error()}); werr != nil {
							observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
						}
						_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed", Reason: "collision.conflict", CollisionKind: "conflict", CollisionETag: dstMeta.ETag, CollisionSize: dstMeta.Size})
						continue
					}
				}
				if headErr != nil && !provider.IsNotFound(headErr) {
					errorCount.Add(1)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "destination head failed", headErr, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
					continue
				}

				bytes, err := transfer.CopyObject(ctx, src, dst, task.SourceKey, dstKey, task.SourceSize, transfer.DefaultRetryBufferMaxMemoryBytes)
				if err != nil {
					errorCount.Add(1)
					code := reflowErrCode(err)
					_ = emitReflowError(context.Background(), w, task.SourceKey, "copy failed", err, map[string]any{"source_uri": task.SourceURI, "dest_uri": dstURI})
					if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "failed", ErrorCode: code, ErrorMessage: err.Error()}); werr != nil {
						observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
					}
					_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "failed"})
					continue
				}

				if werr := state.NoteDestKeySource(context.Background(), dstKey, task.SourceURI, srcETag, srcSize); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				if werr := state.UpsertItem(context.Background(), reflowstate.UpsertItemParams{SourceURI: task.SourceURI, DestURI: dstURI, SourceKey: task.SourceKey, DestKey: dstKey, SourceETag: srcETag, SourceSize: srcSize, Status: "complete", Bytes: bytes}); werr != nil {
					observability.CLILogger.Debug("Checkpoint write failed", zap.Error(werr))
				}
				_ = w.WriteAny(ctx, reflowRecordType, reflowRecord{SourceURI: task.SourceURI, SourceKey: task.SourceKey, SourceETag: srcETag, SourceSize: srcSize, DestURI: dstURI, DestKey: dstKey, Status: "complete", Bytes: bytes})
			}
		}()
	}

	// Feed tasks from stdin / positional.
	var inputErr error
	if reflowStdin {
		s := bufio.NewScanner(cmd.InOrStdin())
		s.Buffer(make([]byte, 64*1024), 1024*1024)
		for s.Scan() {
			line := strings.TrimSpace(s.Text())
			if line == "" {
				continue
			}
			srcBucket, inputErr = enqueueReflowLine(ctx, line, srcBucket, getProviders, tasks)
			if inputErr != nil {
				invalidCount.Add(1)
				_ = emitReflowError(context.Background(), w, "", "invalid input", inputErr, map[string]any{"input": line})
				inputErr = nil
				continue
			}
		}
		if err := s.Err(); err != nil {
			inputErr = err
		}
	} else {
		srcBucket, inputErr = enqueueReflowLine(ctx, args[0], srcBucket, getProviders, tasks)
	}
	close(tasks)
	wg.Wait()

	if inputErr != nil {
		return exitError(foundry.ExitInvalidArgument, "Failed to read input", inputErr)
	}
	if ctx.Err() != nil {
		return exitError(foundry.ExitSignalInt, "reflow cancelled", ctx.Err())
	}
	if invalidCount.Load() > 0 {
		return exitError(foundry.ExitInvalidArgument, "reflow completed with invalid inputs", fmt.Errorf("invalid_inputs=%d", invalidCount.Load()))
	}
	if errorCount.Load() > 0 {
		return exitError(foundry.ExitExternalServiceUnavailable, "reflow completed with errors", fmt.Errorf("errors=%d", errorCount.Load()))
	}
	return nil
}

func resolveReflowCheckpointPath(jobID string) (string, error) {
	root, err := indexRootDir()
	if err != nil {
		return "", err
	}
	// Keep reflow artifacts near index artifacts for consistent ops tooling.
	return filepath.Join(root, "reflow", "runs", jobID, "state.db"), nil
}

func enqueueReflowLine(ctx context.Context, line string, srcBucket string, getProviders func(bucket string) (*s3.Provider, *s3.Provider, error), out chan<- reflowTask) (string, error) {
	// JSONL: index object record.
	if strings.HasPrefix(line, "{") {
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return srcBucket, err
		}
		switch env.Type {
		case "gonimbus.index.object.v1":
			var data struct {
				BaseURI   string  `json:"base_uri"`
				Key       string  `json:"key"`
				ETag      string  `json:"etag"`
				SizeBytes int64   `json:"size_bytes"`
				RelKey    string  `json:"rel_key"`
				DeletedAt *string `json:"deleted_at"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcBucket, err
			}
			if data.DeletedAt != nil {
				return srcBucket, fmt.Errorf("deleted objects are not supported in reflow input")
			}
			base, err := ParseURI(data.BaseURI)
			if err != nil {
				return srcBucket, fmt.Errorf("invalid base_uri: %w", err)
			}
			if base.Provider != string(provider.ProviderS3) {
				return srcBucket, fmt.Errorf("unsupported provider %q", base.Provider)
			}
			if srcBucket == "" {
				srcBucket = base.Bucket
			} else if srcBucket != base.Bucket {
				return srcBucket, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", base.Bucket, srcBucket)
			}
			_, _, err = getProviders(srcBucket)
			if err != nil {
				return srcBucket, err
			}
			key := strings.TrimPrefix(data.Key, "/")
			if key == "" {
				key = strings.TrimPrefix(data.RelKey, "/")
			}
			if key == "" {
				return srcBucket, fmt.Errorf("missing key in index record")
			}
			uri := fmt.Sprintf("%s://%s/%s", base.Provider, base.Bucket, key)
			select {
			case out <- reflowTask{SourceBucket: base.Bucket, SourceURI: uri, SourceKey: key, SourceETag: data.ETag, SourceSize: data.SizeBytes}:
				return srcBucket, nil
			case <-ctx.Done():
				return srcBucket, ctx.Err()
			}
		case "gonimbus.reflow.input.v1":
			var data struct {
				SourceURI  string            `json:"source_uri"`
				SourceKey  string            `json:"source_key"`
				SourceETag string            `json:"source_etag"`
				SourceSize int64             `json:"source_size_bytes"`
				Vars       map[string]string `json:"vars"`
				DestRelKey string            `json:"dest_rel_key"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcBucket, err
			}
			if strings.TrimSpace(data.SourceURI) == "" {
				return srcBucket, fmt.Errorf("missing data.source_uri")
			}
			u, err := ParseURI(data.SourceURI)
			if err != nil {
				return srcBucket, err
			}
			if u.Provider != string(provider.ProviderS3) {
				return srcBucket, fmt.Errorf("unsupported provider %q", u.Provider)
			}
			if u.IsPrefix() || u.IsPattern() {
				return srcBucket, fmt.Errorf("reflow input source_uri must be an exact object URI")
			}
			if srcBucket == "" {
				srcBucket = u.Bucket
			} else if srcBucket != u.Bucket {
				return srcBucket, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", u.Bucket, srcBucket)
			}
			_, _, err = getProviders(srcBucket)
			if err != nil {
				return srcBucket, err
			}
			key := u.Key
			if strings.TrimSpace(data.SourceKey) != "" {
				key = strings.TrimPrefix(strings.TrimSpace(data.SourceKey), "/")
			}
			destRel := strings.Trim(strings.TrimSpace(data.DestRelKey), "/")
			srcURI := fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key)
			select {
			case out <- reflowTask{SourceBucket: u.Bucket, SourceURI: srcURI, SourceKey: key, SourceETag: data.SourceETag, SourceSize: data.SourceSize, Vars: data.Vars, DestRelKey: destRel}:
				return srcBucket, nil
			case <-ctx.Done():
				return srcBucket, ctx.Err()
			}
		default:
			return srcBucket, fmt.Errorf("unsupported json record type %q", env.Type)
		}
	}

	parsed, err := ParseURI(line)
	if err != nil {
		return srcBucket, err
	}
	if parsed.Provider != string(provider.ProviderS3) {
		return srcBucket, fmt.Errorf("unsupported provider %q", parsed.Provider)
	}
	if srcBucket == "" {
		srcBucket = parsed.Bucket
	} else if srcBucket != parsed.Bucket {
		return srcBucket, fmt.Errorf("multiple source buckets are not supported: got %q expected %q", parsed.Bucket, srcBucket)
	}
	prov, _, err := getProviders(srcBucket)
	if err != nil {
		return srcBucket, err
	}

	if !parsed.IsPrefix() && !parsed.IsPattern() {
		select {
		case out <- reflowTask{SourceBucket: parsed.Bucket, SourceURI: parsed.String(), SourceKey: parsed.Key}:
			return srcBucket, nil
		case <-ctx.Done():
			return srcBucket, ctx.Err()
		}
	}

	var m *match.Matcher
	if parsed.IsPattern() {
		matcher, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			return srcBucket, err
		}
		m = matcher
	}

	var token string
	for {
		res, err := prov.List(ctx, provider.ListOptions{Prefix: parsed.Key, ContinuationToken: token})
		if err != nil {
			return srcBucket, err
		}
		for _, obj := range res.Objects {
			if m != nil && !m.Match(obj.Key) {
				continue
			}
			uri := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, obj.Key)
			select {
			case out <- reflowTask{SourceBucket: parsed.Bucket, SourceURI: uri, SourceKey: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size}:
				// ok
			case <-ctx.Done():
				return srcBucket, ctx.Err()
			}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}
	return srcBucket, nil
}

func emitReflowError(ctx context.Context, w output.Writer, key, msg string, err error, details map[string]any) error {
	if details == nil {
		details = map[string]any{}
	}
	details["mode"] = "transfer_reflow"
	code := reflowErrCode(err)
	if werr := w.WriteError(ctx, &output.ErrorRecord{Code: code, Message: fmt.Sprintf("%s: %s", msg, err.Error()), Key: key, Details: details}); werr != nil {
		observability.CLILogger.Debug("Failed to emit reflow error record", zap.Error(werr))
	}
	return nil
}

func reflowErrCode(err error) string {
	switch {
	case provider.IsNotFound(err):
		return output.ErrCodeNotFound
	case provider.IsAccessDenied(err):
		return output.ErrCodeAccessDenied
	case provider.IsThrottled(err):
		return output.ErrCodeThrottled
	case provider.IsProviderUnavailable(err):
		return output.ErrCodeProviderUnavailable
	default:
		return output.ErrCodeInternal
	}
}
