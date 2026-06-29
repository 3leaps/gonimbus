package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/3leaps/gonimbus/internal/observability"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/uri"
)

type reflowTask struct {
	SourceProvider    string
	SourceBucket      string
	SourceRoot        string
	SourceURI         string
	SourceCheckpoint  string
	SourceKey         string
	SourceETag        string
	SourceSize        int64
	SourceLastMod     time.Time
	SourceMode        fs.FileMode
	SourceFailure     string
	Vars              map[string]string
	Probe             *probe.ProbeAudit
	DestRelKey        string
	RoutingClass      string
	QuarantinePrefix  string
	RejectSymlinkPath bool
}

type reflowFilePreflightSummary struct {
	SourceRoot string
	FileCount  int64
	TotalBytes int64
}

func (t reflowTask) withSourceMeta(etag string, size int64) reflowTask {
	t.SourceETag = etag
	t.SourceSize = size
	return t
}

func (t reflowTask) auditSourceURI() string {
	if t.SourceProvider == string(provider.ProviderFile) && t.SourceRoot != "" {
		return fileAuditSourceURI(t.SourceRoot, t.SourceKey)
	}
	return t.SourceURI
}

func (t reflowTask) checkpointSourceURI() string {
	if t.SourceCheckpoint != "" {
		return t.SourceCheckpoint
	}
	if t.SourceProvider == string(provider.ProviderFile) && t.SourceRoot != "" {
		return fileCheckpointSourceURI(t.SourceKey)
	}
	return t.SourceURI
}

func (t reflowTask) sourceProviderURI() *uri.ObjectURI {
	switch t.SourceProvider {
	case string(provider.ProviderFile):
		return &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: reflowpkg.SourceBucketFile, Key: t.SourceRoot}
	default:
		return &uri.ObjectURI{Provider: string(provider.ProviderS3), Bucket: t.SourceBucket}
	}
}

func (t reflowTask) reflowRecord(destURI, destKey, status string) reflowpkg.Record {
	rec := reflowpkg.Record{
		SourceURI:  t.auditSourceURI(),
		SourceKey:  t.SourceKey,
		SourceETag: t.SourceETag,
		SourceSize: t.SourceSize,
		DestURI:    destURI,
		DestKey:    destKey,
		Status:     status,
	}
	switch t.SourceProvider {
	case string(provider.ProviderFile):
		rec.SourceBucket = reflowpkg.SourceBucketFile
		if verbose {
			rec.SourceRoot = t.SourceRoot
		}
	case string(provider.ProviderS3), "":
		rec.SourceBucket = t.SourceBucket
	}
	return rec
}

func fileReflowInputRootAndKey(sourcePath string, sourceKey string) (string, string, error) {
	cleanSourcePath := filepath.Clean(sourcePath)
	key := strings.TrimSpace(sourceKey)
	if key == "" {
		return filepath.Dir(cleanSourcePath), filepath.ToSlash(filepath.Base(cleanSourcePath)), nil
	}
	key = strings.TrimPrefix(filepath.ToSlash(key), "/")
	key = pathpkg.Clean(key)
	if key == "." || key == ".." || strings.HasPrefix(key, "../") {
		return "", "", fmt.Errorf("file reflow input source_key must be relative")
	}

	sourceSlash := filepath.ToSlash(cleanSourcePath)
	suffix := "/" + key
	if !strings.HasSuffix(sourceSlash, suffix) {
		return "", "", fmt.Errorf("file reflow input source_key must match source_uri path suffix")
	}
	rootSlash := strings.TrimSuffix(sourceSlash, suffix)
	if rootSlash == "" {
		rootSlash = "/"
	}
	return filepath.Clean(filepath.FromSlash(rootSlash)), key, nil
}

func fileAuditSourceURI(root string, rel string) string {
	rel = strings.TrimPrefix(filepath.ToSlash(rel), "/")
	if verbose {
		return fileURI(filepath.Join(root, filepath.FromSlash(rel)))
	}
	if rel == "" {
		return "file://local/"
	}
	return "file://local/" + rel
}

func fileCheckpointSourceURI(rel string) string {
	rel = strings.TrimPrefix(filepath.ToSlash(rel), "/")
	if rel == "" {
		return "file-checkpoint://local/"
	}
	return "file-checkpoint://local/" + rel
}

func filePathContainsSymlink(path string) (bool, error) {
	cleanPath := filepath.Clean(path)
	volume := filepath.VolumeName(cleanPath)
	rest := strings.TrimPrefix(cleanPath, volume)
	if filepath.IsAbs(cleanPath) {
		rest = strings.TrimPrefix(rest, string(filepath.Separator))
	}
	parts := strings.Split(rest, string(filepath.Separator))

	cur := volume
	if filepath.IsAbs(cleanPath) {
		cur += string(filepath.Separator)
	}
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if cur == "" || cur == string(filepath.Separator) || strings.HasSuffix(cur, string(filepath.Separator)) {
			cur += part
		} else {
			cur = filepath.Join(cur, part)
		}
		info, err := os.Lstat(cur)
		if err != nil {
			return false, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return true, nil
		}
	}
	return false, nil
}

func runFileReflowPreflight(ctx context.Context, w output.Writer, parsed *uri.ObjectURI, dest *reflowDestSpec, srcCfg reflowSourceConfig) (reflowFilePreflightSummary, error) {
	summary := reflowFilePreflightSummary{SourceRoot: filepath.Clean(parsed.Key)}
	pathErrOpts := reflowpkg.PathErrorOptions{Verbose: verbose}
	rec := &output.PreflightRecord{Mode: "reflow-file-source"}
	add := func(capability string, allowed bool, method string, err error, detail string) {
		result := output.PreflightCheckResult{Capability: capability, Allowed: allowed, Method: method, Detail: detail}
		if err != nil {
			result.ErrorCode = preflightErrorCode(err)
			if result.Detail == "" {
				result.Detail = err.Error()
			}
		}
		rec.Results = append(rec.Results, result)
	}

	st, err := os.Stat(summary.SourceRoot)
	if err != nil {
		reportErr := reflowpkg.NewPathError("source root is not accessible", err.Error(), pathErrOpts)
		add("source.file.stat", false, "Stat(source_root)", reportErr, "")
		_ = w.WritePreflight(ctx, rec)
		return summary, reportErr
	}
	if !st.IsDir() {
		reportErr := reflowpkg.NewPathError("file source root must be a directory", fmt.Sprintf("file source root must be a directory: %s", summary.SourceRoot), pathErrOpts)
		add("source.file.stat", false, "Stat(source_root)", reportErr, "")
		_ = w.WritePreflight(ctx, rec)
		return summary, reportErr
	}
	add("source.file.stat", true, "Stat(source_root)", nil, "")

	if err := summarizeFileSource(ctx, summary.SourceRoot, srcCfg, &summary); err != nil {
		reportErr := reflowpkg.NewPathError("source root could not be enumerated", err.Error(), pathErrOpts)
		add("source.file.enumerate", false, "Walk(source_root)", reportErr, "")
		_ = w.WritePreflight(ctx, rec)
		return summary, reportErr
	}
	add("source.file.enumerate", true, "Walk(source_root)", nil, fmt.Sprintf("files=%d bytes=%d", summary.FileCount, summary.TotalBytes))

	if dest.Provider == string(provider.ProviderFile) {
		destRoot := filepath.Clean(dest.BaseDir)
		if pathWithinRoot(summary.SourceRoot, destRoot) || pathWithinRoot(destRoot, summary.SourceRoot) {
			err := reflowpkg.NewPathError("file source and destination paths overlap", fmt.Sprintf("file source and destination paths overlap: source=%s dest=%s", summary.SourceRoot, destRoot), pathErrOpts)
			add("destination.file.self_copy", false, "Compare(source_root,dest_root)", err, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, err
		}
		add("destination.file.self_copy", true, "Compare(source_root,dest_root)", nil, "")

		if err := ensureDirWritable(destRoot); err != nil {
			reportErr := reflowpkg.NewPathError("file destination is not writable", err.Error(), pathErrOpts)
			add("destination.file.write", false, "CreateTemp(dest_root)", reportErr, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, reportErr
		}
		add("destination.file.write", true, "CreateTemp(dest_root)", nil, "")

		free, err := availableBytes(destRoot)
		if err != nil {
			reportErr := reflowpkg.NewPathError("file destination space could not be checked", err.Error(), pathErrOpts)
			add("destination.file.space", false, "Statfs(dest_root)", reportErr, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, reportErr
		}
		if summary.TotalBytes > free {
			err := fmt.Errorf("insufficient destination free space: source_bytes=%d free_bytes=%d", summary.TotalBytes, free)
			add("destination.file.space", false, "Statfs(dest_root)", err, "")
			_ = w.WritePreflight(ctx, rec)
			return summary, err
		}
		add("destination.file.space", true, "Statfs(dest_root)", nil, fmt.Sprintf("source_bytes=%d free_bytes=%d", summary.TotalBytes, free))
	}

	_ = w.WritePreflight(ctx, rec)
	return summary, nil
}

func summarizeFileSource(ctx context.Context, root string, srcCfg reflowSourceConfig, summary *reflowFilePreflightSummary) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if fileSourceSkipped(key, srcCfg) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			if srcCfg.Symlinks == reflowSymlinkSkip || srcCfg.Symlinks == "" {
				return nil
			}
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if info.Mode().IsRegular() {
			summary.FileCount++
			summary.TotalBytes += info.Size()
		}
		return nil
	})
}

func ensureDirWritable(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".gonimbus-preflight-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if rerr := os.Remove(name); rerr != nil && err == nil {
		err = rerr
	}
	return err
}

func emitReflowSourceRunRecord(ctx context.Context, w interface {
	WriteAny(context.Context, string, any) error
}, state reflowStateStore, parsed *uri.ObjectURI) {
	if parsed == nil {
		return
	}
	rec := reflowpkg.SourceRunRecord{Provider: parsed.Provider, Bucket: parsed.Bucket, URI: parsed.String()}
	if parsed.Provider == string(provider.ProviderFile) {
		rec.Bucket = reflowpkg.SourceBucketFile
		rec.Root = filepath.Clean(parsed.Key)
		rec.URI = "file://local/"
		rec.OutputOnly = true
	}
	_ = w.WriteAny(ctx, reflowpkg.SourceRecordType, rec)
	if err := state.SetSourceMetadata(ctx, rec.Provider, rec.Bucket, rec.Root, parsed.String()); err != nil {
		observability.CLILogger.Debug("Checkpoint source metadata write failed", zap.Error(err))
	}
}

func enqueueReflowLine(ctx context.Context, line string, srcIdentity string, srcCfg reflowSourceConfig, getProviders func(srcURI *uri.ObjectURI) (provider.Provider, provider.Provider, error), out chan<- reflowTask) (string, error) {
	// JSONL: index object record.
	if strings.HasPrefix(line, "{") {
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return srcIdentity, err
		}
		switch env.Type {
		case "gonimbus.index.object.v1":
			var data struct {
				BaseURI      string    `json:"base_uri"`
				Key          string    `json:"key"`
				ETag         string    `json:"etag"`
				SizeBytes    int64     `json:"size_bytes"`
				LastModified time.Time `json:"last_modified"`
				RelKey       string    `json:"rel_key"`
				DeletedAt    *string   `json:"deleted_at"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcIdentity, err
			}
			if data.DeletedAt != nil {
				return srcIdentity, fmt.Errorf("deleted objects are not supported in reflow input")
			}
			base, err := uri.ParseURI(data.BaseURI)
			if err != nil {
				return srcIdentity, fmt.Errorf("invalid base_uri: %w", err)
			}
			if base.Provider != string(provider.ProviderS3) && base.Provider != string(provider.ProviderFile) {
				return srcIdentity, fmt.Errorf("unsupported provider %q", base.Provider)
			}
			identity := reflowSourceIdentity(base)
			if srcIdentity == "" {
				srcIdentity = identity
			} else if srcIdentity != identity {
				return srcIdentity, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcIdentity)
			}
			_, _, err = getProviders(base)
			if err != nil {
				return srcIdentity, err
			}
			key := strings.TrimPrefix(data.Key, "/")
			if key == "" {
				key = strings.TrimPrefix(data.RelKey, "/")
			}
			if key == "" {
				return srcIdentity, fmt.Errorf("missing key in index record")
			}
			srcURI := fmt.Sprintf("%s://%s/%s", base.Provider, base.Bucket, key)
			sourceBucket := base.Bucket
			sourceRoot := ""
			sourceCheckpoint := srcURI
			if base.Provider == string(provider.ProviderFile) {
				sourceBucket = reflowpkg.SourceBucketFile
				sourceRoot = base.Key
				sourceCheckpoint = fileCheckpointSourceURI(key)
				srcURI = fileURI(filepath.Join(sourceRoot, filepath.FromSlash(key)))
			}
			select {
			case out <- reflowTask{SourceProvider: base.Provider, SourceBucket: sourceBucket, SourceRoot: sourceRoot, SourceURI: srcURI, SourceCheckpoint: sourceCheckpoint, SourceKey: key, SourceETag: data.ETag, SourceSize: data.SizeBytes, SourceLastMod: data.LastModified, RejectSymlinkPath: base.Provider == string(provider.ProviderFile)}:
				return srcIdentity, nil
			case <-ctx.Done():
				return srcIdentity, ctx.Err()
			}
		case "gonimbus.reflow.input.v1":
			var data struct {
				SourceURI        string            `json:"source_uri"`
				SourceKey        string            `json:"source_key"`
				SourceETag       string            `json:"source_etag"`
				SourceSize       int64             `json:"source_size_bytes"`
				SourceLastMod    time.Time         `json:"source_last_modified"`
				Vars             map[string]string `json:"vars"`
				Probe            *probe.ProbeAudit `json:"probe"`
				DestRelKey       string            `json:"dest_rel_key"`
				RoutingClass     string            `json:"routing_class"`
				QuarantinePrefix string            `json:"quarantine_prefix"`
			}
			if err := json.Unmarshal(env.Data, &data); err != nil {
				return srcIdentity, err
			}
			if strings.TrimSpace(data.SourceURI) == "" {
				return srcIdentity, fmt.Errorf("missing data.source_uri")
			}
			u, err := uri.ParseURI(data.SourceURI)
			if err != nil {
				return srcIdentity, err
			}
			if u.Provider != string(provider.ProviderS3) && u.Provider != string(provider.ProviderFile) {
				return srcIdentity, fmt.Errorf("unsupported provider %q", u.Provider)
			}
			if u.IsPrefix() || u.IsPattern() {
				return srcIdentity, fmt.Errorf("reflow input source_uri must be an exact object URI")
			}
			sourceProviderURI := u
			key := u.Key
			srcURI := fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key)
			sourceBucket := u.Bucket
			sourceRoot := ""
			sourceCheckpoint := srcURI
			if u.Provider == string(provider.ProviderFile) {
				sourceBucket = reflowpkg.SourceBucketFile
				sourceRoot, key, err = fileReflowInputRootAndKey(u.Key, data.SourceKey)
				if err != nil {
					return srcIdentity, err
				}
				sourceCheckpoint = fileCheckpointSourceURI(key)
				srcURI = fileURI(filepath.Join(sourceRoot, filepath.FromSlash(key)))
				sourceProviderURI = &uri.ObjectURI{Provider: string(provider.ProviderFile), Bucket: reflowpkg.SourceBucketFile, Key: sourceRoot}
			} else if strings.TrimSpace(data.SourceKey) != "" {
				key = strings.TrimPrefix(strings.TrimSpace(data.SourceKey), "/")
				srcURI = fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key)
				sourceCheckpoint = srcURI
			}
			identity := reflowSourceIdentity(sourceProviderURI)
			if srcIdentity == "" {
				srcIdentity = identity
			} else if srcIdentity != identity {
				return srcIdentity, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcIdentity)
			}
			_, _, err = getProviders(sourceProviderURI)
			if err != nil {
				return srcIdentity, err
			}
			destRel := strings.Trim(strings.TrimSpace(data.DestRelKey), "/")
			routingClass := strings.TrimSpace(data.RoutingClass)
			if routingClass == "" {
				routingClass = "normal"
			}
			switch routingClass {
			case "normal", "quarantine":
				// ok
			default:
				return srcIdentity, fmt.Errorf("unsupported routing_class %q", data.RoutingClass)
			}
			quarantinePrefix := strings.Trim(strings.TrimSpace(data.QuarantinePrefix), "/")
			if routingClass == "quarantine" && quarantinePrefix == "" {
				return srcIdentity, fmt.Errorf("quarantine_prefix is required when routing_class=quarantine")
			}
			if routingClass == "quarantine" && !isRelativeQuarantinePrefix(data.QuarantinePrefix) {
				return srcIdentity, fmt.Errorf("quarantine_prefix must be a relative destination prefix")
			}
			select {
			case out <- reflowTask{SourceProvider: u.Provider, SourceBucket: sourceBucket, SourceRoot: sourceRoot, SourceURI: srcURI, SourceCheckpoint: sourceCheckpoint, SourceKey: key, SourceETag: data.SourceETag, SourceSize: data.SourceSize, SourceLastMod: data.SourceLastMod, Vars: data.Vars, Probe: data.Probe, DestRelKey: destRel, RoutingClass: routingClass, QuarantinePrefix: quarantinePrefix, RejectSymlinkPath: u.Provider == string(provider.ProviderFile)}:
				return srcIdentity, nil
			case <-ctx.Done():
				return srcIdentity, ctx.Err()
			}
		default:
			return srcIdentity, fmt.Errorf("unsupported json record type %q", env.Type)
		}
	}

	parsed, err := uri.ParseURI(line)
	if err != nil {
		return srcIdentity, err
	}
	if parsed.Provider != string(provider.ProviderS3) && parsed.Provider != string(provider.ProviderFile) {
		return srcIdentity, fmt.Errorf("unsupported provider %q", parsed.Provider)
	}
	identity := reflowSourceIdentity(parsed)
	if srcIdentity == "" {
		srcIdentity = identity
	} else if srcIdentity != identity {
		return srcIdentity, fmt.Errorf("multiple source roots are not supported: got %q expected %q", identity, srcIdentity)
	}
	prov, _, err := getProviders(parsed)
	if err != nil {
		return srcIdentity, err
	}

	if parsed.Provider == string(provider.ProviderFile) {
		return enqueueFileReflowSource(ctx, parsed, srcCfg, srcIdentity, out)
	}

	if !parsed.IsPrefix() && !parsed.IsPattern() {
		select {
		case out <- reflowTask{SourceProvider: parsed.Provider, SourceBucket: parsed.Bucket, SourceURI: parsed.String(), SourceKey: parsed.Key}:
			return srcIdentity, nil
		case <-ctx.Done():
			return srcIdentity, ctx.Err()
		}
	}

	var m *match.Matcher
	if parsed.IsPattern() {
		matcher, err := match.New(match.Config{Includes: []string{parsed.Pattern}})
		if err != nil {
			return srcIdentity, err
		}
		m = matcher
	}

	var token string
	for {
		res, err := prov.List(ctx, provider.ListOptions{Prefix: parsed.Key, ContinuationToken: token})
		if err != nil {
			return srcIdentity, err
		}
		for _, obj := range res.Objects {
			if m != nil && !m.Match(obj.Key) {
				continue
			}
			uri := fmt.Sprintf("%s://%s/%s", parsed.Provider, parsed.Bucket, obj.Key)
			select {
			case out <- reflowTask{SourceProvider: parsed.Provider, SourceBucket: parsed.Bucket, SourceURI: uri, SourceKey: obj.Key, SourceETag: obj.ETag, SourceSize: obj.Size, SourceLastMod: obj.LastModified}:
				// ok
			case <-ctx.Done():
				return srcIdentity, ctx.Err()
			}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}
	return srcIdentity, nil
}

func enqueueFileReflowSource(ctx context.Context, parsed *uri.ObjectURI, srcCfg reflowSourceConfig, srcIdentity string, out chan<- reflowTask) (string, error) {
	pathErrOpts := reflowpkg.PathErrorOptions{Verbose: verbose}
	st, err := os.Stat(parsed.Key)
	if err != nil {
		return srcIdentity, reflowpkg.NewPathError("source root is not accessible", err.Error(), pathErrOpts)
	}
	if !st.IsDir() {
		return srcIdentity, reflowpkg.NewPathError("file source root must be a directory", fmt.Sprintf("file source root must be a directory: %s", parsed.Key), pathErrOpts)
	}

	root := filepath.Clean(parsed.Key)
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		canonicalRoot = root
	}
	ancestors := map[string]bool{filepath.Clean(canonicalRoot): true}
	err = walkFileReflowDir(ctx, root, root, canonicalRoot, ancestors, srcCfg, out)
	if err != nil {
		return srcIdentity, err
	}
	return srcIdentity, nil
}

func walkFileReflowDir(ctx context.Context, root string, dir string, canonicalRoot string, ancestors map[string]bool, srcCfg reflowSourceConfig, out chan<- reflowTask) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsPermission(err) {
			return enqueueFileSourceFailure(ctx, out, root, dir, "source.read.permission_denied", srcCfg)
		}
		return enqueueFileSourceFailure(ctx, out, root, dir, "source.read.io_error", srcCfg)
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		path := filepath.Join(dir, entry.Name())
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if fileSourceSkipped(key, srcCfg) {
			continue
		}

		if entry.Type()&os.ModeSymlink != 0 {
			if srcCfg.Symlinks == reflowSymlinkSkip || srcCfg.Symlinks == "" {
				if err := enqueueFileSourceFailure(ctx, out, root, path, "source.symlink.skipped", srcCfg); err != nil {
					return err
				}
				continue
			}
			resolved, err := filepath.EvalSymlinks(path)
			if err != nil {
				if err := enqueueFileSourceFailure(ctx, out, root, path, "source.read.io_error", srcCfg); err != nil {
					return err
				}
				continue
			}
			resolved = filepath.Clean(resolved)
			if !pathWithinRoot(canonicalRoot, resolved) {
				if err := enqueueFileSourceFailure(ctx, out, root, path, "source.symlink.escapes_root", srcCfg); err != nil {
					return err
				}
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				if os.IsPermission(err) {
					return enqueueFileSourceFailure(ctx, out, root, path, "source.read.permission_denied", srcCfg)
				}
				return enqueueFileSourceFailure(ctx, out, root, path, "source.read.io_error", srcCfg)
			}
			if info.IsDir() {
				if ancestors[resolved] {
					if err := enqueueFileSourceFailure(ctx, out, root, path, "source.symlink.cycle", srcCfg); err != nil {
						return err
					}
					continue
				}
				nextAncestors := cloneAncestorSet(ancestors)
				nextAncestors[resolved] = true
				if err := walkFileReflowDir(ctx, root, path, canonicalRoot, nextAncestors, srcCfg, out); err != nil {
					return err
				}
				continue
			}
			if err := enqueueFileReflowTask(ctx, out, root, key, info); err != nil {
				return err
			}
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if os.IsPermission(err) {
				return enqueueFileSourceFailure(ctx, out, root, path, "source.read.permission_denied", srcCfg)
			}
			return enqueueFileSourceFailure(ctx, out, root, path, "source.read.io_error", srcCfg)
		}
		if info.IsDir() {
			resolved := path
			if eval, err := filepath.EvalSymlinks(path); err == nil {
				resolved = eval
			}
			nextAncestors := cloneAncestorSet(ancestors)
			nextAncestors[filepath.Clean(resolved)] = true
			if err := walkFileReflowDir(ctx, root, path, canonicalRoot, nextAncestors, srcCfg, out); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			if err := enqueueFileSourceFailure(ctx, out, root, path, "source.unsupported_type", srcCfg); err != nil {
				return err
			}
			continue
		}
		if err := enqueueFileReflowTask(ctx, out, root, key, info); err != nil {
			return err
		}
	}
	return nil
}

func enqueueFileReflowTask(ctx context.Context, out chan<- reflowTask, root string, key string, info fs.FileInfo) error {
	select {
	case out <- reflowTask{
		SourceProvider:   string(provider.ProviderFile),
		SourceBucket:     reflowpkg.SourceBucketFile,
		SourceRoot:       root,
		SourceURI:        fileURI(filepath.Join(root, filepath.FromSlash(key))),
		SourceCheckpoint: fileCheckpointSourceURI(key),
		SourceKey:        key,
		SourceSize:       info.Size(),
		SourceLastMod:    info.ModTime(),
		SourceMode:       info.Mode(),
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func enqueueFileSourceFailure(ctx context.Context, out chan<- reflowTask, root string, path string, reason string, srcCfg reflowSourceConfig) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		rel = filepath.Base(path)
	}
	key := filepath.ToSlash(rel)
	select {
	case out <- reflowTask{
		SourceProvider:   string(provider.ProviderFile),
		SourceBucket:     reflowpkg.SourceBucketFile,
		SourceRoot:       root,
		SourceURI:        fileURI(filepath.Join(root, filepath.FromSlash(key))),
		SourceCheckpoint: fileCheckpointSourceURI(key),
		SourceKey:        key,
		SourceFailure:    reason,
	}:
		if srcCfg.OnSourceFailure == reflowSourceFailFail {
			return errors.New(reason)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fileSourceExcluded(rel string, patterns []string) bool {
	rel = filepath.ToSlash(strings.TrimPrefix(rel, "/"))
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		if strings.HasSuffix(pattern, "/*") && strings.TrimSuffix(pattern, "/*") == rel {
			return true
		}
		if ok, _ := filepath.Match(pattern, rel); ok {
			return true
		}
	}
	return false
}

func fileSourceSkipped(rel string, cfg reflowSourceConfig) bool {
	rel = filepath.ToSlash(strings.TrimPrefix(rel, "/"))
	if cfg.Hidden == "" || cfg.Hidden == reflowHiddenSkip {
		for _, segment := range strings.Split(rel, "/") {
			if strings.HasPrefix(segment, ".") && segment != "." && segment != ".." {
				return true
			}
		}
	}
	return fileSourceExcluded(rel, cfg.Excludes)
}

func cloneAncestorSet(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func pathWithinRoot(root string, path string) bool {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	if path == root {
		return true
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
