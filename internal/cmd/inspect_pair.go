package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
	inspectPairRecordType  = "gonimbus.inspect.pair.v1"
	inspectPairSummaryType = "gonimbus.inspect.pair.summary.v1"
)

var inspectPairCmd = &cobra.Command{
	Use:   "inspect-pair",
	Short: "Verify reflow destination writes from audit JSONL",
	Long: `Verify destination objects claimed by transfer reflow audit JSONL.

inspect-pair is audit-driven: it reads gonimbus.reflow.v1 records, validates
each destination URI against an operator-supplied expected destination prefix,
then HEADs in-scope claimed writes and emits JSONL verdict records.`,
	Args: cobra.NoArgs,
	RunE: runInspectPairCommand,
}

var (
	inspectPairStdin                bool
	inspectPairFromReflow           string
	inspectPairExpectedDestPrefixes []string
	inspectPairRegion               string
	inspectPairProfile              string
	inspectPairEndpoint             string
	inspectPairGCPProject           string
)

type inspectPairProviderFactory func(context.Context, inspectPairScope) (provider.Provider, error)

var newInspectPairProvider inspectPairProviderFactory = func(ctx context.Context, scope inspectPairScope) (provider.Provider, error) {
	return providerdispatch.NewDestination(ctx, providerdispatch.DestinationOptions{
		Command:     "inspect-pair",
		Provider:    scope.Provider,
		S3Bucket:    scope.Bucket,
		S3Prefix:    scope.Prefix,
		GCSBucket:   scope.Bucket,
		GCSPrefix:   scope.Prefix,
		FileBaseDir: scope.FileRoot,
		S3: providerdispatch.S3Options{
			Region:         inspectPairRegion,
			Endpoint:       inspectPairEndpoint,
			Profile:        inspectPairProfile,
			ForcePathStyle: inspectPairEndpoint != "",
		},
		GCS: providerdispatch.GCSOptions{
			Project: inspectPairGCPProject,
		},
	})
}

func init() {
	rootCmd.AddCommand(inspectPairCmd)

	inspectPairCmd.Flags().BoolVar(&inspectPairStdin, "stdin", false, "Read reflow audit JSONL from stdin")
	inspectPairCmd.Flags().StringVar(&inspectPairFromReflow, "from-reflow", "", "Read reflow audit JSONL from a file")
	inspectPairCmd.Flags().StringArrayVar(&inspectPairExpectedDestPrefixes, "expected-dest-prefix", nil, "Allowed destination scope URI (repeatable)")
	inspectPairCmd.Flags().StringVarP(&inspectPairRegion, "region", "r", "", "AWS region for destination HEADs")
	inspectPairCmd.Flags().StringVarP(&inspectPairProfile, "profile", "p", "", "AWS profile for destination HEADs")
	inspectPairCmd.Flags().StringVar(&inspectPairEndpoint, "endpoint", "", "Custom S3 endpoint for destination HEADs")
	inspectPairCmd.Flags().StringVar(&inspectPairGCPProject, "gcp-project", "", "GCP project hint for GCS destination HEADs")
}

type inspectPairOptions struct {
	UseStdin             bool
	FromReflow           string
	ExpectedDestPrefixes []string
	ProviderFactory      inspectPairProviderFactory
}

type inspectPairScope struct {
	Provider string
	Bucket   string
	Prefix   string
	FileRoot string
}

func (s inspectPairScope) cacheKey() string {
	switch s.Provider {
	case string(provider.ProviderFile):
		return s.Provider + ":" + s.FileRoot
	default:
		return s.Provider + ":" + s.Bucket + "/" + s.Prefix
	}
}

type inspectPairReflowRecord struct {
	SourceURI  string                      `json:"source_uri"`
	DestURI    string                      `json:"dest_uri"`
	SourceETag string                      `json:"source_etag,omitempty"`
	SourceSize int64                       `json:"source_size_bytes,omitempty"`
	Status     string                      `json:"status"`
	Reason     string                      `json:"reason,omitempty"`
	Collision  *inspectPairReflowCollision `json:"collision,omitempty"`
}

type inspectPairReflowCollision struct {
	Kind string `json:"kind"`
}

type inspectPairRawReflowEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type inspectPairRecord struct {
	SourceURI          string `json:"source_uri,omitempty"`
	DestURI            string `json:"dest_uri,omitempty"`
	Verdict            string `json:"verdict"`
	SourceSizeBytes    int64  `json:"source_size_bytes"`
	DestSizeBytes      int64  `json:"dest_size_bytes"`
	SourceETag         string `json:"source_etag,omitempty"`
	DestETagObserved   string `json:"dest_etag_observed,omitempty"`
	ETagComparable     bool   `json:"etag_comparable"`
	Reason             string `json:"reason,omitempty"`
	UpstreamStatus     string `json:"upstream_status,omitempty"`
	UpstreamReason     string `json:"upstream_reason,omitempty"`
	ExpectedDestPrefix string `json:"expected_dest_prefix,omitempty"`
}

type inspectPairSummary struct {
	Total                   int64 `json:"total"`
	Verified                int64 `json:"verified"`
	VerifiedSizeETagDiffers int64 `json:"verified_size_etag_differs"`
	SizeMismatch            int64 `json:"size_mismatch"`
	Missing                 int64 `json:"missing"`
	Error                   int64 `json:"error"`
	InvalidDest             int64 `json:"invalid_dest"`
	NotVerified             int64 `json:"not_verified"`
	IgnoredNonterminal      int64 `json:"ignored_nonterminal"`
}

func runInspectPairCommand(cmd *cobra.Command, _ []string) error {
	opts := inspectPairOptions{
		UseStdin:             inspectPairStdin,
		FromReflow:           inspectPairFromReflow,
		ExpectedDestPrefixes: append([]string(nil), inspectPairExpectedDestPrefixes...),
		ProviderFactory:      newInspectPairProvider,
	}
	if err := runInspectPair(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), opts); err != nil {
		return err
	}
	return nil
}

func runInspectPair(ctx context.Context, stdin io.Reader, stdout io.Writer, opts inspectPairOptions) error {
	if opts.UseStdin == (strings.TrimSpace(opts.FromReflow) != "") {
		return exitError(foundry.ExitInvalidArgument, "Invalid inspect-pair input", fmt.Errorf("set exactly one of --stdin or --from-reflow"))
	}
	if len(opts.ExpectedDestPrefixes) == 0 {
		return exitError(foundry.ExitInvalidArgument, "Invalid inspect-pair scope", fmt.Errorf("--expected-dest-prefix is required"))
	}
	factory := opts.ProviderFactory
	if factory == nil {
		factory = newInspectPairProvider
	}
	scopes, err := parseInspectPairScopes(opts.ExpectedDestPrefixes)
	if err != nil {
		return exitError(foundry.ExitInvalidArgument, "Invalid --expected-dest-prefix", err)
	}

	in := stdin
	var f *os.File
	if strings.TrimSpace(opts.FromReflow) != "" {
		f, err = os.Open(opts.FromReflow) // #nosec G304 -- user-supplied input file path is the command's explicit data source.
		if err != nil {
			return exitError(foundry.ExitFileReadError, "Failed to read reflow audit", err)
		}
		defer func() { _ = f.Close() }()
		in = f
	}

	w := output.NewJSONLWriter(stdout, uuid.New().String(), "inspect-pair")
	defer func() { _ = w.Close() }()

	providers := map[string]provider.Provider{}
	defer func() {
		for _, p := range providers {
			_ = p.Close()
		}
	}()

	summary := inspectPairSummary{}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		rec, ok, err := parseInspectPairReflowLine(line)
		if err != nil {
			return exitError(foundry.ExitInvalidArgument, "Invalid reflow audit record", err)
		}
		if !ok {
			continue
		}
		if isInspectPairNonterminalStatus(rec.Status) {
			summary.IgnoredNonterminal++
			continue
		}

		out, matchedScope, shouldHead := inspectPairRecordForReflow(rec, scopes)
		if !shouldHead {
			summary.add(out.Verdict)
			if err := w.WriteAny(ctx, inspectPairRecordType, out); err != nil {
				return err
			}
			continue
		}

		prov, err := inspectPairProviderForScope(ctx, matchedScope, providers, factory)
		if err != nil {
			return exitError(foundry.ExitExternalServiceUnavailable, "Failed to connect to destination provider", err)
		}
		completeInspectPairHead(ctx, prov, rec, matchedScope, &out)
		summary.add(out.Verdict)
		if err := w.WriteAny(ctx, inspectPairRecordType, out); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return exitError(foundry.ExitFileReadError, "Failed to read reflow audit", err)
	}
	if err := w.WriteAny(ctx, inspectPairSummaryType, summary); err != nil {
		return err
	}
	if summary.hasFailure() {
		return exitError(foundry.ExitExternalServiceUnavailable, "inspect-pair completed with verification failures", fmt.Errorf("size_mismatch=%d missing=%d error=%d invalid_dest=%d", summary.SizeMismatch, summary.Missing, summary.Error, summary.InvalidDest))
	}
	return nil
}

func parseInspectPairScopes(rawScopes []string) ([]inspectPairScope, error) {
	scopes := make([]inspectPairScope, 0, len(rawScopes))
	for _, raw := range rawScopes {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, fmt.Errorf("empty destination scope")
		}
		scope, err := parseInspectPairScope(raw)
		if err != nil {
			return nil, err
		}
		if scope.Provider == string(provider.ProviderFile) {
			abs, err := filepath.Abs(scope.FileRoot)
			if err != nil {
				return nil, err
			}
			scope.FileRoot = filepath.Clean(abs)
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

func parseInspectPairScope(raw string) (inspectPairScope, error) {
	parsedURI, err := uri.ParseURI(raw)
	if err == nil && parsedURI.Provider == string(provider.ProviderGCS) {
		if !parsedURI.IsPrefix() {
			return inspectPairScope{}, fmt.Errorf("expected destination scope prefix URI ending with '/'")
		}
		return inspectPairScope{Provider: parsedURI.Provider, Bucket: parsedURI.Bucket, Prefix: parsedURI.Key}, nil
	}
	parsed, err := parseReflowDest(raw)
	if err != nil {
		return inspectPairScope{}, err
	}
	return inspectPairScope{Provider: parsed.Provider, Bucket: parsed.Bucket, Prefix: parsed.Prefix, FileRoot: parsed.BaseDir}, nil
}

func parseInspectPairReflowLine(line string) (inspectPairReflowRecord, bool, error) {
	var env inspectPairRawReflowEnvelope
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		return inspectPairReflowRecord{}, false, err
	}
	if env.Type != reflowpkg.RecordType {
		return inspectPairReflowRecord{}, false, nil
	}
	var rec inspectPairReflowRecord
	if err := json.Unmarshal(env.Data, &rec); err != nil {
		return inspectPairReflowRecord{}, false, err
	}
	rec.Status = strings.TrimSpace(rec.Status)
	return rec, true, nil
}

func inspectPairRecordForReflow(rec inspectPairReflowRecord, scopes []inspectPairScope) (inspectPairRecord, inspectPairScope, bool) {
	out := inspectPairRecord{
		SourceURI:       rec.SourceURI,
		DestURI:         rec.DestURI,
		SourceSizeBytes: rec.SourceSize,
		SourceETag:      rec.SourceETag,
		UpstreamStatus:  rec.Status,
		UpstreamReason:  rec.Reason,
	}
	if rec.Status == "skipped" || rec.Status == "failed" {
		out.Verdict = "not_verified"
		out.Reason = rec.Reason
		return out, inspectPairScope{}, false
	}
	if !isInspectPairWriteClaim(rec) {
		out.Verdict = "not_verified"
		out.Reason = "status_not_write_claim"
		return out, inspectPairScope{}, false
	}

	dest, err := uri.ParseURI(rec.DestURI)
	if err != nil {
		out.Verdict = "invalid_dest"
		out.Reason = "invalid_dest_uri"
		return out, inspectPairScope{}, false
	}
	matched, ok, reason := matchInspectPairScope(dest, scopes)
	if !ok {
		out.Verdict = "invalid_dest"
		out.Reason = reason
		return out, inspectPairScope{}, false
	}
	out.ExpectedDestPrefix = inspectPairScopeString(matched)
	return out, matched, true
}

func isInspectPairNonterminalStatus(status string) bool {
	return status == "in_progress" || status == "planned"
}

func isInspectPairWriteClaim(rec inspectPairReflowRecord) bool {
	if rec.Status == "complete" || rec.Status == "quarantined" {
		return true
	}
	return rec.Collision != nil && rec.Collision.Kind == collisionOverwritten
}

func matchInspectPairScope(dest *uri.ObjectURI, scopes []inspectPairScope) (inspectPairScope, bool, string) {
	for _, scope := range scopes {
		if dest.Provider != scope.Provider {
			continue
		}
		switch scope.Provider {
		case string(provider.ProviderS3), string(provider.ProviderGCS):
			if dest.Bucket != scope.Bucket {
				continue
			}
			if strings.HasPrefix(dest.Key, scope.Prefix) {
				return scope, true, ""
			}
		case string(provider.ProviderFile):
			if inspectPairFileDestInScope(dest.Key, scope.FileRoot) {
				return scope, true, ""
			}
		}
	}
	return inspectPairScope{}, false, "outside_expected_dest_prefix"
}

func inspectPairFileDestInScope(destPath string, root string) bool {
	destAbs, err := filepath.Abs(filepath.Clean(filepath.FromSlash(destPath)))
	if err != nil {
		return false
	}
	rootAbs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return false
	}
	if !inspectPairPathWithinRoot(destAbs, rootAbs) {
		return false
	}

	evalRoot := rootAbs
	if resolved, err := filepath.EvalSymlinks(rootAbs); err == nil {
		evalRoot = resolved
	} else if _, statErr := os.Lstat(rootAbs); statErr == nil {
		return false
	}

	rel, err := filepath.Rel(rootAbs, destAbs)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	current := rootAbs
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." || part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return os.IsNotExist(err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			continue
		}
		resolved, err := filepath.EvalSymlinks(current)
		if err != nil {
			return false
		}
		if !inspectPairPathWithinRoot(resolved, evalRoot) {
			return false
		}
		if current == destAbs {
			return true
		}
	}
	return true
}

func inspectPairPathWithinRoot(pathValue string, root string) bool {
	rel, err := filepath.Rel(root, pathValue)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func inspectPairScopeString(scope inspectPairScope) string {
	if scope.Provider == string(provider.ProviderFile) {
		return fileURI(scope.FileRoot) + "/"
	}
	if scope.Provider == string(provider.ProviderGCS) {
		return fmt.Sprintf("gs://%s/%s", scope.Bucket, scope.Prefix)
	}
	return fmt.Sprintf("%s://%s/%s", scope.Provider, scope.Bucket, scope.Prefix)
}

func inspectPairProviderForScope(ctx context.Context, scope inspectPairScope, providers map[string]provider.Provider, factory inspectPairProviderFactory) (provider.Provider, error) {
	key := scope.cacheKey()
	if p, ok := providers[key]; ok {
		return p, nil
	}
	p, err := factory(ctx, scope)
	if err != nil {
		return nil, err
	}
	providers[key] = p
	return p, nil
}

func completeInspectPairHead(ctx context.Context, prov provider.Provider, rec inspectPairReflowRecord, scope inspectPairScope, out *inspectPairRecord) {
	dest, err := uri.ParseURI(rec.DestURI)
	if err != nil {
		out.Verdict = "invalid_dest"
		out.Reason = "invalid_dest_uri"
		return
	}
	key := dest.Key
	if scope.Provider == string(provider.ProviderFile) {
		rel, err := filepath.Rel(scope.FileRoot, filepath.Clean(filepath.FromSlash(dest.Key)))
		if err != nil {
			out.Verdict = "invalid_dest"
			out.Reason = "outside_expected_dest_prefix"
			return
		}
		key = filepath.ToSlash(rel)
	}
	meta, err := prov.Head(ctx, key)
	if err != nil {
		if provider.IsNotFound(err) {
			out.Verdict = "missing"
			out.Reason = "not_found"
			return
		}
		out.Verdict = "error"
		out.Reason = err.Error()
		return
	}
	out.DestSizeBytes = meta.Size
	out.DestETagObserved = meta.ETag
	out.Verdict, out.ETagComparable = inspectPairVerdict(rec.SourceSize, meta.Size, rec.SourceETag, meta.ETag)
}

func inspectPairVerdict(sourceSize int64, destSize int64, sourceETag string, destETag string) (string, bool) {
	src := normalizeInspectPairETag(sourceETag)
	dst := normalizeInspectPairETag(destETag)
	comparable := src != "" && dst != "" && !isMultipartETag(src) && !isMultipartETag(dst)
	if sourceSize != destSize {
		return "size_mismatch", comparable
	}
	if src != "" && dst != "" && src != dst {
		return "verified_size_etag_differs", comparable
	}
	return "verified", comparable
}

func normalizeInspectPairETag(etag string) string {
	etag = strings.TrimSpace(etag)
	etag = strings.TrimPrefix(etag, "W/")
	return strings.Trim(etag, `"`)
}

func isMultipartETag(etag string) bool {
	parts := strings.Split(etag, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, r := range parts[1] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *inspectPairSummary) add(verdict string) {
	s.Total++
	switch verdict {
	case "verified":
		s.Verified++
	case "verified_size_etag_differs":
		s.VerifiedSizeETagDiffers++
	case "size_mismatch":
		s.SizeMismatch++
	case "missing":
		s.Missing++
	case "error":
		s.Error++
	case "invalid_dest":
		s.InvalidDest++
	case "not_verified":
		s.NotVerified++
	}
}

func (s inspectPairSummary) hasFailure() bool {
	return s.SizeMismatch > 0 || s.Missing > 0 || s.Error > 0 || s.InvalidDest > 0
}
