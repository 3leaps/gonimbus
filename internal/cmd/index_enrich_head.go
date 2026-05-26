package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
	enrichHeadSourceType   = "enrich_head"
	enrichHeadDefaultBatch = 100
	enrichHeadMaxRetries   = 3
)

var (
	enrichHeadProfile  string
	enrichHeadRegion   string
	enrichHeadEndpoint string

	newEnrichHeadS3Provider = func(ctx context.Context, cfg providers3.Config) (provider.Provider, error) {
		return providers3.New(ctx, cfg)
	}
)

var indexEnrichWithHeadCmd = &cobra.Command{
	Use:   "enrich-with-head <index-set-id>",
	Short: "Enrich indexed objects with HEAD-derived metadata",
	Args:  cobra.ExactArgs(1),
	RunE:  runIndexEnrichWithHead,
}

func init() {
	indexCmd.AddCommand(indexEnrichWithHeadCmd)

	indexEnrichWithHeadCmd.Flags().String("pattern", "", "Doublestar glob pattern to match keys")
	indexEnrichWithHeadCmd.Flags().String("key-regex", "", "Regex pattern to match keys")
	indexEnrichWithHeadCmd.Flags().String("min-size", "", "Minimum object size (e.g., 1KB, 1MB)")
	indexEnrichWithHeadCmd.Flags().String("max-size", "", "Maximum object size (e.g., 100MB, 1GB)")
	indexEnrichWithHeadCmd.Flags().StringArray("storage-class", nil, "Storage class filter (exact, case-sensitive); comma-separated and repeatable")
	indexEnrichWithHeadCmd.Flags().Bool("include-deleted", false, "Include soft-deleted objects")
	indexEnrichWithHeadCmd.Flags().Int("parallel", 32, "Max concurrent HEAD operations")
	indexEnrichWithHeadCmd.Flags().Bool("resume", false, "Skip rows with non-null head_enriched_at")
	indexEnrichWithHeadCmd.Flags().String("state-out", "", "Write per-candidate audit JSONL to this path")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadProfile, "profile", "", "AWS profile")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadRegion, "region", "", "AWS region override")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadEndpoint, "endpoint", "", "Custom S3 endpoint override")
}

type enrichHeadStateRecord struct {
	Type string              `json:"type"`
	TS   string              `json:"ts"`
	Data enrichHeadStateData `json:"data"`
}

type enrichHeadStateData struct {
	IndexSetID     string  `json:"index_set_id"`
	RelKey         string  `json:"rel_key"`
	Key            string  `json:"key"`
	Status         string  `json:"status"`
	Attempts       int     `json:"attempts,omitempty"`
	ErrorCode      string  `json:"error_code,omitempty"`
	Error          string  `json:"error,omitempty"`
	ArchiveStatus  *string `json:"archive_status,omitempty"`
	RestoreState   *string `json:"restore_state,omitempty"`
	RestoreExpiry  *string `json:"restore_expiry,omitempty"`
	ContentType    *string `json:"content_type,omitempty"`
	HeadEnrichedAt *string `json:"head_enriched_at,omitempty"`
}

type enrichHeadSummaryRecord struct {
	Type string                `json:"type"`
	TS   string                `json:"ts"`
	Data enrichHeadSummaryData `json:"data"`
}

type enrichHeadSummaryData struct {
	IndexSetID      string `json:"index_set_id"`
	Candidates      int64  `json:"candidates"`
	Enriched        int64  `json:"enriched"`
	ResumeSkipped   int64  `json:"resume_skipped"`
	Failed          int64  `json:"failed"`
	HeadCalls       int64  `json:"head_calls"`
	StorageFiltered bool   `json:"storage_filtered"`
	Status          string `json:"status"`
}

type enrichHeadResult struct {
	candidate indexstore.HeadEnrichmentCandidate
	update    indexstore.HeadEnrichmentUpdate
	meta      *provider.ObjectMeta
	status    string
	attempts  int
	headCalls int64
	errCode   string
	err       error
}

func runIndexEnrichWithHead(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	indexSetID := strings.TrimSpace(args[0])
	db, indexSet, err := openIndexDBByID(ctx, indexSetID)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if err := rejectConcurrentBuild(ctx, db, indexSet.IndexSetID); err != nil {
		return err
	}

	params, storageFiltered, err := enrichHeadQueryParams(cmd, indexSet.IndexSetID)
	if err != nil {
		return err
	}
	candidates, stats, err := indexstore.QueryHeadEnrichmentCandidates(ctx, db, params)
	if err != nil {
		return err
	}
	if stats.TimestampParseErrors > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "warning: %d candidate timestamp parse errors\n", stats.TimestampParseErrors)
	}

	prov, err := reconstructEnrichHeadProvider(ctx, indexSet)
	if err != nil {
		return err
	}
	defer func() { _ = prov.Close() }()

	stateOut, err := openEnrichHeadStateOut(cmd)
	if err != nil {
		return err
	}
	if stateOut != nil {
		defer func() { _ = stateOut.Close() }()
	}

	run, err := indexstore.CreateIndexRun(ctx, db, indexSet.IndexSetID, enrichHeadSourceType)
	if err != nil {
		return fmt.Errorf("create enrich run: %w", err)
	}

	summary, runErr := executeEnrichHead(ctx, db, prov, indexSet, candidates, cmd, stateOut, storageFiltered)
	status := enrichHeadRunStatus(summary, runErr)
	if err := indexstore.UpdateIndexRunStatus(context.Background(), db, run.RunID, status, nil); err != nil && runErr == nil {
		runErr = fmt.Errorf("update enrich run status: %w", err)
	}

	summary.Status = string(status)
	ts := time.Now().UTC().Format(time.RFC3339)
	if err := json.NewEncoder(cmd.OutOrStdout()).Encode(enrichHeadSummaryRecord{
		Type: "gonimbus.index.enrich_with_head.summary.v1",
		TS:   ts,
		Data: summary,
	}); err != nil && runErr == nil {
		runErr = fmt.Errorf("write summary: %w", err)
	}
	if runErr != nil {
		return runErr
	}
	if summary.Failed > 0 {
		return fmt.Errorf("HEAD enrichment completed with %d failure(s)", summary.Failed)
	}
	return nil
}

func enrichHeadRunStatus(summary enrichHeadSummaryData, runErr error) indexstore.RunStatus {
	if runErr != nil {
		return indexstore.RunStatusFailed
	}
	if summary.Failed > 0 {
		return indexstore.RunStatusPartial
	}
	return indexstore.RunStatusSuccess
}

func enrichHeadQueryParams(cmd *cobra.Command, indexSetID string) (indexstore.QueryParams, bool, error) {
	pattern, _ := cmd.Flags().GetString("pattern")
	keyRegex, _ := cmd.Flags().GetString("key-regex")
	minSizeStr, _ := cmd.Flags().GetString("min-size")
	maxSizeStr, _ := cmd.Flags().GetString("max-size")
	storageClassRaw, _ := cmd.Flags().GetStringArray("storage-class")
	includeDeleted, _ := cmd.Flags().GetBool("include-deleted")

	params := indexstore.QueryParams{
		IndexSetID:     indexSetID,
		Pattern:        pattern,
		KeyRegex:       keyRegex,
		IncludeDeleted: includeDeleted,
	}
	storageClasses, err := parseStorageClassFilterValues(storageClassRaw)
	if err != nil {
		return params, false, err
	}
	params.StorageClasses = storageClasses
	if minSizeStr != "" {
		minSize, err := match.ParseSize(minSizeStr)
		if err != nil {
			return params, false, fmt.Errorf("invalid --min-size: %w", err)
		}
		params.MinSize = minSize
	}
	if maxSizeStr != "" {
		maxSize, err := match.ParseSize(maxSizeStr)
		if err != nil {
			return params, false, fmt.Errorf("invalid --max-size: %w", err)
		}
		params.MaxSize = maxSize
	}
	return params, len(storageClasses) > 0, nil
}

func reconstructEnrichHeadProvider(ctx context.Context, indexSet *indexstore.IndexSet) (provider.Provider, error) {
	if indexSet == nil {
		return nil, fmt.Errorf("index_set is nil")
	}
	parsed, err := uri.ParseURI(indexSet.BaseURI)
	if err != nil {
		return nil, fmt.Errorf("parse index base_uri: %w", err)
	}
	if parsed.Provider != string(provider.ProviderS3) || indexSet.Provider != string(provider.ProviderS3) {
		return nil, fmt.Errorf("unsupported provider for enrich-with-head: %s", indexSet.Provider)
	}
	region := strings.TrimSpace(enrichHeadRegion)
	if region == "" {
		region = indexSet.Region
	}
	endpoint := strings.TrimSpace(enrichHeadEndpoint)
	if endpoint == "" {
		endpoint = indexSet.Endpoint
	}
	return newEnrichHeadS3Provider(ctx, providers3.Config{
		Bucket:         parsed.Bucket,
		Region:         region,
		Profile:        enrichHeadProfile,
		Endpoint:       endpoint,
		ForcePathStyle: endpoint != "",
	})
}

func rejectConcurrentBuild(ctx context.Context, db *sql.DB, indexSetID string) error {
	runs, err := indexstore.ListIndexRuns(ctx, db, indexSetID)
	if err != nil {
		return err
	}
	for _, run := range runs {
		if run.Status == indexstore.RunStatusRunning && run.SourceType != enrichHeadSourceType {
			return fmt.Errorf("cannot enrich while index run %s is running", run.RunID)
		}
	}
	return nil
}

func executeEnrichHead(ctx context.Context, db *sql.DB, prov provider.Provider, indexSet *indexstore.IndexSet, candidates []indexstore.HeadEnrichmentCandidate, cmd *cobra.Command, stateOut *os.File, storageFiltered bool) (enrichHeadSummaryData, error) {
	parallel, _ := cmd.Flags().GetInt("parallel")
	resume, _ := cmd.Flags().GetBool("resume")
	if parallel <= 0 {
		return enrichHeadSummaryData{}, fmt.Errorf("--parallel must be greater than zero")
	}

	summary := enrichHeadSummaryData{
		IndexSetID:      indexSet.IndexSetID,
		Candidates:      int64(len(candidates)),
		StorageFiltered: storageFiltered,
	}
	jobs := make(chan indexstore.HeadEnrichmentCandidate)
	results := make(chan enrichHeadResult)

	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for candidate := range jobs {
				if resume && candidate.HeadEnrichedAt != nil {
					results <- enrichHeadResult{candidate: candidate, status: "resume_skipped"}
					continue
				}
				results <- enrichOneHead(ctx, prov, indexSet, candidate)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, candidate := range candidates {
			select {
			case <-ctx.Done():
				return
			case jobs <- candidate:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	var updates []indexstore.HeadEnrichmentUpdate
	for result := range results {
		summary.HeadCalls += result.headCalls
		switch result.status {
		case "success":
			summary.Enriched++
			updates = append(updates, result.update)
			if len(updates) >= enrichHeadDefaultBatch {
				if err := indexstore.BatchUpdateHeadEnrichment(ctx, db, updates); err != nil {
					return summary, err
				}
				updates = updates[:0]
			}
		case "resume_skipped":
			summary.ResumeSkipped++
		default:
			summary.Failed++
		}
		if stateOut != nil {
			if err := writeEnrichHeadState(stateOut, indexSet, result); err != nil {
				return summary, err
			}
		}
	}
	if len(updates) > 0 {
		if err := indexstore.BatchUpdateHeadEnrichment(ctx, db, updates); err != nil {
			return summary, err
		}
	}
	if ctx.Err() != nil {
		return summary, ctx.Err()
	}
	return summary, nil
}

func enrichOneHead(ctx context.Context, prov provider.Provider, indexSet *indexstore.IndexSet, candidate indexstore.HeadEnrichmentCandidate) enrichHeadResult {
	key := reconstructFullKey(indexSet.BaseURI, candidate.RelKey)
	var lastErr error
	for attempt := 1; attempt <= enrichHeadMaxRetries; attempt++ {
		meta, err := prov.Head(ctx, key)
		if err == nil {
			now := time.Now().UTC()
			return enrichHeadResult{
				candidate: candidate,
				meta:      meta,
				status:    "success",
				attempts:  attempt,
				headCalls: int64(attempt),
				update: indexstore.HeadEnrichmentUpdate{
					IndexSetID:     indexSet.IndexSetID,
					RelKey:         candidate.RelKey,
					ArchiveStatus:  nonEmptyStringPtr(meta.ArchiveStatus),
					RestoreState:   nonEmptyStringPtr(meta.RestoreState),
					RestoreExpiry:  meta.RestoreExpiry,
					ContentType:    nonEmptyStringPtr(meta.ContentType),
					HeadEnrichedAt: now,
				},
			}
		}
		lastErr = err
		code, retry := classifyEnrichHeadError(err)
		if !retry || attempt == enrichHeadMaxRetries {
			return enrichHeadResult{
				candidate: candidate,
				status:    "failed",
				attempts:  attempt,
				headCalls: int64(attempt),
				errCode:   code,
				err:       sanitizeProviderError(err),
			}
		}
		sleep := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
		sleep += time.Duration(rand.Intn(50)) * time.Millisecond
		select {
		case <-ctx.Done():
			return enrichHeadResult{candidate: candidate, status: "failed", attempts: attempt, headCalls: int64(attempt), errCode: "interrupted", err: ctx.Err()}
		case <-time.After(sleep):
		}
	}
	return enrichHeadResult{candidate: candidate, status: "failed", attempts: enrichHeadMaxRetries, headCalls: enrichHeadMaxRetries, errCode: "unknown", err: sanitizeProviderError(lastErr)}
}

func classifyEnrichHeadError(err error) (string, bool) {
	switch {
	case provider.IsThrottled(err):
		return "throttled", true
	case provider.IsProviderUnavailable(err):
		return "provider_unavailable", true
	case provider.IsAccessDenied(err):
		return "access_denied", false
	case provider.IsNotFound(err):
		return "not_found", false
	case provider.IsInvalidCredentials(err):
		return "invalid_credentials", false
	default:
		return "provider_error", false
	}
}

func sanitizeProviderError(err error) error {
	if err == nil {
		return nil
	}
	var providerErr *provider.ProviderError
	if errors.As(err, &providerErr) {
		return fmt.Errorf("%s %s failed: %v", providerErr.Provider, providerErr.Op, providerErr.Err)
	}
	return err
}

func openEnrichHeadStateOut(cmd *cobra.Command) (*os.File, error) {
	path, _ := cmd.Flags().GetString("state-out")
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
}

func writeEnrichHeadState(file *os.File, indexSet *indexstore.IndexSet, result enrichHeadResult) error {
	ts := time.Now().UTC().Format(time.RFC3339)
	data := enrichHeadStateData{
		IndexSetID: indexSet.IndexSetID,
		RelKey:     result.candidate.RelKey,
		Key:        reconstructFullKey(indexSet.BaseURI, result.candidate.RelKey),
		Status:     result.status,
		Attempts:   result.attempts,
		ErrorCode:  result.errCode,
	}
	if result.err != nil {
		msg := result.err.Error()
		data.Error = msg
	}
	if result.status == "success" {
		data.ArchiveStatus = result.update.ArchiveStatus
		data.RestoreState = result.update.RestoreState
		data.ContentType = result.update.ContentType
		enrichedAt := result.update.HeadEnrichedAt.Format(time.RFC3339)
		data.HeadEnrichedAt = &enrichedAt
		if result.update.RestoreExpiry != nil {
			restoreExpiry := result.update.RestoreExpiry.Format(time.RFC3339)
			data.RestoreExpiry = &restoreExpiry
		}
	}
	record := enrichHeadStateRecord{
		Type: "gonimbus.index.enrich_with_head.state.v1",
		TS:   ts,
		Data: data,
	}
	return json.NewEncoder(file).Encode(record)
}
