package cmd

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
	enrichHeadSourceType   = "enrich_head"
	enrichHeadDefaultBatch = 100
	enrichHeadMaxRetries   = 3
)

var (
	enrichHeadProfile    string
	enrichHeadRegion     string
	enrichHeadEndpoint   string
	enrichHeadGCPProject string

	newEnrichHeadProvider = func(ctx context.Context, src *uri.ObjectURI, opts providerdispatch.SourceOptions) (provider.Provider, error) {
		return providerdispatch.NewSource(ctx, src, opts)
	}
)

var indexEnrichWithHeadCmd = &cobra.Command{
	Use:   "enrich-with-head <index-set-id>",
	Short: "Enrich indexed objects with HEAD-derived metadata",
	Args:  validateEnrichHeadArgs,
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
	indexEnrichWithHeadCmd.Flags().String("resume-run", "", "Resume a failed-resumable enrich-with-head run by run id")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadProfile, "profile", "", "AWS profile")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadRegion, "region", "", "AWS region override")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadEndpoint, "endpoint", "", "Custom S3 endpoint override")
	indexEnrichWithHeadCmd.Flags().StringVar(&enrichHeadGCPProject, "gcp-project", "", "GCP project hint for GCS")
}

type enrichHeadCheckpointPayload struct {
	Config  enrichHeadCheckpointConfig `json:"config"`
	Summary enrichHeadSummaryData      `json:"summary"`
}

type enrichHeadCheckpointConfig struct {
	IndexSetID string                    `json:"index_set_id"`
	Query      enrichHeadQueryOptions    `json:"query"`
	Provider   enrichHeadProviderOptions `json:"provider"`
}

type enrichHeadQueryOptions struct {
	Pattern        string   `json:"pattern,omitempty"`
	KeyRegex       string   `json:"key_regex,omitempty"`
	MinSize        string   `json:"min_size,omitempty"`
	MaxSize        string   `json:"max_size,omitempty"`
	StorageClasses []string `json:"storage_classes,omitempty"`
	IncludeDeleted bool     `json:"include_deleted,omitempty"`
	Parallel       int      `json:"parallel"`
}

type enrichHeadProviderOptions struct {
	Profile  string `json:"profile,omitempty"`
	Region   string `json:"region,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
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

	resumeRun, _ := cmd.Flags().GetString("resume-run")
	resumeRun = strings.TrimSpace(resumeRun)
	if resumeRun != "" {
		return runIndexEnrichWithHeadResume(ctx, cmd, args, resumeRun)
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

	checkpointCfg, params, storageFiltered, err := enrichHeadCheckpointConfigFromCommand(cmd, indexSet.IndexSetID)
	if err != nil {
		return err
	}
	fingerprint, err := checkpointFingerprint(checkpointCfg)
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

	prov, err := reconstructEnrichHeadProvider(ctx, indexSet, checkpointCfg.Provider)
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
	cmd.SilenceUsage = true

	summary, runErr := executeEnrichHead(ctx, db, prov, indexSet, candidates, cmd, stateOut, storageFiltered)
	status := enrichHeadRunStatus(summary, runErr)
	classification := classifyEnrichHeadRunError(runErr, checkpointCfg.Provider)
	if runErr != nil && classification.Resumable {
		progress := enrichHeadProgress(summary)
		payload := enrichHeadCheckpointPayload{Config: checkpointCfg, Summary: summary}
		opStore, storeErr := openDefaultOperationCheckpointStore(context.Background())
		if storeErr != nil {
			runErr = fmt.Errorf("%w; open operation checkpoint store: %v", runErr, storeErr)
		} else if writeErr := writeIndexRunCheckpoint(context.Background(), opStore, db, run.RunID, operationIndexEnrichWithHead, fingerprint, classification.Class, progress, payload); writeErr != nil {
			runErr = fmt.Errorf("%w; write operation checkpoint: %v", runErr, writeErr)
		} else {
			status = indexstore.RunStatusFailedResumable
		}
	} else if err := indexstore.UpdateIndexRunStatus(context.Background(), db, run.RunID, status, nil); err != nil && runErr == nil {
		runErr = fmt.Errorf("update enrich run status: %w", err)
	}

	summary.Status = string(status)
	ts := time.Now().UTC().Format(time.RFC3339)
	enc := json.NewEncoder(cmd.OutOrStdout())
	if err := enc.Encode(enrichHeadSummaryRecord{
		Type: "gonimbus.index.enrich_with_head.summary.v1",
		TS:   ts,
		Data: summary,
	}); err != nil && runErr == nil {
		runErr = fmt.Errorf("write summary: %w", err)
	}
	if runErr != nil && classification.Resumable && status == indexstore.RunStatusFailedResumable {
		progress := enrichHeadProgress(summary)
		writeOperationErrorSummary(cmd.ErrOrStderr(), "HEAD enrichment failed with resumable checkpoint", operationIndexEnrichWithHead, run.RunID, classification.Class, progress)
		if err := emitOperationErrorRecord(context.Background(), enc, operationIndexEnrichWithHead, run.RunID, classification.Class, progress); err != nil {
			runErr = fmt.Errorf("%w; write operation error record: %v", runErr, err)
		}
	}
	if runErr != nil {
		return runErr
	}
	if summary.Failed > 0 {
		return fmt.Errorf("HEAD enrichment completed with %d failure(s)", summary.Failed)
	}
	return nil
}

func runIndexEnrichWithHeadResume(ctx context.Context, cmd *cobra.Command, args []string, runID string) error {
	opStore, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return err
	}
	env, err := opStore.ReadCheckpoint(ctx, operationIndexEnrichWithHead, runID)
	if err != nil {
		return fmt.Errorf("read operation checkpoint: %w", err)
	}
	var payload enrichHeadCheckpointPayload
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return fmt.Errorf("parse enrich-with-head checkpoint payload: %w", err)
	}
	resolved, err := findIndexRunInDefaultIndexes(ctx, runID)
	if err != nil {
		return err
	}
	defer closeResolvedIndexRun(resolved)
	db := resolved.db
	indexSet := resolved.indexSet
	run := resolved.run

	if err := validateIndexRunResumeCandidate(run, indexSet, enrichHeadSourceType, "enrich-with-head", env.Status); err != nil {
		return err
	}
	if payload.Config.IndexSetID != run.IndexSetID {
		return opcheckpoint.ErrIdentityMismatch
	}
	if len(args) == 1 && strings.TrimSpace(args[0]) != run.IndexSetID {
		return fmt.Errorf("--resume-run %s is bound to index_set_id %s", runID, run.IndexSetID)
	}
	fingerprint, err := validateCheckpointIdentityAgainstIndexRun(ctx, db, env, operationIndexEnrichWithHead, payload.Config)
	if err != nil {
		return err
	}
	if err := opStore.ValidateIdentity(env, opcheckpoint.Identity{
		Operation:         operationIndexEnrichWithHead,
		RunID:             runID,
		ConfigFingerprint: fingerprint,
	}); err != nil {
		return err
	}

	lease, err := opStore.ClaimLease(ctx, operationIndexEnrichWithHead, runID, "gonimbus-"+uuid.NewString(), resumeLeaseTTL)
	if err != nil {
		return err
	}
	heartbeat, leaseCtx, err := startResumeLeaseHeartbeat(ctx, opStore, operationIndexEnrichWithHead, lease)
	if err != nil {
		return err
	}
	ctx = leaseCtx
	defer func() {
		_ = heartbeat.Stop()
		_ = opStore.ReleaseLease(operationIndexEnrichWithHead, *lease)
	}()
	if err := recoverIndexRunResumeCrash(context.Background(), db, run); err != nil {
		return err
	}

	params, storageFiltered, err := enrichHeadQueryParamsFromOptions(payload.Config.IndexSetID, payload.Config.Query)
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

	prov, err := reconstructEnrichHeadProvider(ctx, indexSet, payload.Config.Provider)
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

	if err := indexstore.MarkIndexRunResumingWithEvents(context.Background(), db, runID, []indexstore.RunEvent{
		indexRunLifecycleEvent(runID, "resume_started", string(opcheckpoint.ErrorClassInterrupted), time.Now().UTC()),
	}); err != nil {
		return err
	}
	cmd.SilenceUsage = true

	summary, runErr := executeEnrichHeadWithOptions(ctx, db, prov, indexSet, candidates, payload.Config.Query.Parallel, true, stateOut, storageFiltered, false)
	status := enrichHeadRunStatus(summary, runErr)
	classification := classifyEnrichHeadRunError(runErr, payload.Config.Provider)
	if runErr != nil && classification.Resumable {
		progress := enrichHeadProgress(summary)
		payload.Summary = summary
		if err := stopResumeLeaseHeartbeatBeforeFailedResumableCheckpoint(heartbeat); err != nil {
			return err
		}
		if writeErr := writeIndexRunCheckpoint(context.Background(), opStore, db, runID, operationIndexEnrichWithHead, fingerprint, classification.Class, progress, payload); writeErr != nil {
			runErr = fmt.Errorf("%w; write operation checkpoint: %v", runErr, writeErr)
		} else {
			status = indexstore.RunStatusFailedResumable
		}
	} else if runErr == nil {
		if status == indexstore.RunStatusSuccess {
			if err := stopResumeLeaseHeartbeat(heartbeat); err != nil {
				return err
			}
		}
		if err := indexstore.UpdateIndexRunStatusWithEvents(context.Background(), db, runID, status, nil, []indexstore.RunEvent{
			indexRunLifecycleEvent(runID, "resume_completed", "", time.Now().UTC()),
		}); err != nil {
			runErr = fmt.Errorf("update enrich run status: %w", err)
		}
	} else if err := indexstore.UpdateIndexRunStatus(context.Background(), db, runID, status, nil); err != nil {
		runErr = fmt.Errorf("update enrich run status: %w", err)
	}

	summary.Status = string(status)
	enc := json.NewEncoder(cmd.OutOrStdout())
	if err := enc.Encode(enrichHeadSummaryRecord{
		Type: "gonimbus.index.enrich_with_head.summary.v1",
		TS:   time.Now().UTC().Format(time.RFC3339),
		Data: summary,
	}); err != nil && runErr == nil {
		runErr = fmt.Errorf("write summary: %w", err)
	}
	if runErr != nil && classification.Resumable && status == indexstore.RunStatusFailedResumable {
		progress := enrichHeadProgress(summary)
		writeOperationErrorSummary(cmd.ErrOrStderr(), "HEAD enrichment resume failed with resumable checkpoint", operationIndexEnrichWithHead, runID, classification.Class, progress)
		if err := emitOperationErrorRecord(context.Background(), enc, operationIndexEnrichWithHead, runID, classification.Class, progress); err != nil {
			runErr = fmt.Errorf("%w; write operation error record: %v", runErr, err)
		}
	}
	if runErr != nil {
		return runErr
	}
	if summary.Failed > 0 {
		return fmt.Errorf("HEAD enrichment completed with %d failure(s)", summary.Failed)
	}

	env.Status = opcheckpoint.StatusSuccess
	env.Progress = enrichHeadProgress(summary)
	env.Events = append(env.Events, opcheckpoint.CheckpointEvent{Type: "resume_completed", At: time.Now().UTC()})
	rawPayload, err := json.Marshal(enrichHeadCheckpointPayload{Config: payload.Config, Summary: summary})
	if err != nil {
		return fmt.Errorf("marshal completed checkpoint payload: %w", err)
	}
	env.Payload = rawPayload
	if err := opStore.WriteCheckpoint(context.Background(), *env); err != nil {
		return fmt.Errorf("write completed checkpoint: %w", err)
	}
	return nil
}

func validateEnrichHeadArgs(cmd *cobra.Command, args []string) error {
	resumeRun, _ := cmd.Flags().GetString("resume-run")
	if strings.TrimSpace(resumeRun) != "" {
		if len(args) > 1 {
			return fmt.Errorf("accepts at most one index-set-id with --resume-run")
		}
		return nil
	}
	return cobra.ExactArgs(1)(cmd, args)
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

func enrichHeadCheckpointConfigFromCommand(cmd *cobra.Command, indexSetID string) (enrichHeadCheckpointConfig, indexstore.QueryParams, bool, error) {
	opts, err := enrichHeadQueryOptionsFromCommand(cmd)
	if err != nil {
		return enrichHeadCheckpointConfig{}, indexstore.QueryParams{}, false, err
	}
	params, storageFiltered, err := enrichHeadQueryParamsFromOptions(indexSetID, opts)
	if err != nil {
		return enrichHeadCheckpointConfig{}, indexstore.QueryParams{}, false, err
	}
	cfg := enrichHeadCheckpointConfig{
		IndexSetID: indexSetID,
		Query:      opts,
		Provider: enrichHeadProviderOptions{
			Profile:  strings.TrimSpace(enrichHeadProfile),
			Region:   strings.TrimSpace(enrichHeadRegion),
			Endpoint: strings.TrimSpace(enrichHeadEndpoint),
		},
	}
	return cfg, params, storageFiltered, nil
}

func enrichHeadQueryOptionsFromCommand(cmd *cobra.Command) (enrichHeadQueryOptions, error) {
	pattern, _ := cmd.Flags().GetString("pattern")
	keyRegex, _ := cmd.Flags().GetString("key-regex")
	minSizeStr, _ := cmd.Flags().GetString("min-size")
	maxSizeStr, _ := cmd.Flags().GetString("max-size")
	storageClassRaw, _ := cmd.Flags().GetStringArray("storage-class")
	includeDeleted, _ := cmd.Flags().GetBool("include-deleted")
	parallel, _ := cmd.Flags().GetInt("parallel")
	if parallel <= 0 {
		return enrichHeadQueryOptions{}, fmt.Errorf("--parallel must be greater than zero")
	}
	storageClasses, err := parseStorageClassFilterValues(storageClassRaw)
	if err != nil {
		return enrichHeadQueryOptions{}, err
	}
	return enrichHeadQueryOptions{
		Pattern:        strings.TrimSpace(pattern),
		KeyRegex:       strings.TrimSpace(keyRegex),
		MinSize:        strings.TrimSpace(minSizeStr),
		MaxSize:        strings.TrimSpace(maxSizeStr),
		StorageClasses: storageClasses,
		IncludeDeleted: includeDeleted,
		Parallel:       parallel,
	}, nil
}

func enrichHeadQueryParamsFromOptions(indexSetID string, opts enrichHeadQueryOptions) (indexstore.QueryParams, bool, error) {
	params := indexstore.QueryParams{
		IndexSetID:     indexSetID,
		Pattern:        opts.Pattern,
		KeyRegex:       opts.KeyRegex,
		IncludeDeleted: opts.IncludeDeleted,
	}
	params.StorageClasses = opts.StorageClasses
	if opts.MinSize != "" {
		minSize, err := match.ParseSize(opts.MinSize)
		if err != nil {
			return params, false, fmt.Errorf("invalid --min-size: %w", err)
		}
		params.MinSize = minSize
	}
	if opts.MaxSize != "" {
		maxSize, err := match.ParseSize(opts.MaxSize)
		if err != nil {
			return params, false, fmt.Errorf("invalid --max-size: %w", err)
		}
		params.MaxSize = maxSize
	}
	return params, len(opts.StorageClasses) > 0, nil
}

func reconstructEnrichHeadProvider(ctx context.Context, indexSet *indexstore.IndexSet, opts enrichHeadProviderOptions) (provider.Provider, error) {
	if indexSet == nil {
		return nil, fmt.Errorf("index_set is nil")
	}
	parsed, err := uri.ParseURI(indexSet.BaseURI)
	if err != nil {
		return nil, fmt.Errorf("parse index base_uri: %w", err)
	}
	parsed.Provider = indexSet.Provider
	region := strings.TrimSpace(opts.Region)
	if region == "" {
		region = indexSet.Region
	}
	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" {
		endpoint = indexSet.Endpoint
	}
	return newEnrichHeadProvider(ctx, parsed, providerdispatch.SourceOptions{
		Command: "index enrich-with-head",
		S3: providerdispatch.S3Options{
			Region:         region,
			Profile:        strings.TrimSpace(opts.Profile),
			Endpoint:       endpoint,
			ForcePathStyle: endpoint != "",
		},
		GCS: providerdispatch.GCSOptions{
			Project: strings.TrimSpace(enrichHeadGCPProject),
		},
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
	return executeEnrichHeadWithOptions(ctx, db, prov, indexSet, candidates, parallel, resume, stateOut, storageFiltered, false)
}

func executeEnrichHeadWithOptions(ctx context.Context, db *sql.DB, prov provider.Provider, indexSet *indexstore.IndexSet, candidates []indexstore.HeadEnrichmentCandidate, parallel int, resume bool, stateOut *os.File, storageFiltered bool, legacyRefreshTextFallback bool) (enrichHeadSummaryData, error) {
	if parallel <= 0 {
		return enrichHeadSummaryData{}, fmt.Errorf("--parallel must be greater than zero")
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()

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
				results <- enrichOneHead(workCtx, prov, indexSet, candidate)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, candidate := range candidates {
			select {
			case <-workCtx.Done():
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
	var fatalErr error
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
			if classification := classifyEnrichHeadFatalError(result.err, legacyRefreshTextFallback); classification.Resumable {
				if fatalErr == nil {
					fatalErr = result.err
					cancelWork()
				}
			}
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
	if fatalErr != nil {
		return summary, fatalErr
	}
	if ctx.Err() != nil {
		return summary, ctx.Err()
	}
	return summary, nil
}

func classifyEnrichHeadRunError(err error, _ enrichHeadProviderOptions) opcheckpoint.Classification {
	return classifyEnrichHeadFatalError(err, false)
}

func classifyEnrichHeadFatalError(err error, legacyRefreshTextFallback bool) opcheckpoint.Classification {
	if err == nil {
		return opcheckpoint.Classification{Class: opcheckpoint.ErrorClassRuntimeFailure, Resumable: false}
	}
	return opcheckpoint.ClassifyFatalError(err, opcheckpoint.ClassifierInput{
		RefreshableCredentials: legacyRefreshTextFallback,
		Interrupted:            errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
	})
}

func enrichHeadProgress(summary enrichHeadSummaryData) map[string]int64 {
	return map[string]int64{
		"candidates":     summary.Candidates,
		"enriched":       summary.Enriched,
		"resume_skipped": summary.ResumeSkipped,
		"failed":         summary.Failed,
		"head_calls":     summary.HeadCalls,
	}
}

func indexRunLifecycleEvent(runID, eventType, detail string, at time.Time) indexstore.RunEvent {
	var detailPtr *string
	if detail != "" {
		detailPtr = &detail
	}
	return indexstore.RunEvent{
		EventID:       "evt_" + uuid.NewString(),
		RunID:         runID,
		OccurredAt:    at,
		EventType:     eventType,
		EventCategory: string(indexstore.EventCategoryInfo),
		Detail:        detailPtr,
	}
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
		sleep += enrichHeadRetryJitter()
		select {
		case <-ctx.Done():
			return enrichHeadResult{candidate: candidate, status: "failed", attempts: attempt, headCalls: int64(attempt), errCode: "interrupted", err: ctx.Err()}
		case <-time.After(sleep):
		}
	}
	return enrichHeadResult{candidate: candidate, status: "failed", attempts: enrichHeadMaxRetries, headCalls: enrichHeadMaxRetries, errCode: "unknown", err: sanitizeProviderError(lastErr)}
}

func enrichHeadRetryJitter() time.Duration {
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(50))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64()) * time.Millisecond
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
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600) // #nosec G304 -- state-out is an explicit operator CLI output path.
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
