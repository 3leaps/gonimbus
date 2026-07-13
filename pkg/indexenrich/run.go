package indexenrich

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const enrichMaxRetries = 3

// enrichCandidate is a storage-neutral HEAD work unit (not the SQLite store type).
type enrichCandidate struct {
	RelKey         string
	SizeBytes      int64
	StorageClass   *string
	HeadEnrichedAt *time.Time
}

// enrichUpdate is a storage-neutral successful HEAD payload for the enrich journal.
type enrichUpdate struct {
	RelKey         string
	ArchiveStatus  *string
	RestoreState   *string
	RestoreExpiry  *time.Time
	ContentType    *string
	HeadEnrichedAt time.Time
}

// runHooks is package-private fault injection for tests. Not part of the public API.
type runHooks struct {
	afterPublishStep func(step string) error
}

// Run executes a durable enrich transaction.
func Run(ctx context.Context, cfg Config) (Result, error) {
	return run(ctx, cfg, runHooks{})
}

// runWithHooks is the test entry point for injectible publish-step faults.
func runWithHooks(ctx context.Context, cfg Config, hooks runHooks) (Result, error) {
	return run(ctx, cfg, hooks)
}

func run(ctx context.Context, cfg Config, hooks runHooks) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Base result so early failures still carry set identity for adapters.
	base := Result{
		ClassificationNote: "internal full-fidelity render; not boundary-safe or de-identified",
		Status:             "failed",
	}
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return base, err
	}
	base.IndexSetID = cfg.IndexSetID
	authority := cfg.Authority
	authorityOwned := false
	if authority == nil {
		authority, err = indexcoord.Acquire(ctx, cfg.SegmentSetRoot, cfg.IndexSetID, "enrich-"+uuid.NewString())
		if err != nil {
			return base, fmt.Errorf("acquire index set authority: %w", err)
		}
		authorityOwned = true
	} else if err := authority.AssertHeldFor(cfg.IndexSetID, cfg.SegmentSetRoot); err != nil {
		return base, fmt.Errorf("index set authority: %w", err)
	}
	if authorityOwned {
		defer func() { _ = authority.Release() }()
	}

	lease, err := indexsubstrate.AcquireWriteLease(cfg.SegmentSetRoot, cfg.IndexSetID, "enrich-"+uuid.NewString(), 0)
	if err != nil {
		return base, fmt.Errorf("acquire durable write lease: %w", err)
	}
	defer func() { _ = lease.Release() }()

	latestPath := filepath.Join(cfg.SegmentSetRoot, "latest.json")
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(latestPath, cfg.MaxMarkerBytes, cfg.MaxManifestBytes)
	if err != nil {
		return base, fmt.Errorf("open durable parent snapshot: %w", err)
	}
	if snap.Complete.IndexSetID != cfg.IndexSetID {
		return base, fmt.Errorf("parent snapshot index_set_id %q does not match config %q", snap.Complete.IndexSetID, cfg.IndexSetID)
	}

	priorRows, err := indexsubstrate.ReadManifestRowsBounded(snap.SegmentDir, snap.Manifest, cfg.MaxPriorRows)
	if err != nil {
		return base, fmt.Errorf("read parent rows: %w", err)
	}
	if len(snap.Manifest.Coverage) == 0 {
		return base, fmt.Errorf("parent snapshot has no coverage evidence; refuse enrich publish without inherited coverage")
	}
	covDigest, err := indexsubstrate.CoverageSHA256(snap.Manifest.Coverage)
	if err != nil {
		return base, fmt.Errorf("hash parent coverage: %w", err)
	}
	parentToken := indexsubstrate.ExpectedParentToken{
		IndexSetID:     snap.Complete.IndexSetID,
		RunID:          snap.Complete.RunID,
		ManifestSHA256: snap.Complete.ManifestSHA256,
		CoverageSHA256: covDigest,
	}

	candidates, err := candidatesFromParentRows(priorRows, cfg.Query)
	if err != nil {
		return base, err
	}
	storageFiltered := len(cfg.Query.StorageClasses) > 0

	runID, err := newRunID()
	if err != nil {
		return base, err
	}
	runStartedAt := cfg.Clock()

	summary, updates, headErr := executeHeads(ctx, cfg, candidates)
	res := Result{
		IndexSetID:         cfg.IndexSetID,
		RunID:              runID,
		Candidates:         summary.candidates,
		HeadSucceeded:      summary.headSucceeded,
		ResumeSkipped:      summary.resumeSkipped,
		Failed:             summary.failed,
		HeadCalls:          summary.headCalls,
		Committed:          0,
		StorageFiltered:    storageFiltered,
		ParentRunID:        parentToken.RunID,
		ParentManifestSHA:  parentToken.ManifestSHA256,
		ParentCoverageSHA:  parentToken.CoverageSHA256,
		ClassificationNote: base.ClassificationNote,
		Status:             "success",
	}
	if headErr != nil {
		res.Status = "failed"
		return res, headErr
	}
	if summary.failed > 0 {
		res.Status = "partial"
		return res, fmt.Errorf("HEAD enrichment completed with %d failure(s); durable latest unchanged", summary.failed)
	}
	if len(updates) == 0 {
		// zero candidates or all resume-skipped: success, no publish
		return res, nil
	}
	if err := authority.AssertHeldFor(cfg.IndexSetID, cfg.SegmentSetRoot); err != nil {
		return res, fmt.Errorf("index set authority at publish: %w", err)
	}

	pub, pubErr := publishEnrich(cfg, snap, priorRows, parentToken, updates, runID, runStartedAt, lease, hooks)
	res.LatestAdvanced = pub.LatestAdvanced
	res.Published = pub.LatestAdvanced
	res.ManifestSHA256 = pub.ManifestSHA256
	if pub.Manifest.Counts.Rows > 0 || pub.LatestAdvanced {
		res.Rows = pub.Manifest.Counts.Rows
	}
	if pub.LatestAdvanced {
		res.Status = "success"
		res.Committed = int64(len(updates))
		// HeadSucceeded already truthful from executeHeads
	} else if pubErr != nil {
		res.Status = "failed"
	}
	if pubErr != nil {
		return res, pubErr
	}
	return res, nil
}

func normalizeConfig(cfg Config) (Config, error) {
	cfg.IndexSetID = strings.TrimSpace(cfg.IndexSetID)
	cfg.BaseURI = strings.TrimSpace(cfg.BaseURI)
	cfg.SegmentSetRoot = strings.TrimSpace(cfg.SegmentSetRoot)
	cfg.JournalRoot = strings.TrimSpace(cfg.JournalRoot)
	if cfg.IndexSetID == "" {
		return cfg, fmt.Errorf("index_set_id is required")
	}
	if strings.Contains(cfg.IndexSetID, "/") || strings.Contains(cfg.IndexSetID, `\`) || strings.Contains(cfg.IndexSetID, "..") {
		return cfg, fmt.Errorf("index_set_id must not contain path separators")
	}
	if cfg.BaseURI == "" {
		return cfg, fmt.Errorf("base_uri is required")
	}
	if cfg.Provider == nil {
		return cfg, fmt.Errorf("provider is required")
	}
	if cfg.SegmentSetRoot == "" {
		return cfg, fmt.Errorf("segment set root is required")
	}
	if cfg.JournalRoot == "" {
		return cfg, fmt.Errorf("journal root is required")
	}
	if cfg.Parallel <= 0 {
		cfg.Parallel = 32
	}
	if cfg.MaxPriorRows <= 0 {
		cfg.MaxPriorRows = DefaultMaxPriorRows
	}
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return time.Now().UTC() }
	}
	if cfg.MaxMarkerBytes <= 0 {
		cfg.MaxMarkerBytes = indexsubstrate.DefaultMaxPublishedMarkerBytes
	}
	if cfg.MaxManifestBytes <= 0 {
		cfg.MaxManifestBytes = indexsubstrate.DefaultMaxPublishedManifestBytes
	}
	return cfg, nil
}

// runID grammar shared with hub/export/query contract.
var validRunIDPattern = regexp.MustCompile(`^run_([0-9]{1,32}|[0-9A-HJKMNP-TV-Z]{26})$`)

func validateRunID(id string) error {
	if !validRunIDPattern.MatchString(id) {
		return fmt.Errorf("invalid run ID: %s (must match run_<digits> or run_<ULID>)", id)
	}
	return nil
}

// newRunID generates a unique run identity independent of Config.Clock.
func newRunID() (string, error) {
	// Pure-digit form: nanosecond wall clock + 6 crypto-random digits.
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", fmt.Errorf("generate run_id entropy: %w", err)
	}
	id := fmt.Sprintf("run_%d%06d", time.Now().UnixNano(), n.Int64())
	if err := validateRunID(id); err != nil {
		return "", err
	}
	return id, nil
}

func candidatesFromParentRows(rows []indexsubstrate.CurrentObjectRow, opts QueryOptions) ([]enrichCandidate, error) {
	var keyRe *regexp.Regexp
	if opts.KeyRegex != "" {
		var err error
		keyRe, err = regexp.Compile(opts.KeyRegex)
		if err != nil {
			return nil, fmt.Errorf("invalid key regex: %w", err)
		}
	}
	if opts.Pattern != "" && !doublestar.ValidatePattern(opts.Pattern) {
		return nil, fmt.Errorf("invalid glob pattern: %s", opts.Pattern)
	}
	var minSize, maxSize int64
	var err error
	if opts.MinSize != "" {
		minSize, err = match.ParseSize(opts.MinSize)
		if err != nil {
			return nil, fmt.Errorf("invalid min size: %w", err)
		}
	}
	if opts.MaxSize != "" {
		maxSize, err = match.ParseSize(opts.MaxSize)
		if err != nil {
			return nil, fmt.Errorf("invalid max size: %w", err)
		}
	}
	storageSet := map[string]struct{}{}
	for _, sc := range opts.StorageClasses {
		storageSet[sc] = struct{}{}
	}
	out := make([]enrichCandidate, 0, len(rows))
	for _, row := range rows {
		if !opts.IncludeDeleted && row.DeletedAt != nil {
			continue
		}
		if opts.Pattern != "" {
			ok, matchErr := doublestar.Match(opts.Pattern, row.RelKey)
			if matchErr != nil {
				return nil, matchErr
			}
			if !ok {
				continue
			}
		}
		if keyRe != nil && !keyRe.MatchString(row.RelKey) {
			continue
		}
		if minSize > 0 && row.SizeBytes < minSize {
			continue
		}
		if maxSize > 0 && row.SizeBytes > maxSize {
			continue
		}
		if len(storageSet) > 0 {
			sc := ""
			if row.StorageClass != nil {
				sc = *row.StorageClass
			}
			if _, ok := storageSet[sc]; !ok {
				continue
			}
		}
		out = append(out, enrichCandidate{
			RelKey:         row.RelKey,
			SizeBytes:      row.SizeBytes,
			StorageClass:   row.StorageClass,
			HeadEnrichedAt: row.HeadEnrichedAt,
		})
	}
	return out, nil
}

type headSummary struct {
	candidates    int64
	headSucceeded int64
	resumeSkipped int64
	failed        int64
	headCalls     int64
}

type headResult struct {
	candidate  enrichCandidate
	update     enrichUpdate
	status     string
	attempts   int
	headCalls  int64
	errCode    string
	controlErr error // original provider/context error for cancel/retry classification
	publicErr  error // sanitized error for StateSink and returned fatal errors
}

func executeHeads(ctx context.Context, cfg Config, candidates []enrichCandidate) (headSummary, []enrichUpdate, error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	summary := headSummary{candidates: int64(len(candidates))}
	jobs := make(chan enrichCandidate)
	results := make(chan headResult)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for candidate := range jobs {
				if cfg.Resume && candidate.HeadEnrichedAt != nil {
					results <- headResult{candidate: candidate, status: "resume_skipped", attempts: 0}
					continue
				}
				results <- enrichOne(workCtx, cfg, candidate)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, c := range candidates {
			select {
			case <-workCtx.Done():
				return
			case jobs <- c:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	var updates []enrichUpdate
	var fatalErr error
	for result := range results {
		summary.headCalls += result.headCalls
		switch result.status {
		case "success":
			summary.headSucceeded++
			updates = append(updates, result.update)
		case "resume_skipped":
			summary.resumeSkipped++
		default:
			summary.failed++
		}
		if err := emitState(cfg, result); err != nil {
			if fatalErr == nil {
				fatalErr = fmt.Errorf("state sink: %w", err)
			}
			cancel()
			continue
		}
		if result.status != "success" && result.status != "resume_skipped" {
			// Control flow uses the original error so throttle/unavailable still cancel.
			if isResumable(result.controlErr) && fatalErr == nil {
				fatalErr = result.publicErr
				if fatalErr == nil {
					fatalErr = result.controlErr
				}
				cancel()
			}
		}
	}
	if fatalErr != nil {
		// Atomic: discard updates; keep truthful HeadSucceeded count in summary.
		return summary, nil, fatalErr
	}
	if ctx.Err() != nil {
		return summary, nil, ctx.Err()
	}
	if summary.failed > 0 {
		return summary, nil, nil // atomic: discard updates
	}
	return summary, updates, nil
}

func isResumable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		provider.IsThrottled(err) || provider.IsProviderUnavailable(err)
}

func enrichOne(ctx context.Context, cfg Config, candidate enrichCandidate) headResult {
	key := reconstructFullKey(cfg.BaseURI, candidate.RelKey)
	var lastErr error
	for attempt := 1; attempt <= enrichMaxRetries; attempt++ {
		meta, err := cfg.Provider.Head(ctx, key)
		if err == nil {
			now := cfg.Clock()
			return headResult{
				candidate: candidate,
				status:    "success",
				attempts:  attempt,
				headCalls: int64(attempt),
				update: enrichUpdate{
					RelKey:         candidate.RelKey,
					ArchiveStatus:  nonEmptyPtr(meta.ArchiveStatus),
					RestoreState:   nonEmptyPtr(meta.RestoreState),
					RestoreExpiry:  meta.RestoreExpiry,
					ContentType:    nonEmptyPtr(meta.ContentType),
					HeadEnrichedAt: now,
				},
			}
		}
		lastErr = err
		code, retry := classifyHeadError(err)
		if !retry || attempt == enrichMaxRetries {
			return headResult{
				candidate:  candidate,
				status:     "failed",
				attempts:   attempt,
				headCalls:  int64(attempt),
				errCode:    code,
				controlErr: err,
				publicErr:  publicHeadError(err),
			}
		}
		sleep := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
		sleep += jitter()
		select {
		case <-ctx.Done():
			return headResult{
				candidate:  candidate,
				status:     "failed",
				attempts:   attempt,
				headCalls:  int64(attempt),
				errCode:    "interrupted",
				controlErr: ctx.Err(),
				publicErr:  ctx.Err(),
			}
		case <-time.After(sleep):
		}
	}
	code, _ := classifyHeadError(lastErr)
	return headResult{
		candidate:  candidate,
		status:     "failed",
		attempts:   enrichMaxRetries,
		headCalls:  enrichMaxRetries,
		errCode:    code,
		controlErr: lastErr,
		publicErr:  publicHeadError(lastErr),
	}
}

func jitter() time.Duration {
	n, err := rand.Int(rand.Reader, big.NewInt(50))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64()) * time.Millisecond
}

func reconstructFullKey(baseURI, relKey string) string {
	baseURI = strings.TrimSpace(baseURI)
	relKey = strings.TrimLeft(strings.TrimSpace(relKey), "/")
	if baseURI == "" {
		return relKey
	}
	rest := baseURI
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	if i := strings.Index(rest, "/"); i >= 0 {
		rest = rest[i+1:]
	} else {
		rest = ""
	}
	prefix := strings.Trim(rest, "/")
	if prefix == "" {
		return relKey
	}
	return prefix + "/" + relKey
}

func nonEmptyPtr(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

func emitState(cfg Config, result headResult) error {
	if cfg.StateSink == nil {
		return nil
	}
	ev := StateEvent{
		IndexSetID: cfg.IndexSetID,
		RelKey:     result.candidate.RelKey,
		FullKey:    reconstructFullKey(cfg.BaseURI, result.candidate.RelKey),
		Status:     result.status,
		Attempts:   result.attempts,
		ErrorCode:  result.errCode,
		EventTime:  cfg.Clock(),
	}
	if result.publicErr != nil {
		ev.ErrorMessage = result.publicErr.Error()
	}
	if result.status == "success" {
		ev.ArchiveStatus = result.update.ArchiveStatus
		ev.RestoreState = result.update.RestoreState
		ev.RestoreExpiry = result.update.RestoreExpiry
		ev.ContentType = result.update.ContentType
		if !result.update.HeadEnrichedAt.IsZero() {
			t := result.update.HeadEnrichedAt
			ev.HeadEnrichedAt = &t
		}
	}
	return cfg.StateSink(ev)
}

func publishEnrich(
	cfg Config,
	snap indexsubstrate.PublishedSnapshot,
	priorRows []indexsubstrate.CurrentObjectRow,
	parentToken indexsubstrate.ExpectedParentToken,
	updates []enrichUpdate,
	runID string,
	runStartedAt time.Time,
	lease *indexsubstrate.WriteLease,
	hooks runHooks,
) (indexsubstrate.PublishResult, error) {
	journalDir := filepath.Join(cfg.JournalRoot, runID)
	if err := ensureDir(journalDir); err != nil {
		return indexsubstrate.PublishResult{}, err
	}
	journalPath := filepath.Join(journalDir, "enrich.jsonl")
	jw, err := indexsubstrate.CreateJournal(journalPath, indexsubstrate.JournalHeader{
		Type:               indexsubstrate.JournalHeaderType,
		JournalID:          "jrn_" + uuid.NewString(),
		IndexSetID:         cfg.IndexSetID,
		RunID:              runID,
		Shard:              "enrich",
		IndexSchemaVersion: indexsubstrate.IndexSchemaVersion,
		StartedAt:          runStartedAt,
	})
	if err != nil {
		return indexsubstrate.PublishResult{}, fmt.Errorf("create enrich journal: %w", err)
	}
	for _, update := range updates {
		enrichedAt := update.HeadEnrichedAt
		if enrichedAt.IsZero() {
			enrichedAt = cfg.Clock()
		}
		if _, err := jw.Append(indexsubstrate.ObjectRecord{
			Type:           indexsubstrate.ObjectRecordType,
			Op:             indexsubstrate.ObjectRecordOpEnrich,
			RelKey:         update.RelKey,
			ObservedAt:     enrichedAt,
			ContentType:    update.ContentType,
			ArchiveStatus:  update.ArchiveStatus,
			RestoreState:   update.RestoreState,
			RestoreExpiry:  update.RestoreExpiry,
			HeadEnrichedAt: &enrichedAt,
		}); err != nil {
			_ = jw.Close()
			return indexsubstrate.PublishResult{}, err
		}
	}
	if err := jw.Seal(cfg.Clock()); err != nil {
		_ = jw.Close()
		return indexsubstrate.PublishResult{}, err
	}
	if err := jw.Close(); err != nil {
		return indexsubstrate.PublishResult{}, err
	}

	runDir := filepath.Join(cfg.SegmentSetRoot, "runs", runID)
	if err := ensureDir(runDir); err != nil {
		return indexsubstrate.PublishResult{}, err
	}
	coverage := append([]indexsubstrate.CoverageAttestation(nil), snap.Manifest.Coverage...)
	createdAt := cfg.Clock()

	var after func(indexsubstrate.PublishStep) error
	if hooks.afterPublishStep != nil {
		after = func(step indexsubstrate.PublishStep) error {
			return hooks.afterPublishStep(string(step))
		}
	}

	latestPath := filepath.Join(cfg.SegmentSetRoot, "latest.json")
	result, err := indexsubstrate.PublishSnapshot(indexsubstrate.PublishConfig{
		IndexSetID:   cfg.IndexSetID,
		RunID:        runID,
		RunStartedAt: runStartedAt,
		CreatedAt:    createdAt,
		ParentManifests: []indexsubstrate.ManifestReference{{
			IndexSetID:     parentToken.IndexSetID,
			RunID:          parentToken.RunID,
			ManifestSHA256: parentToken.ManifestSHA256,
		}},
		PriorRows:            priorRows,
		JournalPaths:         []string{journalPath},
		Coverage:             coverage,
		SegmentDir:           runDir,
		ManifestPath:         filepath.Join(runDir, "manifest.json"),
		CompletePath:         filepath.Join(runDir, "complete.json"),
		LatestPath:           latestPath,
		Mode:                 indexsubstrate.PublicationModeEnrichOnly,
		ExpectedParent:       &parentToken,
		WriteLease:           lease,
		TargetRowsPerSegment: snap.Manifest.SegmentSizing.TargetRowsPerSegment,
		AfterStep:            after,
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o700)
}
