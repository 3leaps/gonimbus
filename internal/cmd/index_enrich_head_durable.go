package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexenrich"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// runIndexEnrichWithHeadDurable is a thin Cobra adapter over pkg/indexenrich.
func runIndexEnrichWithHeadDurable(ctx context.Context, cmd *cobra.Command, meta indexreader.Meta) error {
	if strings.TrimSpace(meta.SourcePath) == "" {
		return fmt.Errorf("durable enrich requires a latest.json source path")
	}

	checkpointCfg, _, storageFiltered, err := enrichHeadCheckpointConfigFromCommand(cmd, meta.IndexSetID)
	if err != nil {
		return err
	}
	_ = storageFiltered

	indexSet, err := durableEnrichIndexSet(meta, checkpointCfg.Provider)
	if err != nil {
		return err
	}
	segmentSetRoot, err := indexSubstrateSegmentCacheDir(meta.IndexSetID)
	if err != nil {
		return err
	}
	// Single path authority: engine always uses SegmentSetRoot/latest.json.
	// Reject a resolved source path that does not name that same marker.
	canonicalLatest := filepath.Join(segmentSetRoot, "latest.json")
	if filepath.Clean(meta.SourcePath) != filepath.Clean(canonicalLatest) {
		// Allow absolute vs relative equivalence after evaluation.
		absSrc, errSrc := filepath.Abs(meta.SourcePath)
		absCan, errCan := filepath.Abs(canonicalLatest)
		if errSrc != nil || errCan != nil || filepath.Clean(absSrc) != filepath.Clean(absCan) {
			return fmt.Errorf("durable enrich source path %q does not match set latest %q", meta.SourcePath, canonicalLatest)
		}
	}
	journalRoot, err := appDataPath(appDataClassCrawlJournals, meta.IndexSetID)
	if err != nil {
		return err
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

	opts, err := enrichHeadQueryOptionsFromCommand(cmd)
	if err != nil {
		return err
	}
	parallel, _ := cmd.Flags().GetInt("parallel")
	resume, _ := cmd.Flags().GetBool("resume")
	cmd.SilenceUsage = true

	var stateSink indexenrich.StateSink
	if stateOut != nil {
		stateSink = func(ev indexenrich.StateEvent) error {
			return writeEnrichHeadStateEvent(stateOut, ev)
		}
	}

	res, runErr := indexenrich.Run(ctx, indexenrich.Config{
		IndexSetID:     meta.IndexSetID,
		BaseURI:        indexSet.BaseURI,
		Provider:       prov,
		SegmentSetRoot: segmentSetRoot,
		JournalRoot:    journalRoot,
		Query: indexenrich.QueryOptions{
			Pattern:        opts.Pattern,
			KeyRegex:       opts.KeyRegex,
			MinSize:        opts.MinSize,
			MaxSize:        opts.MaxSize,
			StorageClasses: opts.StorageClasses,
			IncludeDeleted: opts.IncludeDeleted,
		},
		Parallel:         parallel,
		Resume:           resume,
		StateSink:        stateSink,
		MaxMarkerBytes:   int64(maxHubMarkerBytes),
		MaxManifestBytes: int64(maxDurableManifestBytes),
	})

	// Suppress structured terminal records when the engine never produced a set identity
	// (config/normalize failure with empty IndexSetID). Lease/parent failures still carry set ID.
	if res.IndexSetID == "" && runErr != nil {
		return runErr
	}

	ts := time.Now().UTC().Format(time.RFC3339)
	enc := json.NewEncoder(cmd.OutOrStdout())
	summary := enrichHeadSummaryData{
		IndexSetID:      res.IndexSetID,
		Candidates:      res.Candidates,
		Enriched:        res.HeadSucceeded,
		ResumeSkipped:   res.ResumeSkipped,
		Failed:          res.Failed,
		HeadCalls:       res.HeadCalls,
		StorageFiltered: res.StorageFiltered,
		Status:          res.Status,
	}
	if err := enc.Encode(enrichHeadSummaryRecord{
		Type: "gonimbus.index.enrich_with_head.summary.v1",
		TS:   ts,
		Data: summary,
	}); err != nil {
		if res.LatestAdvanced {
			return fmt.Errorf("write summary after commit: %w (committed index_set_id=%s run_id=%s manifest_sha256=%s latest_advanced=true)",
				err, res.IndexSetID, res.RunID, res.ManifestSHA256)
		}
		if runErr == nil {
			runErr = fmt.Errorf("write summary: %w", err)
		}
	}

	if res.Published || res.LatestAdvanced {
		if err := enc.Encode(map[string]any{
			"type":                   "gonimbus.index.enrich_with_head.durable_result.v1",
			"ts":                     ts,
			"status":                 "success",
			"index_set_id":           res.IndexSetID,
			"run_id":                 res.RunID,
			"parent_run_id":          res.ParentRunID,
			"parent_manifest_sha256": res.ParentManifestSHA,
			"manifest_sha256":        res.ManifestSHA256,
			"rows":                   res.Rows,
			"head_succeeded":         res.HeadSucceeded,
			"committed":              res.Committed,
			"published":              true,
			"latest_advanced":        res.LatestAdvanced,
			"time_semantics":         "durable publication times; enrichment is internal-render-only",
			"classification_note":    res.ClassificationNote,
		}); err != nil {
			// Post-commit output failure must still surface recoverable identity.
			return fmt.Errorf("write durable result after commit: %w (committed index_set_id=%s run_id=%s manifest_sha256=%s latest_advanced=true)",
				err, res.IndexSetID, res.RunID, res.ManifestSHA256)
		}
	} else {
		if err := enc.Encode(map[string]any{
			"type":           "gonimbus.index.enrich_with_head.durable_result.v1",
			"ts":             ts,
			"status":         res.Status,
			"index_set_id":   res.IndexSetID,
			"published":      false,
			"committed":      0,
			"head_succeeded": res.HeadSucceeded,
			"note":           "no latest advance; HEAD observations were not committed as a durable snapshot",
		}); err != nil && runErr == nil {
			runErr = fmt.Errorf("write durable non-commit result: %w", err)
		}
	}

	if runErr != nil {
		if res.LatestAdvanced {
			return fmt.Errorf("%w (committed index_set_id=%s run_id=%s manifest_sha256=%s latest_advanced=true)",
				runErr, res.IndexSetID, res.RunID, res.ManifestSHA256)
		}
		return runErr
	}
	if res.Failed > 0 {
		return fmt.Errorf("HEAD enrichment completed with %d failure(s); durable latest unchanged", res.Failed)
	}
	return nil
}

// writeEnrichHeadStateEvent encodes the established v1 state record from a typed engine event.
func writeEnrichHeadStateEvent(file *os.File, ev indexenrich.StateEvent) error {
	ts := ev.EventTime.UTC().Format(time.RFC3339)
	if ev.EventTime.IsZero() {
		ts = time.Now().UTC().Format(time.RFC3339)
	}
	data := enrichHeadStateData{
		IndexSetID: ev.IndexSetID,
		RelKey:     ev.RelKey,
		Key:        ev.FullKey,
		Status:     ev.Status,
		Attempts:   ev.Attempts,
		ErrorCode:  ev.ErrorCode,
		Error:      ev.ErrorMessage,
	}
	if ev.Status == "success" {
		data.ArchiveStatus = ev.ArchiveStatus
		data.RestoreState = ev.RestoreState
		data.ContentType = ev.ContentType
		if ev.HeadEnrichedAt != nil {
			s := ev.HeadEnrichedAt.UTC().Format(time.RFC3339)
			data.HeadEnrichedAt = &s
		}
		if ev.RestoreExpiry != nil {
			s := ev.RestoreExpiry.UTC().Format(time.RFC3339)
			data.RestoreExpiry = &s
		}
	}
	return json.NewEncoder(file).Encode(enrichHeadStateRecord{
		Type: "gonimbus.index.enrich_with_head.state.v1",
		TS:   ts,
		Data: data,
	})
}

func durableEnrichIndexSet(meta indexreader.Meta, providerOpts enrichHeadProviderOptions) (*indexstore.IndexSet, error) {
	indexSet := &indexstore.IndexSet{
		IndexSetID: meta.IndexSetID,
		BaseURI:    meta.BaseURI,
		Provider:   meta.Provider,
	}
	if meta.IdentityDir != "" {
		idPath := filepath.Join(meta.IdentityDir, "identity.json")
		if idFile, err := indexreader.ReadLocalIdentityFile(idPath, int64(maxHubMarkerBytes)); err == nil {
			indexSet.StorageProvider = idFile.Payload.StorageProvider
			indexSet.CloudProvider = idFile.Payload.CloudProvider
			indexSet.Region = idFile.Payload.Region
			indexSet.RegionKind = idFile.Payload.RegionKind
			indexSet.EndpointHost = idFile.Payload.EndpointHost
			if indexSet.BaseURI == "" {
				indexSet.BaseURI = idFile.Payload.BaseURI
			}
			if indexSet.Provider == "" {
				indexSet.Provider = idFile.Payload.Provider
			}
		}
	}
	_ = providerOpts
	if strings.TrimSpace(indexSet.BaseURI) == "" {
		return nil, fmt.Errorf("durable enrich: base_uri is required for index set %s", meta.IndexSetID)
	}
	if strings.TrimSpace(indexSet.Provider) == "" {
		return nil, fmt.Errorf("durable enrich: provider is required for index set %s", meta.IndexSetID)
	}
	return indexSet, nil
}
