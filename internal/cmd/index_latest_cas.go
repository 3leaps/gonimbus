package cmd

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/spf13/cobra"
)

const (
	latestWriteModeConditional   = "conditional"
	latestWriteModeUnconditional = "unconditional"

	defaultLatestRetryMax  = 3
	defaultLatestRetryBase = 250 * time.Millisecond

	casRetryRecordType = "gonimbus.cas.retry.v1"
	casYieldRecordType = "gonimbus.cas.yield.v1"
	casFailRecordType  = "gonimbus.cas.fail.v1"
)

type latestPointerOptions struct {
	Mode      string
	RetryMax  int
	RetryBase time.Duration
	Events    io.Writer
}

type latestPointerOutcome string

const (
	latestPointerUpdated   latestPointerOutcome = "updated"
	latestPointerYielded   latestPointerOutcome = "yielded"
	latestPointerUnchanged latestPointerOutcome = "unchanged"
)

type latestPointerDoc struct {
	Version    string `json:"version"`
	IndexSetID string `json:"index_set_id"`
	RunID      string `json:"run_id"`
	UpdatedAt  string `json:"updated_at"`
	UpdatedBy  string `json:"updated_by"`
}

type hubCompleteDoc struct {
	Version     string `json:"version"`
	IndexSetID  string `json:"index_set_id"`
	RunID       string `json:"run_id"`
	CompletedAt string `json:"completed_at"`
}

type latestCASRecord struct {
	Hub        string `json:"hub,omitempty"`
	IndexSet   string `json:"index_set_id"`
	RunID      string `json:"run_id"`
	Current    string `json:"current_run_id,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	MaxRetries int    `json:"max_retries,omitempty"`
	Message    string `json:"message"`
}

func defaultLatestPointerOptions() latestPointerOptions {
	return latestPointerOptions{
		Mode:      latestWriteModeConditional,
		RetryMax:  defaultLatestRetryMax,
		RetryBase: defaultLatestRetryBase,
		Events:    os.Stderr,
	}
}

func addLatestPointerFlags(cmd *cobra.Command) {
	cmd.Flags().String("latest-write-mode", latestWriteModeConditional, "latest.json write mode: conditional or unconditional")
	cmd.Flags().Int("latest-retry-max", defaultLatestRetryMax, "Maximum latest.json CAS retries before fail-closed")
	cmd.Flags().Duration("latest-retry-base", defaultLatestRetryBase, "Base duration for latest.json CAS retry backoff")
}

func latestPointerOptionsFromCommand(cmd *cobra.Command) (latestPointerOptions, error) {
	opts := defaultLatestPointerOptions()
	opts.Mode, _ = cmd.Flags().GetString("latest-write-mode")
	opts.RetryMax, _ = cmd.Flags().GetInt("latest-retry-max")
	opts.RetryBase, _ = cmd.Flags().GetDuration("latest-retry-base")
	opts.Events = cmd.ErrOrStderr()
	return normalizeLatestPointerOptions(opts)
}

func normalizeLatestPointerOptions(opts latestPointerOptions) (latestPointerOptions, error) {
	if opts.Mode == "" {
		opts.Mode = latestWriteModeConditional
	}
	switch opts.Mode {
	case latestWriteModeConditional, latestWriteModeUnconditional:
	default:
		return opts, fmt.Errorf("--latest-write-mode must be conditional or unconditional")
	}
	if opts.RetryMax < 0 {
		return opts, fmt.Errorf("--latest-retry-max must be >= 0")
	}
	if opts.RetryBase < 0 {
		return opts, fmt.Errorf("--latest-retry-base must be >= 0")
	}
	if opts.Events == nil {
		opts.Events = io.Discard
	}
	return opts, nil
}

func advanceLatestPointer(ctx context.Context, hub *hubDestSpec, getter provider.ObjectGetter, putter provider.ObjectPutter, indexSetID, runID string, opts latestPointerOptions) (latestPointerOutcome, error) {
	opts, err := normalizeLatestPointerOptions(opts)
	if err != nil {
		return "", err
	}

	latestKey := hubArtifactKey(hub, "index-sets", indexSetID, "latest.json")
	latestJSON, err := buildLatestJSONForRun(indexSetID, runID)
	if err != nil {
		return "", err
	}

	if opts.Mode == latestWriteModeUnconditional {
		if err := uploadBytes(ctx, putter, latestKey, latestJSON); err != nil {
			return "", err
		}
		return latestPointerUpdated, nil
	}

	versioned, ok := getter.(provider.VersionedGetter)
	if !ok {
		return "", fmt.Errorf("hub provider does not support versioned reads; manual reconciliation required before re-running with --latest-write-mode unconditional")
	}
	conditional, ok := putter.(provider.ConditionalPutter)
	if !ok {
		return "", fmt.Errorf("hub provider does not support conditional writes; manual reconciliation required before re-running with --latest-write-mode unconditional")
	}

	candidate, err := readCompleteDoc(ctx, getter, hub, indexSetID, runID)
	if err != nil {
		return "", fmt.Errorf("read candidate complete.json: %w", err)
	}

	for attempt := 0; ; attempt++ {
		current, currentETag, err := readLatestPointerVersioned(ctx, versioned, latestKey)
		if err != nil && !provider.IsNotFound(err) {
			return "", fmt.Errorf("read latest.json: %w", err)
		}

		var precond provider.PutPrecondition
		var currentRunID string
		if provider.IsNotFound(err) {
			precond = provider.PutPrecondition{IfAbsent: true}
		} else {
			currentRunID = current.RunID
			if current.RunID == runID {
				return latestPointerUnchanged, nil
			}
			currentComplete, readErr := readCompleteDoc(ctx, getter, hub, indexSetID, current.RunID)
			if readErr != nil {
				return "", fmt.Errorf("read current latest complete.json: %w; manual reconciliation required before re-running with --latest-write-mode unconditional", readErr)
			}
			if compareCompleteDocs(candidate, currentComplete) <= 0 {
				_ = emitLatestCASEvent(ctx, opts.Events, hub, casYieldRecordType, latestCASRecord{
					IndexSet: indexSetID,
					RunID:    runID,
					Current:  current.RunID,
					Message:  "current latest is newer; yielding pointer update",
				})
				return latestPointerYielded, nil
			}
			precond = provider.PutPrecondition{IfMatchETag: &currentETag}
		}

		if _, err := conditional.PutObjectConditional(ctx, latestKey, bytes.NewReader(latestJSON), int64(len(latestJSON)), precond); err == nil {
			return latestPointerUpdated, nil
		} else if !provider.IsAlreadyExists(err) && !provider.IsPreconditionFailed(err) {
			return "", fmt.Errorf("write latest.json: %w", err)
		}

		if attempt >= opts.RetryMax {
			_ = emitLatestCASEvent(ctx, opts.Events, hub, casFailRecordType, latestCASRecord{
				IndexSet:   indexSetID,
				RunID:      runID,
				Current:    currentRunID,
				Attempt:    attempt + 1,
				MaxRetries: opts.RetryMax,
				Message:    "CAS conflict budget exhausted; manual reconciliation required before re-running with --latest-write-mode unconditional",
			})
			return "", fmt.Errorf("CAS conflict budget exhausted; manual reconciliation required before re-running with --latest-write-mode unconditional")
		}

		_ = emitLatestCASEvent(ctx, opts.Events, hub, casRetryRecordType, latestCASRecord{
			IndexSet:   indexSetID,
			RunID:      runID,
			Current:    currentRunID,
			Attempt:    attempt + 1,
			MaxRetries: opts.RetryMax,
			Message:    "CAS conflict observed; retrying latest pointer update",
		})
		if delay := latestRetryDelay(opts.RetryBase, attempt); delay > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}
	}
}

func buildLatestJSONForRun(indexSetID, runID string) ([]byte, error) {
	doc := latestPointerDoc{
		Version:    "1.0",
		IndexSetID: indexSetID,
		RunID:      runID,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		UpdatedBy:  exportedByString(),
	}
	return json.MarshalIndent(doc, "", "  ")
}

func printLatestPointerOutcome(w io.Writer, outcome latestPointerOutcome, indexSetID, runID string) {
	switch outcome {
	case latestPointerUpdated:
		_, _ = fmt.Fprintf(w, "Set latest for %s to %s\n", indexSetID, runID)
	case latestPointerYielded:
		_, _ = fmt.Fprintf(w, "Latest for %s unchanged; newer run already current\n", indexSetID)
	case latestPointerUnchanged:
		_, _ = fmt.Fprintf(w, "Latest for %s already points to %s\n", indexSetID, runID)
	}
}

func readLatestPointerVersioned(ctx context.Context, getter provider.VersionedGetter, key string) (latestPointerDoc, string, error) {
	body, meta, err := getter.GetObjectVersioned(ctx, key)
	if err != nil {
		return latestPointerDoc{}, "", err
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(body)
	if err != nil {
		return latestPointerDoc{}, "", err
	}
	var doc latestPointerDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return latestPointerDoc{}, "", fmt.Errorf("parse latest.json: %w", err)
	}
	if strings.TrimSpace(doc.RunID) == "" {
		return latestPointerDoc{}, "", fmt.Errorf("parse latest.json: run_id is required")
	}
	if strings.TrimSpace(meta.ETag) == "" {
		return latestPointerDoc{}, "", fmt.Errorf("versioned latest.json read did not return an ETag")
	}
	return doc, meta.ETag, nil
}

func readCompleteDoc(ctx context.Context, getter provider.ObjectGetter, hub *hubDestSpec, indexSetID, runID string) (hubCompleteDoc, error) {
	key := hubArtifactKey(hub, "index-sets", indexSetID, "runs", runID, "complete.json")
	data, err := downloadBytes(ctx, getter, key)
	if err != nil {
		return hubCompleteDoc{}, err
	}
	var doc hubCompleteDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return hubCompleteDoc{}, fmt.Errorf("parse complete.json for %s: %w", runID, err)
	}
	if strings.TrimSpace(doc.RunID) == "" {
		doc.RunID = runID
	}
	if strings.TrimSpace(doc.CompletedAt) == "" {
		return hubCompleteDoc{}, fmt.Errorf("parse complete.json for %s: completed_at is required", runID)
	}
	if _, err := time.Parse(time.RFC3339Nano, doc.CompletedAt); err != nil {
		return hubCompleteDoc{}, fmt.Errorf("parse complete.json for %s: invalid completed_at: %w", runID, err)
	}
	return doc, nil
}

func compareCompleteDocs(candidate, current hubCompleteDoc) int {
	candidateTime, candidateErr := time.Parse(time.RFC3339Nano, candidate.CompletedAt)
	currentTime, currentErr := time.Parse(time.RFC3339Nano, current.CompletedAt)
	if candidateErr == nil && currentErr == nil {
		switch {
		case candidateTime.After(currentTime):
			return 1
		case candidateTime.Before(currentTime):
			return -1
		}
	}
	return strings.Compare(candidate.RunID, current.RunID)
}

func latestRetryDelay(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	delay := base
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	jitterMax := delay / 2
	if jitterMax <= 0 {
		return delay
	}
	jitter, err := rand.Int(rand.Reader, big.NewInt(int64(jitterMax)))
	if err != nil {
		return delay
	}
	return delay + time.Duration(jitter.Int64())
}

func emitLatestCASEvent(ctx context.Context, w io.Writer, hub *hubDestSpec, recordType string, rec latestCASRecord) error {
	if w == nil {
		return nil
	}
	rec.Hub = sanitizedHubForRecord(hub)
	writer := output.NewJSONLWriter(w, "", hub.Provider)
	return writer.WriteAny(ctx, recordType, rec)
}

func sanitizedHubForRecord(hub *hubDestSpec) string {
	switch hub.Provider {
	case string(provider.ProviderS3):
		if hub.Prefix == "" {
			return "s3://" + hub.Bucket + "/"
		}
		return "s3://" + hub.Bucket + "/" + hub.Prefix
	case string(provider.ProviderFile):
		return "file://<local-hub>"
	default:
		return hub.Provider
	}
}
