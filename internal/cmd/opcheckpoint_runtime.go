package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

const operationIndexEnrichWithHead = "index-enrich-with-head"

const eventTypeOperationCheckpointIdentity = "operation_checkpoint_identity"

type indexRunCheckpointIdentityEvent struct {
	Operation         string `json:"operation"`
	ConfigFingerprint string `json:"config_fingerprint"`
}

type resolvedIndexRun struct {
	db       *sql.DB
	indexSet *indexstore.IndexSet
	run      *indexstore.IndexRun
	path     string
}

func openDefaultOperationCheckpointStore(ctx context.Context) (*opcheckpoint.Store, error) {
	dataDir, err := indexDataDir()
	if err != nil {
		return nil, err
	}
	forbidden := []string{}
	if wd, err := os.Getwd(); err == nil && wd != "" {
		if root, err := discoverRepositoryRoot(wd); err == nil && root != "" {
			forbidden = appendForbiddenRoot(forbidden, root)
		}
		forbidden = appendForbiddenRoot(forbidden, wd)
	}
	return opcheckpoint.Open(ctx, opcheckpoint.Config{
		AppDataDir:     dataDir,
		ForbiddenRoots: forbidden,
	})
}

func discoverRepositoryRoot(start string) (string, error) {
	if strings.TrimSpace(start) == "" {
		return "", fmt.Errorf("repository search start is empty")
	}
	dir, err := filepath.Abs(filepath.Clean(start))
	if err != nil {
		return "", err
	}
	for {
		for _, marker := range []string{".git", "go.mod"} {
			if _, err := os.Stat(filepath.Join(dir, marker)); err == nil {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repository root not found from %s", start)
		}
		dir = parent
	}
}

func appendForbiddenRoot(roots []string, root string) []string {
	root = strings.TrimSpace(root)
	if root == "" {
		return roots
	}
	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return roots
	}
	for _, existing := range roots {
		if existing == abs {
			return roots
		}
	}
	return append(roots, abs)
}

func checkpointFingerprint(v any) (string, error) {
	fp, err := opcheckpoint.FingerprintConfig(v)
	if err != nil {
		return "", fmt.Errorf("compute checkpoint fingerprint: %w", err)
	}
	return fp, nil
}

func writeIndexRunCheckpoint(
	ctx context.Context,
	store *opcheckpoint.Store,
	db *sql.DB,
	runID string,
	operation string,
	fingerprint string,
	class opcheckpoint.ErrorClass,
	progress map[string]int64,
	payload any,
) error {
	if store == nil {
		return fmt.Errorf("operation checkpoint store is nil")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal checkpoint payload: %w", err)
	}
	now := time.Now().UTC()
	env := opcheckpoint.Envelope{
		SchemaVersion:     opcheckpoint.SchemaVersion,
		Operation:         operation,
		RunID:             runID,
		ConfigFingerprint: fingerprint,
		Status:            opcheckpoint.StatusFailedResumable,
		ErrorClass:        class,
		CreatedAt:         now,
		Progress:          progress,
		Payload:           raw,
		Events: []opcheckpoint.CheckpointEvent{{
			Type:       "failed_resumable",
			At:         now,
			ErrorClass: class,
		}},
	}
	if err := store.WriteCheckpoint(ctx, env); err != nil {
		return err
	}
	if db != nil {
		if err := indexstore.UpdateIndexRunStatus(context.Background(), db, runID, indexstore.RunStatusFailedResumable, nil); err != nil {
			return err
		}
		if err := recordIndexRunCheckpointIdentity(context.Background(), db, runID, operation, fingerprint, now); err != nil {
			return err
		}
		detail := string(class)
		if err := indexstore.RecordRunEvent(context.Background(), db, indexstore.RunEvent{
			EventID:       "evt_" + uuid.NewString(),
			RunID:         runID,
			OccurredAt:    now,
			EventType:     "failed_resumable",
			EventCategory: string(indexstore.EventCategoryError),
			Detail:        &detail,
			ErrorCode:     checkpointStringPtr(string(class)),
		}); err != nil {
			return err
		}
	}
	return nil
}

func recordIndexRunCheckpointIdentity(ctx context.Context, db *sql.DB, runID, operation, fingerprint string, at time.Time) error {
	detail, err := json.Marshal(indexRunCheckpointIdentityEvent{
		Operation:         operation,
		ConfigFingerprint: fingerprint,
	})
	if err != nil {
		return fmt.Errorf("marshal checkpoint identity event: %w", err)
	}
	detailText := string(detail)
	return indexstore.RecordRunEvent(ctx, db, indexstore.RunEvent{
		EventID:       "evt_" + uuid.NewString(),
		RunID:         runID,
		OccurredAt:    at,
		EventType:     eventTypeOperationCheckpointIdentity,
		EventCategory: string(indexstore.EventCategoryInfo),
		Detail:        &detailText,
	})
}

func expectedIndexRunCheckpointFingerprint(ctx context.Context, db *sql.DB, runID, operation string) (string, error) {
	events, err := indexstore.ListRunEvents(ctx, db, runID, nil)
	if err != nil {
		return "", err
	}
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.EventType != eventTypeOperationCheckpointIdentity || event.Detail == nil {
			continue
		}
		var identity indexRunCheckpointIdentityEvent
		if err := json.Unmarshal([]byte(*event.Detail), &identity); err != nil {
			return "", fmt.Errorf("parse checkpoint identity event: %w", err)
		}
		if identity.Operation == operation {
			if identity.ConfigFingerprint == "" {
				return "", fmt.Errorf("checkpoint identity event missing config fingerprint")
			}
			return identity.ConfigFingerprint, nil
		}
	}
	return "", fmt.Errorf("checkpoint identity event not found for run %s", runID)
}

func validateCheckpointIdentityAgainstIndexRun(ctx context.Context, db *sql.DB, env *opcheckpoint.Envelope, operation string, config any) (string, error) {
	if env == nil {
		return "", fmt.Errorf("checkpoint envelope is nil")
	}
	expected, err := expectedIndexRunCheckpointFingerprint(ctx, db, env.RunID, operation)
	if err != nil {
		return "", err
	}
	if env.Operation != operation || env.ConfigFingerprint != expected {
		return "", opcheckpoint.ErrIdentityMismatch
	}
	actual, err := checkpointFingerprint(config)
	if err != nil {
		return "", err
	}
	if actual != expected {
		return "", opcheckpoint.ErrIdentityMismatch
	}
	return expected, nil
}

func findIndexRunInDefaultIndexes(ctx context.Context, runID string) (*resolvedIndexRun, error) {
	paths, err := listIndexDBPaths()
	if err != nil {
		return nil, err
	}
	var firstErr error
	for _, path := range paths {
		db, err := openMigratedIndexDB(ctx, path)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		run, err := indexstore.GetIndexRun(ctx, db, runID)
		if err != nil {
			_ = db.Close()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		indexSet, err := indexstore.GetIndexSet(ctx, db, run.IndexSetID)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		return &resolvedIndexRun{db: db, indexSet: indexSet, run: run, path: path}, nil
	}
	if firstErr != nil {
		return nil, fmt.Errorf("find index_run %s: %w", runID, firstErr)
	}
	return nil, fmt.Errorf("index_run not found: %s", runID)
}

func closeResolvedIndexRun(resolved *resolvedIndexRun) {
	if resolved != nil && resolved.db != nil {
		_ = resolved.db.Close()
	}
}

func emitOperationErrorRecord(ctx context.Context, cmdOut jsonEncoder, operation, runID string, class opcheckpoint.ErrorClass, progress map[string]int64) error {
	rec, err := opcheckpoint.NewErrorRecord(operation, runID, class, progress, time.Now().UTC())
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return cmdOut.Encode(rec)
}

type jsonEncoder interface {
	Encode(v any) error
}

func checkpointStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
