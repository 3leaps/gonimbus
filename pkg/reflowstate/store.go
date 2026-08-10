package reflowstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/producer"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const schemaVersion = 2

type Store struct {
	db     *sql.DB
	writer *coordinator
}

// Config opens a reflow checkpoint store.
//
// Writer diagnostics for measure-first work are snapshot-only via
// Store.WriterStats(); there is no callback observer on Config (C1 deliberately
// keeps sinks off the sole-writer / post-COMMIT acknowledgement path).
//
// Experimental: the package is Experimental; this surface may change with only
// an in-release note.
type Config struct {
	Path string

	// ElideRawExecSavepoints is an experimental default-off switch: when true,
	// the write coordinator omits SAVEPOINT/RELEASE around raw writer.exec
	// (SQL statement) mutations. execTx paths always keep savepoints so designed
	// request-local refusals still work. Product default remains false — this is
	// not a recommended production setting. Optional measure-only override:
	// GONIMBUS_REFLOW_ELIDE_RAW_EXEC_SAVEPOINTS=1 (or "true") also enables this
	// when Config does not already set it true. Leave unset for normal use.
	ElideRawExecSavepoints bool
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	if !cfg.ElideRawExecSavepoints {
		if v := strings.TrimSpace(os.Getenv("GONIMBUS_REFLOW_ELIDE_RAW_EXEC_SAVEPOINTS")); v == "1" || strings.EqualFold(v, "true") {
			cfg.ElideRawExecSavepoints = true
		}
	}
	return openStore(ctx, cfg, nil)
}

// openStore opens the checkpoint store and starts its write coordinator. The
// configure hook, when non-nil, runs against the coordinator before its writer
// goroutine starts; it exists so in-package tests can inject a deterministic
// writer failure without racing the goroutine.
func openStore(ctx context.Context, cfg Config, configure func(*coordinator)) (*Store, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("reflow state path is required")
	}
	// Reuse indexstore's local SQLite configuration (WAL, busy_timeout, single
	// conn). The reflow checkpoint store is the resume authority, so its
	// terminal-state durability is asserted with synchronous=FULL rather than
	// inherited from the driver default. SynchronousFull also makes Open fail
	// closed on a target that cannot carry local WAL+FULL.
	db, err := indexstore.Open(ctx, indexstore.Config{Path: cfg.Path, SynchronousFull: true})
	if err != nil {
		return nil, err
	}

	s := &Store{db: db}
	// Bounded pre-coordinator setup phase: schema DDL runs on the single pooled
	// connection before the writer pins it as the sole mutation authority. After
	// the coordinator starts, no mutation may bypass it via s.db.
	if err := s.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	s.writer = newCoordinator(db)
	s.writer.elideRawExecSavepoints = cfg.ElideRawExecSavepoints
	if configure != nil {
		configure(s.writer)
	}
	if err := s.writer.start(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// WriterStats returns an immutable aggregate snapshot of checkpoint writer
// diagnostics (queue/admission, barrier, batch size, batch/tx wall duration,
// refusals and fatals). It is safe to call concurrently with in-flight work.
// Queue occupancy is approximate; see WriterStats field docs. A nil Store
// returns a zero value.
//
// Experimental: the package is Experimental; this surface may change with only
// an in-release note.
func (s *Store) WriterStats() WriterStats {
	if s == nil || s.writer == nil {
		return WriterStats{}
	}
	return s.writer.statsSnapshot()
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	if s.writer != nil {
		return s.writer.close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS reflow_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			schema_version INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			source_provider TEXT,
			source_bucket TEXT,
			source_root TEXT,
			source_uri TEXT,
			opcheckpoint_operation TEXT,
			opcheckpoint_config_fingerprint TEXT
		);`,
		`INSERT OR IGNORE INTO reflow_meta (id, schema_version, created_at) VALUES (1, ?, ?);`,
		`CREATE TABLE IF NOT EXISTS reflow_items (
			source_uri TEXT NOT NULL,
			dest_uri TEXT NOT NULL,
			unit_key TEXT,
			source_key TEXT,
			dest_key TEXT,
			source_etag TEXT,
			source_size_bytes INTEGER,
			status TEXT NOT NULL,
			bytes INTEGER,
			reason TEXT,
			error_code TEXT,
			error_message TEXT,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (source_uri, dest_uri)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_reflow_items_dest_key ON reflow_items(dest_key);`,
		`CREATE TABLE IF NOT EXISTS reflow_lanes (
			plan_version INTEGER NOT NULL,
			plan_digest TEXT NOT NULL,
			lane_ordinal INTEGER NOT NULL,
			eof INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (plan_version, plan_digest, lane_ordinal)
		);`,
		`CREATE TABLE IF NOT EXISTS reflow_admissions (
			admission_key TEXT PRIMARY KEY,
			plan_version INTEGER NOT NULL,
			plan_digest TEXT NOT NULL,
			lane_ordinal INTEGER NOT NULL,
			source_provider TEXT NOT NULL,
			base_identity TEXT NOT NULL,
			source_key TEXT NOT NULL,
			revision_kind TEXT NOT NULL,
			revision_value TEXT NOT NULL,
			admitted_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_reflow_admissions_lane ON reflow_admissions(plan_version, plan_digest, lane_ordinal);`,
		`CREATE TABLE IF NOT EXISTS reflow_probe_outcomes (
			admission_key TEXT PRIMARY KEY,
			outcome TEXT NOT NULL,
			recorded_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS reflow_work_units (
			unit_key TEXT PRIMARY KEY,
			admission_key TEXT NOT NULL,
			kind TEXT NOT NULL,
			recorded_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_reflow_work_units_admission ON reflow_work_units(admission_key);`,
		`CREATE TABLE IF NOT EXISTS reflow_unit_terminals (
			unit_key TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			acknowledged_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS reflow_dest_key_sources (
			dest_key TEXT NOT NULL,
			source_uri TEXT NOT NULL,
			source_etag TEXT,
			source_size_bytes INTEGER,
			first_seen_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			seen_count INTEGER NOT NULL,
			PRIMARY KEY (dest_key, source_uri)
		);`,
		`CREATE TABLE IF NOT EXISTS reflow_dest_keys (
			dest_key TEXT PRIMARY KEY,
			first_observed_at TEXT NOT NULL,
			last_observed_at TEXT NOT NULL,
			observed_count INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS reflow_collisions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			dest_key TEXT NOT NULL,
			kind TEXT NOT NULL,
			source_uri TEXT NOT NULL,
			source_etag TEXT,
			source_size_bytes INTEGER,
			dest_etag TEXT,
			dest_size_bytes INTEGER,
			noted_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_reflow_collisions_dest_key ON reflow_collisions(dest_key);`,
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i, stmt := range stmts {
		if i == 1 {
			if _, err := s.db.ExecContext(ctx, stmt, schemaVersion, now); err != nil {
				return fmt.Errorf("init schema meta: %w", err)
			}
			continue
		}
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	for _, stmt := range []string{
		`ALTER TABLE reflow_meta ADD COLUMN source_provider TEXT;`,
		`ALTER TABLE reflow_meta ADD COLUMN source_bucket TEXT;`,
		`ALTER TABLE reflow_meta ADD COLUMN source_root TEXT;`,
		`ALTER TABLE reflow_meta ADD COLUMN source_uri TEXT;`,
		`ALTER TABLE reflow_meta ADD COLUMN opcheckpoint_operation TEXT;`,
		`ALTER TABLE reflow_meta ADD COLUMN opcheckpoint_config_fingerprint TEXT;`,
		`ALTER TABLE reflow_items ADD COLUMN unit_key TEXT;`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("migrate schema: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_reflow_items_unit_key ON reflow_items(unit_key) WHERE unit_key IS NOT NULL AND unit_key <> ''`); err != nil {
		return fmt.Errorf("migrate unit-key index: %w", err)
	}
	// An earlier pre-release build created this index without the uniqueness
	// invariant. Inspect its shape so normal opens keep the existing index and
	// only that development schema pays the one-time rebuild cost.
	var sourceSlotUnique int
	err := s.db.QueryRowContext(ctx, `
		SELECT "unique"
		FROM pragma_index_list('reflow_admissions')
		WHERE name = 'idx_reflow_admissions_source_slot'
	`).Scan(&sourceSlotUnique)
	switch {
	case err == nil && sourceSlotUnique == 1:
	case err == nil || errors.Is(err, sql.ErrNoRows):
		if _, err := s.db.ExecContext(ctx, `DROP INDEX IF EXISTS idx_reflow_admissions_source_slot`); err != nil {
			return fmt.Errorf("migrate admission source-slot index: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `CREATE UNIQUE INDEX idx_reflow_admissions_source_slot ON reflow_admissions(
			plan_version, plan_digest, lane_ordinal, source_provider, base_identity, source_key
		)`); err != nil {
			return fmt.Errorf(
				"migrate admission source-slot index: start with a fresh checkpoint; "+
					"pre-release checkpoints containing multiple revisions for one source slot cannot be migrated automatically: %w",
				err,
			)
		}
	default:
		return fmt.Errorf("inspect admission source-slot index: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE reflow_meta SET schema_version = ? WHERE id = 1`, schemaVersion); err != nil {
		return fmt.Errorf("migrate schema version: %w", err)
	}
	return nil
}

func (s *Store) SetSourceMetadata(ctx context.Context, provider, bucket, root, sourceURI string) error {
	return s.writer.exec(ctx, `
		UPDATE reflow_meta
		SET source_provider = ?, source_bucket = ?, source_root = ?, source_uri = ?
		WHERE id = 1
	`, provider, bucket, root, sourceURI)
}

func (s *Store) SetOperationCheckpointIdentity(ctx context.Context, operation, fingerprint string) error {
	if strings.TrimSpace(operation) == "" {
		return fmt.Errorf("operation is required")
	}
	if strings.TrimSpace(fingerprint) == "" {
		return fmt.Errorf("config fingerprint is required")
	}
	return s.writer.exec(ctx, `
		UPDATE reflow_meta
		SET opcheckpoint_operation = ?, opcheckpoint_config_fingerprint = ?
		WHERE id = 1
	`, operation, fingerprint)
}

func (s *Store) OperationCheckpointFingerprint(ctx context.Context, operation string) (string, error) {
	var gotOperation, fingerprint string
	err := s.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `
			SELECT COALESCE(opcheckpoint_operation, ''), COALESCE(opcheckpoint_config_fingerprint, '')
			FROM reflow_meta
			WHERE id = 1
		`).Scan(&gotOperation, &fingerprint)
	})
	if err != nil {
		return "", err
	}
	if gotOperation != operation || strings.TrimSpace(fingerprint) == "" {
		return "", fmt.Errorf("operation checkpoint identity not found for %s", operation)
	}
	return fingerprint, nil
}

func (s *Store) ItemDone(ctx context.Context, sourceURI, destURI string) (bool, string, error) {
	var status string
	err := s.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT status FROM reflow_items WHERE source_uri = ? AND dest_uri = ?`, sourceURI, destURI).Scan(&status)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, "", nil
		}
		return false, "", err
	}
	switch status {
	case "complete", "skipped":
		return true, status, nil
	default:
		return false, status, nil
	}
}

func (s *Store) PersistAdmissions(ctx context.Context, admissions []producer.Admission, replay bool) ([]producer.AdmissionRefusal, error) {
	if len(admissions) == 0 {
		return nil, nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	refusals := make([]producer.AdmissionRefusal, 0)
	err := s.writer.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, admission := range admissions {
			var existing producer.Admission
			var existingRevisionKind string
			superseded := false
			err := tx.QueryRowContext(ctx, `
				SELECT admission_key, plan_version, plan_digest, lane_ordinal,
					source_provider, base_identity, source_key, revision_kind,
					revision_value
				FROM reflow_admissions
				WHERE admission_key = ?
			`, admission.AdmissionKey).Scan(
				&existing.AdmissionKey, &existing.Lane.PlanVersion,
				&existing.Lane.PlanDigest, &existing.Lane.Ordinal,
				&existing.SourceProvider, &existing.BaseIdentity, &existing.SourceKey,
				&existingRevisionKind, &existing.SourceRevision.Value,
			)
			switch {
			case err == nil:
				existing.SourceRevision.Kind = providerRevisionKind(existingRevisionKind)
				if existing != admission {
					return refuseRequest(fmt.Errorf("admission key %s conflicts with durable identity", admission.AdmissionKey))
				}
				if replay {
					continue
				}
				if err := supersedeAdmissionTx(ctx, tx, existing.AdmissionKey); err != nil {
					return err
				}
				superseded = true
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}

			if !superseded {
				var priorAdmissionKey string
				err = tx.QueryRowContext(ctx, `
					SELECT admission_key
					FROM reflow_admissions
					WHERE plan_version = ? AND plan_digest = ? AND lane_ordinal = ?
						AND source_provider = ? AND base_identity = ? AND source_key = ?
				`, admission.Lane.PlanVersion, admission.Lane.PlanDigest, admission.Lane.Ordinal,
					admission.SourceProvider, admission.BaseIdentity, admission.SourceKey).Scan(&priorAdmissionKey)
				switch {
				case err == nil:
					if replay {
						refusals = append(refusals, producer.AdmissionRefusal{
							AdmissionKey: admission.AdmissionKey,
							Err: fmt.Errorf(
								"%w: source %q revision differs from its durable admission",
								provider.ErrSourceChanged, admission.SourceKey,
							),
						})
						continue
					}
					if err := supersedeAdmissionTx(ctx, tx, priorAdmissionKey); err != nil {
						return err
					}
				case !errors.Is(err, sql.ErrNoRows):
					return err
				}
			}

			_, err = tx.ExecContext(ctx, `
				INSERT INTO reflow_admissions (
					admission_key, plan_version, plan_digest, lane_ordinal,
					source_provider, base_identity, source_key, revision_kind,
					revision_value, admitted_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, admission.AdmissionKey, admission.Lane.PlanVersion, admission.Lane.PlanDigest,
				admission.Lane.Ordinal, admission.SourceProvider, admission.BaseIdentity,
				admission.SourceKey, string(admission.SourceRevision.Kind),
				admission.SourceRevision.Value, now)
			if err != nil {
				return err
			}
			var got producer.Admission
			var revisionKind string
			err = tx.QueryRowContext(ctx, `
				SELECT plan_version, plan_digest, lane_ordinal, source_provider,
					base_identity, source_key, revision_kind, revision_value
				FROM reflow_admissions WHERE admission_key = ?
			`, admission.AdmissionKey).Scan(
				&got.Lane.PlanVersion, &got.Lane.PlanDigest, &got.Lane.Ordinal,
				&got.SourceProvider, &got.BaseIdentity, &got.SourceKey,
				&revisionKind, &got.SourceRevision.Value,
			)
			if err != nil {
				return err
			}
			got.AdmissionKey = admission.AdmissionKey
			got.SourceRevision.Kind = providerRevisionKind(revisionKind)
			if got != admission {
				return refuseRequest(fmt.Errorf("admission key %s conflicts with durable identity", admission.AdmissionKey))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refusals, nil
}

// supersedeAdmissionTx resets one source slot for a fresh (non-resume) run,
// whether its revision changed or not. It removes the old keyed protocol graph
// and clears only its item-to-unit bindings so the fresh admission can mint its
// unit without growing historical admissions or colliding with the prior unit.
// The item rows themselves remain as downstream collision/resume evidence until
// the new unit checkpoints its terminal result.
func supersedeAdmissionTx(ctx context.Context, tx *sql.Tx, admissionKey string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE reflow_items
		SET unit_key = NULL
		WHERE unit_key IN (
			SELECT unit_key FROM reflow_work_units WHERE admission_key = ?
		)
	`, admissionKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM reflow_unit_terminals
		WHERE unit_key IN (
			SELECT unit_key FROM reflow_work_units WHERE admission_key = ?
		)
	`, admissionKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM reflow_work_units WHERE admission_key = ?`, admissionKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM reflow_probe_outcomes WHERE admission_key = ?`, admissionKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM reflow_admissions WHERE admission_key = ?`, admissionKey); err != nil {
		return err
	}
	return nil
}

func (s *Store) PersistDurableBatch(ctx context.Context, batch producer.DurableBatch) error {
	if len(batch.Outcomes) == 0 && len(batch.WorkUnits) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.writer.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, outcome := range batch.Outcomes {
			var exists int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM reflow_admissions WHERE admission_key = ?`, outcome.AdmissionKey).Scan(&exists); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return refuseRequest(fmt.Errorf("outcome admission %s is not durable: %w", outcome.AdmissionKey, err))
				}
				return err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT OR IGNORE INTO reflow_probe_outcomes (admission_key, outcome, recorded_at)
				VALUES (?, ?, ?)
			`, outcome.AdmissionKey, string(outcome.Outcome), now); err != nil {
				return err
			}
			var got string
			if err := tx.QueryRowContext(ctx, `SELECT outcome FROM reflow_probe_outcomes WHERE admission_key = ?`, outcome.AdmissionKey).Scan(&got); err != nil {
				return err
			}
			if got != string(outcome.Outcome) {
				return refuseRequest(fmt.Errorf("admission %s already has outcome %q, cannot replace it with %q", outcome.AdmissionKey, got, outcome.Outcome))
			}
		}
		for _, unit := range batch.WorkUnits {
			var outcome string
			if err := tx.QueryRowContext(ctx, `SELECT outcome FROM reflow_probe_outcomes WHERE admission_key = ?`, unit.AdmissionKey).Scan(&outcome); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return refuseRequest(fmt.Errorf("work unit admission %s has no durable outcome: %w", unit.AdmissionKey, err))
				}
				return err
			}
			if outcome != string(producer.OutcomeEmitted) {
				return refuseRequest(fmt.Errorf("work unit %s belongs to non-emitting outcome %q", unit.UnitKey, outcome))
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT OR IGNORE INTO reflow_work_units (unit_key, admission_key, kind, recorded_at)
				VALUES (?, ?, ?, ?)
			`, unit.UnitKey, unit.AdmissionKey, unit.Kind, now); err != nil {
				return err
			}
			var gotAdmission, gotKind string
			if err := tx.QueryRowContext(ctx, `
				SELECT admission_key, kind FROM reflow_work_units WHERE unit_key = ?
			`, unit.UnitKey).Scan(&gotAdmission, &gotKind); err != nil {
				return err
			}
			if gotAdmission != unit.AdmissionKey || gotKind != unit.Kind {
				return refuseRequest(fmt.Errorf("unit key %s conflicts with durable identity", unit.UnitKey))
			}
		}
		return nil
	})
}

func (s *Store) MarkLaneEOF(ctx context.Context, lanes []partition.LaneRef) error {
	if len(lanes) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.writer.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, lane := range lanes {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO reflow_lanes (plan_version, plan_digest, lane_ordinal, eof, updated_at)
				VALUES (?, ?, ?, 1, ?)
				ON CONFLICT(plan_version, plan_digest, lane_ordinal) DO UPDATE SET
					eof=1, updated_at=excluded.updated_at
			`, lane.PlanVersion, lane.PlanDigest, lane.Ordinal, now); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) AcknowledgeTerminals(ctx context.Context, acks []producer.TerminalAck) error {
	if len(acks) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.writer.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, ack := range acks {
			if err := acknowledgeTerminalTx(ctx, tx, ack, now); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) LaneStatus(ctx context.Context, lane partition.LaneRef) (producer.LaneStatus, error) {
	var status producer.LaneStatus
	var eof int
	err := s.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `
			SELECT
				COALESCE((SELECT eof FROM reflow_lanes
					WHERE plan_version = ? AND plan_digest = ? AND lane_ordinal = ?), 0),
				(SELECT COUNT(*) FROM reflow_admissions
					WHERE plan_version = ? AND plan_digest = ? AND lane_ordinal = ?),
				(SELECT COUNT(*) FROM reflow_probe_outcomes o
					JOIN reflow_admissions a ON a.admission_key = o.admission_key
					WHERE a.plan_version = ? AND a.plan_digest = ? AND a.lane_ordinal = ?),
				(SELECT COUNT(*) FROM reflow_work_units u
					JOIN reflow_admissions a ON a.admission_key = u.admission_key
					WHERE a.plan_version = ? AND a.plan_digest = ? AND a.lane_ordinal = ?),
				(SELECT COUNT(*) FROM reflow_unit_terminals t
					JOIN reflow_work_units u ON u.unit_key = t.unit_key
					JOIN reflow_admissions a ON a.admission_key = u.admission_key
					WHERE a.plan_version = ? AND a.plan_digest = ? AND a.lane_ordinal = ?),
				(SELECT COUNT(*) FROM reflow_admissions a
					LEFT JOIN reflow_probe_outcomes o ON o.admission_key = a.admission_key
					WHERE a.plan_version = ? AND a.plan_digest = ? AND a.lane_ordinal = ?
						AND o.admission_key IS NULL),
				(SELECT COUNT(*) FROM reflow_work_units u
					JOIN reflow_admissions a ON a.admission_key = u.admission_key
					LEFT JOIN reflow_unit_terminals t ON t.unit_key = u.unit_key
					WHERE a.plan_version = ? AND a.plan_digest = ? AND a.lane_ordinal = ?
						AND t.unit_key IS NULL)
		`,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
			lane.PlanVersion, lane.PlanDigest, lane.Ordinal,
		).Scan(&eof, &status.Admissions, &status.Outcomes, &status.WorkUnits,
			&status.DurableTerminals, &status.MissingOutcomes, &status.MissingTerminals)
	})
	if err != nil {
		return producer.LaneStatus{}, err
	}
	status.EOF = eof != 0
	status.Terminal = status.EOF && status.MissingOutcomes == 0 && status.MissingTerminals == 0
	return status, nil
}

type UpsertItemParams struct {
	UnitKey        string
	SourceURI      string
	DestURI        string
	SourceKey      string
	DestKey        string
	SourceETag     string
	SourceSize     int64
	Status         string
	Bytes          int64
	Reason         string
	ErrorCode      string
	ErrorMessage   string
	UpdatedAtRFC33 string
}

func (s *Store) UpsertItem(ctx context.Context, p UpsertItemParams) error {
	if p.UpdatedAtRFC33 == "" {
		p.UpdatedAtRFC33 = time.Now().UTC().Format(time.RFC3339Nano)
	}
	return s.writer.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return upsertItemTx(ctx, tx, p)
	})
}

// CheckpointUnit atomically records the downstream terminal and acknowledges
// its keyed work unit in the same durable transaction.
func (s *Store) CheckpointUnit(ctx context.Context, p UpsertItemParams, unitKey string) error {
	if p.UpdatedAtRFC33 == "" {
		p.UpdatedAtRFC33 = time.Now().UTC().Format(time.RFC3339Nano)
	}
	p.UnitKey = unitKey
	return s.writer.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := upsertItemTx(ctx, tx, p); err != nil {
			return err
		}
		return acknowledgeTerminalTx(ctx, tx, producer.TerminalAck{UnitKey: unitKey, Status: p.Status}, p.UpdatedAtRFC33)
	})
}

func (s *Store) UnitDone(ctx context.Context, unitKey string) (bool, string, error) {
	var status string
	err := s.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT status FROM reflow_items WHERE unit_key = ?`, unitKey).Scan(&status)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	switch status {
	case "complete", "skipped":
		return true, status, nil
	default:
		return false, status, nil
	}
}

func upsertItemTx(ctx context.Context, tx *sql.Tx, p UpsertItemParams) error {
	if p.UnitKey != "" {
		var sourceURI, destURI string
		err := tx.QueryRowContext(ctx, `
			SELECT source_uri, dest_uri FROM reflow_items WHERE unit_key = ?
		`, p.UnitKey).Scan(&sourceURI, &destURI)
		switch {
		case err == nil && (sourceURI != p.SourceURI || destURI != p.DestURI):
			return refuseRequest(fmt.Errorf(
				"reflow unit %q already belongs to item %s -> %s",
				p.UnitKey, sourceURI, destURI,
			))
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO reflow_items (
			source_uri, dest_uri, unit_key, source_key, dest_key, source_etag, source_size_bytes, status, bytes, reason, error_code, error_message, updated_at
		) VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_uri, dest_uri) DO UPDATE SET
			unit_key=COALESCE(reflow_items.unit_key, excluded.unit_key),
			source_key=excluded.source_key,
			dest_key=excluded.dest_key,
			source_etag=excluded.source_etag,
			source_size_bytes=excluded.source_size_bytes,
			status=excluded.status,
			bytes=excluded.bytes,
			reason=excluded.reason,
			error_code=excluded.error_code,
			error_message=excluded.error_message,
			updated_at=excluded.updated_at
	`,
		p.SourceURI, p.DestURI, p.UnitKey, p.SourceKey, p.DestKey, p.SourceETag, p.SourceSize, p.Status, p.Bytes, p.Reason, p.ErrorCode, p.ErrorMessage, p.UpdatedAtRFC33,
	)
	if err != nil || p.UnitKey == "" {
		return err
	}
	var durableUnit sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT unit_key FROM reflow_items WHERE source_uri = ? AND dest_uri = ?
	`, p.SourceURI, p.DestURI).Scan(&durableUnit); err != nil {
		return err
	}
	if !durableUnit.Valid || durableUnit.String != p.UnitKey {
		return refuseRequest(fmt.Errorf(
			"reflow item %s -> %s already belongs to unit %q, cannot replace it with %q",
			p.SourceURI, p.DestURI, durableUnit.String, p.UnitKey,
		))
	}
	return nil
}

func acknowledgeTerminalTx(ctx context.Context, tx *sql.Tx, ack producer.TerminalAck, now string) error {
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM reflow_work_units WHERE unit_key = ?`, ack.UnitKey).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return refuseRequest(fmt.Errorf("terminal unit %s is not durable: %w", ack.UnitKey, err))
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO reflow_unit_terminals (unit_key, status, acknowledged_at)
		VALUES (?, ?, ?)
	`, ack.UnitKey, ack.Status, now); err != nil {
		return err
	}
	var got string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM reflow_unit_terminals WHERE unit_key = ?`, ack.UnitKey).Scan(&got); err != nil {
		return err
	}
	if got != ack.Status {
		return refuseRequest(fmt.Errorf("unit %s already has terminal status %q, cannot replace it with %q", ack.UnitKey, got, ack.Status))
	}
	return nil
}

func providerRevisionKind(value string) provider.RevisionKind {
	return provider.RevisionKind(value)
}

func (s *Store) NoteDestKeySource(ctx context.Context, destKey, sourceURI, sourceETag string, sourceSize int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.writer.exec(ctx, `
		INSERT INTO reflow_dest_key_sources (dest_key, source_uri, source_etag, source_size_bytes, first_seen_at, last_seen_at, seen_count)
		VALUES (?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT(dest_key, source_uri) DO UPDATE SET
			source_etag=excluded.source_etag,
			source_size_bytes=excluded.source_size_bytes,
			last_seen_at=excluded.last_seen_at,
			seen_count=reflow_dest_key_sources.seen_count + 1
	`, destKey, sourceURI, sourceETag, sourceSize, now, now)
}

func (s *Store) DestKeyObserved(ctx context.Context, destKey string) (bool, error) {
	var exists int
	err := s.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `SELECT 1 FROM reflow_dest_keys WHERE dest_key = ?`, destKey).Scan(&exists)
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) MarkDestKeyObserved(ctx context.Context, destKey string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.writer.exec(ctx, `
		INSERT INTO reflow_dest_keys (dest_key, first_observed_at, last_observed_at, observed_count)
		VALUES (?, ?, ?, 1)
		ON CONFLICT(dest_key) DO UPDATE SET
			last_observed_at=excluded.last_observed_at,
			observed_count=reflow_dest_keys.observed_count + 1
	`, destKey, now, now)
}

type CollisionKind string

const (
	CollisionDuplicate CollisionKind = "duplicate"
	CollisionConflict  CollisionKind = "conflict"
	CollisionOverwrite CollisionKind = "overwrite"
)

func (s *Store) NoteCollision(ctx context.Context, destKey string, kind CollisionKind, sourceURI, sourceETag string, sourceSize int64, destETag string, destSize int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return s.writer.exec(ctx, `
		INSERT INTO reflow_collisions (dest_key, kind, source_uri, source_etag, source_size_bytes, dest_etag, dest_size_bytes, noted_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, destKey, string(kind), sourceURI, sourceETag, sourceSize, destETag, destSize, now)
}
