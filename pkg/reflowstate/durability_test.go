package reflowstate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

// TestOpenSelectsDurableConfigAndReopen pins the reflow WIRING: the
// resume-authority constructor must resolve to WAL + synchronous=FULL +
// busy_timeout on the connection it actually writes through, on first open and
// again after the handle is dropped. The PRAGMAs are read through the write
// coordinator, so this asserts the configuration on the exact pinned writer
// connection (the sole mutation authority), not on some other pooled connection.
//
// Scope note: because the pool's single connection is already FULL from Open,
// this wiring test would still pass if ConfigureDurableConn were a no-op. The
// load-bearing negative control that proves ConfigureDurableConn itself resolves
// and verifies FULL lives in indexstore
// (TestConfigureDurableConnDrivesConnToFull); the two together gate the
// "FULL on the exact writer connection via ConfigureDurableConn" claim.
func TestOpenSelectsDurableConfigAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reflow-state.db")

	readPragma := func(t *testing.T, store *Store, pragma string, dst any) {
		t.Helper()
		if err := store.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
			return conn.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(dst)
		}); err != nil {
			t.Fatalf("read %s: %v", pragma, err)
		}
	}

	assertDurable := func(t *testing.T) {
		store, err := Open(ctx, Config{Path: path})
		if err != nil {
			t.Fatalf("open reflow state: %v", err)
		}
		defer func() { _ = store.Close() }()

		var synchronous int
		readPragma(t, store, "synchronous", &synchronous)
		if synchronous != 2 { // 2 == FULL
			t.Fatalf("synchronous = %d, want 2 (FULL)", synchronous)
		}

		var journal string
		readPragma(t, store, "journal_mode", &journal)
		if !strings.EqualFold(journal, "wal") {
			t.Fatalf("journal_mode = %q, want wal", journal)
		}

		var busyTimeout int
		readPragma(t, store, "busy_timeout", &busyTimeout)
		if busyTimeout != 5000 {
			t.Fatalf("busy_timeout = %d, want 5000", busyTimeout)
		}
	}

	assertDurable(t) // initial open
	assertDurable(t) // reopen verification after the first handle is dropped
}

func TestOpenKeepsCurrentAdmissionSourceSlotIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reflow-state.db")

	store, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}

	readSchemaVersion := func() int {
		t.Helper()
		db, err := indexstore.Open(ctx, indexstore.Config{Path: path})
		if err != nil {
			t.Fatalf("open for schema version: %v", err)
		}
		defer func() { _ = db.Close() }()
		var version int
		if err := db.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&version); err != nil {
			t.Fatalf("read schema version: %v", err)
		}
		return version
	}

	before := readSchemaVersion()
	store, err = Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("steady-state open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("steady-state close: %v", err)
	}
	after := readSchemaVersion()
	if after != before {
		t.Fatalf("steady-state open changed SQLite schema version from %d to %d", before, after)
	}
}

func TestOpenRebuildsPreReleasePlainAdmissionSourceSlotIndex(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reflow-state.db")

	store, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}

	db, err := indexstore.Open(ctx, indexstore.Config{Path: path})
	if err != nil {
		t.Fatalf("open development schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX idx_reflow_admissions_source_slot`); err != nil {
		t.Fatalf("drop unique index: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX idx_reflow_admissions_source_slot ON reflow_admissions(
		plan_version, plan_digest, lane_ordinal, source_provider, base_identity, source_key
	)`); err != nil {
		t.Fatalf("create plain index: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close development schema: %v", err)
	}

	store, err = Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("migrate development schema: %v", err)
	}
	defer func() { _ = store.Close() }()
	var unique int
	if err := store.writer.query(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, `
			SELECT "unique"
			FROM pragma_index_list('reflow_admissions')
			WHERE name = 'idx_reflow_admissions_source_slot'
		`).Scan(&unique)
	}); err != nil {
		t.Fatalf("inspect migrated index: %v", err)
	}
	if unique != 1 {
		t.Fatalf("migrated source-slot index unique = %d, want 1", unique)
	}
}

func TestOpenExplainsConflictingPreReleaseAdmissions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reflow-state.db")

	store, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}

	db, err := indexstore.Open(ctx, indexstore.Config{Path: path})
	if err != nil {
		t.Fatalf("open development schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX idx_reflow_admissions_source_slot`); err != nil {
		t.Fatalf("drop unique index: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX idx_reflow_admissions_source_slot ON reflow_admissions(
		plan_version, plan_digest, lane_ordinal, source_provider, base_identity, source_key
	)`); err != nil {
		t.Fatalf("create plain index: %v", err)
	}
	for _, key := range []string{"admission-old", "admission-new"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO reflow_admissions (
				admission_key, plan_version, plan_digest, lane_ordinal,
				source_provider, base_identity, source_key, revision_kind,
				revision_value, admitted_at
			) VALUES (?, 1, 'plan', 1, 's3', 's3:bucket', 'data/item', 'etag', ?, 'now')
		`, key, key); err != nil {
			t.Fatalf("insert conflicting admission: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close development schema: %v", err)
	}

	_, err = Open(ctx, Config{Path: path})
	if err == nil {
		t.Fatal("open accepted conflicting pre-release admissions")
	}
	if !strings.Contains(err.Error(), "start with a fresh checkpoint") {
		t.Fatalf("migration error does not explain the remedy: %v", err)
	}
}
