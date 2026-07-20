package indexstore

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenSynchronousFullAssertedAndReopen verifies the durability-authoritative
// store sets and re-verifies synchronous=FULL + WAL on open and again on reopen.
// The value is asserted, not inherited from the driver default, and the reopen
// path re-runs the verification after the handle is dropped.
func TestOpenSynchronousFullAssertedAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "durable.db")

	assertFullWAL := func(t *testing.T) {
		db, err := Open(ctx, Config{Path: path, SynchronousFull: true})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()

		var sync int
		if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync); err != nil {
			t.Fatalf("read synchronous: %v", err)
		}
		if sync != sqliteSynchronousFull {
			t.Fatalf("synchronous = %d, want %d (FULL)", sync, sqliteSynchronousFull)
		}
		var journal string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
			t.Fatalf("read journal_mode: %v", err)
		}
		if !strings.EqualFold(journal, "wal") {
			t.Fatalf("journal_mode = %q, want wal", journal)
		}
	}

	assertFullWAL(t) // initial open
	assertFullWAL(t) // reopen verification after the first handle is dropped
}

// TestOpenWithoutSynchronousFullStillOpens keeps the rebuildable index-build path
// (SynchronousFull=false) unaffected by the durability-config addition, including
// the in-memory target that a rebuildable store may still use.
func TestOpenWithoutSynchronousFullStillOpens(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rebuildable.db")
	db, err := Open(ctx, Config{Path: path})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	mem, err := Open(ctx, Config{Path: ":memory:"})
	if err != nil {
		t.Fatalf("open in-memory rebuildable store: %v", err)
	}
	_ = mem.Close()
}

// TestSynchronousFullRejectsInMemoryTarget is a negative control: a
// durability-authoritative store must fail closed rather than silently succeed
// when the resolved target is :memory:, which cannot carry local WAL+FULL.
func TestSynchronousFullRejectsInMemoryTarget(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Path: ":memory:", SynchronousFull: true})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open with SynchronousFull on :memory: succeeded; want fail-closed error")
	}
	if db != nil {
		t.Fatal("Open returned a non-nil handle alongside the fail-closed error")
	}
}

// TestConfigureLocalSQLiteRejectsNonFileDSN is a negative control at the
// configuration boundary: SynchronousFull on a non-file (URL/remote) DSN must
// fail closed, while a rebuildable store on the same DSN stays permissive.
func TestConfigureLocalSQLiteRejectsNonFileDSN(t *testing.T) {
	ctx := context.Background()
	// A live handle is required only so the nil-guard is not the branch under
	// test; the DSN string is what drives the fail-closed decision.
	db, err := sql.Open(driverLibsql, ":memory:")
	if err != nil {
		t.Fatalf("open probe handle: %v", err)
	}
	defer func() { _ = db.Close() }()

	const nonFileDSN = "libsql://example.invalid"
	if err := configureLocalSQLite(ctx, db, nonFileDSN, true); err == nil {
		t.Fatal("configureLocalSQLite(synchronousFull=true) on non-file DSN succeeded; want fail-closed error")
	}
	if err := configureLocalSQLite(ctx, db, nonFileDSN, false); err != nil {
		t.Fatalf("configureLocalSQLite(synchronousFull=false) on non-file DSN errored: %v", err)
	}
}
