package reflowstate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
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
