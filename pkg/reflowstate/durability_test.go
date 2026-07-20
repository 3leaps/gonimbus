package reflowstate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestOpenSelectsDurableConfigAndReopen pins the reflow wiring itself: the
// resume-authority constructor must resolve to WAL + synchronous=FULL +
// busy_timeout on the store it actually opens, on first open and again after
// the handle is dropped. Exercising reflowstate.Open (not indexstore.Open
// directly) is what proves the durability requirement is not lost between the
// reflow opener and the shared low-level store.
func TestOpenSelectsDurableConfigAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reflow-state.db")

	assertDurable := func(t *testing.T) {
		store, err := Open(ctx, Config{Path: path})
		if err != nil {
			t.Fatalf("open reflow state: %v", err)
		}
		defer func() { _ = store.Close() }()

		var synchronous int
		if err := store.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
			t.Fatalf("read synchronous: %v", err)
		}
		if synchronous != 2 { // 2 == FULL
			t.Fatalf("synchronous = %d, want 2 (FULL)", synchronous)
		}

		var journal string
		if err := store.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil {
			t.Fatalf("read journal_mode: %v", err)
		}
		if !strings.EqualFold(journal, "wal") {
			t.Fatalf("journal_mode = %q, want wal", journal)
		}

		var busyTimeout int
		if err := store.db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
			t.Fatalf("read busy_timeout: %v", err)
		}
		if busyTimeout != 5000 {
			t.Fatalf("busy_timeout = %d, want 5000", busyTimeout)
		}
	}

	assertDurable(t) // initial open
	assertDurable(t) // reopen verification after the first handle is dropped
}
