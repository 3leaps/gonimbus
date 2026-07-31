package reflowthroughput

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Teardown was one DeleteObject at a time. A measured 50k run spent about two
// and a half hours removing ~94k objects after the measurement had finished —
// a cost paid on every run, including failed ones. These controls drive the
// real deletePrefixVerified through an injected backend.

type fakeDeleter struct {
	mu       sync.Mutex
	keys     []string
	deleted  map[string]bool
	inFlight int
	maxSeen  int
	delay    time.Duration
	failOn   string
	// countOverride reports remaining objects regardless of what was deleted,
	// so the verification step can be exercised independently.
	countOverride *int64
}

func newFakeDeleter(n int) *fakeDeleter {
	f := &fakeDeleter{deleted: map[string]bool{}}
	for i := 0; i < n; i++ {
		f.keys = append(f.keys, fmt.Sprintf("p/obj-%06d", i))
	}
	return f
}

// ListPage pages at 1000 keys, matching the real lister's shape.
func (f *fakeDeleter) ListPage(ctx context.Context, prefix, token string) ([]string, string, error) {
	start := 0
	if token != "" {
		if _, err := fmt.Sscanf(token, "%d", &start); err != nil {
			return nil, "", err
		}
	}
	end := start + 1000
	if end > len(f.keys) {
		end = len(f.keys)
	}
	next := ""
	if end < len(f.keys) {
		next = fmt.Sprintf("%d", end)
	}
	return f.keys[start:end], next, nil
}

func (f *fakeDeleter) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxSeen {
		f.maxSeen = f.inFlight
	}
	f.mu.Unlock()

	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
		}
	}

	f.mu.Lock()
	f.inFlight--
	f.deleted[key] = true
	f.mu.Unlock()

	if f.failOn != "" && strings.HasSuffix(key, f.failOn) {
		return fmt.Errorf("synthetic delete failure on %s", key)
	}
	return nil
}

func (f *fakeDeleter) Count(ctx context.Context, prefix string) (int64, error) {
	if f.countOverride != nil {
		return *f.countOverride, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var left int64
	for _, k := range f.keys {
		if !f.deleted[k] {
			left++
		}
	}
	return left, nil
}

func (f *fakeDeleter) stats() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleted), f.maxSeen
}

// The load-bearing control: teardown must overlap. Serial deletion is what made
// a failed run cost hours after the measurement ended.
func TestTeardownDeletesConcurrently(t *testing.T) {
	d := newFakeDeleter(2500)
	d.delay = 2 * time.Millisecond

	if err := deletePrefixVerified(context.Background(), d, "p/", 16); err != nil {
		t.Fatalf("deletePrefixVerified: %v", err)
	}
	deleted, maxSeen := d.stats()
	if maxSeen < 2 {
		t.Fatalf("observed max concurrency %d: teardown is serialized", maxSeen)
	}
	if deleted != 2500 {
		t.Fatalf("deleted %d, want 2500", deleted)
	}
}

// Every key across every page must be removed — a paging bug that dropped a
// page would leave residue the verification is supposed to catch.
func TestTeardownCoversAllPages(t *testing.T) {
	d := newFakeDeleter(3500) // four pages at 1000 per page
	if err := deletePrefixVerified(context.Background(), d, "p/", 8); err != nil {
		t.Fatalf("deletePrefixVerified: %v", err)
	}
	if deleted, _ := d.stats(); deleted != 3500 {
		t.Fatalf("deleted %d of 3500: a page was dropped", deleted)
	}
}

// The verification must still fail the teardown when objects remain, otherwise
// a run could claim zero residue it never achieved.
func TestTeardownFailsWhenObjectsRemain(t *testing.T) {
	d := newFakeDeleter(10)
	remaining := int64(3)
	d.countOverride = &remaining

	err := deletePrefixVerified(context.Background(), d, "p/", 4)
	if err == nil {
		t.Fatal("teardown reported success while objects remained")
	}
	if !strings.Contains(err.Error(), "cleanup incomplete") {
		t.Fatalf("error %q does not name incomplete cleanup", err)
	}
}

// A delete failure must propagate rather than be swallowed by the verification.
func TestTeardownPropagatesDeleteFailure(t *testing.T) {
	d := newFakeDeleter(500)
	d.delay = time.Millisecond
	d.failOn = "obj-000003"

	err := deletePrefixVerified(context.Background(), d, "p/", 8)
	if err == nil {
		t.Fatal("expected the delete failure to propagate")
	}
	if strings.Contains(err.Error(), "cleanup incomplete") {
		t.Fatalf("delete failure was masked by the verification: %v", err)
	}
}

// The empty/root refusal must survive the refactor.
func TestDeletePrefixRefusesEmptyPrefix(t *testing.T) {
	for _, bad := range []string{"", "/"} {
		if err := DeleteS3PrefixVerified(context.Background(), nil, bad); err == nil {
			t.Fatalf("prefix %q was not refused: an empty prefix widens the delete to the whole bucket", bad)
		}
	}
}
