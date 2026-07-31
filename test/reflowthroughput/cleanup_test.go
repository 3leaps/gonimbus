package reflowthroughput

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// recordingDeleter records what cleanup was asked to remove and can fail on
// demand, so the lifecycle is controlled without a bucket.
type recordingDeleter struct {
	deleted []string
	failOn  map[string]error
}

func (d *recordingDeleter) delete(_ context.Context, prefix string) error {
	d.deleted = append(d.deleted, prefix)
	if err, ok := d.failOn[prefix]; ok {
		return err
	}
	return nil
}

func (d *recordingDeleter) sawPrefix(prefix string) bool {
	for _, p := range d.deleted {
		if p == prefix {
			return true
		}
	}
	return false
}

// The window this exists for: minting and the first object are separated by an
// entire corpus upload. Registering ownership only after a successful upload
// leaves a partial upload with nothing responsible for removing it.
func TestPartialUploadFailureStillCleansTheSourcePrefix(t *testing.T) {
	d := &recordingDeleter{}
	ledger := &CleanupLedger{DeletePrefix: d.delete}

	uploadErr := errors.New("connection reset partway through the corpus")
	prefix, err := PrepareSourcePrefix(ledger,
		func() string { return "root/src-partial/" },
		func(string) error { return uploadErr },
	)
	if !errors.Is(err, uploadErr) {
		t.Fatalf("upload error not returned: %v", err)
	}

	owned, _ := ledger.Owned()
	if owned != prefix {
		t.Fatalf("source prefix %q is not owned after a failed upload (owned %q); "+
			"a partial upload would be stranded", prefix, owned)
	}
	if err := ledger.Run(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if !d.sawPrefix(prefix) {
		t.Fatalf("cleanup did not remove the partially uploaded prefix; removed %v", d.deleted)
	}
}

// A point can die between a successful upload and minting its destination. The
// source is owned; there is no destination to own.
func TestFailureAfterUploadBeforeDestMintCleansSourceOnly(t *testing.T) {
	d := &recordingDeleter{}
	ledger := &CleanupLedger{DeletePrefix: d.delete}

	prefix, err := PrepareSourcePrefix(ledger,
		func() string { return "root/src-uploaded/" },
		func(string) error { return nil },
	)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	_, dests := ledger.Owned()
	if len(dests) != 0 {
		t.Fatalf("destinations owned before any were minted: %v", dests)
	}
	if err := ledger.Run(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if !d.sawPrefix(prefix) || len(d.deleted) != 1 {
		t.Fatalf("expected exactly the source removed, got %v", d.deleted)
	}
}

// After a destination is minted, both prefixes are owned and both are removed.
func TestPointFailureAfterDestMintCleansBothPrefixes(t *testing.T) {
	d := &recordingDeleter{}
	ledger := &CleanupLedger{DeletePrefix: d.delete}

	src, err := PrepareSourcePrefix(ledger,
		func() string { return "root/src-both/" },
		func(string) error { return nil },
	)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	ledger.RegisterDestPrefix("root/dst-p01/")
	ledger.RegisterDestPrefix("root/dst-p02/")

	if err := ledger.Run(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	for _, want := range []string{src, "root/dst-p01/", "root/dst-p02/"} {
		if !d.sawPrefix(want) {
			t.Fatalf("cleanup did not remove %q; removed %v", want, d.deleted)
		}
	}
}

// A cleanup failure must be reported, and must not conceal a second one. The
// operator needs the full extent of what was left behind, not the first item.
func TestCleanupReportsEveryFailureNotJustTheFirst(t *testing.T) {
	srcErr := errors.New("source still has objects")
	dstErr := errors.New("dest still has objects")
	d := &recordingDeleter{failOn: map[string]error{
		"root/src-fail/": srcErr,
		"root/dst-fail/": dstErr,
	}}
	ledger := &CleanupLedger{DeletePrefix: d.delete}
	ledger.RegisterSourcePrefix("root/src-fail/")
	ledger.RegisterDestPrefix("root/dst-fail/")

	err := ledger.Run()
	if err == nil {
		t.Fatal("cleanup failure was not reported")
	}
	if !errors.Is(err, srcErr) {
		t.Fatalf("source failure not surfaced: %v", err)
	}
	if !errors.Is(err, dstErr) {
		t.Fatalf("destination failure hidden behind the source failure: %v", err)
	}
}

// A run that succeeded but left residue is not a success, so the cleanup error
// reaches the caller joined with whatever the run itself reported.
func TestCleanupFailureJoinsThePrimaryError(t *testing.T) {
	primary := errors.New("point 3 failed")
	cleanupFail := errors.New("dest prefix cleanup: objects remain")

	joined := joinCleanup(primary, cleanupFail)
	if !errors.Is(joined, primary) {
		t.Fatal("joining dropped the primary error; why the run failed would be lost")
	}
	if !errors.Is(joined, cleanupFail) {
		t.Fatal("joining dropped the cleanup error; residue would go unreported")
	}
}

func TestCleanupFailureTurnsSuccessfulRunIntoFailure(t *testing.T) {
	cleanupFail := errors.New("source prefix cleanup: objects remain")

	joined := joinCleanup(nil, cleanupFail)
	if !errors.Is(joined, cleanupFail) {
		t.Fatal("cleanup failure was dropped when the primary run succeeded")
	}
	if !strings.Contains(joined.Error(), "cleanup:") {
		t.Fatalf("cleanup failure %q is not labeled for the operator", joined)
	}
}

func TestSuccessfulCleanupPreservesPrimaryError(t *testing.T) {
	primary := errors.New("point 3 failed")

	if got := joinCleanup(primary, nil); got != primary {
		t.Fatalf("successful cleanup changed the primary error: %v", got)
	}
}

// The ordinary path: everything registered is removed, and a second call — the
// deferred fail-safe after an explicit one — is a no-op rather than a repeat.
func TestSuccessfulCleanupIsIdempotent(t *testing.T) {
	d := &recordingDeleter{}
	ledger := &CleanupLedger{DeletePrefix: d.delete}
	ledger.RegisterSourcePrefix("root/src-ok/")
	ledger.RegisterDestPrefix("root/dst-ok/")

	if err := ledger.Run(); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}
	first := len(d.deleted)
	if first != 2 {
		t.Fatalf("expected 2 removals, got %v", d.deleted)
	}
	if err := ledger.Run(); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if len(d.deleted) != first {
		t.Fatalf("second cleanup repeated removals: %v", d.deleted)
	}
}

// Keep is an operator choice to retain artifacts, so residue under it is
// expected rather than a defect. This is the flag that made me misread retained
// objects as a leak, so it is pinned explicitly.
func TestKeepRetainsEverythingAndRemovesNothing(t *testing.T) {
	d := &recordingDeleter{}
	ledger := &CleanupLedger{Keep: true, DeletePrefix: d.delete}
	ledger.RegisterSourcePrefix("root/src-keep/")
	ledger.RegisterDestPrefix("root/dst-keep/")

	if err := ledger.Run(); err != nil {
		t.Fatalf("cleanup under Keep: %v", err)
	}
	if len(d.deleted) != 0 {
		t.Fatalf("Keep removed %v; retention is the operator's choice", d.deleted)
	}
	owned, dests := ledger.Owned()
	if owned == "" || len(dests) != 1 {
		t.Fatal("Keep discarded ownership; a later non-Keep cleanup could not find the residue")
	}
}

// Local residue is verified gone, not assumed: the removal is checked by
// stat-ing afterwards.
func TestLocalResidueIsVerifiedRemoved(t *testing.T) {
	root := t.TempDir()
	destDir := filepath.Join(root, "dest-p01")
	ckPath := filepath.Join(root, "ckpt-p01.db")
	if err := os.MkdirAll(filepath.Join(destDir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ckPath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	ledger := &CleanupLedger{}
	ledger.RegisterPoint(MintedPoint{DestDir: destDir, CheckpointPath: ckPath})
	if err := ledger.Run(); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if _, err := os.Stat(destDir); !os.IsNotExist(err) {
		t.Fatal("destination directory survived cleanup")
	}
	if _, err := os.Stat(ckPath); !os.IsNotExist(err) {
		t.Fatal("checkpoint survived cleanup")
	}
}

// A run with no object-store residue must not require a deleter.
func TestLocalOnlyRunNeedsNoPrefixDeleter(t *testing.T) {
	ledger := &CleanupLedger{}
	ledger.RegisterPoint(MintedPoint{})
	if err := ledger.Run(); err != nil {
		t.Fatalf("local-only cleanup: %v", err)
	}
}

func TestCleanupErrorNamesWhichPrefixFailed(t *testing.T) {
	d := &recordingDeleter{failOn: map[string]error{"root/dst-x/": errors.New("boom")}}
	ledger := &CleanupLedger{DeletePrefix: d.delete}
	ledger.RegisterDestPrefix("root/dst-x/")

	err := ledger.Run()
	if err == nil || !strings.Contains(err.Error(), "dest prefix cleanup") {
		t.Fatalf("error %v does not say which side of the run failed to clean", err)
	}
}
