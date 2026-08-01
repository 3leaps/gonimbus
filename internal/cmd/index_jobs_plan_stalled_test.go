package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestResolveJobIDStrictIsBytePreservingForDeadRunningJob(t *testing.T) {
	root := t.TempDir()
	store := jobregistry.NewStore(root)
	now := time.Now().UTC()
	start := uint64(1)
	// Dead PID under running state — mutating Get would demote to unknown.
	rec := &jobregistry.JobRecord{
		JobID:                  "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		Type:                   jobregistry.JobTypeIndexBuild,
		State:                  jobregistry.JobStateRunning,
		PID:                    1 << 30,
		ProcessStartTimeUnixMS: &start,
		CreatedAt:              now,
		StartedAt:              &now,
		LastHeartbeat:          &now,
	}
	if err := store.Write(rec); err != nil {
		t.Fatal(err)
	}
	jobPath := filepath.Join(root, rec.JobID, "job.json")
	before, err := os.ReadFile(jobPath)
	if err != nil {
		t.Fatal(err)
	}
	sumBefore := sha256.Sum256(before)

	got, err := resolveJobIDStrict(store, rec.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got != rec.JobID {
		t.Fatalf("resolved %q", got)
	}

	after, err := os.ReadFile(jobPath)
	if err != nil {
		t.Fatal(err)
	}
	sumAfter := sha256.Sum256(after)
	if hex.EncodeToString(sumBefore[:]) != hex.EncodeToString(sumAfter[:]) {
		t.Fatalf("resolveJobIDStrict must be byte-preserving for dead running job\nbefore=%s\nafter=%s", before, after)
	}

	// Prefix path must also be strict.
	got, err = resolveJobIDStrict(store, "aaaaaaaa-aaaa")
	if err != nil {
		t.Fatal(err)
	}
	if got != rec.JobID {
		t.Fatalf("prefix resolved %q", got)
	}
	after2, err := os.ReadFile(jobPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after2) != string(before) {
		t.Fatal("prefix resolveJobIDStrict must be byte-preserving")
	}
}
