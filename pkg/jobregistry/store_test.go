package jobregistry

import (
	"testing"
	"time"
)

func TestStore_WriteGetRoundTrip(t *testing.T) {
	root := t.TempDir()
	s := NewStore(root)

	now := time.Date(2026, 1, 19, 12, 0, 0, 0, time.UTC)
	rec := &JobRecord{
		JobID:        "job-1",
		Name:         "demo",
		State:        JobStateRunning,
		ManifestPath: "/tmp/manifest.yaml",
		CreatedAt:    now,
		StartedAt:    &now,
		Identity: &EffectiveIdentity{
			StorageProvider: "aws_s3",
			CloudProvider:   "aws",
			RegionKind:      "aws",
			Region:          "us-east-1",
		},
	}

	if err := s.Write(rec); err != nil {
		t.Fatalf("Write() error: %v", err)
	}

	got, err := s.Get("job-1")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got.JobID != rec.JobID {
		t.Fatalf("job_id mismatch: got=%q want=%q", got.JobID, rec.JobID)
	}
	if got.State != rec.State {
		t.Fatalf("state mismatch: got=%q want=%q", got.State, rec.State)
	}
	if got.Identity == nil || got.Identity.StorageProvider != "aws_s3" {
		t.Fatalf("identity not persisted")
	}
}

func TestStore_ListSortsNewestFirst(t *testing.T) {
	root := t.TempDir()
	s := NewStore(root)

	t1 := time.Date(2026, 1, 19, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 19, 13, 0, 0, 0, time.UTC)

	if err := s.Write(&JobRecord{JobID: "job-1", State: JobStateRunning, ManifestPath: "/tmp/a", CreatedAt: t1, StartedAt: &t1}); err != nil {
		t.Fatalf("Write job-1: %v", err)
	}
	if err := s.Write(&JobRecord{JobID: "job-2", State: JobStateRunning, ManifestPath: "/tmp/b", CreatedAt: t2, StartedAt: &t2}); err != nil {
		t.Fatalf("Write job-2: %v", err)
	}

	got, err := s.List()
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("unexpected job count: %d", len(got))
	}
	if got[0].JobID != "job-2" {
		t.Fatalf("expected newest first, got[0]=%q", got[0].JobID)
	}
}
