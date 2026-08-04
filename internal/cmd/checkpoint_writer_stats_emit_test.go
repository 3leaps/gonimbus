package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/output"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/reflowstate"
)

// emitCheckpointWriterStatsIfPresent must surface sterile aggregates after a
// mutation on a real reflowstate.Store (type-assert path), and must no-op for
// stores that do not implement WriterStats.
func TestEmitCheckpointWriterStatsIfPresent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := reflowstate.Open(ctx, reflowstate.Config{Path: path})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.UpsertItem(ctx, reflowstate.UpsertItemParams{
		SourceURI: "s3://source-bucket/k", DestURI: "s3://dest-bucket/k",
		Status: "complete", Bytes: 1,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "job", "file")
	emitCheckpointWriterStatsIfPresent(ctx, w, store)
	_ = w.Close()

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("expected one JSONL stats record")
	}
	var env struct {
		Type string                                `json:"type"`
		Data reflowpkg.CheckpointWriterStatsRecord `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		t.Fatalf("unmarshal: %v\nline=%s", err, line)
	}
	if env.Type != reflowpkg.CheckpointWriterStatsRecordType {
		t.Fatalf("type=%q", env.Type)
	}
	if env.Data.Admissions < 1 || env.Data.Batches < 1 || env.Data.MaxBatch != 256 {
		t.Fatalf("stats=%+v", env.Data)
	}
	// Privacy: payload must not smuggle path/SQL/keys (struct has no such fields;
	// also assert raw line does not contain the DB path or URIs used above).
	if strings.Contains(line, path) || strings.Contains(line, "s3://") {
		t.Fatalf("stats line leaked path or URI: %s", line)
	}
}

func TestEmitCheckpointWriterStatsSkipsNilStore(t *testing.T) {
	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "job", "file")
	emitCheckpointWriterStatsIfPresent(context.Background(), w, nil)
	_ = w.Close()
	if buf.Len() != 0 {
		t.Fatalf("expected no emission for nil store, got %q", buf.String())
	}
}

// Dual-path control (entarch P1): CLI-pool and engine paths must both call
// emitCheckpointWriterStatsIfPresent. A mutation deleting either site fails this
// source inspection without a live provider.
func TestEmitCheckpointWriterStatsCallSites(t *testing.T) {
	src, err := os.ReadFile("transfer_reflow.go")
	if err != nil {
		t.Fatalf("read transfer_reflow.go: %v", err)
	}
	count := strings.Count(string(src), "emitCheckpointWriterStatsIfPresent(")
	// Definition lives in transfer_reflow_checkpoint.go; call sites only here.
	if count < 2 {
		t.Fatalf("emitCheckpointWriterStatsIfPresent call sites in transfer_reflow.go = %d, want >= 2 (engine + CLI-pool)", count)
	}
}
