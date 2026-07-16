package indexbuild

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/output"
)

type journalWriterConfig struct {
	Path       string
	IndexSetID string
	RunID      string
	StartedAt  time.Time
	BaseURI    string
	BasePrefix string
	// CrawlPrefixes is the canonical provider-key observation plan sealed into
	// the journal header as coverage-authority provenance for recovery re-publish.
	CrawlPrefixes []string
	Now           Clock
	Events        EventSink
}

type journalWriter struct {
	cfg     journalWriterConfig
	writer  *indexsubstrate.JournalWriter
	mu      sync.Mutex
	closed  bool
	objects atomic.Int64
	errors  atomic.Int64
}

func newJournalWriter(cfg journalWriterConfig) (*journalWriter, error) {
	if err := ensureDir(filepath.Dir(cfg.Path)); err != nil {
		return nil, err
	}
	jw, err := indexsubstrate.CreateJournal(cfg.Path, indexsubstrate.JournalHeader{
		Type:               indexsubstrate.JournalHeaderType,
		JournalID:          "jrn_" + cfg.RunID + "_0001",
		IndexSetID:         cfg.IndexSetID,
		RunID:              cfg.RunID,
		Shard:              "shard-0001",
		Scope:              &indexsubstrate.Scope{Prefix: cfg.BasePrefix},
		CrawlPrefixes:      append([]string(nil), cfg.CrawlPrefixes...),
		IndexSchemaVersion: indexsubstrate.IndexSchemaVersion,
		StartedAt:          cfg.StartedAt,
	})
	if err != nil {
		return nil, err
	}
	cfg.Now = normalizeClock(cfg.Now)
	return &journalWriter{cfg: cfg, writer: jw}, nil
}

func (w *journalWriter) WriteObject(ctx context.Context, obj *output.ObjectRecord) error {
	if obj == nil {
		return nil
	}
	size := obj.Size
	storageClass := nonEmptyStringPtr(obj.StorageClass)
	_, err := w.writer.Append(indexsubstrate.ObjectRecord{
		Op:           indexsubstrate.ObjectRecordOpObserve,
		RelKey:       deriveRelKey(w.cfg.BaseURI, obj.Key),
		ObservedAt:   w.cfg.Now(),
		SizeBytes:    &size,
		ETag:         obj.ETag,
		LastModified: &obj.LastModified,
		StorageClass: storageClass,
	})
	if err != nil {
		return err
	}
	w.objects.Add(1)
	return nil
}

func (w *journalWriter) WriteError(ctx context.Context, errRec *output.ErrorRecord) error {
	w.errors.Add(1)
	if errRec == nil {
		return nil
	}
	return emitEvent(ctx, w.cfg.Events, Event{
		Type:    EventTypeCrawlError,
		RunID:   w.cfg.RunID,
		Message: errRec.Message,
		Details: map[string]any{
			"code":   errRec.Code,
			"key":    errRec.Key,
			"prefix": errRec.Prefix,
		},
	})
}

func (w *journalWriter) WriteProgress(context.Context, *output.ProgressRecord) error   { return nil }
func (w *journalWriter) WriteSummary(context.Context, *output.SummaryRecord) error     { return nil }
func (w *journalWriter) WritePrefix(context.Context, *output.PrefixRecord) error       { return nil }
func (w *journalWriter) WritePreflight(context.Context, *output.PreflightRecord) error { return nil }
func (w *journalWriter) WriteTransfer(context.Context, *output.TransferRecord) error   { return nil }
func (w *journalWriter) WriteSkip(context.Context, *output.SkipRecord) error           { return nil }

func (w *journalWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if err := w.writer.Close(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *journalWriter) Seal() error {
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if closed {
		return fmt.Errorf("journal is closed")
	}
	if err := w.writer.Seal(w.cfg.Now()); err != nil {
		return fmt.Errorf("seal journal: %w", err)
	}
	return nil
}

func (w *journalWriter) ObjectCount() int64 { return w.objects.Load() }
func (w *journalWriter) ErrorCount() int64  { return w.errors.Load() }

func nonEmptyStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	out := value
	return &out
}

var _ output.Writer = (*journalWriter)(nil)
