package indexsubstrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Spill/merge is the current-state row source drained by the production publish
// path (PublishSnapshot) in place of the materialized Compact projection. The
// source itself does not publish artifacts, advance latest, or emit lineage —
// PublishSnapshot and the durable build path own those: ordinary builds stream
// the verified parent's rows through this source and publish continuity
// metadata derived from that same verified capture.

const (
	spillMergeWorkspaceDir  = "spillmerge"
	spillMergeMetaName      = "attempt.json"
	spillMergeLockName      = "attempt.lock"
	spillMergeFormatVersion = 1
	spillRunKindParent      = "parent"
	spillRunKindEvents      = "events"
	spillMagic              = "GNSPILL1"
	defaultMaxBufferedRows  = 64_000
	defaultMaxBufferedBytes = 64 << 20
	// defaultMaxRecordBytes bounds a single journal line. The journal header line
	// carries the full crawl-prefix plan, so it grows with scope prefix count; at
	// a very wide/dense scope (tens of thousands of prefixes) the header exceeds
	// the old 1 MiB and fails a successive build closed at the journal phase. This
	// 16 MiB ceiling covers headers up to roughly the scope-prefix cap with
	// headroom; overridable per build (PublishConfig.SpillBudget.MaxRecordBytes).
	defaultMaxRecordBytes    = 16 << 20
	defaultMaxJournalSources = 256
	// defaultMaxWorkspaceBytes is the live on-disk spill workspace ceiling. A
	// successive build stages the full prior current-state into this workspace
	// before merging, so peak demand scales ~linearly with corpus size; the prior
	// 512 MiB froze successive builds at the ~single-segment boundary. Field peak
	// runs ~1.2-1.4 KiB/row and rises with scale, so this 16 GiB ceiling covers
	// roughly the ~10M-row tier; larger runs must set an explicit bound. It is
	// overridable per build (PublishConfig.SpillBudget.MaxWorkspaceBytes); the
	// value is a ceiling, not a reservation, and not a guarantee at 10M+.
	defaultMaxWorkspaceBytes = 16 << 30
	defaultMaxSpillRuns      = 4096
	defaultMaxFanIn          = 16
	defaultMaxMergePasses    = 64
)

// SpillMergeCategory classifies stable spill/merge failures.
type SpillMergeCategory string

const (
	SpillMergeInvalidConfig   SpillMergeCategory = "invalid_config"
	SpillMergeParentOrder     SpillMergeCategory = "parent_order"
	SpillMergeJournal         SpillMergeCategory = "journal"
	SpillMergeBudgetExhausted SpillMergeCategory = "budget"
	SpillMergeWorkspace       SpillMergeCategory = "workspace"
	SpillMergeSpillIntegrity  SpillMergeCategory = "spill_integrity"
	SpillMergeIO              SpillMergeCategory = "io"
	SpillMergeCanceled        SpillMergeCategory = "canceled"
)

// SpillMergeError is the typed failure surface for prepare/drain/close.
type SpillMergeError struct {
	Category  SpillMergeCategory
	Phase     string
	Message   string
	AttemptID string
	Cause     error
}

func (e *SpillMergeError) Error() string {
	if e == nil {
		return "spill merge error"
	}
	// Disclosure contract: category/phase/message/attempt only. Never render
	// Cause.Error() (may contain configured paths). Unwrap preserves classification.
	msg := fmt.Sprintf("spill_merge %s phase=%s: %s", e.Category, e.Phase, e.Message)
	if e.AttemptID != "" {
		msg += " attempt=" + e.AttemptID
	}
	if e.Cause != nil {
		msg += " cause_class=" + classifySpillCause(e.Cause)
	}
	return msg
}

func classifySpillCause(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, os.ErrPermission):
		return "permission"
	case errors.Is(err, os.ErrNotExist):
		return "not_found"
	case errors.Is(err, os.ErrExist):
		return "exist"
	case errors.Is(err, os.ErrClosed):
		return "closed"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	default:
		// ENOSPC / EDQUOT often surface as *os.PathError / *fs.PathError with Op.
		var pe *os.PathError
		if errors.As(err, &pe) {
			// Do not include pe.Path. Map common ops only.
			if pe.Err != nil {
				msg := pe.Err.Error()
				if strings.Contains(msg, "no space") || strings.Contains(msg, "ENOSPC") {
					return "no_space"
				}
				if strings.Contains(msg, "permission") || strings.Contains(msg, "denied") {
					return "permission"
				}
			}
			return "path_op"
		}
		return "other"
	}
}

func (e *SpillMergeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *SpillMergeError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == context.Canceled && e.Category == SpillMergeCanceled {
		return true
	}
	return false
}

func spillErr(cat SpillMergeCategory, phase, msg string, attempt string, cause error) error {
	return &SpillMergeError{Category: cat, Phase: phase, Message: msg, AttemptID: attempt, Cause: cause}
}

// SpillMergeBudget bounds memory, workspace disk, and merge topology.
// Zero fields are filled by DefaultSpillMergeBudget.
type SpillMergeBudget struct {
	MaxBufferedRows   int
	MaxBufferedBytes  int64
	MaxRecordBytes    int64
	MaxJournalSources int
	MaxWorkspaceBytes int64
	MaxSpillRuns      int
	MaxFanIn          int
	MaxMergePasses    int
}

// DefaultSpillMergeBudget returns the frozen spill-merge budget defaults.
func DefaultSpillMergeBudget() SpillMergeBudget {
	return SpillMergeBudget{
		MaxBufferedRows:   defaultMaxBufferedRows,
		MaxBufferedBytes:  defaultMaxBufferedBytes,
		MaxRecordBytes:    defaultMaxRecordBytes,
		MaxJournalSources: defaultMaxJournalSources,
		MaxWorkspaceBytes: defaultMaxWorkspaceBytes,
		MaxSpillRuns:      defaultMaxSpillRuns,
		MaxFanIn:          defaultMaxFanIn,
		MaxMergePasses:    defaultMaxMergePasses,
	}
}

func (b SpillMergeBudget) withDefaults() SpillMergeBudget {
	d := DefaultSpillMergeBudget()
	if b.MaxBufferedRows == 0 {
		b.MaxBufferedRows = d.MaxBufferedRows
	}
	if b.MaxBufferedBytes == 0 {
		b.MaxBufferedBytes = d.MaxBufferedBytes
	}
	if b.MaxRecordBytes == 0 {
		b.MaxRecordBytes = d.MaxRecordBytes
	}
	if b.MaxJournalSources == 0 {
		b.MaxJournalSources = d.MaxJournalSources
	}
	if b.MaxWorkspaceBytes == 0 {
		b.MaxWorkspaceBytes = d.MaxWorkspaceBytes
	}
	if b.MaxSpillRuns == 0 {
		b.MaxSpillRuns = d.MaxSpillRuns
	}
	if b.MaxFanIn == 0 {
		b.MaxFanIn = d.MaxFanIn
	}
	if b.MaxMergePasses == 0 {
		b.MaxMergePasses = d.MaxMergePasses
	}
	return b
}

func (b SpillMergeBudget) validate() error {
	b = b.withDefaults()
	switch {
	case b.MaxBufferedRows < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxBufferedRows must be >= 1", "", nil)
	case b.MaxBufferedBytes < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxBufferedBytes must be >= 1", "", nil)
	case b.MaxRecordBytes < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxRecordBytes must be >= 1", "", nil)
	case b.MaxJournalSources < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxJournalSources must be >= 1", "", nil)
	case b.MaxWorkspaceBytes < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxWorkspaceBytes must be >= 1", "", nil)
	case b.MaxSpillRuns < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxSpillRuns must be >= 1", "", nil)
	case b.MaxFanIn < 3:
		// Spill-run FD budget must allow >=2 inputs + 1 output during multi-pass merge
		// (and parent + event(s) at drain). MaxFanIn bounds spill-run FDs only.
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxFanIn must be >= 3", "", nil)
	case b.MaxMergePasses < 1:
		return spillErr(SpillMergeInvalidConfig, "validate", "MaxMergePasses must be >= 1", "", nil)
	default:
		return nil
	}
}

// SpillMergeStats are deterministic high-water counters for hermetic envelope proofs.
type SpillMergeStats struct {
	ObservedRecords    int
	EnrichmentRecords  int
	EmittedRows        int
	NewTombstones      int
	PeakBufferedRows   int
	PeakBufferedBytes  int64
	PeakWorkspaceBytes int64
	SpillRunCount      int
	MergePasses        int
	PeakOpenFiles      int
	Complete           bool
}

// ParentRowSource supplies already-authorized, strictly sorted unique parent rows.
// Parent selection and digest binding remain for continuity activation.
type ParentRowSource interface {
	Next(ctx context.Context) (CurrentObjectRow, error)
	Close() error
}

// SliceParentRows adapts a pre-sorted unique slice as a ParentRowSource.
type SliceParentRows struct {
	rows []CurrentObjectRow
	i    int
}

// NewSliceParentRows returns a parent source over rows. Order is not sorted here;
// prepare rejects out-of-order or duplicate keys.
func NewSliceParentRows(rows []CurrentObjectRow) *SliceParentRows {
	cp := make([]CurrentObjectRow, len(rows))
	copy(cp, rows)
	return &SliceParentRows{rows: cp}
}

func (s *SliceParentRows) Next(ctx context.Context) (CurrentObjectRow, error) {
	if s == nil {
		return CurrentObjectRow{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return CurrentObjectRow{}, err
	}
	if s.i >= len(s.rows) {
		return CurrentObjectRow{}, io.EOF
	}
	row := s.rows[s.i]
	s.i++
	return row, nil
}

func (s *SliceParentRows) Close() error { return nil }

// SpillMergeConfig configures prepare-then-drain current-state merge.
type SpillMergeConfig struct {
	IndexSetID   string
	RunID        string
	RunStartedAt time.Time
	Parent       ParentRowSource
	JournalPaths []string
	Coverage     []CoverageAttestation
	Mode         PublicationMode
	// SpillRoot is an explicit operator-controlled directory. Required.
	SpillRoot string
	Budget    SpillMergeBudget
}

// CurrentStateSource is a pull row source that exposes no rows until READY.
type CurrentStateSource struct {
	cfg          SpillMergeConfig
	budget       SpillMergeBudget
	attemptID    string
	workspace    string
	spillRoot    *os.Root // held for lifetime of attempt
	wsRoot       *os.Root // attempt directory root
	lockFile     *os.File
	attemptBound os.FileInfo
	// ownedTrash is the spillmerge-relative quarantine name for an already
	// renamed owned attempt dentry. Retained across failed Close retries until
	// its checked deletion succeeds. Never points at a non-owned epoch.
	ownedTrash string

	parentRun   string   // absolute path (debug / tests)
	eventRuns   []string // absolute paths
	parentHeld  *heldSpillRun
	eventHeld   []*heldSpillRun
	runSizes    map[string]int64 // absolute path -> live charged bytes (incl footer)
	metaBytes   int64
	runsCreated int
	runSeq      int

	// live resource ledger
	liveWSBytes int64
	// openFDs counts infrastructure (roots/lock) + spill run FDs for peaks.
	openFDs int
	// spillFDs counts only spill run readers/writers against MaxFanIn.
	spillFDs int

	ready          bool
	closed         bool
	closeErr       error
	termErr        error
	atEOF          bool
	drainedOK      bool
	parentClosed   bool
	parentCloseErr error
	drainCloseErr  error
	// heldCloseErr is terminal once a held spill-run Close fails: retained across
	// later Close calls; Complete must never become true after it is set.
	heldCloseErr error

	// drain state
	parentReader *spillRunReader
	eventReaders []*spillRunReader
	parentPeek   *CurrentObjectRow
	eventPeek    *spillEvent
	hasParent    bool
	hasEvent     bool
	parentDone   bool
	eventDone    bool

	stats SpillMergeStats
}

// PrepareCurrentStateSource validates inputs, stages sealed spill runs, and
// returns a READY source. No row is exposed until preparation succeeds.
func PrepareCurrentStateSource(ctx context.Context, cfg SpillMergeConfig) (*CurrentStateSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, spillErr(SpillMergeCanceled, "validate", "context canceled", "", err)
	}
	budget := cfg.Budget.withDefaults()
	if err := budget.validate(); err != nil {
		return nil, err
	}
	if err := validateAuthoritativeRunStartedAt(cfg.RunStartedAt); err != nil {
		return nil, spillErr(SpillMergeInvalidConfig, "validate", "run_started_at refused", "", err)
	}
	cfg.IndexSetID = strings.TrimSpace(cfg.IndexSetID)
	cfg.RunID = strings.TrimSpace(cfg.RunID)
	if cfg.IndexSetID == "" || cfg.RunID == "" {
		return nil, spillErr(SpillMergeInvalidConfig, "validate", "index_set_id and run_id are required", "", nil)
	}
	if strings.TrimSpace(cfg.SpillRoot) == "" {
		return nil, spillErr(SpillMergeInvalidConfig, "validate", "SpillRoot is required", "", nil)
	}
	if len(cfg.JournalPaths) > budget.MaxJournalSources {
		return nil, spillErr(SpillMergeBudgetExhausted, "validate", "journal source count exceeds MaxJournalSources", "", nil)
	}
	if cfg.Parent == nil {
		cfg.Parent = NewSliceParentRows(nil)
	}

	src := &CurrentStateSource{
		cfg:      cfg,
		budget:   budget,
		runSizes: make(map[string]int64),
	}
	if err := src.createWorkspace(); err != nil {
		_ = src.closeParent()
		return nil, err
	}
	defer func() {
		if src.ready {
			return
		}
		_ = src.closeParent()
		_ = src.cleanupWorkspace(false)
	}()

	if err := src.stageParent(ctx); err != nil {
		return nil, err
	}
	if err := src.stageJournals(ctx); err != nil {
		return nil, err
	}
	if err := src.prepareMerge(ctx); err != nil {
		return nil, err
	}
	src.ready = true
	src.stats.SpillRunCount = src.runsCreated
	if src.liveWSBytes > src.stats.PeakWorkspaceBytes {
		src.stats.PeakWorkspaceBytes = src.liveWSBytes
	}
	src.stats.PeakOpenFiles = max(src.stats.PeakOpenFiles, src.openFDs)
	return src, nil
}

// Next returns the next sorted current-state row. Ownership of pointer fields
// is transferred to the caller (no buffer reuse across calls).
func (s *CurrentStateSource) Next(ctx context.Context) (CurrentObjectRow, error) {
	if s == nil {
		return CurrentObjectRow{}, spillErr(SpillMergeInvalidConfig, "drain", "nil source", "", nil)
	}
	if s.closed {
		return CurrentObjectRow{}, spillErr(SpillMergeInvalidConfig, "drain", "source closed", s.attemptID, nil)
	}
	if !s.ready {
		return CurrentObjectRow{}, spillErr(SpillMergeInvalidConfig, "drain", "source not ready", s.attemptID, nil)
	}
	if s.termErr != nil {
		return CurrentObjectRow{}, s.termErr
	}
	if s.atEOF {
		return CurrentObjectRow{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		s.termErr = spillErr(SpillMergeCanceled, "drain", "context canceled", s.attemptID, err)
		return CurrentObjectRow{}, s.termErr
	}
	if err := s.ensureDrainOpen(ctx); err != nil {
		s.termErr = err
		return CurrentObjectRow{}, err
	}
	row, err := s.nextRow(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.atEOF = true
			s.drainedOK = true
			// Complete requires successful final Close after full EOF.
			return CurrentObjectRow{}, io.EOF
		}
		s.termErr = err
		return CurrentObjectRow{}, err
	}
	s.stats.EmittedRows++
	return row, nil
}

// Stats returns a copy of high-water counters. Complete is true only after
// full EOF plus successful final integrity/close.
func (s *CurrentStateSource) Stats() SpillMergeStats {
	if s == nil {
		return SpillMergeStats{}
	}
	return s.stats
}

// AttemptID returns the opaque workspace attempt identifier.
func (s *CurrentStateSource) AttemptID() string {
	if s == nil {
		return ""
	}
	return s.attemptID
}

// WorkspaceDir returns the attempt directory (for tests / orphan checks).
func (s *CurrentStateSource) WorkspaceDir() string {
	if s == nil {
		return ""
	}
	return s.workspace
}

// Close releases drain handles and removes the owned attempt workspace.
// Idempotent on full success. Partial close/cleanup failures are sticky:
// subsequent Close retries failed components and returns the same class of
// error until success.
// Complete is set only when drain reached EOF with no terminal error and
// Close finishes without error.
func (s *CurrentStateSource) Close() error {
	if s == nil {
		return nil
	}
	// Terminal held-run close failure: always return it; never Complete.
	if s.heldCloseErr != nil && s.closed {
		return s.heldCloseErr
	}
	if s.closed && s.closeErr == nil && s.heldCloseErr == nil {
		return nil
	}
	var first error
	if err := s.closeDrainErr(); err != nil {
		s.drainCloseErr = err
		if first == nil {
			first = err
		}
	} else {
		s.drainCloseErr = nil
	}
	if err := s.closeParent(); err != nil {
		if first == nil {
			first = err
		}
	}
	if err := s.closeHeldRuns(); err != nil {
		// Terminal: sticky for every subsequent Close.
		s.heldCloseErr = err
		if first == nil {
			first = err
		}
	}
	if err := s.cleanupWorkspace(true); err != nil {
		s.closeErr = err
		if first == nil {
			first = err
		}
		// Workspace not fully cleaned — allow cleanup retry, but heldCloseErr stays.
		return first
	}
	if first != nil {
		s.closeErr = first
		s.closed = true
		// Never Complete after heldCloseErr or other finalization failure.
		return first
	}
	s.closed = true
	s.closeErr = nil
	if s.drainedOK && s.termErr == nil && s.heldCloseErr == nil {
		s.stats.Complete = true
	}
	return nil
}

func (s *CurrentStateSource) closeParent() error {
	if s == nil {
		return nil
	}
	if s.parentClosed && s.parentCloseErr == nil {
		return nil
	}
	if s.cfg.Parent == nil {
		s.parentClosed = true
		s.parentCloseErr = nil
		return nil
	}
	if err := s.cfg.Parent.Close(); err != nil {
		s.parentCloseErr = err
		// Do not set parentClosed: allow retry.
		return s.fail(SpillMergeIO, "close", "parent close failed", err)
	}
	s.parentClosed = true
	s.parentCloseErr = nil
	return nil
}

func (s *CurrentStateSource) closeHeldRuns() error {
	if s.heldCloseErr != nil {
		return s.heldCloseErr
	}
	var first error
	if s.parentHeld != nil {
		err := s.parentHeld.Close()
		// Always drop the handle bookkeeping (avoid double-close); OS Close is once.
		s.parentHeld = nil
		s.releaseSpillFD(1)
		if err != nil && first == nil {
			first = s.fail(SpillMergeIO, "close", "held parent run close failed", err)
		}
	}
	for i, h := range s.eventHeld {
		if h == nil {
			continue
		}
		err := h.Close()
		s.eventHeld[i] = nil
		s.releaseSpillFD(1)
		if err != nil && first == nil {
			first = s.fail(SpillMergeIO, "close", "held event run close failed", err)
		}
	}
	s.eventHeld = nil
	return first
}

func (s *CurrentStateSource) fail(cat SpillMergeCategory, phase, msg string, cause error) error {
	return spillErr(cat, phase, msg, s.attemptID, cause)
}

// --- workspace ---

type spillAttemptMeta struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	AttemptID string `json:"attempt_id"`
	State     string `json:"state"`
	CreatedAt string `json:"created_at"`
}

func (s *CurrentStateSource) createWorkspace() error {
	rootPath, err := resolveProtectedSpillRoot(s.cfg.SpillRoot)
	if err != nil {
		return s.fail(SpillMergeWorkspace, "workspace", "spill root refused", err)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return s.fail(SpillMergeWorkspace, "workspace", "open spill root", err)
	}
	s.spillRoot = root
	s.reserveInfraFD(1)

	// Refuse intermediate symlink for spillmerge/; create if missing.
	if err := ensureRealDir(root, spillMergeWorkspaceDir); err != nil {
		return s.fail(SpillMergeWorkspace, "workspace", "spillmerge dir refused", err)
	}
	smRoot, err := root.OpenRoot(spillMergeWorkspaceDir)
	if err != nil {
		return s.fail(SpillMergeWorkspace, "workspace", "open spillmerge dir", err)
	}

	id, err := newAttemptID()
	if err != nil {
		_ = smRoot.Close()
		return s.fail(SpillMergeWorkspace, "workspace", "allocate attempt id", err)
	}
	s.attemptID = id
	if err := smRoot.Mkdir(id, 0o700); err != nil {
		_ = smRoot.Close()
		return s.fail(SpillMergeWorkspace, "workspace", "create attempt dir", err)
	}
	info, err := smRoot.Lstat(id)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		_ = smRoot.RemoveAll(id)
		_ = smRoot.Close()
		return s.fail(SpillMergeWorkspace, "workspace", "attempt dir must be a real directory", nil)
	}
	s.attemptBound = info
	wsRoot, err := smRoot.OpenRoot(id)
	_ = smRoot.Close()
	if err != nil {
		return s.fail(SpillMergeWorkspace, "workspace", "open attempt dir", err)
	}
	s.wsRoot = wsRoot
	s.reserveInfraFD(1)
	s.workspace = filepath.Join(rootPath, spillMergeWorkspaceDir, id)

	s.reserveInfraFD(1) // lock
	lf, err := wsRoot.OpenFile(spillMergeLockName, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		s.releaseInfraFD(1)
		_ = s.cleanupWorkspace(false)
		return s.fail(SpillMergeWorkspace, "workspace", "create attempt lock", err)
	}
	if err := lockFileExclusive(lf); err != nil {
		_ = lf.Close()
		s.releaseInfraFD(1)
		_ = s.cleanupWorkspace(false)
		return s.fail(SpillMergeWorkspace, "workspace", "lock attempt", err)
	}
	// Re-bind lock is regular file under attempt.
	bound, err := lf.Stat()
	if err != nil || !bound.Mode().IsRegular() {
		_ = unlockFile(lf)
		_ = lf.Close()
		s.releaseInfraFD(1)
		_ = s.cleanupWorkspace(false)
		return s.fail(SpillMergeWorkspace, "workspace", "lock must be regular file", nil)
	}
	s.lockFile = lf

	meta := spillAttemptMeta{
		Type:      "gonimbus.index.spillmerge_attempt.v1",
		Version:   spillMergeFormatVersion,
		AttemptID: id,
		State:     "preparing",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := s.writeMeta(meta); err != nil {
		_ = s.cleanupWorkspace(false)
		return err
	}
	return nil
}

func ensureRealDir(root *os.Root, name string) error {
	info, err := root.Lstat(name)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := root.Mkdir(name, 0o700); err != nil && !os.IsExist(err) {
			return err
		}
		info, err = root.Lstat(name)
		if err != nil {
			return err
		}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%s must not be a symlink", name)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s must be a directory", name)
	}
	return nil
}

func (s *CurrentStateSource) writeMeta(meta spillAttemptMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return s.fail(SpillMergeWorkspace, "workspace", "encode meta", err)
	}
	newSize := int64(len(data))
	// Charge new before replace; release prior meta size after successful publish.
	if err := s.chargeWorkspace(newSize); err != nil {
		return err
	}
	if s.wsRoot == nil {
		return s.fail(SpillMergeWorkspace, "workspace", "attempt root missing", nil)
	}
	tmpName := spillMergeMetaName + ".tmp"
	_ = s.wsRoot.Remove(tmpName)
	s.reserveInfraFD(1)
	tf, err := s.wsRoot.OpenFile(tmpName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		s.releaseInfraFD(1)
		s.releaseWorkspace(newSize)
		return s.fail(SpillMergeIO, "workspace", "write meta temp", err)
	}
	_, werr := tf.Write(data)
	cerr := tf.Close()
	s.releaseInfraFD(1)
	if werr != nil {
		_ = s.wsRoot.Remove(tmpName)
		s.releaseWorkspace(newSize)
		return s.fail(SpillMergeIO, "workspace", "write meta temp", werr)
	}
	if cerr != nil {
		_ = s.wsRoot.Remove(tmpName)
		s.releaseWorkspace(newSize)
		return s.fail(SpillMergeIO, "workspace", "close meta temp", cerr)
	}
	_ = s.wsRoot.Remove(spillMergeMetaName)
	if err := s.wsRoot.Rename(tmpName, spillMergeMetaName); err != nil {
		_ = s.wsRoot.Remove(tmpName)
		s.releaseWorkspace(newSize)
		return s.fail(SpillMergeIO, "workspace", "publish meta", err)
	}
	if s.metaBytes > 0 {
		s.releaseWorkspace(s.metaBytes)
	}
	s.metaBytes = newSize
	return nil
}

// testAfterBoundEpochOpenHook runs after a trash/attempt directory has been
// opened and SameFile-verified on the FD, before any child unlink or rmdir.
// Tests use this to swap the live dentry without the implementation doing a
// separate pathname RemoveAll after check.
var testAfterBoundEpochOpenHook func(fullPath string)

// testForceBoundEpochDeleteErr, when non-nil, is returned after the bound-FD
// wipe (tests: inject finalization failure while keeping ownedTrash for retry).
var testForceBoundEpochDeleteErr error

func (s *CurrentStateSource) cleanupWorkspace(held bool) error {
	// Drain readers may still be open; prefer explicit close first.
	_ = s.closeDrainErr()
	_ = s.closeHeldRuns()

	// Phase A: wipe contents through the bound attempt directory handle (owned inode).
	if s.wsRoot != nil {
		if s.lockFile != nil {
			_ = unlockFile(s.lockFile)
			_ = s.lockFile.Close()
			s.lockFile = nil
			s.releaseInfraFD(1)
		}
		if err := removeAllInRoot(s.wsRoot); err != nil {
			if held {
				return s.fail(SpillMergeWorkspace, "close", "cleanup owned attempt contents failed", err)
			}
		}
		_ = s.wsRoot.Close()
		s.wsRoot = nil
		s.releaseInfraFD(1)
	} else if s.lockFile != nil {
		_ = unlockFile(s.lockFile)
		_ = s.lockFile.Close()
		s.lockFile = nil
		s.releaseInfraFD(1)
	}

	// Phase B: remove the attempt dentry only when it still matches attemptBound.
	// Never pathname-delete a substitute installed at the live attempt name.
	if s.spillRoot == nil {
		if !held && s.workspace != "" {
			_ = os.RemoveAll(s.workspace)
		}
		s.clearCleanupState()
		return nil
	}
	if s.attemptID == "" {
		if held {
			return s.fail(SpillMergeWorkspace, "close", "missing attempt id for cleanup", nil)
		}
		s.clearCleanupState()
		return nil
	}

	sm, err := s.spillRoot.OpenRoot(spillMergeWorkspaceDir)
	if err != nil {
		if held {
			return s.fail(SpillMergeWorkspace, "close", "reopen spillmerge for cleanup", err)
		}
		// Keep spillRoot for a held retry path when possible.
		return nil
	}

	// Resume an already-quarantined owned trash from a prior partial Close.
	if s.ownedTrash != "" {
		if err := s.finishOwnedTrash(sm, held); err != nil {
			_ = sm.Close()
			return err
		}
		_ = sm.Close()
		s.finishCleanupSuccess()
		return nil
	}

	if s.attemptBound == nil {
		_ = sm.Close()
		if held {
			return s.fail(SpillMergeWorkspace, "close", "missing attempt binding for cleanup", nil)
		}
		s.clearCleanupState()
		return nil
	}

	cur, lerr := sm.Lstat(s.attemptID)
	switch {
	case lerr == nil && os.SameFile(s.attemptBound, cur):
		// Live name still refers to the owned epoch — quarantine then delete
		// via a bound directory FD (never pathname RemoveAll after a separate check).
		trash := ".trash-" + s.attemptID
		if err := sm.Rename(s.attemptID, trash); err != nil {
			_ = sm.Close()
			if held {
				return s.fail(SpillMergeWorkspace, "close", "quarantine owned attempt failed", err)
			}
			return nil
		}
		s.ownedTrash = trash
		if err := s.removeBoundEpochDir(sm, trash); err != nil {
			_ = sm.Close()
			if held {
				return s.fail(SpillMergeWorkspace, "close", "remove quarantined attempt failed", err)
			}
			return nil
		}
		s.ownedTrash = ""
		_ = sm.Close()
		s.finishCleanupSuccess()
		return nil

	case lerr == nil && !os.SameFile(s.attemptBound, cur):
		// Replacement at the live attempt name — never delete it.
		_ = sm.Close()
		// Owned contents already wiped via wsRoot; leave the substitute intact.
		s.finishCleanupSuccess()
		return nil

	case os.IsNotExist(lerr):
		// Live name gone (renamed away or already cleaned). Do not search for
		// foreign trash; only finish ownedTrash if we set it ourselves.
		_ = sm.Close()
		s.finishCleanupSuccess()
		return nil

	default:
		_ = sm.Close()
		if held {
			return s.fail(SpillMergeWorkspace, "close", "stat attempt for cleanup", lerr)
		}
		return nil
	}
}

// finishOwnedTrash deletes a previously quarantine-renamed owned attempt using
// a bound directory FD. Leaves ownedTrash set on failure for retry.
func (s *CurrentStateSource) finishOwnedTrash(sm *os.Root, held bool) error {
	if s.ownedTrash == "" {
		return nil
	}
	// If the name is gone, owned finalization of that dentry is done.
	if _, err := sm.Lstat(s.ownedTrash); err != nil {
		if os.IsNotExist(err) {
			s.ownedTrash = ""
			return nil
		}
		if held {
			return s.fail(SpillMergeWorkspace, "close", "stat owned trash", err)
		}
		return nil
	}
	if err := s.removeBoundEpochDir(sm, s.ownedTrash); err != nil {
		if held {
			return s.fail(SpillMergeWorkspace, "close", "remove owned trash failed", err)
		}
		return nil
	}
	s.ownedTrash = ""
	return nil
}

// removeBoundEpochDir opens name under the spillmerge root with no-follow, verifies
// the opened directory FD matches attemptBound, unlinks residual children, then
// removes an empty directory entry.
//
// Trust model (spill/merge source): SpillRoot is exclusive operator-controlled space;
// concurrent hostile namespace mutation is out of scope. Under that model this
// refuses live-name / non-empty post-bind substitutes and avoids recursive
// pathname RemoveAll after a detached identity check. It does NOT claim
// atomic protection against an empty dentry swap in the final name-based rmdir
// window (portable POSIX limitation). Windows residual child removal is
// path-based after a no-follow open — see docs/architecture/durable-spill-merge.md.
func (s *CurrentStateSource) removeBoundEpochDir(sm *os.Root, name string) error {
	if s.attemptBound == nil {
		return fmt.Errorf("missing attemptBound")
	}
	// Resolve full path under the spillmerge Root's name for no-follow open.
	// sm.Name() is the spillmerge directory path.
	full := filepath.Join(sm.Name(), name)
	dir, err := openDirNoFollow(full)
	if err != nil {
		return err
	}
	st, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return err
	}
	if !os.SameFile(s.attemptBound, st) {
		_ = dir.Close()
		return fmt.Errorf("opened directory is not the owned attempt epoch")
	}
	if testAfterBoundEpochOpenHook != nil {
		testAfterBoundEpochOpenHook(full)
	}
	// Wipe any residual children only through the bound FD.
	if err := unlinkChildrenAt(dir); err != nil {
		_ = dir.Close()
		return err
	}
	// Confirm empty via FD before releasing it.
	left, err := dir.Readdirnames(-1)
	if err != nil {
		_ = dir.Close()
		return err
	}
	if len(left) > 0 {
		_ = dir.Close()
		return fmt.Errorf("owned directory not empty after FD wipe")
	}
	_ = dir.Close()

	if testForceBoundEpochDeleteErr != nil {
		return testForceBoundEpochDeleteErr
	}

	// Empty-directory remove only (never RemoveAll). Non-empty post-bind
	// substitutes fail closed. Empty post-bind swaps are out of scope under the
	// exclusive-SpillRoot contract (final rmdir is name-based on portable POSIX).
	parent, base, err := openParentDirNoFollow(full)
	if err != nil {
		// Fallback: Root.Remove only succeeds for empty dirs / files.
		if err2 := sm.Remove(name); err2 != nil {
			return err2
		}
		return nil
	}
	defer func() { _ = parent.Close() }()
	if err := rmdirAt(parent, base); err != nil {
		// One more empty-only attempt via Root.
		if err2 := sm.Remove(name); err2 != nil {
			return err
		}
	}
	return nil
}

func (s *CurrentStateSource) finishCleanupSuccess() {
	if s.spillRoot != nil {
		_ = s.spillRoot.Close()
		s.spillRoot = nil
		s.releaseInfraFD(1)
	}
	s.clearCleanupState()
}

func (s *CurrentStateSource) clearCleanupState() {
	s.workspace = ""
	s.liveWSBytes = 0
	s.metaBytes = 0
	s.runSizes = make(map[string]int64)
	s.attemptBound = nil
	s.ownedTrash = ""
}

// removeAllInRoot deletes every entry under an already-bound attempt root handle.
func removeAllInRoot(root *os.Root) error {
	if root == nil {
		return nil
	}
	// Root has no ReadDir in older API surface — use FS().
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		// Fallback: try known names if listing fails.
		for _, name := range []string{spillMergeLockName, spillMergeMetaName} {
			_ = root.Remove(name)
		}
		return err
	}
	var first error
	for _, e := range entries {
		name := e.Name()
		if name == "." || name == ".." {
			continue
		}
		if e.IsDir() {
			if err := root.RemoveAll(name); err != nil && first == nil {
				first = err
			}
			continue
		}
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) && first == nil {
			first = err
		}
	}
	return first
}

func resolveProtectedSpillRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", fmt.Errorf("spill root is empty")
	}
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("spill root must be absolute")
	}
	clean := filepath.Clean(root)
	info, err := os.Lstat(clean)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(clean, 0o700); err != nil {
				return "", fmt.Errorf("create spill root: %w", err)
			}
			info, err = os.Lstat(clean)
		}
		if err != nil {
			return "", fmt.Errorf("stat spill root: %w", err)
		}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("spill root must not be a symlink")
	}
	if !info.IsDir() {
		return "", fmt.Errorf("spill root must be a directory")
	}
	// Refuse following symlinks in resolution for the final path component.
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", fmt.Errorf("resolve spill root: %w", err)
	}
	// Re-check leaf is not a symlink after eval (parent aliases like /var are ok).
	leaf, err := os.Lstat(resolved)
	if err != nil || leaf.Mode()&os.ModeSymlink != 0 || !leaf.IsDir() {
		return "", fmt.Errorf("spill root must resolve to a real directory")
	}
	return filepath.Clean(resolved), nil
}

func newAttemptID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *CurrentStateSource) chargeWorkspace(n int64) error {
	if n < 0 {
		return s.fail(SpillMergeBudgetExhausted, "budget", "negative workspace charge", nil)
	}
	next, ok := checkedAddInt64(s.liveWSBytes, n)
	if !ok {
		return s.fail(SpillMergeBudgetExhausted, "budget", "workspace byte overflow", nil)
	}
	if next > s.budget.MaxWorkspaceBytes {
		return s.fail(SpillMergeBudgetExhausted, "budget", "MaxWorkspaceBytes exceeded", nil)
	}
	s.liveWSBytes = next
	if next > s.stats.PeakWorkspaceBytes {
		s.stats.PeakWorkspaceBytes = next
	}
	return nil
}

func (s *CurrentStateSource) releaseWorkspace(n int64) {
	if n <= 0 {
		return
	}
	s.liveWSBytes -= n
	if s.liveWSBytes < 0 {
		s.liveWSBytes = 0
	}
}

// reserveInfraFD tracks roots/lock/journal source handles for peak observability.
// These are NOT charged against MaxFanIn (spill-run concurrency budget).
func (s *CurrentStateSource) reserveInfraFD(n int) {
	s.openFDs += n
	if s.openFDs > s.stats.PeakOpenFiles {
		s.stats.PeakOpenFiles = s.openFDs
	}
}

func (s *CurrentStateSource) releaseInfraFD(n int) {
	s.openFDs -= n
	if s.openFDs < 0 {
		s.openFDs = 0
	}
}

// reserveSpillFD charges MaxFanIn for sealed-run readers/writers only.
func (s *CurrentStateSource) reserveSpillFD(n int) error {
	if n < 0 {
		return s.fail(SpillMergeBudgetExhausted, "budget", "negative FD reserve", nil)
	}
	next := s.spillFDs + n
	if next > s.budget.MaxFanIn {
		return s.fail(SpillMergeBudgetExhausted, "budget", "MaxFanIn spill-run open-file budget exceeded", nil)
	}
	s.spillFDs = next
	s.openFDs += n
	if s.openFDs > s.stats.PeakOpenFiles {
		s.stats.PeakOpenFiles = s.openFDs
	}
	return nil
}

func (s *CurrentStateSource) releaseSpillFD(n int) {
	s.spillFDs -= n
	if s.spillFDs < 0 {
		s.spillFDs = 0
	}
	s.openFDs -= n
	if s.openFDs < 0 {
		s.openFDs = 0
	}
}

// Backward-compatible names used by run writer.
func (s *CurrentStateSource) reserveFD(n int) error { return s.reserveSpillFD(n) }
func (s *CurrentStateSource) releaseFD(n int)       { s.releaseSpillFD(n) }

func (s *CurrentStateSource) noteRunCreate() error {
	if s.runsCreated+1 > s.budget.MaxSpillRuns {
		return s.fail(SpillMergeBudgetExhausted, "budget", "MaxSpillRuns exceeded", nil)
	}
	s.runsCreated++
	s.stats.SpillRunCount = s.runsCreated
	return nil
}

func (s *CurrentStateSource) trackSealedRun(path string, payloadBytes int64) {
	if s.runSizes == nil {
		s.runSizes = make(map[string]int64)
	}
	// payloadBytes is header+records; footer bytes already charged into liveWSBytes.
	s.runSizes[path] = payloadBytes
}

func (s *CurrentStateSource) dropRunFile(path string) error {
	// Confirm deletion before releasing the live workspace charge.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return s.fail(SpillMergeIO, "merge", "remove spill run failed", err)
	}
	if sz, ok := s.runSizes[path]; ok {
		s.releaseWorkspace(sz)
		delete(s.runSizes, path)
	}
	return nil
}

func (s *CurrentStateSource) nextRunName(prefix string) string {
	s.runSeq++
	return fmt.Sprintf("%s-%04d.run", prefix, s.runSeq)
}

func checkedAddInt64(a, b int64) (int64, bool) {
	if b > 0 && a > (1<<63-1)-b {
		return 0, false
	}
	if b < 0 && a < (-1<<63)-b {
		return 0, false
	}
	return a + b, true
}

// --- parent staging ---

func (s *CurrentStateSource) stageParent(ctx context.Context) error {
	phase := "parent"
	path := filepath.Join(s.workspace, s.nextRunName("parent"))
	w, err := newSpillRunWriter(path, spillRunKindParent, s.attemptID, s)
	if err != nil {
		return err
	}
	defer func() { _ = w.Abort() }()

	var prev string
	var count int
	for {
		if err := ctx.Err(); err != nil {
			return s.fail(SpillMergeCanceled, phase, "context canceled", err)
		}
		row, err := s.cfg.Parent.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return s.fail(SpillMergeIO, phase, "parent read failed", err)
		}
		row = normalizeCurrentObjectRow(row)
		if strings.TrimSpace(row.RelKey) == "" {
			return s.fail(SpillMergeParentOrder, phase, "parent rel_key is required", nil)
		}
		if row.IndexSetID == "" {
			row.IndexSetID = s.cfg.IndexSetID
		}
		if row.IndexSetID != s.cfg.IndexSetID {
			return s.fail(SpillMergeInvalidConfig, phase, "parent index_set_id mismatch", nil)
		}
		if count > 0 {
			if row.RelKey == prev {
				return s.fail(SpillMergeParentOrder, phase, "duplicate parent rel_key", nil)
			}
			if row.RelKey < prev {
				return s.fail(SpillMergeParentOrder, phase, "parent rel_key out of order", nil)
			}
		}
		prev = row.RelKey
		rowBytes := estimateRowBytes(row)
		// MaxBufferedBytes is a hard resident envelope: a single parent row that
		// cannot fit refuses before write (same class as event-buffer refuse).
		if rowBytes > s.budget.MaxBufferedBytes {
			return s.fail(SpillMergeBudgetExhausted, phase, "parent row exceeds MaxBufferedBytes", nil)
		}
		if err := w.WriteParent(row); err != nil {
			return err
		}
		count++
		// Parent stream retains one logical row at a time; peak is max(rowBytes, …).
		s.noteBuffered(1, rowBytes)
	}
	if err := w.Seal(); err != nil {
		return err
	}
	s.parentRun = path
	return nil
}

func (s *CurrentStateSource) noteBuffered(rows int, bytes int64) {
	if rows > s.stats.PeakBufferedRows {
		s.stats.PeakBufferedRows = rows
	}
	if bytes > s.stats.PeakBufferedBytes {
		s.stats.PeakBufferedBytes = bytes
	}
}

func estimateRowBytes(row CurrentObjectRow) int64 {
	// Conservative residency: encoded JSON + fixed overhead (mirrors event estimate).
	data, err := json.Marshal(row)
	if err != nil {
		return 1 << 62
	}
	const residencyOverhead = 192
	n := int64(len(data)) + residencyOverhead
	// keep symbols used if any tail remains
	_ = row
	return n
}

// --- journal staging (same-open, bounded) ---

func (s *CurrentStateSource) stageJournals(ctx context.Context) error {
	phase := "journal"
	seenIDs := make(map[string]struct{}, len(s.cfg.JournalPaths))
	var runs []string
	for i, path := range s.cfg.JournalPaths {
		if err := ctx.Err(); err != nil {
			return s.fail(SpillMergeCanceled, phase, "context canceled", err)
		}
		journalID, journalRuns, err := s.stageOneJournal(ctx, path, i)
		if err != nil {
			return err
		}
		if _, ok := seenIDs[journalID]; ok {
			return s.fail(SpillMergeJournal, phase, "duplicate journal_id", nil)
		}
		seenIDs[journalID] = struct{}{}
		runs = append(runs, journalRuns...)
	}
	merged, passes, err := s.mergeEventRuns(ctx, runs)
	if err != nil {
		return err
	}
	s.eventRuns = merged
	s.stats.MergePasses = passes
	return nil
}

func (s *CurrentStateSource) stageOneJournal(ctx context.Context, journalPath string, journalIndex int) (string, []string, error) {
	phase := "journal"
	journalPath = filepath.Clean(strings.TrimSpace(journalPath))
	if journalPath == "" || !filepath.IsAbs(journalPath) {
		return "", nil, s.fail(SpillMergeJournal, phase, "journal path must be absolute", nil)
	}
	// Same-open + no-follow leaf: never re-open the pathname later.
	file, err := openRegularNoFollow(journalPath)
	if err != nil {
		return "", nil, s.fail(SpillMergeJournal, phase, "open journal", err)
	}
	s.reserveInfraFD(1)
	defer func() {
		_ = file.Close()
		s.releaseInfraFD(1)
	}()

	var (
		buffer   []spillEvent
		bufBytes int64
		part     int
		runs     []string
		header   JournalHeader
		haveHdr  bool
	)
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		sort.Slice(buffer, func(i, j int) bool { return eventLess(buffer[i], buffer[j]) })
		name := filepath.Join(s.workspace, fmt.Sprintf("events-j%04d-p%04d.run", journalIndex, part))
		part++
		w, err := newSpillRunWriter(name, spillRunKindEvents, s.attemptID, s)
		if err != nil {
			return err
		}
		for _, ev := range buffer {
			if err := w.WriteEvent(ev); err != nil {
				_ = w.Abort()
				return err
			}
		}
		if err := w.Seal(); err != nil {
			_ = w.Abort()
			return err
		}
		runs = append(runs, name)
		s.noteBuffered(len(buffer), bufBytes)
		// Drop payload references so PeakBufferedBytes reflects live residency.
		for i := range buffer {
			buffer[i] = spillEvent{}
		}
		buffer = buffer[:0]
		bufBytes = 0
		return nil
	}
	abortRuns := func() {
		for _, p := range runs {
			_ = s.dropRunFile(p)
		}
		runs = nil
	}

	err = scanJournalStreaming(ctx, file, s.cfg, s.budget, func(ev spillEvent) error {
		if err := ctx.Err(); err != nil {
			return s.fail(SpillMergeCanceled, phase, "context canceled", err)
		}
		if s.cfg.Mode == PublicationModeEnrichOnly && ev.Op != ObjectRecordOpEnrich {
			return s.fail(SpillMergeJournal, phase, "enrich-only mode rejects non-enrich op", nil)
		}
		eb := estimateEventBytes(ev)
		// Single event that cannot fit the resident budget refuses before append.
		if eb > s.budget.MaxBufferedBytes || s.budget.MaxBufferedRows < 1 {
			return s.fail(SpillMergeBudgetExhausted, phase, "event exceeds MaxBufferedBytes/Rows", nil)
		}
		// Flush before exceeding resident buffer limits.
		if len(buffer) > 0 {
			if len(buffer)+1 > s.budget.MaxBufferedRows || bufBytes+eb > s.budget.MaxBufferedBytes {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		// After flush, empty buffer must still fit this event.
		if len(buffer)+1 > s.budget.MaxBufferedRows || bufBytes+eb > s.budget.MaxBufferedBytes {
			return s.fail(SpillMergeBudgetExhausted, phase, "event exceeds MaxBufferedBytes/Rows", nil)
		}
		buffer = append(buffer, ev)
		bufBytes += eb
		s.noteBuffered(len(buffer), bufBytes)
		if len(buffer) >= s.budget.MaxBufferedRows || bufBytes >= s.budget.MaxBufferedBytes {
			return flush()
		}
		return nil
	}, func(h JournalHeader) {
		header = h
		haveHdr = true
	})
	if err != nil {
		abortRuns()
		return "", nil, s.mapJournalErr(err)
	}
	if !haveHdr {
		abortRuns()
		return "", nil, s.fail(SpillMergeJournal, phase, "missing header", nil)
	}
	if err := flush(); err != nil {
		abortRuns()
		return "", nil, err
	}
	return header.JournalID, runs, nil
}

func (s *CurrentStateSource) mapJournalErr(err error) error {
	if err == nil {
		return nil
	}
	var sm *SpillMergeError
	if errors.As(err, &sm) {
		if sm.AttemptID == "" {
			sm.AttemptID = s.attemptID
		}
		return sm
	}
	if errors.Is(err, ErrInvalidJournal) || errors.Is(err, ErrIncompleteJournal) {
		return s.fail(SpillMergeJournal, "journal", "journal validation failed", err)
	}
	return s.fail(SpillMergeJournal, "journal", "journal scan failed", err)
}

// mergeEventRuns multi-pass merges until drain can open lock + parent + event
// runs without exceeding MaxFanIn (lock is already reserved).
func (s *CurrentStateSource) mergeEventRuns(ctx context.Context, runs []string) ([]string, int, error) {
	if len(runs) == 0 {
		return nil, 0, nil
	}
	passes := 0
	// MaxFanIn bounds concurrent spill-run FDs only.
	// Drain needs parent + eventRuns <= MaxFanIn (held FDs after prepare).
	target := s.budget.MaxFanIn - 1 // reserve one slot for parent run
	if target < 1 {
		target = 1
	}
	// Event multi-pass: inputs + one output writer <= MaxFanIn (validated >= 3).
	chunkSize := s.budget.MaxFanIn - 1
	if chunkSize < 2 {
		return nil, 0, s.fail(SpillMergeInvalidConfig, "merge", "MaxFanIn too small for multi-pass merge", nil)
	}
	for len(runs) > target {
		if passes >= s.budget.MaxMergePasses {
			return nil, passes, s.fail(SpillMergeBudgetExhausted, "merge", "MaxMergePasses exceeded", nil)
		}
		var next []string
		for i := 0; i < len(runs); i += chunkSize {
			if err := ctx.Err(); err != nil {
				return nil, passes, s.fail(SpillMergeCanceled, "merge", "context canceled", err)
			}
			end := i + chunkSize
			if end > len(runs) {
				end = len(runs)
			}
			chunk := runs[i:end]
			if len(chunk) == 1 {
				next = append(next, chunk[0])
				continue
			}
			out := filepath.Join(s.workspace, fmt.Sprintf("events-m%02d-%04d.run", passes, len(next)))
			if err := s.mergeEventChunk(ctx, chunk, out); err != nil {
				return nil, passes, err
			}
			for _, p := range chunk {
				if err := s.dropRunFile(p); err != nil {
					return nil, passes, err
				}
			}
			next = append(next, out)
		}
		if len(next) >= len(runs) {
			return nil, passes, s.fail(SpillMergeBudgetExhausted, "merge", "merge pass did not reduce run count", nil)
		}
		runs = next
		passes++
		s.stats.MergePasses = passes
	}
	return runs, passes, nil
}

func (s *CurrentStateSource) mergeEventChunk(ctx context.Context, inputs []string, outPath string) error {
	// Prospective FD reserve: inputs + will-be-opened writer (writer reserves itself).
	if err := s.reserveFD(len(inputs)); err != nil {
		return err
	}
	readers := make([]*spillRunReader, 0, len(inputs))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
			s.releaseFD(1)
		}
	}()
	for _, p := range inputs {
		r, err := openSpillRunReader(p, spillRunKindEvents, s.attemptID, s.budget.MaxRecordBytes)
		if err != nil {
			return s.fail(SpillMergeSpillIntegrity, "merge", "open event run", err)
		}
		readers = append(readers, r)
	}
	w, err := newSpillRunWriter(outPath, spillRunKindEvents, s.attemptID, s)
	if err != nil {
		return err
	}
	defer func() { _ = w.Abort() }()

	// k-way merge
	type head struct {
		ev spillEvent
		ri int
		ok bool
	}
	heads := make([]head, len(readers))
	for i, r := range readers {
		ev, err := r.ReadEvent()
		if errors.Is(err, io.EOF) {
			heads[i].ok = false
			continue
		}
		if err != nil {
			return s.fail(SpillMergeSpillIntegrity, "merge", "read event run", err)
		}
		heads[i] = head{ev: ev, ri: i, ok: true}
	}
	for {
		if err := ctx.Err(); err != nil {
			return s.fail(SpillMergeCanceled, "merge", "context canceled", err)
		}
		best := -1
		for i := range heads {
			if !heads[i].ok {
				continue
			}
			if best < 0 || eventLess(heads[i].ev, heads[best].ev) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		if err := w.WriteEvent(heads[best].ev); err != nil {
			return err
		}
		ev, err := readers[best].ReadEvent()
		if errors.Is(err, io.EOF) {
			heads[best].ok = false
			continue
		}
		if err != nil {
			return s.fail(SpillMergeSpillIntegrity, "merge", "read event run", err)
		}
		heads[best].ev = ev
	}
	// Every input must end with validated footer + physical EOF (no trailing data).
	for _, r := range readers {
		if r == nil {
			continue
		}
		if err := r.Finish(); err != nil {
			return s.fail(SpillMergeSpillIntegrity, "merge", "input run trailing data or incomplete footer", err)
		}
	}
	return w.Seal()
}

func (s *CurrentStateSource) prepareMerge(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return s.fail(SpillMergeCanceled, "prepare", "context canceled", err)
	}
	// Full integrity attestation before READY — retain FDs so drain cannot
	// observe a substituted pathname. Zero rows exposed to callers.
	if s.parentRun == "" {
		return s.fail(SpillMergeSpillIntegrity, "prepare", "missing parent run", nil)
	}
	if err := s.reserveSpillFD(1); err != nil {
		return err
	}
	ph, err := holdAttestedRun(ctx, s.parentRun, spillRunKindParent, s.attemptID, s.budget.MaxRecordBytes)
	if err != nil {
		s.releaseSpillFD(1)
		return s.fail(SpillMergeSpillIntegrity, "prepare", "parent run attestation failed", err)
	}
	s.parentHeld = ph

	var held []*heldSpillRun
	for _, p := range s.eventRuns {
		if err := ctx.Err(); err != nil {
			_ = s.closeHeldRuns()
			return s.fail(SpillMergeCanceled, "prepare", "context canceled", err)
		}
		if err := s.reserveSpillFD(1); err != nil {
			_ = s.closeHeldRuns()
			return err
		}
		eh, err := holdAttestedRun(ctx, p, spillRunKindEvents, s.attemptID, s.budget.MaxRecordBytes)
		if err != nil {
			s.releaseSpillFD(1)
			_ = s.closeHeldRuns()
			return s.fail(SpillMergeSpillIntegrity, "prepare", "event run attestation failed", err)
		}
		held = append(held, eh)
	}
	s.eventHeld = held

	return s.writeMeta(spillAttemptMeta{
		Type:      "gonimbus.index.spillmerge_attempt.v1",
		Version:   spillMergeFormatVersion,
		AttemptID: s.attemptID,
		State:     "ready",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// --- drain / reduce ---

func (s *CurrentStateSource) ensureDrainOpen(ctx context.Context) error {
	if s.parentReader != nil {
		return nil
	}
	if s.parentHeld == nil {
		return s.fail(SpillMergeSpillIntegrity, "drain", "missing held parent run", nil)
	}
	// Re-attest held FDs before any row is exposed (late corruption / replace).
	if err := revalidateHeld(ctx, s.parentHeld, s.attemptID, s.budget.MaxRecordBytes); err != nil {
		return s.fail(SpillMergeSpillIntegrity, "drain", "parent revalidation failed", err)
	}
	for _, h := range s.eventHeld {
		if err := revalidateHeld(ctx, h, s.attemptID, s.budget.MaxRecordBytes); err != nil {
			return s.fail(SpillMergeSpillIntegrity, "drain", "event revalidation failed", err)
		}
	}
	pr, err := newSpillRunReaderFromFile(s.parentHeld.file, spillRunKindParent, s.attemptID, s.budget.MaxRecordBytes)
	if err != nil {
		return s.fail(SpillMergeSpillIntegrity, "drain", "open parent run", err)
	}
	s.parentReader = pr
	s.eventReaders = make([]*spillRunReader, 0, len(s.eventHeld))
	for _, h := range s.eventHeld {
		r, err := newSpillRunReaderFromFile(h.file, spillRunKindEvents, s.attemptID, s.budget.MaxRecordBytes)
		if err != nil {
			_ = s.closeDrainErr()
			return s.fail(SpillMergeSpillIntegrity, "drain", "open event run", err)
		}
		s.eventReaders = append(s.eventReaders, r)
	}
	if err := s.advanceParent(ctx); err != nil {
		return err
	}
	if err := s.advanceEvent(ctx); err != nil {
		return err
	}
	return nil
}

func (s *CurrentStateSource) closeDrainErr() error {
	// Drain readers share held FDs — detach without closing the underlying file.
	if s.parentReader != nil {
		s.parentReader.file = nil
		s.parentReader = nil
	}
	for i, r := range s.eventReaders {
		if r != nil {
			r.file = nil
		}
		s.eventReaders[i] = nil
	}
	s.eventReaders = nil
	return s.drainCloseErr
}

func (s *CurrentStateSource) advanceParent(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return s.fail(SpillMergeCanceled, "drain", "context canceled", err)
	}
	if s.parentDone {
		s.hasParent = false
		return nil
	}
	row, err := s.parentReader.ReadParent()
	if errors.Is(err, io.EOF) {
		s.parentDone = true
		s.hasParent = false
		return nil
	}
	if err != nil {
		return s.fail(SpillMergeSpillIntegrity, "drain", "read parent", err)
	}
	s.parentPeek = &row
	s.hasParent = true
	return nil
}

func (s *CurrentStateSource) advanceEvent(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return s.fail(SpillMergeCanceled, "drain", "context canceled", err)
	}
	// k-way among event readers — for simplicity re-merge already-capped set.
	// We keep a single merged virtual stream by picking min each time.
	type cand struct {
		ev spillEvent
		ri int
	}
	var best *cand
	// Lazy: if eventPeek already set, keep it.
	if s.hasEvent {
		return nil
	}
	if s.eventDone {
		return nil
	}
	// Initialize per-reader peeks if needed via reading into local.
	// Store peeks on readers.
	for i, r := range s.eventReaders {
		if r == nil {
			continue
		}
		if !r.hasPeek {
			ev, err := r.ReadEvent()
			if errors.Is(err, io.EOF) {
				r.done = true
				continue
			}
			if err != nil {
				return s.fail(SpillMergeSpillIntegrity, "drain", "read event", err)
			}
			r.peekEv = ev
			r.hasPeek = true
		}
		if r.done || !r.hasPeek {
			continue
		}
		if best == nil || eventLess(r.peekEv, best.ev) {
			best = &cand{ev: r.peekEv, ri: i}
		}
	}
	if best == nil {
		s.eventDone = true
		s.hasEvent = false
		return nil
	}
	// consume
	s.eventReaders[best.ri].hasPeek = false
	ev := best.ev
	s.eventPeek = &ev
	s.hasEvent = true
	return nil
}

func (s *CurrentStateSource) nextRow(ctx context.Context) (CurrentObjectRow, error) {
	for {
		if err := ctx.Err(); err != nil {
			return CurrentObjectRow{}, s.fail(SpillMergeCanceled, "drain", "context canceled", err)
		}
		if !s.hasParent && !s.hasEvent {
			return CurrentObjectRow{}, io.EOF
		}
		// Determine next key.
		var key string
		switch {
		case s.hasParent && s.hasEvent:
			if s.parentPeek.RelKey < s.eventPeek.RelKey {
				key = s.parentPeek.RelKey
			} else {
				key = s.eventPeek.RelKey
			}
		case s.hasParent:
			key = s.parentPeek.RelKey
		default:
			key = s.eventPeek.RelKey
		}

		var state CurrentObjectRow
		observed := false
		if s.hasParent && s.parentPeek.RelKey == key {
			state = *s.parentPeek
			if err := s.advanceParent(ctx); err != nil {
				return CurrentObjectRow{}, err
			}
		}

		// Events are already globally ordered by (rel_key, phase, journal_id, sequence).
		// Apply incrementally — do not accumulate unbounded per-key slices.
		input := CompactionInput{
			IndexSetID:   s.cfg.IndexSetID,
			RunID:        s.cfg.RunID,
			RunStartedAt: s.cfg.RunStartedAt.UTC(), // already validated UTC offset 0
			Coverage:     s.cfg.Coverage,
			Mode:         s.cfg.Mode,
		}
		for s.hasEvent && s.eventPeek.RelKey == key {
			if err := ctx.Err(); err != nil {
				return CurrentObjectRow{}, s.fail(SpillMergeCanceled, "drain", "context canceled", err)
			}
			rec := s.eventPeek.Record
			switch s.eventPeek.Op {
			case ObjectRecordOpObserve:
				state = applyObserve(state, input, rec)
				observed = true
				s.stats.ObservedRecords++
			case ObjectRecordOpEnrich:
				if strings.TrimSpace(state.RelKey) != "" {
					state = applyEnrich(state, rec)
					s.stats.EnrichmentRecords++
				}
			default:
				return CurrentObjectRow{}, s.fail(SpillMergeJournal, "drain", "unsupported op", nil)
			}
			s.hasEvent = false
			if err := s.advanceEvent(ctx); err != nil {
				return CurrentObjectRow{}, err
			}
		}

		// Keys with neither parent nor successful materialization should not happen
		// for observes (apply creates state). Parent-only or observe/enrich handled.

		if strings.TrimSpace(state.RelKey) == "" {
			// only enrich-for-missing-key events — emit nothing
			continue
		}

		// Coverage tombstones for unobserved active parents (and observe-created rows are observed).
		if s.cfg.Mode != PublicationModeEnrichOnly {
			if state.DeletedAt == nil && !observed && coverageAllowsTombstone(s.cfg.Coverage, state.RelKey) {
				deletedAt := s.cfg.RunStartedAt.UTC()
				state.DeletedAt = &deletedAt
				s.stats.NewTombstones++
			}
		}

		// Own pointer fields for caller.
		out := normalizeCurrentObjectRow(state)
		out.StorageClass = stringPtrCopy(out.StorageClass)
		out.ArchiveStatus = stringPtrCopy(out.ArchiveStatus)
		out.RestoreState = stringPtrCopy(out.RestoreState)
		out.ContentType = stringPtrCopy(out.ContentType)
		out.LastModified = canonicalTimePtr(out.LastModified)
		out.RestoreExpiry = canonicalTimePtr(out.RestoreExpiry)
		out.HeadEnrichedAt = canonicalTimePtr(out.HeadEnrichedAt)
		out.DeletedAt = canonicalTimePtr(out.DeletedAt)
		return out, nil
	}
}
