package indexbuild

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/crawler"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// ParentToken is the public expected-parent CAS token for durable latest advance.
// Callers capture this from a verified latest snapshot before a write transaction.
type ParentToken struct {
	IndexSetID     string
	RunID          string
	ManifestSHA256 string
	// CoverageSHA256 binds inherited coverage for enrich-only publications.
	CoverageSHA256 string
}

// Clock returns the current time for run metadata. Supplying a deterministic
// clock makes retry identity and CLI/library parity byte-testable.
type Clock func() time.Time

// Source is the provider input for an index build. Provider construction and
// credential resolution are caller responsibilities.
type Source struct {
	Provider     provider.Provider
	ProviderName string
}

// String returns a redacted source summary that never formats the provider
// handle, which may carry credential material.
func (s Source) String() string {
	return fmt.Sprintf("indexbuild.Source{Provider:%s, ProviderName:%q}", ifacePresence(s.Provider == nil), s.ProviderName)
}

// GoString implements fmt %#v with the same redaction as String.
func (s Source) GoString() string { return s.String() }

// MatchConfig is the library form of the index build match settings. Patterns
// are relative to BaseURI unless they are already full provider keys.
type MatchConfig struct {
	Includes      []string
	Excludes      []string
	IncludeHidden bool
}

// PathConfig holds explicit engine storage locations.
//
// JournalDir and SegmentDir should be resolved by the adapter through the
// app-data path classes. If IndexDBDir is supplied, the engine rejects journal
// and segment paths below it so callers cannot silently place v2 working state
// under the legacy SQLite index directory.
type PathConfig struct {
	JournalDir   string
	SegmentDir   string
	ManifestPath string
	CompletePath string
	LatestPath   string
	IndexDBDir   string
}

// String returns presence-only path state so local filesystem layouts are not
// accidentally logged by embedders.
func (p PathConfig) String() string {
	return fmt.Sprintf("indexbuild.PathConfig{JournalDir:%s, SegmentDir:%s, ManifestPath:%s, CompletePath:%s, LatestPath:%s, IndexDBDir:%s}",
		fieldPresence(p.JournalDir == ""), fieldPresence(p.SegmentDir == ""), fieldPresence(p.ManifestPath == ""),
		fieldPresence(p.CompletePath == ""), fieldPresence(p.LatestPath == ""), fieldPresence(p.IndexDBDir == ""))
}

// GoString implements fmt %#v with the same redaction as String.
func (p PathConfig) GoString() string { return p.String() }

// CoverageBasis states the trust level of coverage evidence.
type CoverageBasis string

const (
	CoverageBasisConfirmed CoverageBasis = "confirmed"
	CoverageBasisInferred  CoverageBasis = "inferred"
)

// Scope identifies a covered key prefix. When BaseURI is set on Config or
// RetryConfig, provider-key coverage is normalized into journal rel_key space
// before publication. Windowed coverage is carried for schema parity; this
// slice publishes only explicit complete prefix coverage.
type Scope struct {
	Prefix string  `json:"prefix,omitempty"`
	Window *Window `json:"window,omitempty"`
}

// Window represents a bounded temporal scope.
type Window struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// CoverageAttestation is the public engine coverage contract.
type CoverageAttestation struct {
	Scope    *Scope        `json:"scope,omitempty"`
	Basis    CoverageBasis `json:"basis"`
	Complete bool          `json:"complete"`
	Gaps     []Scope       `json:"gaps,omitempty"`
}

// ObjectState is the public current-row form used for retry or compaction from
// a prior snapshot. It mirrors the durable manifest row shape without exposing
// the internal substrate package.
type ObjectState struct {
	IndexSetID       string
	RelKey           string
	SizeBytes        int64
	LastModified     *time.Time
	ETag             string
	StorageClass     *string
	ArchiveStatus    *string
	RestoreState     *string
	RestoreExpiry    *time.Time
	ContentType      *string
	HeadEnrichedAt   *time.Time
	FirstSeenRunID   string
	FirstSeenAt      time.Time
	LastChangedRunID string
	LastChangedAt    time.Time
	LastSeenRunID    string
	LastSeenAt       time.Time
	DeletedAt        *time.Time
}

// Config configures a full crawl-to-publish durable index build.
type Config struct {
	IndexSetID string
	RunID      string
	BaseURI    string
	Source     Source
	Match      MatchConfig
	Filter     *match.CompositeFilter
	Crawl      crawler.Config
	// CrawlPrefixes, when supplied, is the exact provider-prefix observation
	// plan. It lets CLI adapters pass a manifest scope plan into the engine
	// without making the engine import manifest or command packages.
	CrawlPrefixes []string
	// ObservationSinks receive the same observed crawl stream as the durable
	// journal materializer. This is the library-owned fanout boundary used by
	// CLI adapters to materialize compatibility formats from one observation.
	ObservationSinks []output.Writer
	Paths            PathConfig
	Coverage         []CoverageAttestation
	PriorRows        []ObjectState
	// ExpectedParent, when set, enforces latest-pointer CAS at publish advance.
	// When nil, Build/Retry capture the current latest (or first-publish) under
	// the write lease. Malformed latest fails closed (not first-publish).
	// Stale provided tokens are validated immediately after lease acquisition
	// and before any crawl/observation mutation.
	ExpectedParent *ParentToken

	RunStartedAt         time.Time
	CreatedAt            time.Time
	Clock                Clock
	TargetRowsPerSegment int
	Events               EventSink
	// OnSegmentProgress is optional observational progress during segment write
	// (counts only). Outside artifact bytes; never a publish failure vector.
	OnSegmentProgress OnSegmentProgressFunc
}

// SegmentProgress is a sanitized segment-write progress signal (counts only).
type SegmentProgress struct {
	Segment  int
	Total    int
	Rows     int
	RowsDone int
}

// OnSegmentProgressFunc is observational best-effort segment progress.
type OnSegmentProgressFunc func(progress SegmentProgress)

// String returns a redacted config summary. Provider handles, callbacks, and
// local paths are rendered by presence only.
func (c Config) String() string {
	return fmt.Sprintf("indexbuild.Config{IndexSetID:%q, RunID:%q, BaseURI:%q, Source:%s, Match:%+v, Filter:%s, Crawl:%+v, CrawlPrefixes:%d, ObservationSinks:%d, Paths:%s, Coverage:%d, PriorRows:%d, Events:%s, OnSegmentProgress:%s}",
		c.IndexSetID, c.RunID, sanitizeURI(c.BaseURI), c.Source, c.Match, ifacePresence(c.Filter == nil), c.Crawl,
		len(c.CrawlPrefixes), len(c.ObservationSinks), c.Paths, len(c.Coverage), len(c.PriorRows), ifacePresence(c.Events == nil),
		ifacePresence(c.OnSegmentProgress == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (c Config) GoString() string { return c.String() }

// RetryConfig configures publication retry from already sealed journals.
type RetryConfig struct {
	IndexSetID   string
	RunID        string
	BaseURI      string
	Paths        PathConfig
	JournalPaths []string
	Coverage     []CoverageAttestation
	PriorRows    []ObjectState
	// ExpectedParent enforces latest CAS when republishing over an existing set.
	ExpectedParent *ParentToken

	RunStartedAt         time.Time
	CreatedAt            time.Time
	Clock                Clock
	TargetRowsPerSegment int
	Events               EventSink
	OnSegmentProgress    OnSegmentProgressFunc
}

// String returns a redacted retry config summary.
func (c RetryConfig) String() string {
	return fmt.Sprintf("indexbuild.RetryConfig{IndexSetID:%q, RunID:%q, BaseURI:%q, Paths:%s, JournalPaths:%d, Coverage:%d, PriorRows:%d, Events:%s, OnSegmentProgress:%s}",
		c.IndexSetID, c.RunID, sanitizeURI(c.BaseURI), c.Paths, len(c.JournalPaths), len(c.Coverage), len(c.PriorRows), ifacePresence(c.Events == nil),
		ifacePresence(c.OnSegmentProgress == nil))
}

// GoString implements fmt %#v with the same redaction as String.
func (c RetryConfig) GoString() string { return c.String() }

func ifacePresence(isNil bool) string {
	if isNil {
		return "<nil>"
	}
	return "<set>"
}

func fieldPresence(isEmpty bool) string {
	if isEmpty {
		return "<empty>"
	}
	return "<set>"
}

func validatePaths(paths PathConfig) error {
	required := map[string]string{
		"journal directory": paths.JournalDir,
		"segment directory": paths.SegmentDir,
		"manifest path":     paths.ManifestPath,
		"complete path":     paths.CompletePath,
		"latest path":       paths.LatestPath,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if strings.TrimSpace(paths.IndexDBDir) != "" {
		indexDir, err := filepath.Abs(filepath.Clean(paths.IndexDBDir))
		if err != nil {
			return fmt.Errorf("index db directory: %w", err)
		}
		for name, value := range map[string]string{"journal directory": paths.JournalDir, "segment directory": paths.SegmentDir} {
			candidate, err := filepath.Abs(filepath.Clean(value))
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if pathWithin(candidate, indexDir) {
				return fmt.Errorf("%s must not be inside index db directory", name)
			}
		}
	}
	return nil
}

func pathWithin(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
