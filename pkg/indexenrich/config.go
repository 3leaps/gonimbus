package indexenrich

import (
	"time"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// DefaultMaxPriorRows is the hard pre-materialization budget for this cut.
const DefaultMaxPriorRows = 2_000_000

// StateEvent is one per-candidate HEAD observation for audit sinks.
// The library does not render CLI JSON; adapters encode the established
// gonimbus.index.enrich_with_head.state.v1 envelope from this value.
type StateEvent struct {
	IndexSetID     string
	RelKey         string
	FullKey        string
	Status         string // success | failed | resume_skipped
	Attempts       int
	ErrorCode      string
	ErrorMessage   string // sanitized; never carries signed URLs or tokens
	ArchiveStatus  *string
	RestoreState   *string
	RestoreExpiry  *time.Time
	ContentType    *string
	HeadEnrichedAt *time.Time
	EventTime      time.Time
}

// StateSink receives per-candidate observations. A non-nil error is fail-closed:
// remaining work is cancelled, updates are discarded, and latest is not advanced.
type StateSink func(StateEvent) error

// Config configures a durable enrich transaction. The caller injects an already
// constructed provider; credential resolution stays outside this package.
type Config struct {
	IndexSetID string
	BaseURI    string

	// Provider performs HEAD. Required. Closed by the caller.
	Provider provider.Provider

	// SegmentSetRoot is the durable set root (…/cache/segments/<index_set_id>/).
	// latest.json is always SegmentSetRoot/latest.json — there is no second path authority.
	SegmentSetRoot string

	// JournalRoot is required (…/journals/crawl/<index_set_id>/). No inferred default.
	JournalRoot string
	// Authority optionally supplies caller-held whole-set exclusion. When nil,
	// Run acquires the stable authority shared by CLI and GC. A supplied lease
	// remains caller-owned and is never released by Run.
	Authority *indexcoord.Lease

	Query    QueryOptions
	Parallel int
	Resume   bool

	// StateSink receives typed per-candidate observations. Optional; when set,
	// sink errors fail closed (no publish).
	StateSink StateSink

	// MaxPriorRows defaults to DefaultMaxPriorRows when <= 0.
	MaxPriorRows int
	// Clock defaults to time.Now.UTC when nil. Run identity is independent of Clock.
	Clock func() time.Time

	// MaxMarkerBytes / MaxManifestBytes bound parent open (defaults applied when <= 0).
	MaxMarkerBytes   int64
	MaxManifestBytes int64
}

// QueryOptions mirrors enrich CLI filters without Cobra coupling.
type QueryOptions struct {
	Pattern        string
	KeyRegex       string
	MinSize        string
	MaxSize        string
	StorageClasses []string
	IncludeDeleted bool
}

// Result is the durable enrich transaction outcome.
type Result struct {
	IndexSetID string
	RunID      string

	Candidates    int64
	HeadSucceeded int64 // successful HEAD observations (independent of commit)
	ResumeSkipped int64
	Failed        int64
	HeadCalls     int64
	// Committed is the number of successful HEAD updates applied in a published child.
	// Zero when Published is false.
	Committed int64

	// LatestAdvanced is true only after latest.json was successfully written.
	LatestAdvanced bool
	// Published is true when LatestAdvanced is true (committed enrich snapshot).
	Published bool

	ParentRunID        string
	ParentManifestSHA  string
	ParentCoverageSHA  string
	ManifestSHA256     string
	Rows               int
	Status             string
	StorageFiltered    bool
	ClassificationNote string
}
