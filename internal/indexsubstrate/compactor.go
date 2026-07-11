package indexsubstrate

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type CoverageBasis string

const (
	CoverageBasisConfirmed CoverageBasis = "confirmed"
	CoverageBasisInferred  CoverageBasis = "inferred"
)

// RelativeRootScopePrefix is an explicit rel_key-space sentinel for complete
// coverage of the whole index-relative root. It is distinct from blank or "/",
// which remain invalid publish scopes.
const RelativeRootScopePrefix = "."

type CoverageAttestation struct {
	Scope    *Scope        `json:"scope,omitempty"`
	Basis    CoverageBasis `json:"basis"`
	Complete bool          `json:"complete"`
	Gaps     []Scope       `json:"gaps,omitempty"`
}

type CurrentObjectRow struct {
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

type Tombstone struct {
	IndexSetID string
	RelKey     string
	RunID      string
	DeletedAt  time.Time
}

// PublicationMode selects compaction/publication policy for a snapshot write.
type PublicationMode string

const (
	// PublicationModeDefault is crawl/observe publication: coverage may
	// tombstone unobserved keys under confirmed complete scopes.
	PublicationModeDefault PublicationMode = ""
	// PublicationModeEnrichOnly is HEAD-enrichment republication: journals must
	// contain only enrich ops, coverage is inherited and never re-attested as
	// observation evidence, and coverage-driven tombstones are disabled.
	PublicationModeEnrichOnly PublicationMode = "enrich-only"
)

type CompactionInput struct {
	IndexSetID   string
	RunID        string
	RunStartedAt time.Time
	PriorRows    []CurrentObjectRow
	Journals     []Journal
	Coverage     []CoverageAttestation
	// Mode selects tombstone and journal-shape policy. Prefer typed modes over
	// ad-hoc booleans so observe journals cannot accidentally suppress deletes.
	Mode PublicationMode
}

type CompactionResult struct {
	Rows              []CurrentObjectRow
	Tombstones        []Tombstone
	ObservedRecords   int
	EnrichmentRecords int
}

func CompactJournalFiles(input CompactionInput, journalPaths []string) (CompactionResult, error) {
	journals := make([]Journal, 0, len(journalPaths))
	for _, path := range journalPaths {
		journal, err := ReadJournal(path)
		if err != nil {
			return CompactionResult{}, err
		}
		journals = append(journals, journal)
	}
	input.Journals = append(input.Journals, journals...)
	return Compact(input)
}

func Compact(input CompactionInput) (CompactionResult, error) {
	input = normalizeCompactionInput(input)
	if err := validateCompactionInput(input); err != nil {
		return CompactionResult{}, err
	}

	state := make(map[string]CurrentObjectRow, len(input.PriorRows))
	observed := make(map[string]struct{})
	for _, row := range input.PriorRows {
		row = normalizeCurrentObjectRow(row)
		if strings.TrimSpace(row.RelKey) == "" {
			return CompactionResult{}, fmt.Errorf("%w: prior rel_key is required", ErrInvalidJournal)
		}
		if row.IndexSetID == "" {
			row.IndexSetID = input.IndexSetID
		}
		if row.IndexSetID != input.IndexSetID {
			return CompactionResult{}, fmt.Errorf("%w: prior row index_set_id mismatch", ErrInvalidJournal)
		}
		state[row.RelKey] = row
	}

	records, err := orderedJournalRecords(input)
	if err != nil {
		return CompactionResult{}, err
	}
	if input.Mode == PublicationModeEnrichOnly {
		for _, record := range records {
			if record.Op != ObjectRecordOpEnrich {
				return CompactionResult{}, fmt.Errorf("%w: enrich-only mode rejects journal op %q", ErrInvalidJournal, record.Op)
			}
		}
	}

	result := CompactionResult{}
	enrichments := make(map[string][]ObjectRecord)
	for _, record := range records {
		switch record.Op {
		case ObjectRecordOpObserve:
			row := applyObserve(state[record.RelKey], input, record)
			state[record.RelKey] = row
			observed[record.RelKey] = struct{}{}
			result.ObservedRecords++
		case ObjectRecordOpEnrich:
			enrichments[record.RelKey] = append(enrichments[record.RelKey], record)
		default:
			return CompactionResult{}, fmt.Errorf("%w: unsupported journal op %q", ErrInvalidJournal, record.Op)
		}
	}

	for relKey, records := range enrichments {
		row, ok := state[relKey]
		if !ok {
			continue
		}
		for _, record := range records {
			row = applyEnrich(row, record)
			result.EnrichmentRecords++
		}
		state[relKey] = row
	}

	// Enrich-only republication never invents deletes from unobserved keys.
	if input.Mode != PublicationModeEnrichOnly {
		for relKey, row := range state {
			if row.DeletedAt == nil {
				if _, ok := observed[relKey]; !ok && coverageAllowsTombstone(input.Coverage, relKey) {
					deletedAt := input.RunStartedAt.UTC()
					row.DeletedAt = &deletedAt
					state[relKey] = row
					result.Tombstones = append(result.Tombstones, Tombstone{
						IndexSetID: input.IndexSetID,
						RelKey:     relKey,
						RunID:      input.RunID,
						DeletedAt:  deletedAt,
					})
				}
			}
		}
	}

	result.Rows = rowsFromState(state)
	sort.Slice(result.Tombstones, func(i, j int) bool {
		return result.Tombstones[i].RelKey < result.Tombstones[j].RelKey
	})
	return result, nil
}

func orderedJournalRecords(input CompactionInput) ([]ObjectRecord, error) {
	seenJournals := make(map[string]struct{}, len(input.Journals))
	var records []ObjectRecord
	for _, journal := range input.Journals {
		journal = normalizeJournal(journal)
		if err := validateCompactionJournal(input, journal); err != nil {
			return nil, err
		}
		if _, ok := seenJournals[journal.Header.JournalID]; ok {
			return nil, fmt.Errorf("%w: duplicate journal_id %q", ErrInvalidJournal, journal.Header.JournalID)
		}
		seenJournals[journal.Header.JournalID] = struct{}{}
		records = append(records, journal.Records...)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].JournalID == records[j].JournalID {
			return records[i].Sequence < records[j].Sequence
		}
		return records[i].JournalID < records[j].JournalID
	})
	return records, nil
}

func applyObserve(existing CurrentObjectRow, input CompactionInput, record ObjectRecord) CurrentObjectRow {
	row := normalizeCurrentObjectRow(existing)
	isNew := strings.TrimSpace(row.RelKey) == ""
	wasDeleted := row.DeletedAt != nil
	changed := isNew || wasDeleted || observeFieldsChanged(row, record)
	observedAt := record.ObservedAt.UTC()

	if isNew {
		row.IndexSetID = input.IndexSetID
		row.RelKey = record.RelKey
		row.FirstSeenRunID = input.RunID
		row.FirstSeenAt = observedAt
	}
	if row.FirstSeenRunID == "" {
		row.FirstSeenRunID = fallbackRunID(row.LastSeenRunID, input.RunID)
	}
	if row.FirstSeenAt.IsZero() {
		row.FirstSeenAt = fallbackTime(row.LastSeenAt, observedAt)
	}
	if changed {
		row.LastChangedRunID = input.RunID
		row.LastChangedAt = observedAt
	} else {
		row.LastChangedRunID = fallbackRunID(row.LastChangedRunID, fallbackRunID(row.LastSeenRunID, input.RunID))
		row.LastChangedAt = fallbackTime(row.LastChangedAt, fallbackTime(row.LastSeenAt, observedAt))
	}

	row.SizeBytes = valueOrZero(record.SizeBytes)
	row.LastModified = canonicalTimePtr(record.LastModified)
	row.ETag = record.ETag
	row.StorageClass = stringPtrCopy(record.StorageClass)
	row.LastSeenRunID = input.RunID
	row.LastSeenAt = observedAt
	row.DeletedAt = nil
	return row
}

func applyEnrich(row CurrentObjectRow, record ObjectRecord) CurrentObjectRow {
	row = normalizeCurrentObjectRow(row)
	row.ArchiveStatus = stringPtrCopy(record.ArchiveStatus)
	row.RestoreState = stringPtrCopy(record.RestoreState)
	row.RestoreExpiry = canonicalTimePtr(record.RestoreExpiry)
	row.ContentType = stringPtrCopy(record.ContentType)
	if record.HeadEnrichedAt != nil {
		row.HeadEnrichedAt = canonicalTimePtr(record.HeadEnrichedAt)
	} else {
		enrichedAt := record.ObservedAt.UTC()
		row.HeadEnrichedAt = &enrichedAt
	}
	return row
}

func observeFieldsChanged(row CurrentObjectRow, record ObjectRecord) bool {
	return row.SizeBytes != valueOrZero(record.SizeBytes) ||
		!timePtrEqual(row.LastModified, canonicalTimePtr(record.LastModified)) ||
		row.ETag != record.ETag ||
		!stringPtrEqual(row.StorageClass, record.StorageClass)
}

func coverageAllowsTombstone(coverage []CoverageAttestation, relKey string) bool {
	if coverageHasGapForRelKey(coverage, relKey) {
		return false
	}
	for _, entry := range coverage {
		if entry.Basis != CoverageBasisConfirmed || !entry.Complete || len(entry.Gaps) != 0 {
			continue
		}
		if tombstoneCoverageScopeContainsRelKey(entry.Scope, relKey) {
			return true
		}
	}
	return false
}

func coverageHasGapForRelKey(coverage []CoverageAttestation, relKey string) bool {
	for _, entry := range coverage {
		for i := range entry.Gaps {
			if gapScopeContainsRelKey(&entry.Gaps[i], relKey) {
				return true
			}
		}
	}
	return false
}

func tombstoneCoverageScopeContainsRelKey(scope *Scope, relKey string) bool {
	if scope == nil {
		return false
	}
	prefix := cleanCoveragePrefix(scope.Prefix)
	if prefix == "" || scope.Window != nil {
		return false
	}
	if prefix == RelativeRootScopePrefix {
		return true
	}
	return prefixContainsRelKey(prefix, relKey)
}

func gapScopeContainsRelKey(scope *Scope, relKey string) bool {
	if scope == nil {
		return true
	}
	prefix := cleanCoveragePrefix(scope.Prefix)
	if prefix == "" {
		return true
	}
	if prefix == RelativeRootScopePrefix {
		return true
	}
	return prefixContainsRelKey(prefix, relKey)
}

func cleanCoveragePrefix(prefix string) string {
	return strings.TrimPrefix(strings.TrimSpace(prefix), "/")
}

func prefixContainsRelKey(prefix string, relKey string) bool {
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(relKey, prefix)
	}
	return relKey == prefix || strings.HasPrefix(relKey, prefix+"/")
}

func rowsFromState(state map[string]CurrentObjectRow) []CurrentObjectRow {
	rows := make([]CurrentObjectRow, 0, len(state))
	for _, row := range state {
		rows = append(rows, normalizeCurrentObjectRow(row))
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].RelKey < rows[j].RelKey
	})
	return rows
}

func normalizeCompactionInput(input CompactionInput) CompactionInput {
	input.IndexSetID = strings.TrimSpace(input.IndexSetID)
	input.RunID = strings.TrimSpace(input.RunID)
	if !input.RunStartedAt.IsZero() {
		input.RunStartedAt = input.RunStartedAt.UTC()
	}
	return input
}

func normalizeJournal(journal Journal) Journal {
	journal.Header = normalizeHeader(journal.Header)
	journal.Footer = normalizeFooter(journal.Footer)
	for i := range journal.Records {
		journal.Records[i] = normalizeObjectRecord(journal.Records[i], journal.Records[i].JournalID, journal.Records[i].Sequence)
	}
	return journal
}

func normalizeCurrentObjectRow(row CurrentObjectRow) CurrentObjectRow {
	row.IndexSetID = strings.TrimSpace(row.IndexSetID)
	if !row.FirstSeenAt.IsZero() {
		row.FirstSeenAt = row.FirstSeenAt.UTC()
	}
	if !row.LastChangedAt.IsZero() {
		row.LastChangedAt = row.LastChangedAt.UTC()
	}
	if !row.LastSeenAt.IsZero() {
		row.LastSeenAt = row.LastSeenAt.UTC()
	}
	row.LastModified = canonicalTimePtr(row.LastModified)
	row.RestoreExpiry = canonicalTimePtr(row.RestoreExpiry)
	row.HeadEnrichedAt = canonicalTimePtr(row.HeadEnrichedAt)
	row.DeletedAt = canonicalTimePtr(row.DeletedAt)
	row.StorageClass = stringPtrCopy(row.StorageClass)
	row.ArchiveStatus = stringPtrCopy(row.ArchiveStatus)
	row.RestoreState = stringPtrCopy(row.RestoreState)
	row.ContentType = stringPtrCopy(row.ContentType)
	return row
}

func validateCompactionInput(input CompactionInput) error {
	switch {
	case input.IndexSetID == "":
		return fmt.Errorf("%w: index_set_id is required", ErrInvalidJournal)
	case input.RunID == "":
		return fmt.Errorf("%w: run_id is required", ErrInvalidJournal)
	case input.RunStartedAt.IsZero():
		return fmt.Errorf("%w: run_started_at is required", ErrInvalidJournal)
	default:
		return nil
	}
}

func validateCompactionJournal(input CompactionInput, journal Journal) error {
	if err := validateHeader(journal.Header); err != nil {
		return err
	}
	if journal.Header.IndexSetID != input.IndexSetID {
		return fmt.Errorf("%w: journal index_set_id mismatch", ErrInvalidJournal)
	}
	if journal.Header.RunID != input.RunID {
		return fmt.Errorf("%w: journal run_id mismatch", ErrInvalidJournal)
	}
	for i, record := range journal.Records {
		if err := validateObjectRecord(record); err != nil {
			return err
		}
		if record.JournalID != journal.Header.JournalID {
			return fmt.Errorf("%w: record journal_id mismatch", ErrInvalidJournal)
		}
		expected := uint64(i + 1)
		if record.Sequence != expected {
			return fmt.Errorf("%w: non-monotonic record sequence %d, expected %d", ErrInvalidJournal, record.Sequence, expected)
		}
	}
	return validateFooter(journal.Header, journal.Footer, uint64(len(journal.Records)))
}

func valueOrZero(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func fallbackRunID(primary string, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}

func fallbackTime(primary time.Time, fallback time.Time) time.Time {
	if !primary.IsZero() {
		return primary.UTC()
	}
	return fallback.UTC()
}

func canonicalTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	out := value.UTC()
	return &out
}

func stringPtrCopy(value *string) *string {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func stringPtrEqual(a *string, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func timePtrEqual(a *time.Time, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.UTC().Equal(b.UTC())
	}
}
