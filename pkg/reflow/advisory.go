package reflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// AdvisoryRecordSkippedWarningCode is the warning code emitted (at most once
	// per distinct record type per run, up to MaxAdvisoryWarningTypes) when a
	// reflow consumer skips an unknown record envelope marked with top-level
	// advisory=true.
	AdvisoryRecordSkippedWarningCode = "REFLOW_ADVISORY_RECORD_SKIPPED"

	// AdvisoryWarningCapReachedCode is the aggregated warning code emitted once
	// at the end of a stream when more than MaxAdvisoryWarningTypes distinct
	// unknown advisory record types were skipped during the run.
	AdvisoryWarningCapReachedCode = "REFLOW_ADVISORY_WARNING_CAP_REACHED"

	// MaxAdvisoryWarningTypes bounds the number of distinct unknown advisory
	// record types that can emit per-type warnings in a single reflow run.
	MaxAdvisoryWarningTypes = 16

	// MaxUnreportedAdvisoryTypesTracked bounds the number of distinct unreported
	// advisory record types tracked in memory after MaxAdvisoryWarningTypes is
	// reached.
	MaxUnreportedAdvisoryTypesTracked = 1024

	maxSanitizedRecordTypeBytes      = 128
	maxSanitizedProducerVersionBytes = 64
)

var errAdvisoryControl = errors.New("skipped advisory control record")

type advisoryRecordSkippedError struct {
	RecordType string
}

func (e *advisoryRecordSkippedError) Error() string {
	return fmt.Sprintf("skipped advisory control record %q", e.RecordType)
}

func (e *advisoryRecordSkippedError) Unwrap() error {
	return errAdvisoryControl
}

// EnvelopeInspection holds the parsed top-level JSONL record envelope fields
// used by reflow input classifiers and parsers across all three intake
// boundaries (classifyReflowFirstRecord, CLI enqueueReflowLine, and engine
// parseReflowInputLine).
type EnvelopeInspection struct {
	Type                 string
	SanitizedType        string
	Data                 json.RawMessage
	Advisory             bool
	AdvisoryMalformed    bool
	DuplicateEnvelopeKey string
	ProducerVersion      string
	UnmarshalErr         error
}

type topLevelEnvelopeScan struct {
	hasExactType         bool
	typeKeyCountFold     int
	hasExactAdvisory     bool
	advisoryKeyCountFold int
	hasExactProducerVer  bool
	duplicateEnvelopeKey string
}

func scanTopLevelEnvelopeKeys(trimmed string) topLevelEnvelopeScan {
	var scan topLevelEnvelopeScan
	dec := json.NewDecoder(strings.NewReader(trimmed))
	tok, err := dec.Token()
	if err != nil {
		return scan
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return scan
	}
	seenFold := make(map[string]struct{}, 8)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return scan
		}
		key, ok := keyTok.(string)
		if !ok {
			return scan
		}
		fold := strings.ToLower(key)
		switch fold {
		case "type", "advisory", "producer_version", "data", "schema_version":
			if _, exists := seenFold[fold]; exists && scan.duplicateEnvelopeKey == "" {
				scan.duplicateEnvelopeKey = fold
			}
			seenFold[fold] = struct{}{}
		}
		switch fold {
		case "type":
			scan.typeKeyCountFold++
			if key == "type" {
				scan.hasExactType = true
			}
		case "advisory":
			scan.advisoryKeyCountFold++
			if key == "advisory" {
				scan.hasExactAdvisory = true
			}
		case "producer_version":
			if key == "producer_version" {
				scan.hasExactProducerVer = true
			}
		}
		if err := skipJSONValue(dec); err != nil {
			return scan
		}
	}
	return scan
}

func skipJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok || (delim != '{' && delim != '[') {
		return nil
	}
	depth := 1
	for depth > 0 {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		if d, ok := t.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

// InspectRecordEnvelope parses the top-level JSONL envelope of line, validating
// that any present top-level "advisory" field is a strict JSON boolean and
// rejecting case-variant or duplicate top-level envelope keys.
func InspectRecordEnvelope(line string) EnvelopeInspection {
	trimmed := strings.TrimSpace(line)
	var raw struct {
		Type            string          `json:"type"`
		Advisory        json.RawMessage `json:"advisory"`
		ProducerVersion json.RawMessage `json:"producer_version"`
		Data            json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return EnvelopeInspection{UnmarshalErr: err}
	}
	scan := scanTopLevelEnvelopeKeys(trimmed)
	recType := raw.Type
	if !scan.hasExactType || scan.typeKeyCountFold != 1 {
		recType = ""
	}
	var topProducerVer json.RawMessage
	if scan.hasExactProducerVer {
		topProducerVer = raw.ProducerVersion
	}
	insp := EnvelopeInspection{
		Type:                 recType,
		SanitizedType:        SanitizeRecordType(recType),
		Data:                 raw.Data,
		DuplicateEnvelopeKey: scan.duplicateEnvelopeKey,
		ProducerVersion:      extractProducerVersion(topProducerVer, raw.Data),
	}
	if scan.duplicateEnvelopeKey != "" {
		insp.AdvisoryMalformed = true
		return insp
	}
	if scan.advisoryKeyCountFold > 0 && !scan.hasExactAdvisory {
		insp.AdvisoryMalformed = true
		return insp
	}
	switch string(bytes.TrimSpace(raw.Advisory)) {
	case "":
	case "true":
		insp.Advisory = true
	case "false":
	default:
		insp.AdvisoryMalformed = true
	}
	return insp
}

// IsSkippableAdvisoryUnknown reports whether an unknown record envelope (after
// known exact record types have been dispatched first) may be skipped under the
// advisory control-record contract.
func (e EnvelopeInspection) IsSkippableAdvisoryUnknown() bool {
	if e.UnmarshalErr != nil || e.AdvisoryMalformed || !e.Advisory {
		return false
	}
	if !IsValidRecordTypeToken(e.Type) {
		return false
	}
	if IsNonAdvisoryFamily(e.Type) {
		return false
	}
	return true
}

// FormatAdvisoryMalformedError returns an attributable error when the top-level
// advisory field is present but not a JSON boolean or when duplicate top-level
// envelope keys are present.
func (e EnvelopeInspection) FormatAdvisoryMalformedError() error {
	if e.DuplicateEnvelopeKey != "" {
		return fmt.Errorf("invalid envelope for record type %q%s: duplicate top-level %q key", e.SanitizedType, e.producerVersionSuffix(), e.DuplicateEnvelopeKey)
	}
	return fmt.Errorf("invalid advisory envelope marker for record type %q%s: advisory must be a boolean", e.SanitizedType, e.producerVersionSuffix())
}

// FormatEngineUnknownTypeError returns the engine's attributable fail-closed
// error for an unsupported record type.
func (e EnvelopeInspection) FormatEngineUnknownTypeError() error {
	if e.AdvisoryMalformed {
		return e.FormatAdvisoryMalformedError()
	}
	if e.Advisory && IsNonAdvisoryFamily(e.Type) {
		return fmt.Errorf("unsupported record type %q%s; work, integrity, and error record families cannot be skipped as advisory", e.SanitizedType, e.producerVersionSuffix())
	}
	return fmt.Errorf("unsupported record type %q%s; RecordStreamSource carries %s records", e.SanitizedType, e.producerVersionSuffix(), ReflowInputRecordType)
}

// FormatCLIUnknownTypeError returns the CLI pool's attributable fail-closed
// error for an unsupported JSON record type.
func (e EnvelopeInspection) FormatCLIUnknownTypeError() error {
	if e.AdvisoryMalformed {
		return e.FormatAdvisoryMalformedError()
	}
	if e.Advisory && IsNonAdvisoryFamily(e.Type) {
		return fmt.Errorf("unsupported json record type %q%s; work, integrity, and error record families cannot be skipped as advisory", e.SanitizedType, e.producerVersionSuffix())
	}
	return fmt.Errorf("unsupported json record type %q%s", e.SanitizedType, e.producerVersionSuffix())
}

// FormatFirstRecordUnknownTypeReason returns the pre-execution first-record
// sniff refusal reason for an unsupported JSON record type.
func (e EnvelopeInspection) FormatFirstRecordUnknownTypeReason(malformedPrefix, unknownTypePrefix string) string {
	if e.AdvisoryMalformed {
		return fmt.Sprintf("%s: %s", malformedPrefix, e.FormatAdvisoryMalformedError().Error())
	}
	if e.Advisory && IsNonAdvisoryFamily(e.Type) {
		return fmt.Sprintf("%s %q%s; work, integrity, and error record families cannot be skipped as advisory", unknownTypePrefix, e.SanitizedType, e.producerVersionSuffix())
	}
	return fmt.Sprintf("%s %q%s", unknownTypePrefix, e.SanitizedType, e.producerVersionSuffix())
}

func (e EnvelopeInspection) producerVersionSuffix() string {
	if e.ProducerVersion == "" {
		return ""
	}
	return fmt.Sprintf(" (producer_version=%q)", e.ProducerVersion)
}

// IsValidRecordTypeToken reports whether raw is a non-empty, bounded lowercase
// ASCII record-type identifier token ([a-z0-9._:-], max 128 bytes).
func IsValidRecordTypeToken(raw string) bool {
	if raw == "" || len(raw) > maxSanitizedRecordTypeBytes {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if !isLowerRecordTypeByte(raw[i]) {
			return false
		}
	}
	return true
}

// SanitizeRecordType returns a bounded, printable single-token representation
// of a record type suitable for logs, warnings, and error messages without
// echoing arbitrary line/payload content.
func SanitizeRecordType(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "<empty>"
	}
	var b strings.Builder
	limit := len(trimmed)
	truncated := false
	if limit > maxSanitizedRecordTypeBytes {
		limit = maxSanitizedRecordTypeBytes
		truncated = true
	}
	b.Grow(limit + 3)
	for i := 0; i < limit; i++ {
		ch := trimmed[i]
		if isSafeTokenByte(ch) {
			b.WriteByte(ch)
		} else {
			b.WriteByte('_')
		}
	}
	if truncated {
		b.WriteString("...")
	}
	return b.String()
}

func isLowerRecordTypeByte(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '.' || ch == '_' || ch == '-' || ch == ':'
}

func isSafeTokenByte(ch byte) bool {
	return isLowerRecordTypeByte(ch) || (ch >= 'A' && ch <= 'Z')
}

// IsNonAdvisoryFamily reports whether recordType belongs to a recognized work,
// admission/completeness/integrity, per-object probe, or error record family.
// Records in these families must never be skipped even if marked advisory=true.
func IsNonAdvisoryFamily(recordType string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(recordType))
	for _, prefix := range []string{
		"gonimbus.atlas.",
		"gonimbus.cas.",
		"gonimbus.content.head.",
		"gonimbus.content.probe.v",
		"gonimbus.crawl.",
		"gonimbus.error.",
		"gonimbus.index.",
		"gonimbus.index_set_identity.",
		"gonimbus.inspect.",
		"gonimbus.object.",
		"gonimbus.operation.",
		"gonimbus.prefix.",
		"gonimbus.preflight.",
		"gonimbus.progress.",
		"gonimbus.provenance.",
		"gonimbus.reflow.",
		"gonimbus.scope.",
		"gonimbus.selection.",
		"gonimbus.service_demand_set.",
		"gonimbus.skip.",
		"gonimbus.stream.",
		"gonimbus.summary.",
		"gonimbus.transfer.",
		"gonimbus.warning.",
	} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	switch trimmed {
	case "gonimbus.atlas",
		"gonimbus.cas",
		"gonimbus.content.head",
		"gonimbus.content.probe",
		"gonimbus.crawl",
		"gonimbus.error",
		"gonimbus.index",
		"gonimbus.index_set_identity",
		"gonimbus.inspect",
		"gonimbus.object",
		"gonimbus.operation",
		"gonimbus.prefix",
		"gonimbus.preflight",
		"gonimbus.progress",
		"gonimbus.provenance",
		"gonimbus.reflow",
		"gonimbus.scope",
		"gonimbus.selection",
		"gonimbus.service_demand_set",
		"gonimbus.skip",
		"gonimbus.stream",
		"gonimbus.summary",
		"gonimbus.transfer",
		"gonimbus.warning":
		return true
	default:
		return false
	}
}

func extractProducerVersion(topLevel json.RawMessage, data json.RawMessage) string {
	if v := parseSafeProducerVersion(topLevel); v != "" {
		return v
	}
	trimmedData := bytes.TrimSpace(data)
	if len(trimmedData) == 0 || trimmedData[0] != '{' {
		return ""
	}
	var payload struct {
		ProducerVersion json.RawMessage `json:"producer_version"`
	}
	if err := json.Unmarshal(trimmedData, &payload); err != nil {
		return ""
	}
	return parseSafeProducerVersion(payload.ProducerVersion)
}

func parseSafeProducerVersion(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return ""
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return ""
	}
	s = strings.TrimSpace(s)
	if s == "" || len(s) > maxSanitizedProducerVersionBytes {
		return ""
	}
	for i := 0; i < len(s); i++ {
		if !isSafeTokenByte(s[i]) && s[i] != '+' {
			return ""
		}
	}
	return s
}

// AdvisoryWarningTracker bounds per-run warnings for skipped unknown advisory
// control records to at most one warning per distinct sanitized record type,
// capped at MaxAdvisoryWarningTypes distinct types per run, while accounting
// for total skipped records and unreported overflow types.
type AdvisoryWarningTracker struct {
	mu                     sync.Mutex
	seen                   map[string]struct{}
	unreportedTypes        map[string]struct{}
	unreportedTypeOverflow int
	skippedRecords         int64
	unreportedRecords      int64
}

// NewAdvisoryWarningTracker constructs a per-run tracker for skipped unknown
// advisory control records.
func NewAdvisoryWarningTracker() *AdvisoryWarningTracker {
	return &AdvisoryWarningTracker{
		seen:            make(map[string]struct{}),
		unreportedTypes: make(map[string]struct{}),
	}
}

// Observe records a skipped unknown advisory control record of recordType and
// returns a sanitized Warning on the first observation of that type (up to
// MaxAdvisoryWarningTypes distinct types).
func (t *AdvisoryWarningTracker) Observe(recordType string) (Warning, bool) {
	if t == nil {
		return Warning{}, false
	}
	sanitized := SanitizeRecordType(recordType)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.skippedRecords++
	if _, exists := t.seen[sanitized]; exists {
		return Warning{}, false
	}
	if len(t.seen) >= MaxAdvisoryWarningTypes {
		t.unreportedRecords++
		if _, exists := t.unreportedTypes[sanitized]; !exists {
			if len(t.unreportedTypes) < MaxUnreportedAdvisoryTypesTracked {
				t.unreportedTypes[sanitized] = struct{}{}
			} else {
				t.unreportedTypeOverflow++
			}
		}
		return Warning{}, false
	}
	t.seen[sanitized] = struct{}{}
	return Warning{
		Code:    AdvisoryRecordSkippedWarningCode,
		Message: fmt.Sprintf("skipped unknown advisory control record type %q", sanitized),
		Details: map[string]any{
			"record_type": sanitized,
		},
	}, true
}

// SkippedRecords returns the total number of unknown advisory control records
// skipped during the run.
func (t *AdvisoryWarningTracker) SkippedRecords() int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.skippedRecords
}

// DistinctTypes returns the total number of distinct unknown advisory control
// record types observed during the run.
func (t *AdvisoryWarningTracker) DistinctTypes() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen) + len(t.unreportedTypes) + t.unreportedTypeOverflow
}

// UnreportedRecords returns the number of skipped unknown advisory control
// records whose record type exceeded MaxAdvisoryWarningTypes.
func (t *AdvisoryWarningTracker) UnreportedRecords() int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.unreportedRecords
}

// UnreportedTypes returns the number of distinct unknown advisory control
// record types omitted after MaxAdvisoryWarningTypes was reached.
func (t *AdvisoryWarningTracker) UnreportedTypes() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.unreportedTypes) + t.unreportedTypeOverflow
}

// CapSummaryWarning returns an aggregated warning when more than
// MaxAdvisoryWarningTypes distinct unknown advisory record types were skipped
// during the run.
func (t *AdvisoryWarningTracker) CapSummaryWarning() (Warning, bool) {
	if t == nil {
		return Warning{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.unreportedRecords == 0 {
		return Warning{}, false
	}
	totalTypes := len(t.seen) + len(t.unreportedTypes) + t.unreportedTypeOverflow
	unreportedTypes := len(t.unreportedTypes) + t.unreportedTypeOverflow
	return Warning{
		Code: AdvisoryWarningCapReachedCode,
		Message: fmt.Sprintf(
			"skipped %d advisory control records across %d distinct types (%d records across %d types unreported after %d-type warning cap)",
			t.skippedRecords, totalTypes, t.unreportedRecords, unreportedTypes, MaxAdvisoryWarningTypes,
		),
		Details: map[string]any{
			"skipped_records":    t.skippedRecords,
			"distinct_types":     totalTypes,
			"unreported_records": t.unreportedRecords,
			"unreported_types":   unreportedTypes,
			"warning_cap":        MaxAdvisoryWarningTypes,
		},
	}, true
}
