package durableindex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ContractCapability                = "contract: durable-index/v0"
	SegmentedSnapshotCapability       = "contract: segmented-snapshot/v0"
	ScopeProfileUnicodePrefixV0       = "unicode-prefix/v0"
	RowProfileObjectInventoryV0       = "object-inventory/v0"
	EncodingProfileParquetInventoryV0 = "parquet-inventory/v0"
	DefaultObjectNamespace            = "objects"

	KindIndexPlan          = "index_plan"
	KindIndexMetadata      = "index_metadata"
	KindIndexCutover       = "index_cutover"
	KindSnapshotPub        = "snapshot_publication"
	LogicalProjectionToken = "logical_projection"
	LogicalProfileID       = "example.index-logical/v0"
	LogicalProjectionMode  = "logical-only/v0"

	ModeBaseline = "baseline"
	ModeObserve  = "observe"
	ModeEnrich   = "enrich"

	AdmissionBaselineNotCompared = "baseline_not_compared"
	AdmissionNoLoss              = "no_loss"
	AdmissionLossAcknowledged    = "loss_acknowledged"
	AdmissionInherited           = "inherited"

	MaxEffectiveScopes         = 65536
	MaxTextUTF8Bytes           = 4096
	MaxEnrichmentBytes         = 65536
	MaxExactInteger            = int64((1 << 53) - 1) // 9007199254740991
	MaxAncestryDepth           = 64
	MaxMetadataBytes           = 64 * 1024 * 1024   // 67108864
	MaxDecompressedMemberBytes = 1024 * 1024 * 1024 // 1073741824
	InstantLayoutNanoUTC       = "2006-01-02T15:04:05.000000000Z"
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	sha256HexPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	instantRegex      = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{9})Z$`)
)

// RefusalError represents a contract-defined refusal with its normative reason code.
type RefusalError struct {
	Reason string
	Detail string
}

func (e *RefusalError) Error() string {
	if e == nil {
		return ""
	}
	if e.Detail == "" {
		return e.Reason
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

// NewRefusal constructs a RefusalError with the given reason code and optional detail.
func NewRefusal(reason, detail string) error {
	return &RefusalError{Reason: reason, Detail: detail}
}

// RefusalReason extracts the contract refusal reason from err if it wraps a *RefusalError.
func RefusalReason(err error) (string, bool) {
	var ref *RefusalError
	if errors.As(err, &ref) && ref != nil {
		return ref.Reason, true
	}
	return "", false
}

// Scope represents a single {namespace, prefix} scope in an index_plan.
type Scope struct {
	Namespace string `json:"namespace"`
	Prefix    string `json:"prefix"`
}

// IndexPlan represents a contract:durable-index/v0/index-plan.schema.json document.
type IndexPlan struct {
	Capabilities []string `json:"capabilities"`
	Kind         string   `json:"kind"`
	SetID        string   `json:"set_id"`
	ScopeProfile string   `json:"scope_profile"`
	Scopes       []Scope  `json:"scopes"`
}

// Selection identifies a segmented-snapshot namespace, snapshot, and artifact.
type Selection struct {
	NamespaceID string `json:"namespace_id"`
	SnapshotID  string `json:"snapshot_id"`
	ArtifactID  string `json:"artifact_id"`
}

// Binding pins an immutable artifact by id, logical ref, sha256, and byte_length.
type Binding struct {
	ID         string `json:"id"`
	Ref        string `json:"ref"`
	SHA256     string `json:"sha256"`
	ByteLength int64  `json:"byte_length"`
}

// ParentBinding binds a parent snapshot selection and publication pin.
type ParentBinding struct {
	Selection   Selection `json:"selection"`
	Publication Binding   `json:"publication"`
}

// LossAcknowledgment is the wire loss_acknowledgment object in index_metadata.
type LossAcknowledgment struct {
	Parent         ParentBinding `json:"parent"`
	PlanSHA256     string        `json:"plan_sha256"`
	AcknowledgedBy string        `json:"acknowledged_by"`
	AcknowledgedAt string        `json:"acknowledged_at"`
}

// TransitionAcknowledgment is the normalized acknowledgment record in transition inputs.
type TransitionAcknowledgment struct {
	ParentSHA256   string `json:"parent_sha256"`
	PlanDigest     string `json:"plan_digest"`
	AcknowledgedBy string `json:"acknowledged_by"`
	AcknowledgedAt string `json:"acknowledged_at"`
}

// ValidateIdentifier reports whether s matches the contract's common id pattern.
func ValidateIdentifier(s string) bool {
	return identifierPattern.MatchString(s)
}

// ValidateSHA256Hex reports whether s is a 64-character lowercase hex SHA-256 digest.
func ValidateSHA256Hex(s string) bool {
	return sha256HexPattern.MatchString(s)
}

// ValidateInstant validates that value is an exact YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ
// Gregorian UTC timestamp (years 0001-9999, seconds 00-59, no leap seconds).
func ValidateInstant(value string) error {
	m := instantRegex.FindStringSubmatch(value)
	if m == nil {
		return NewRefusal("invalid_instant", "timestamp does not match YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ")
	}
	year, _ := strconv.Atoi(m[1])
	month, _ := strconv.Atoi(m[2])
	day, _ := strconv.Atoi(m[3])
	hour, _ := strconv.Atoi(m[4])
	minute, _ := strconv.Atoi(m[5])
	sec, _ := strconv.Atoi(m[6])
	if year < 1 || month < 1 || month > 12 || hour > 23 || minute > 59 || sec > 59 {
		return NewRefusal("invalid_instant", "timestamp field out of range")
	}
	leap := (year%4 == 0 && year%100 != 0) || (year%400 == 0)
	daysInMonth := [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	if leap {
		daysInMonth[1] = 29
	}
	if day < 1 || day > daysInMonth[month-1] {
		return NewRefusal("invalid_instant", "invalid calendar day")
	}
	return nil
}

// FormatInstant formats t as an exact 9-digit UTC timestamp string.
func FormatInstant(t time.Time) (string, error) {
	if t.IsZero() {
		return "", NewRefusal("invalid_instant", "zero timestamp")
	}
	u := t.UTC()
	y := u.Year()
	if y < 1 || y > 9999 {
		return "", NewRefusal("invalid_instant", "year out of range 0001-9999")
	}
	s := u.Format(InstantLayoutNanoUTC)
	if err := ValidateInstant(s); err != nil {
		return "", err
	}
	return s, nil
}

// ValidateTextUTF8 checks that s is valid UTF-8 and at most MaxTextUTF8Bytes bytes.
func ValidateTextUTF8(s string) error {
	if !utf8.ValidString(s) {
		return NewRefusal("invalid_unicode", "string is not valid UTF-8")
	}
	if len(s) > MaxTextUTF8Bytes {
		return NewRefusal("capacity", "UTF-8 byte length exceeds 4096")
	}
	return nil
}

// CompareScopes compares two scopes by unsigned UTF-8 byte order on (namespace, prefix).
func CompareScopes(a, b Scope) int {
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Prefix, b.Prefix)
}

// ScopeContains reports whether key in namespace belongs to scope by exact UTF-8 prefix bytes.
func ScopeContains(scope Scope, namespace, key string) bool {
	return scope.Namespace == namespace && strings.HasPrefix(key, scope.Prefix)
}

// ScopeCovers reports whether outer covers inner (same namespace and inner.Prefix starts with outer.Prefix).
func ScopeCovers(outer, inner Scope) bool {
	return ScopeContains(outer, inner.Namespace, inner.Prefix)
}

// CanonicalizeScopes sorts, deduplicates, and removes prefix-contained scopes without
// modifying individual prefix bytes (I2). Enforces the 65,536 scope count, identifier syntax
// on namespace, and 4,096 UTF-8 byte ceiling per namespace and prefix.
func CanonicalizeScopes(scopes []Scope) ([]Scope, error) {
	if len(scopes) > MaxEffectiveScopes {
		return nil, NewRefusal("capacity", "scope count exceeds 65536")
	}
	for _, s := range scopes {
		if err := ValidateTextUTF8(s.Namespace); err != nil {
			return nil, err
		}
		if !ValidateIdentifier(s.Namespace) {
			return nil, NewRefusal("invalid_shape", "scope namespace is not a valid contract identifier")
		}
		if err := ValidateTextUTF8(s.Prefix); err != nil {
			return nil, err
		}
	}
	sorted := make([]Scope, len(scopes))
	copy(sorted, scopes)
	sort.Slice(sorted, func(i, j int) bool {
		return CompareScopes(sorted[i], sorted[j]) < 0
	})
	out := make([]Scope, 0, len(sorted))
	for _, s := range sorted {
		if len(out) > 0 && ScopeCovers(out[len(out)-1], s) {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// ValidateCanonicalScopes checks capacity bounds on scopes and refuses with
// noncanonical_plan if scopes is not already sorted, unique, and prefix-minimal.
func ValidateCanonicalScopes(scopes []Scope) error {
	canon, err := CanonicalizeScopes(scopes)
	if err != nil {
		return err
	}
	if !ScopesEqual(scopes, canon) {
		return NewRefusal("noncanonical_plan", "wire scopes are not sorted, unique, and prefix-minimal")
	}
	return nil
}

// ScopesEqual reports whether two Scope slices are identical in length and elements.
func ScopesEqual(a, b []Scope) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func appendFramedString(buf *bytes.Buffer, s string) {
	var lenBuf [4]byte
	//nolint:gosec // length is bounded by MaxTextUTF8Bytes / MaxEffectiveScopes
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s))) // #nosec G115 -- length is bounded by MaxTextUTF8Bytes / MaxEffectiveScopes
	_, _ = buf.Write(lenBuf[:])
	_, _ = buf.WriteString(s)
}

// PlanFramedBytes returns the semantic length-framed plan bytes (I2):
//  1. ASCII "durable-index/v0/plan\x00"
//  2. set_id framed as 4-byte big-endian UTF-8 byte length + bytes
//  3. 4-byte big-endian effective scope count
//  4. For each canonical scope, framed namespace + framed prefix.
func PlanFramedBytes(setID string, scopes []Scope) ([]byte, error) {
	canon, err := CanonicalizeScopes(scopes)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	_, _ = buf.WriteString("durable-index/v0/plan\x00")
	appendFramedString(&buf, setID)
	var countBuf [4]byte
	//nolint:gosec // len(canon) <= MaxEffectiveScopes (65536)
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(canon))) // #nosec G115 -- len(canon) <= MaxEffectiveScopes (65536)
	_, _ = buf.Write(countBuf[:])
	for _, s := range canon {
		appendFramedString(&buf, s.Namespace)
		appendFramedString(&buf, s.Prefix)
	}
	return buf.Bytes(), nil
}

// PlanDigest computes the lowercase hex SHA-256 semantic plan_digest over PlanFramedBytes (I2).
func PlanDigest(setID string, scopes []Scope) (string, error) {
	framed, err := PlanFramedBytes(setID, scopes)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(framed)
	return hex.EncodeToString(sum[:]), nil
}

// BuildIndexPlan canonicalizes scopes and builds a canonical IndexPlan document, its JSON bytes, and its semantic plan_digest.
func BuildIndexPlan(setID string, scopes []Scope) (IndexPlan, []byte, string, error) {
	if !ValidateIdentifier(setID) {
		return IndexPlan{}, nil, "", NewRefusal("invalid_shape", "set_id is not a valid contract identifier")
	}
	canon, err := CanonicalizeScopes(scopes)
	if err != nil {
		return IndexPlan{}, nil, "", err
	}
	digest, err := PlanDigest(setID, canon)
	if err != nil {
		return IndexPlan{}, nil, "", err
	}
	plan := IndexPlan{
		Capabilities: []string{ContractCapability},
		Kind:         KindIndexPlan,
		SetID:        setID,
		ScopeProfile: ScopeProfileUnicodePrefixV0,
		Scopes:       canon,
	}
	raw, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return IndexPlan{}, nil, "", err
	}
	raw = append(raw, '\n')
	return plan, raw, digest, nil
}

// HasScopeLoss reports whether nextScopes loses any effective prefix coverage relative to parentScopes (I5).
// Every canonical parent scope must be covered by some canonical next scope in the same namespace.
func HasScopeLoss(parentScopes, nextScopes []Scope) (bool, error) {
	canonParent, err := CanonicalizeScopes(parentScopes)
	if err != nil {
		return false, err
	}
	canonNext, err := CanonicalizeScopes(nextScopes)
	if err != nil {
		return false, err
	}
	for _, oldScope := range canonParent {
		idx := sort.Search(len(canonNext), func(i int) bool {
			return CompareScopes(canonNext[i], oldScope) > 0
		})
		if idx == 0 || !ScopeCovers(canonNext[idx-1], oldScope) {
			return true, nil
		}
	}
	return false, nil
}

// EvaluateAdmission evaluates the I5 admission status for a baseline or successor transition.
func EvaluateAdmission(
	mode string,
	setID string,
	parentPubSHA256 string,
	parentScopes []Scope,
	nextScopes []Scope,
	ack *TransitionAcknowledgment,
) (string, error) {
	switch mode {
	case ModeBaseline:
		if ack != nil {
			return "", NewRefusal("unexpected_acknowledgment", "baseline cannot supply a loss acknowledgment")
		}
		return AdmissionBaselineNotCompared, nil
	case ModeEnrich:
		if !ScopesEqual(parentScopes, nextScopes) {
			return "", NewRefusal("enrichment_scope_change", "enrichment cannot change the effective plan")
		}
		if ack != nil {
			return "", NewRefusal("unexpected_acknowledgment", "enrichment cannot supply a loss acknowledgment")
		}
		return AdmissionInherited, nil
	case ModeObserve:
		lost, err := HasScopeLoss(parentScopes, nextScopes)
		if err != nil {
			return "", err
		}
		if !lost {
			if ack != nil {
				return "", NewRefusal("unexpected_acknowledgment", "no-loss transition cannot supply a loss acknowledgment")
			}
			return AdmissionNoLoss, nil
		}
		if ack == nil {
			return "", NewRefusal("scope_loss", "reduced effective scope union requires explicit acknowledgment")
		}
		if !ValidateIdentifier(ack.AcknowledgedBy) {
			return "", NewRefusal("acknowledgment_actor", "acknowledgment lacks a valid actor identifier")
		}
		if err := ValidateInstant(ack.AcknowledgedAt); err != nil {
			return "", err
		}
		expectedDigest, err := PlanDigest(setID, nextScopes)
		if err != nil {
			return "", err
		}
		if ack.ParentSHA256 != parentPubSHA256 || ack.PlanDigest != expectedDigest {
			return "", NewRefusal("acknowledgment_mismatch", "acknowledgment does not bind the exact parent and proposed plan digest")
		}
		return AdmissionLossAcknowledged, nil
	default:
		return "", NewRefusal("invalid_shape", fmt.Sprintf("unsupported mode %q", mode))
	}
}

// SubjectURI computes segmented-snapshot's immutable coverage-subject URI for selection.
func SubjectURI(selection Selection) string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("segmented-snapshot/v0/subject\x00")
	_, _ = buf.WriteString(selection.NamespaceID)
	_ = buf.WriteByte(0)
	_, _ = buf.WriteString(selection.SnapshotID)
	sum := sha256.Sum256(buf.Bytes())
	return "snapshot:" + hex.EncodeToString(sum[:])
}
