package durableindex

import (
	"encoding/json"
	"sort"
	"strings"
	"unicode/utf8"

	durableindexv0 "github.com/3leaps/gonimbus/internal/contracts/durableindex/v0"
)

// Stamp represents a {snapshot_id, at} lineage stamp.
type Stamp struct {
	SnapshotID string `json:"snapshot_id"`
	At         string `json:"at"`
}

// Revision represents an opaque {kind, value} object revision token.
type Revision struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

// EnrichmentAttribute represents a per-attribute enrichment bound to against_revision.
type EnrichmentAttribute struct {
	Name            string   `json:"name"`
	Value           *string  `json:"value"`
	Operation       string   `json:"operation"`
	ObservedAt      string   `json:"observed_at"`
	AgainstRevision Revision `json:"against_revision"`
}

// LogicalRow represents a logical object-inventory/v0 row (index-row.schema.json).
type LogicalRow struct {
	Namespace    string                `json:"namespace"`
	Key          string                `json:"key"`
	Revision     *Revision             `json:"revision"`
	SizeBytes    int64                 `json:"size_bytes"`
	LastModified *string               `json:"last_modified"`
	StorageClass *string               `json:"storage_class"`
	FirstSeen    Stamp                 `json:"first_seen"`
	LastSeen     Stamp                 `json:"last_seen"`
	LastChanged  Stamp                 `json:"last_changed"`
	Deleted      *Stamp                `json:"deleted"`
	Enrichments  []EnrichmentAttribute `json:"enrichments"`
}

// ObservationInput represents a positive observation input to a transition.
type ObservationInput struct {
	Namespace    string    `json:"namespace"`
	Key          string    `json:"key"`
	Revision     *Revision `json:"revision"`
	SizeBytes    int64     `json:"size_bytes"`
	LastModified *string   `json:"last_modified"`
	StorageClass *string   `json:"storage_class"`
}

// EnrichmentUpdateInput represents a per-key attribute update in a transition.
type EnrichmentUpdateInput struct {
	Namespace string              `json:"namespace"`
	Key       string              `json:"key"`
	Attribute EnrichmentAttribute `json:"attribute"`
}

// TransitionParentInput represents the verified parent state in a transition.
type TransitionParentInput struct {
	PublicationSHA256   string       `json:"publication_sha256"`
	SetID               string       `json:"set_id"`
	SnapshotID          string       `json:"snapshot_id"`
	Generation          int64        `json:"generation"`
	Scopes              []Scope      `json:"scopes"`
	Rows                []LogicalRow `json:"rows"`
	AncestorSnapshotIDs []string     `json:"ancestor_snapshot_ids,omitempty"`
}

// TransitionInput represents the full logical transition input.
type TransitionInput struct {
	Mode                 string                    `json:"mode"`
	SetID                string                    `json:"set_id"`
	SnapshotID           string                    `json:"snapshot_id"`
	Generation           int64                     `json:"generation"`
	ObservationAt        *string                   `json:"observation_at"`
	DiscoveryComplete    bool                      `json:"discovery_complete"`
	Parent               *TransitionParentInput    `json:"parent"`
	AdmittedParentSHA256 *string                   `json:"admitted_parent_sha256"`
	Scopes               []Scope                   `json:"scopes"`
	Acknowledgment       *TransitionAcknowledgment `json:"acknowledgment"`
	Observations         []ObservationInput        `json:"observations"`
	AbsenceAuthority     []Scope                   `json:"absence_authority"`
	Enrichments          []EnrichmentUpdateInput   `json:"enrichments"`
}

// TransitionOutcome is the computed result of FoldTransition.
type TransitionOutcome struct {
	Rows                   []LogicalRow `json:"rows"`
	Admission              string       `json:"admission"`
	RetainedUnobservedRows int64        `json:"retained_unobserved_rows"`
}

// IndexCutoverReceipt represents contract:durable-index/v0/index-cutover.schema.json.
type IndexCutoverReceipt struct {
	Capabilities []string      `json:"capabilities"`
	Kind         string        `json:"kind"`
	Old          ParentBinding `json:"old"`
	New          ParentBinding `json:"new"`
	AuthorizedBy string        `json:"authorized_by"`
	AcceptedAt   string        `json:"accepted_at"`
	Rollback     Binding       `json:"rollback"`
}

// BuildCutoverReceipt constructs and validates a canonical IndexCutoverReceipt.
func BuildCutoverReceipt(
	oldEndpoint ParentBinding,
	newEndpoint ParentBinding,
	authorizedBy string,
	acceptedAt string,
) (IndexCutoverReceipt, []byte, error) {
	if !ValidateIdentifier(authorizedBy) {
		return IndexCutoverReceipt{}, nil, NewRefusal("invalid_shape", "authorized_by is not a valid identifier")
	}
	if err := ValidateInstant(acceptedAt); err != nil {
		return IndexCutoverReceipt{}, nil, err
	}
	if oldEndpoint.Publication == newEndpoint.Publication ||
		oldEndpoint.Selection.SnapshotID == newEndpoint.Selection.SnapshotID {
		return IndexCutoverReceipt{}, nil, NewRefusal("cutover_selection", "old and new endpoints must differ")
	}
	rec := IndexCutoverReceipt{
		Capabilities: []string{ContractCapability},
		Kind:         KindIndexCutover,
		Old:          oldEndpoint,
		New:          newEndpoint,
		AuthorizedBy: authorizedBy,
		AcceptedAt:   acceptedAt,
		Rollback:     oldEndpoint.Publication,
	}
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return IndexCutoverReceipt{}, nil, err
	}
	raw = append(raw, '\n')
	if err := durableindexv0.ValidateJSON(durableindexv0.IndexCutoverSchemaPath, raw); err != nil {
		return IndexCutoverReceipt{}, nil, NewRefusal("invalid_shape", err.Error())
	}
	return rec, raw, nil
}

// CompareRowKeys compares two (namespace, key) pairs by unsigned UTF-8 byte order.
func CompareRowKeys(nsA, keyA, nsB, keyB string) int {
	if c := strings.Compare(nsA, nsB); c != 0 {
		return c
	}
	return strings.Compare(keyA, keyB)
}

func validateShapeSchema(value any, schemaRelPath string) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return NewRefusal("invalid_shape", err.Error())
	}
	if err := durableindexv0.ValidateJSON(schemaRelPath, raw); err != nil {
		return NewRefusal("invalid_shape", err.Error())
	}
	return nil
}

func validateEnrichmentAttributeStrings(attr EnrichmentAttribute) error {
	for _, s := range []string{attr.Name, attr.Operation, attr.AgainstRevision.Kind, attr.AgainstRevision.Value} {
		if err := ValidateTextUTF8(s); err != nil {
			return err
		}
	}
	if attr.Value != nil {
		if !utf8.ValidString(*attr.Value) {
			return NewRefusal("invalid_unicode", "enrichment value is not valid UTF-8")
		}
		if len(*attr.Value) > MaxEnrichmentBytes {
			return NewRefusal("capacity", "enrichment value UTF-8 byte length exceeds 65536")
		}
	}
	return nil
}

func validateLogicalRowShapeAndSource(row LogicalRow) error {
	if err := ValidateTextUTF8(row.Namespace); err != nil {
		return err
	}
	if err := ValidateTextUTF8(row.Key); err != nil {
		return err
	}
	if row.Revision != nil {
		if err := ValidateTextUTF8(row.Revision.Kind); err != nil {
			return err
		}
		if err := ValidateTextUTF8(row.Revision.Value); err != nil {
			return err
		}
	}
	if row.StorageClass != nil {
		if err := ValidateTextUTF8(*row.StorageClass); err != nil {
			return err
		}
	}
	for _, attr := range row.Enrichments {
		if err := validateEnrichmentAttributeStrings(attr); err != nil {
			return err
		}
	}
	if err := validateShapeSchema(row, durableindexv0.IndexRowSchemaPath); err != nil {
		return err
	}
	if row.LastModified != nil {
		if err := ValidateInstant(*row.LastModified); err != nil {
			return err
		}
	}
	return nil
}

// ValidateRows checks a slice of logical rows for shape, capacity, strict primary-key order,
// uniqueness, stamp consistency, lineage order, and enrichment invariants (I4, I6).
func ValidateRows(rows []LogicalRow, forbiddenSnapshotID string) error {
	var prev *LogicalRow
	stamps := make(map[string]string)
	for i := range rows {
		row := rows[i]
		if err := validateLogicalRowShapeAndSource(row); err != nil {
			return err
		}
		if prev != nil {
			cmp := CompareRowKeys(prev.Namespace, prev.Key, row.Namespace, row.Key)
			if cmp == 0 {
				return NewRefusal("duplicate_key_row", "duplicate logical row key")
			}
			if cmp > 0 {
				return NewRefusal("row_order", "rows are not strictly increasing by primary key")
			}
		}
		prevCopy := row
		prev = &prevCopy

		stampPtrs := []*Stamp{&row.FirstSeen, &row.LastSeen, &row.LastChanged, row.Deleted}
		for _, sp := range stampPtrs {
			if sp == nil {
				continue
			}
			if err := ValidateInstant(sp.At); err != nil {
				return err
			}
			if forbiddenSnapshotID != "" && sp.SnapshotID == forbiddenSnapshotID {
				return NewRefusal("fabricated_freshness", "inherited stamp names proposed successor snapshot")
			}
			if priorAt, ok := stamps[sp.SnapshotID]; ok && priorAt != sp.At {
				return NewRefusal("stamp_conflict", "snapshot_id appears with conflicting observation times")
			}
			stamps[sp.SnapshotID] = sp.At
		}
		if row.FirstSeen.At > row.LastSeen.At || row.FirstSeen.At > row.LastChanged.At {
			return NewRefusal("lineage_order", "first_seen follows last_seen or last_changed")
		}
		if row.Deleted != nil {
			if row.LastSeen.At > row.Deleted.At || *row.Deleted != row.LastChanged {
				return NewRefusal("lineage_order", "deleted stamp precedes last_seen or differs from last_changed")
			}
		}
		for j := 1; j < len(row.Enrichments); j++ {
			if strings.Compare(row.Enrichments[j-1].Name, row.Enrichments[j].Name) >= 0 {
				return NewRefusal("enrichment_order", "enrichments are unsorted or duplicate")
			}
		}
		for _, attr := range row.Enrichments {
			if err := ValidateInstant(attr.ObservedAt); err != nil {
				return err
			}
			if row.Deleted != nil {
				return NewRefusal("enrichment_target", "tombstone row cannot carry enrichments")
			}
			if row.Revision == nil || *row.Revision != attr.AgainstRevision {
				return NewRefusal("enrichment_revision", "enrichment against_revision does not match active row revision")
			}
		}
	}
	return nil
}

func revisionsEqual(a, b *Revision) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func optStringsEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func cloneLogicalRow(r LogicalRow) LogicalRow {
	out := r
	if r.Revision != nil {
		rev := *r.Revision
		out.Revision = &rev
	}
	if r.LastModified != nil {
		lm := *r.LastModified
		out.LastModified = &lm
	}
	if r.StorageClass != nil {
		sc := *r.StorageClass
		out.StorageClass = &sc
	}
	if r.Deleted != nil {
		del := *r.Deleted
		out.Deleted = &del
	}
	out.Enrichments = make([]EnrichmentAttribute, len(r.Enrichments))
	for i, e := range r.Enrichments {
		out.Enrichments[i] = e
		if e.Value != nil {
			v := *e.Value
			out.Enrichments[i].Value = &v
		}
	}
	return out
}

// FoldTransition executes the I4/I5/I6 logical transition fold over normalized inputs.
func FoldTransition(in TransitionInput) (TransitionOutcome, error) {
	if !in.DiscoveryComplete {
		return TransitionOutcome{}, NewRefusal("discovery_failed", "plan discovery did not complete")
	}
	if err := ValidateCanonicalScopes(in.Scopes); err != nil {
		return TransitionOutcome{}, err
	}

	type rowKey struct {
		ns  string
		key string
	}
	rows := make(map[rowKey]LogicalRow)
	var admission string

	if in.Generation < 0 || in.Generation > MaxExactInteger {
		return TransitionOutcome{}, NewRefusal("invalid_shape", "generation out of exact integer bounds 0..2^53-1")
	}
	if in.Mode == ModeBaseline {
		if in.Parent != nil || in.Generation != 0 {
			return TransitionOutcome{}, NewRefusal("invalid_baseline", "baseline must have null parent and generation 0")
		}
		if in.Acknowledgment != nil {
			return TransitionOutcome{}, NewRefusal("unexpected_acknowledgment", "baseline cannot supply acknowledgment")
		}
		admission = AdmissionBaselineNotCompared
	} else {
		if in.Parent == nil {
			return TransitionOutcome{}, NewRefusal("parent_unavailable", "non-baseline transition requires parent")
		}
		p := in.Parent
		if p.Generation < 0 || p.Generation > MaxExactInteger {
			return TransitionOutcome{}, NewRefusal("invalid_shape", "parent generation out of exact integer bounds 0..2^53-1")
		}
		if p.SetID != in.SetID {
			return TransitionOutcome{}, NewRefusal("set_mismatch", "parent set_id differs from successor set_id")
		}
		if p.SnapshotID == in.SnapshotID {
			return TransitionOutcome{}, NewRefusal("snapshot_reuse", "successor reuses parent snapshot_id")
		}
		for _, anc := range p.AncestorSnapshotIDs {
			if anc == in.SnapshotID {
				return TransitionOutcome{}, NewRefusal("parent_cycle", "parent ancestry repeats current snapshot_id")
			}
		}
		if err := ValidateRows(p.Rows, in.SnapshotID); err != nil {
			return TransitionOutcome{}, err
		}
		if p.Generation == MaxExactInteger || p.Generation+1 != in.Generation {
			return TransitionOutcome{}, NewRefusal("generation_mismatch", "successor generation is not parent + 1")
		}
		if in.AdmittedParentSHA256 == nil || *in.AdmittedParentSHA256 != p.PublicationSHA256 {
			return TransitionOutcome{}, NewRefusal("stale_parent", "parent publication_sha256 differs from admitted_parent_sha256")
		}
		for _, r := range p.Rows {
			k := rowKey{ns: r.Namespace, key: r.Key}
			rows[k] = cloneLogicalRow(r)
		}
		if in.Mode == ModeEnrich {
			if !ScopesEqual(in.Scopes, p.Scopes) {
				return TransitionOutcome{}, NewRefusal("enrichment_scope_change", "enrich mode cannot change scopes")
			}
			if len(in.Observations) > 0 || in.ObservationAt != nil {
				return TransitionOutcome{}, NewRefusal("enrichment_observation", "enrich mode cannot supply observations or observation_at")
			}
			if in.Acknowledgment != nil {
				return TransitionOutcome{}, NewRefusal("unexpected_acknowledgment", "enrich mode cannot supply acknowledgment")
			}
			admission = AdmissionInherited
		} else {
			var err error
			admission, err = EvaluateAdmission(in.Mode, in.SetID, p.PublicationSHA256, p.Scopes, in.Scopes, in.Acknowledgment)
			if err != nil {
				return TransitionOutcome{}, err
			}
		}
	}

	if in.Mode != ModeEnrich {
		if in.ObservationAt == nil {
			return TransitionOutcome{}, NewRefusal("invalid_instant", "observation_at is required")
		}
		if err := ValidateInstant(*in.ObservationAt); err != nil {
			return TransitionOutcome{}, err
		}
		nowStamp := Stamp{SnapshotID: in.SnapshotID, At: *in.ObservationAt}
		for _, oldRow := range rows {
			for _, sp := range []*Stamp{&oldRow.FirstSeen, &oldRow.LastSeen, &oldRow.LastChanged, oldRow.Deleted} {
				if sp != nil && sp.At > nowStamp.At {
					return TransitionOutcome{}, NewRefusal("clock_reversal", "observation_at precedes inherited stamp")
				}
			}
		}
		observed := make(map[rowKey]struct{}, len(in.Observations))
		for _, obs := range in.Observations {
			candidate := LogicalRow{
				Namespace:    obs.Namespace,
				Key:          obs.Key,
				Revision:     obs.Revision,
				SizeBytes:    obs.SizeBytes,
				LastModified: obs.LastModified,
				StorageClass: obs.StorageClass,
				FirstSeen:    nowStamp,
				LastSeen:     nowStamp,
				LastChanged:  nowStamp,
				Deleted:      nil,
				Enrichments:  []EnrichmentAttribute{},
			}
			if err := validateLogicalRowShapeAndSource(candidate); err != nil {
				return TransitionOutcome{}, err
			}
			k := rowKey{ns: obs.Namespace, key: obs.Key}
			if _, dup := observed[k]; dup {
				return TransitionOutcome{}, NewRefusal("duplicate_observation", "duplicate observation key")
			}
			observed[k] = struct{}{}
			inPlan := false
			for _, sc := range in.Scopes {
				if ScopeContains(sc, obs.Namespace, obs.Key) {
					inPlan = true
					break
				}
			}
			if !inPlan {
				return TransitionOutcome{}, NewRefusal("observation_outside_plan", "positive observation lies outside effective plan")
			}
			oldRow, exists := rows[k]
			if !exists {
				rows[k] = candidate
			} else {
				updated := cloneLogicalRow(oldRow)
				reappeared := oldRow.Deleted != nil
				sourceDiff := !revisionsEqual(oldRow.Revision, obs.Revision) ||
					oldRow.SizeBytes != obs.SizeBytes ||
					!optStringsEqual(oldRow.LastModified, obs.LastModified) ||
					!optStringsEqual(oldRow.StorageClass, obs.StorageClass)
				if reappeared || sourceDiff {
					updated.LastChanged = nowStamp
				}
				if obs.Revision == nil || !revisionsEqual(obs.Revision, oldRow.Revision) || reappeared {
					updated.Enrichments = []EnrichmentAttribute{}
				}
				updated.Revision = candidate.Revision
				updated.SizeBytes = obs.SizeBytes
				updated.LastModified = candidate.LastModified
				updated.StorageClass = candidate.StorageClass
				updated.LastSeen = nowStamp
				updated.Deleted = nil
				rows[k] = updated
			}
		}
		for _, authScope := range in.AbsenceAuthority {
			inScopes := false
			for _, sc := range in.Scopes {
				if sc == authScope {
					inScopes = true
					break
				}
			}
			if !inScopes {
				return TransitionOutcome{}, NewRefusal("coverage_outside_plan", "absence authority scope not in plan")
			}
		}
		for k, r := range rows {
			if _, wasObserved := observed[k]; wasObserved || r.Deleted != nil {
				continue
			}
			authorized := false
			for _, authScope := range in.AbsenceAuthority {
				if ScopeContains(authScope, r.Namespace, r.Key) {
					authorized = true
					break
				}
			}
			if authorized {
				tomb := cloneLogicalRow(r)
				delStamp := nowStamp
				tomb.Deleted = &delStamp
				tomb.LastChanged = nowStamp
				tomb.Enrichments = []EnrichmentAttribute{}
				rows[k] = tomb
			}
		}
	} else if len(in.AbsenceAuthority) > 0 {
		return TransitionOutcome{}, NewRefusal("enrichment_observation", "enrich mode cannot supply absence authority")
	}

	type attrTarget struct {
		ns   string
		key  string
		name string
	}
	seenUpdates := make(map[attrTarget]struct{}, len(in.Enrichments))
	for _, upd := range in.Enrichments {
		if err := ValidateTextUTF8(upd.Namespace); err != nil {
			return TransitionOutcome{}, err
		}
		if err := ValidateTextUTF8(upd.Key); err != nil {
			return TransitionOutcome{}, err
		}
		if err := validateEnrichmentAttributeStrings(upd.Attribute); err != nil {
			return TransitionOutcome{}, err
		}
		k := rowKey{ns: upd.Namespace, key: upd.Key}
		row, exists := rows[k]
		if !exists || row.Deleted != nil {
			return TransitionOutcome{}, NewRefusal("enrichment_target", "enrichment targets absent or deleted row")
		}
		target := attrTarget{ns: upd.Namespace, key: upd.Key, name: upd.Attribute.Name}
		if _, dup := seenUpdates[target]; dup {
			return TransitionOutcome{}, NewRefusal("duplicate_enrichment", "duplicate attribute update in transition")
		}
		seenUpdates[target] = struct{}{}
		if row.Revision == nil || *row.Revision != upd.Attribute.AgainstRevision {
			return TransitionOutcome{}, NewRefusal("enrichment_revision", "enrichment against_revision does not match active row revision")
		}
		if err := ValidateInstant(upd.Attribute.ObservedAt); err != nil {
			return TransitionOutcome{}, err
		}
		byName := make(map[string]EnrichmentAttribute, len(row.Enrichments)+1)
		for _, existing := range row.Enrichments {
			byName[existing.Name] = existing
		}
		if oldAttr, had := byName[upd.Attribute.Name]; had {
			if oldAttr.ObservedAt > upd.Attribute.ObservedAt {
				return TransitionOutcome{}, NewRefusal("stale_enrichment", "enrichment update predates retained attribute")
			}
		}
		attrCopy := upd.Attribute
		if upd.Attribute.Value != nil {
			v := *upd.Attribute.Value
			attrCopy.Value = &v
		}
		byName[upd.Attribute.Name] = attrCopy
		names := make([]string, 0, len(byName))
		for name := range byName {
			names = append(names, name)
		}
		sort.Strings(names)
		nextEnrich := make([]EnrichmentAttribute, 0, len(names))
		for _, name := range names {
			nextEnrich = append(nextEnrich, byName[name])
		}
		row.Enrichments = nextEnrich
		rows[k] = row
	}

	keys := make([]rowKey, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return CompareRowKeys(keys[i].ns, keys[i].key, keys[j].ns, keys[j].key) < 0
	})
	output := make([]LogicalRow, 0, len(keys))
	var retained int64
	for _, k := range keys {
		r := rows[k]
		if r.LastSeen.SnapshotID != in.SnapshotID {
			retained++
		}
		output = append(output, r)
	}
	if err := ValidateRows(output, ""); err != nil {
		return TransitionOutcome{}, err
	}
	return TransitionOutcome{
		Rows:                   output,
		Admission:              admission,
		RetainedUnobservedRows: retained,
	}, nil
}
