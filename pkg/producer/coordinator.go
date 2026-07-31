package producer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// DurableBatchSize bounds the admission/outcome frontier committed before
// downstream handoff. A store may group-commit several concurrent batches.
const DurableBatchSize = 128

// Outcome is the coordinator-owned terminal disposition of one admitted object.
type Outcome string

const (
	OutcomeEmitted     Outcome = "emitted"
	OutcomeFiltered    Outcome = "filtered"
	OutcomeRefused     Outcome = "refused"
	OutcomeQuarantined Outcome = "quarantined"
	OutcomeFailed      Outcome = "failed"
)

// Admission binds one enumerated object and its observed source revision to an
// authenticated lane. SourceRevision may be empty on an initial run, but such
// an admission cannot be safely replayed.
type Admission struct {
	Lane           partition.LaneRef
	AdmissionKey   string
	SourceProvider string
	BaseIdentity   string
	SourceKey      string
	SourceRevision provider.SourceRevision
}

// AdmittedObject pairs an admission candidate with the enumerated object that
// produced it. A non-refused object is returned only after its admission
// commits; Refusal marks a candidate that the replay store declined to admit.
type AdmittedObject struct {
	Item      LaneObject
	Admission Admission
	// Refusal is a recoverable, per-object replay refusal reported by the durable
	// store. A refused object was not admitted at this revision and must not
	// produce a durable outcome or work unit.
	Refusal error
}

// AdmissionRefusal reports that one proposed admission was refused without
// failing the durable writer. The admission key identifies the corresponding
// object in the submitted batch.
type AdmissionRefusal struct {
	AdmissionKey string
	Err          error
}

// ProbeOutcome is the keyed terminal producer outcome for one admission.
type ProbeOutcome struct {
	AdmissionKey string
	Outcome      Outcome
}

// WorkUnit is one stable downstream unit emitted by an admission.
type WorkUnit struct {
	AdmissionKey string
	UnitKey      string
	Kind         string
}

// DurableBatch records outcomes and any emitted work units in one durable
// store barrier. Admissions are written by AdmitBatch in an earlier barrier.
type DurableBatch struct {
	Outcomes  []ProbeOutcome
	WorkUnits []WorkUnit
}

// TerminalAck records that the downstream checkpoint for UnitKey committed.
type TerminalAck struct {
	UnitKey string
	Status  string
}

// LaneStatus is a set-derived projection. Terminal is authoritative only when
// EOF is durable and MissingOutcomes and MissingTerminals are both zero.
type LaneStatus struct {
	Admissions       int64
	Outcomes         int64
	WorkUnits        int64
	DurableTerminals int64
	MissingOutcomes  int64
	MissingTerminals int64
	EOF              bool
	Terminal         bool
}

// DurableStore is the storage boundary owned by the run coordinator. Every
// mutating method returns only after the batch containing it is durable.
type DurableStore interface {
	PersistAdmissions(ctx context.Context, admissions []Admission, replay bool) ([]AdmissionRefusal, error)
	PersistDurableBatch(ctx context.Context, batch DurableBatch) error
	MarkLaneEOF(ctx context.Context, lanes []partition.LaneRef) error
	AcknowledgeTerminals(ctx context.Context, acks []TerminalAck) error
	LaneStatus(ctx context.Context, lane partition.LaneRef) (LaneStatus, error)
}

// Coordinator is the sole writer of admission, outcome, work-unit, EOF, and
// terminal-ack state for one validated partition authority.
type Coordinator struct {
	authority *partition.Authority
	store     DurableStore
	replay    bool
}

// NewCoordinator validates the complete durable authority before any write.
func NewCoordinator(authority *partition.Authority, store DurableStore, replay bool) (*Coordinator, error) {
	if authority == nil {
		return nil, errors.New("producer: durable coordinator requires a validated partition authority")
	}
	if store == nil {
		return nil, errors.New("producer: durable coordinator requires a durable store")
	}
	return &Coordinator{authority: authority, store: store, replay: replay}, nil
}

// AdmitBatch authenticates and persists all objects before returning any of
// them for probe/selection handling.
func (c *Coordinator) AdmitBatch(ctx context.Context, items []LaneObject) ([]AdmittedObject, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if len(items) > DurableBatchSize {
		return nil, fmt.Errorf("producer: admission batch contains %d objects, ceiling is %d", len(items), DurableBatchSize)
	}
	admitted := make([]AdmittedObject, len(items))
	records := make([]Admission, len(items))
	for i, item := range items {
		if err := c.authority.AuthorizeLane(item.Lane); err != nil {
			return nil, fmt.Errorf("producer: object carries unauthorized lane identity: %w", err)
		}
		revision := revisionFromSummary(item.Object)
		record := Admission{
			Lane:           item.Lane,
			SourceProvider: sourceProvider(c.authority.BaseIdentity()),
			BaseIdentity:   c.authority.BaseIdentity(),
			SourceKey:      item.Object.Key,
			SourceRevision: revision,
		}
		record.AdmissionKey = admissionKey(record)
		records[i] = record
		admitted[i] = AdmittedObject{Item: item, Admission: record}
	}
	refusals, err := c.store.PersistAdmissions(ctx, records, c.replay)
	if err != nil {
		return nil, fmt.Errorf("producer: persist admissions: %w", err)
	}
	index := make(map[string]int, len(admitted))
	for i := range admitted {
		index[admitted[i].Admission.AdmissionKey] = i
	}
	for _, refusal := range refusals {
		i, ok := index[refusal.AdmissionKey]
		if !ok {
			return nil, fmt.Errorf("producer: store refused unknown admission key %q", refusal.AdmissionKey)
		}
		if refusal.Err == nil {
			return nil, fmt.Errorf("producer: store refused admission key %q without a reason", refusal.AdmissionKey)
		}
		admitted[i].Refusal = refusal.Err
	}
	return admitted, nil
}

// PersistDurableBatch records every terminal outcome and emitted unit before
// the caller hands any unit downstream.
func (c *Coordinator) PersistDurableBatch(ctx context.Context, batch DurableBatch) error {
	if len(batch.Outcomes) > DurableBatchSize || len(batch.WorkUnits) > DurableBatchSize {
		return fmt.Errorf("producer: durable batch exceeds the %d-object ceiling", DurableBatchSize)
	}
	for _, outcome := range batch.Outcomes {
		if strings.TrimSpace(outcome.AdmissionKey) == "" {
			return errors.New("producer: probe outcome requires an admission key")
		}
		if !outcome.Outcome.valid() {
			return fmt.Errorf("producer: unknown probe outcome %q", outcome.Outcome)
		}
	}
	for _, unit := range batch.WorkUnits {
		if strings.TrimSpace(unit.AdmissionKey) == "" || strings.TrimSpace(unit.UnitKey) == "" || strings.TrimSpace(unit.Kind) == "" {
			return errors.New("producer: work unit requires admission key, unit key, and kind")
		}
	}
	if err := c.store.PersistDurableBatch(ctx, batch); err != nil {
		return fmt.Errorf("producer: persist outcomes and work units: %w", err)
	}
	return nil
}

// WorkUnit derives the stable identity of one emitted unit.
func (c *Coordinator) WorkUnit(admission Admission, kind string) (WorkUnit, error) {
	if strings.TrimSpace(admission.AdmissionKey) == "" {
		return WorkUnit{}, errors.New("producer: work unit requires a durable admission")
	}
	if strings.TrimSpace(kind) == "" {
		return WorkUnit{}, errors.New("producer: work unit kind is required")
	}
	return WorkUnit{
		AdmissionKey: admission.AdmissionKey,
		UnitKey:      hashFields("gonimbus-producer-unit-v1", admission.AdmissionKey, kind),
		Kind:         kind,
	}, nil
}

// MarkEOF records EOF for every lane only after clean enumerator completion.
func (c *Coordinator) MarkEOF(ctx context.Context) error {
	lanes := make([]partition.LaneRef, 0, c.authority.LaneCount())
	for ordinal := 1; ordinal <= c.authority.LaneCount(); ordinal++ {
		ref, err := c.authority.LaneRef(ordinal)
		if err != nil {
			return err
		}
		lanes = append(lanes, ref)
	}
	if err := c.store.MarkLaneEOF(ctx, lanes); err != nil {
		return fmt.Errorf("producer: persist lane EOF: %w", err)
	}
	return nil
}

func (o Outcome) valid() bool {
	switch o {
	case OutcomeEmitted, OutcomeFiltered, OutcomeRefused, OutcomeQuarantined, OutcomeFailed:
		return true
	default:
		return false
	}
}

func revisionFromSummary(obj provider.ObjectSummary) provider.SourceRevision {
	if strings.TrimSpace(obj.Revision) != "" {
		return provider.SourceRevision{Kind: provider.RevisionNative, Value: obj.Revision}
	}
	if strings.TrimSpace(obj.ETag) != "" {
		return provider.SourceRevision{Kind: provider.RevisionETag, Value: obj.ETag}
	}
	return provider.SourceRevision{}
}

func admissionKey(a Admission) string {
	return hashFields(
		"gonimbus-producer-admission-v1",
		strconv.Itoa(a.Lane.PlanVersion),
		a.Lane.PlanDigest,
		strconv.Itoa(a.Lane.Ordinal),
		a.SourceProvider,
		a.BaseIdentity,
		a.SourceKey,
		string(a.SourceRevision.Kind),
		a.SourceRevision.Value,
	)
}

func hashFields(fields ...string) string {
	h := sha256.New()
	for _, field := range fields {
		_, _ = h.Write([]byte(strconv.Itoa(len(field))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write([]byte(field))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sourceProvider(baseIdentity string) string {
	if i := strings.IndexByte(baseIdentity, ':'); i > 0 {
		return baseIdentity[:i]
	}
	return baseIdentity
}
