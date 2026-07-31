package producer

import (
	"context"
	"testing"

	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

type coordinatorStore struct {
	admissions []Admission
	batches    []DurableBatch
	eof        []partition.LaneRef
	refusals   []AdmissionRefusal
}

func (s *coordinatorStore) PersistAdmissions(_ context.Context, admissions []Admission, _ bool) ([]AdmissionRefusal, error) {
	s.admissions = append(s.admissions, admissions...)
	return s.refusals, nil
}
func (s *coordinatorStore) PersistDurableBatch(_ context.Context, batch DurableBatch) error {
	s.batches = append(s.batches, batch)
	return nil
}
func (s *coordinatorStore) MarkLaneEOF(_ context.Context, lanes []partition.LaneRef) error {
	s.eof = append(s.eof, lanes...)
	return nil
}
func (*coordinatorStore) AcknowledgeTerminals(context.Context, []TerminalAck) error { return nil }
func (*coordinatorStore) LaneStatus(context.Context, partition.LaneRef) (LaneStatus, error) {
	return LaneStatus{}, nil
}

func coordinatorAuthority(t *testing.T) (*partition.Authority, partition.LaneRef) {
	t.Helper()
	authority, err := partition.CompileAuthority(partition.PlanRequest{
		Prefixes:          []string{"a/"},
		Coverage:          partition.CoverageComplete,
		BaseIdentity:      "s3:source-bucket",
		ConfigFingerprint: "test-config",
		MaxLanes:          1,
	})
	require.NoError(t, err)
	ref, err := authority.LaneRef(1)
	require.NoError(t, err)
	return authority, ref
}

func TestCoordinatorPersistsRevisionBoundAdmissionBeforeReturningObject(t *testing.T) {
	authority, ref := coordinatorAuthority(t)
	store := &coordinatorStore{}
	coordinator, err := NewCoordinator(authority, store, false)
	require.NoError(t, err)

	admitted, err := coordinator.AdmitBatch(context.Background(), []LaneObject{{
		Lane: ref,
		Object: provider.ObjectSummary{
			Key: "a/object.xml", ETag: "etag-1", Size: 7,
		},
	}})
	require.NoError(t, err)
	require.Len(t, store.admissions, 1, "the store barrier precedes object handoff")
	require.Equal(t, store.admissions[0], admitted[0].Admission)
	require.Equal(t, provider.SourceRevision{Kind: provider.RevisionETag, Value: "etag-1"}, admitted[0].Admission.SourceRevision)
	require.NotEmpty(t, admitted[0].Admission.AdmissionKey)
}

func TestCoordinatorRevisionAndLaneAreLoadBearingAdmissionIdentity(t *testing.T) {
	authority, ref := coordinatorAuthority(t)
	store := &coordinatorStore{}
	coordinator, err := NewCoordinator(authority, store, false)
	require.NoError(t, err)

	admit := func(ref partition.LaneRef, etag string) string {
		got, err := coordinator.AdmitBatch(context.Background(), []LaneObject{{
			Lane: ref, Object: provider.ObjectSummary{Key: "a/object.xml", ETag: etag},
		}})
		require.NoError(t, err)
		return got[0].Admission.AdmissionKey
	}
	first := admit(ref, "etag-1")
	require.NotEqual(t, first, admit(ref, "etag-2"))

	foreignAuthority, foreignRef := coordinatorAuthorityWithFingerprint(t, "other-config")
	_ = foreignAuthority
	_, err = coordinator.AdmitBatch(context.Background(), []LaneObject{{
		Lane: foreignRef, Object: provider.ObjectSummary{Key: "a/object.xml", ETag: "etag-1"},
	}})
	require.ErrorContains(t, err, "unauthorized")
	require.Len(t, store.admissions, 2, "an unauthorized claim cannot write an admission")
}

func TestCoordinatorReturnsRecoverableAdmissionRefusalOnItsObject(t *testing.T) {
	authority, ref := coordinatorAuthority(t)
	store := &coordinatorStore{}
	coordinator, err := NewCoordinator(authority, store, true)
	require.NoError(t, err)

	item := LaneObject{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/object.xml", ETag: "etag-2"},
	}
	record := Admission{
		Lane:           ref,
		SourceProvider: "s3",
		BaseIdentity:   authority.BaseIdentity(),
		SourceKey:      item.Object.Key,
		SourceRevision: provider.SourceRevision{Kind: provider.RevisionETag, Value: "etag-2"},
	}
	record.AdmissionKey = admissionKey(record)
	store.refusals = []AdmissionRefusal{{
		AdmissionKey: record.AdmissionKey,
		Err:          provider.ErrSourceChanged,
	}}

	admitted, err := coordinator.AdmitBatch(context.Background(), []LaneObject{item})
	require.NoError(t, err)
	require.ErrorIs(t, admitted[0].Refusal, provider.ErrSourceChanged)
}

func coordinatorAuthorityWithFingerprint(t *testing.T, fingerprint string) (*partition.Authority, partition.LaneRef) {
	t.Helper()
	authority, err := partition.CompileAuthority(partition.PlanRequest{
		Prefixes:          []string{"a/"},
		Coverage:          partition.CoverageComplete,
		BaseIdentity:      "s3:source-bucket",
		ConfigFingerprint: fingerprint,
		MaxLanes:          1,
	})
	require.NoError(t, err)
	ref, err := authority.LaneRef(1)
	require.NoError(t, err)
	return authority, ref
}

func TestCoordinatorPersistsOutcomeAndUnitBeforeEOF(t *testing.T) {
	authority, ref := coordinatorAuthority(t)
	store := &coordinatorStore{}
	coordinator, err := NewCoordinator(authority, store, false)
	require.NoError(t, err)
	admitted, err := coordinator.AdmitBatch(context.Background(), []LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/object.xml", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	unit, err := coordinator.WorkUnit(admitted[0].Admission, "reflow")
	require.NoError(t, err)
	require.NoError(t, coordinator.PersistDurableBatch(context.Background(), DurableBatch{
		Outcomes:  []ProbeOutcome{{AdmissionKey: admitted[0].Admission.AdmissionKey, Outcome: OutcomeEmitted}},
		WorkUnits: []WorkUnit{unit},
	}))
	require.Empty(t, store.eof)
	require.NoError(t, coordinator.MarkEOF(context.Background()))
	require.Equal(t, []partition.LaneRef{ref}, store.eof)
	require.Len(t, store.batches, 1)
	require.Equal(t, unit, store.batches[0].WorkUnits[0])
}
