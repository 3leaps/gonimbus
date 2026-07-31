package reflowstate

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/3leaps/gonimbus/pkg/partition"
	"github.com/3leaps/gonimbus/pkg/producer"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func durableProducerFixture(t *testing.T) (*Store, *producer.Coordinator, partition.LaneRef) {
	t.Helper()
	return durableProducerFixtureAt(t, filepath.Join(t.TempDir(), "state.db"))
}

func durableProducerFixtureAt(t *testing.T, path string) (*Store, *producer.Coordinator, partition.LaneRef) {
	t.Helper()
	store, err := Open(context.Background(), Config{Path: path})
	require.NoError(t, err)
	authority, err := partition.CompileAuthority(partition.PlanRequest{
		Prefixes:          []string{"a/"},
		Coverage:          partition.CoverageComplete,
		BaseIdentity:      "s3:source-bucket",
		ConfigFingerprint: "state-test",
		MaxLanes:          1,
	})
	require.NoError(t, err)
	ref, err := authority.LaneRef(1)
	require.NoError(t, err)
	coordinator, err := producer.NewCoordinator(authority, store, false)
	require.NoError(t, err)
	return store, coordinator, ref
}

func TestLaneTerminalityRequiresKeyedOutcomeAfterEOF(t *testing.T) {
	ctx := context.Background()
	store, coordinator, ref := durableProducerFixture(t)
	defer func() { require.NoError(t, store.Close()) }()

	admitted, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/filtered.txt", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	require.NoError(t, coordinator.MarkEOF(ctx))

	status, err := store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.True(t, status.EOF)
	require.Equal(t, int64(1), status.MissingOutcomes)
	require.False(t, status.Terminal, "EOF cannot hide an admitted non-emitting object")

	require.NoError(t, coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		Outcomes: []producer.ProbeOutcome{{
			AdmissionKey: admitted[0].Admission.AdmissionKey,
			Outcome:      producer.OutcomeFiltered,
		}},
	}))
	status, err = store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.True(t, status.Terminal)
	require.Equal(t, int64(1), status.Outcomes)
	require.Zero(t, status.WorkUnits)
}

func TestAdmissionRefusesChangedRevisionForDurableSourceSlot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, coordinator, ref := durableProducerFixtureAt(t, path)

	_, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	_, err = coordinator.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-2"},
	}})
	require.NoError(t, err, "a fresh run supersedes the current durable revision")

	authority, err := partition.CompileAuthority(partition.PlanRequest{
		Prefixes:          []string{"a/"},
		Coverage:          partition.CoverageComplete,
		BaseIdentity:      "s3:source-bucket",
		ConfigFingerprint: "state-test",
		MaxLanes:          1,
	})
	require.NoError(t, err)
	replay, err := producer.NewCoordinator(authority, store, true)
	require.NoError(t, err)
	refused, err := replay.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-3"},
	}})
	require.NoError(t, err, "source drift is a recoverable object refusal")
	require.ErrorIs(t, refused[0].Refusal, provider.ErrSourceChanged)

	oldRevision, err := replay.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	require.ErrorIs(t, oldRevision[0].Refusal, provider.ErrSourceChanged,
		"a superseded revision must not remain an exact replay admission")

	exact, err := replay.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-2"},
	}})
	require.NoError(t, err)
	require.NoError(t, exact[0].Refusal)

	status, err := store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, int64(1), status.Admissions,
		"one source slot must retain only its current revision")
	require.NoError(t, store.Close(), "a logical refusal must not poison the writer")
}

func TestDuplicateTerminalCannotMaskMissingUnit(t *testing.T) {
	ctx := context.Background()
	store, coordinator, ref := durableProducerFixture(t)
	defer func() { require.NoError(t, store.Close()) }()

	admitted, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{
		{Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-1"}},
		{Lane: ref, Object: provider.ObjectSummary{Key: "a/two.xml", ETag: "etag-2"}},
	})
	require.NoError(t, err)
	unitOne, err := coordinator.WorkUnit(admitted[0].Admission, "reflow")
	require.NoError(t, err)
	unitTwo, err := coordinator.WorkUnit(admitted[1].Admission, "reflow")
	require.NoError(t, err)
	require.NoError(t, coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		Outcomes: []producer.ProbeOutcome{
			{AdmissionKey: admitted[0].Admission.AdmissionKey, Outcome: producer.OutcomeEmitted},
			{AdmissionKey: admitted[1].Admission.AdmissionKey, Outcome: producer.OutcomeEmitted},
		},
		WorkUnits: []producer.WorkUnit{unitOne, unitTwo},
	}))
	require.NoError(t, coordinator.MarkEOF(ctx))
	ack := producer.TerminalAck{UnitKey: unitOne.UnitKey, Status: "complete"}
	require.NoError(t, store.AcknowledgeTerminals(ctx, []producer.TerminalAck{ack, ack}))

	status, err := store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, int64(2), status.WorkUnits)
	require.Equal(t, int64(1), status.DurableTerminals)
	require.Equal(t, int64(1), status.MissingTerminals)
	require.False(t, status.Terminal, "two acknowledgements of one key cannot answer for another key")
}

func TestTerminalAcknowledgementRefusesUnknownUnit(t *testing.T) {
	ctx := context.Background()
	store, _, _ := durableProducerFixture(t)

	err := store.AcknowledgeTerminals(ctx, []producer.TerminalAck{{
		UnitKey: "unknown-unit", Status: "complete",
	}})
	require.ErrorContains(t, err, "is not durable")
	require.NotErrorIs(t, err, ErrWriterFailed)
	require.NoError(t, store.Close(), "a logical refusal must not poison the writer")
}

func TestWorkUnitRefusesNonEmittingOutcome(t *testing.T) {
	ctx := context.Background()
	store, coordinator, ref := durableProducerFixture(t)

	admitted, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/filtered.txt", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	unit, err := coordinator.WorkUnit(admitted[0].Admission, "reflow")
	require.NoError(t, err)
	require.NoError(t, coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		Outcomes: []producer.ProbeOutcome{{
			AdmissionKey: admitted[0].Admission.AdmissionKey,
			Outcome:      producer.OutcomeFiltered,
		}},
	}))

	err = coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		WorkUnits: []producer.WorkUnit{unit},
	})
	require.ErrorContains(t, err, "belongs to non-emitting outcome")
	require.NotErrorIs(t, err, ErrWriterFailed)
	require.NoError(t, store.Close(), "a logical refusal must not poison the writer")
}

func TestCheckpointReconciliationAcknowledgesWithoutSecondMutation(t *testing.T) {
	ctx := context.Background()
	store, coordinator, ref := durableProducerFixture(t)
	defer func() { require.NoError(t, store.Close()) }()

	admitted, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	unit, err := coordinator.WorkUnit(admitted[0].Admission, "reflow")
	require.NoError(t, err)
	require.NoError(t, coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		Outcomes: []producer.ProbeOutcome{{
			AdmissionKey: admitted[0].Admission.AdmissionKey,
			Outcome:      producer.OutcomeEmitted,
		}},
		WorkUnits: []producer.WorkUnit{unit},
	}))
	require.NoError(t, coordinator.MarkEOF(ctx))

	// Models a crash after the downstream checkpoint commit but before the
	// coordinator acknowledgement.
	require.NoError(t, store.UpsertItem(ctx, UpsertItemParams{
		UnitKey: unit.UnitKey, SourceURI: "s3://source-bucket/a/one.xml",
		DestURI: "s3://dest-bucket/a/one.xml", Status: "complete",
	}))
	done, status, err := store.UnitDone(ctx, unit.UnitKey)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "complete", status)

	before, err := store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.False(t, before.Terminal)
	require.Equal(t, int64(1), before.MissingTerminals)

	require.NoError(t, store.AcknowledgeTerminals(ctx, []producer.TerminalAck{{
		UnitKey: unit.UnitKey, Status: status,
	}}))
	after, err := store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.True(t, after.Terminal)
	require.Zero(t, after.MissingTerminals)
}

func TestCheckpointUnitAtomicallyMakesLaneTerminal(t *testing.T) {
	ctx := context.Background()
	store, coordinator, ref := durableProducerFixture(t)
	defer func() { require.NoError(t, store.Close()) }()

	admitted, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{{
		Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-1"},
	}})
	require.NoError(t, err)
	unit, err := coordinator.WorkUnit(admitted[0].Admission, "reflow")
	require.NoError(t, err)
	require.NoError(t, coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		Outcomes: []producer.ProbeOutcome{{
			AdmissionKey: admitted[0].Admission.AdmissionKey,
			Outcome:      producer.OutcomeEmitted,
		}},
		WorkUnits: []producer.WorkUnit{unit},
	}))
	require.NoError(t, coordinator.MarkEOF(ctx))

	require.NoError(t, store.CheckpointUnit(ctx, UpsertItemParams{
		SourceURI: "s3://source-bucket/a/one.xml",
		DestURI:   "s3://dest-bucket/a/one.xml",
		Status:    "complete",
	}, unit.UnitKey))

	done, status, err := store.UnitDone(ctx, unit.UnitKey)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "complete", status)
	laneStatus, err := store.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.True(t, laneStatus.Terminal,
		"the item checkpoint and keyed acknowledgement must become visible together")
}

func TestCheckpointUnitRefusesToReplaceDurableUnitIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store, coordinator, ref := durableProducerFixtureAt(t, path)

	admitted, err := coordinator.AdmitBatch(ctx, []producer.LaneObject{
		{Lane: ref, Object: provider.ObjectSummary{Key: "a/one.xml", ETag: "etag-1"}},
		{Lane: ref, Object: provider.ObjectSummary{Key: "a/two.xml", ETag: "etag-2"}},
	})
	require.NoError(t, err)
	first, err := coordinator.WorkUnit(admitted[0].Admission, "reflow")
	require.NoError(t, err)
	second, err := coordinator.WorkUnit(admitted[1].Admission, "reflow")
	require.NoError(t, err)
	require.NoError(t, coordinator.PersistDurableBatch(ctx, producer.DurableBatch{
		Outcomes: []producer.ProbeOutcome{
			{AdmissionKey: admitted[0].Admission.AdmissionKey, Outcome: producer.OutcomeEmitted},
			{AdmissionKey: admitted[1].Admission.AdmissionKey, Outcome: producer.OutcomeEmitted},
		},
		WorkUnits: []producer.WorkUnit{first, second},
	}))
	require.NoError(t, coordinator.MarkEOF(ctx))

	item := UpsertItemParams{
		SourceURI: "s3://source-bucket/a/one.xml",
		DestURI:   "s3://dest-bucket/a/one.xml",
		Status:    "complete",
	}
	require.NoError(t, store.CheckpointUnit(ctx, item, first.UnitKey))
	err = store.CheckpointUnit(ctx, item, second.UnitKey)
	require.ErrorContains(t, err, "already belongs to unit")
	require.NotErrorIs(t, err, ErrWriterFailed)
	require.NoError(t, store.Close(), "a logical refusal must not poison the writer")

	verify, err := Open(ctx, Config{Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, verify.Close()) }()
	done, _, err := verify.UnitDone(ctx, second.UnitKey)
	require.NoError(t, err)
	require.False(t, done, "the conflicting transaction must not acknowledge the second unit")
	status, err := verify.LaneStatus(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, int64(1), status.MissingTerminals)
	require.False(t, status.Terminal)
}
