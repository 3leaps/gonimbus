package reflow

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/uri"
)

const (
	// RecordType is the JSONL type for per-object transfer reflow records.
	RecordType = "gonimbus.reflow.v1"
	// RunRecordType is the JSONL type for transfer reflow run configuration.
	RunRecordType = "gonimbus.reflow.run.v1"
	// SourceRecordType is the JSONL type for transfer reflow source metadata.
	SourceRecordType = "gonimbus.reflow.source.v1"
	// SummaryRecordType is the JSONL type for transfer reflow summaries.
	SummaryRecordType = "gonimbus.reflow.summary.v1"
	// CheckpointWriterStatsRecordType is the JSONL type for sterile checkpoint
	// writer diagnostics (queue/barrier/batch/duration aggregates). Emitted by
	// the CLI after a run when the checkpoint store exposes WriterStats.
	// Measure-first only: not product throughput marketing.
	CheckpointWriterStatsRecordType = "gonimbus.reflow.checkpoint_writer_stats.v1"
	// WarningRecordType is the JSONL type for transfer reflow warnings.
	WarningRecordType = "gonimbus.warning.v1"

	SourceBucketFile = "local"
)

// Record is the payload for gonimbus.reflow.v1 JSONL records.
type Record struct {
	SourceURI    string         `json:"source_uri"`
	SourceBucket string         `json:"source_bucket,omitempty"`
	SourceRoot   string         `json:"source_root,omitempty"`
	SourceKey    string         `json:"source_key"`
	SourceETag   string         `json:"source_etag,omitempty"`
	SourceSize   int64          `json:"source_size_bytes,omitempty"`
	DestURI      string         `json:"dest_uri"`
	DestKey      string         `json:"dest_key"`
	Bytes        int64          `json:"bytes,omitempty"`
	Status       string         `json:"status"`
	Reason       string         `json:"reason,omitempty"`
	RoutingClass string         `json:"routing_class,omitempty"`
	Collision    *CollisionInfo `json:"collision,omitempty"`
	Provenance   *ProvenanceRef `json:"provenance,omitempty"`
	Details      map[string]any `json:"details,omitempty"`
}

func (r Record) MarshalJSON() ([]byte, error) {
	type alias Record
	out := alias(r)
	if out.SourceBucket == "" {
		switch {
		case strings.HasPrefix(out.SourceURI, "file://local/"):
			out.SourceBucket = SourceBucketFile
		default:
			if parsed, err := uri.ParseURI(out.SourceURI); err == nil {
				out.SourceBucket = parsed.Bucket
			}
		}
	}
	return json.Marshal(out)
}

// Execution-path values for the dispatch-transparency contract: every run and
// summary record names the path that executed it, so path selection between the
// library engine and the CLI worker pool is observable evidence rather than an
// implementation detail. Requested concurrency stays in `parallel`; the resolved
// ceiling and observed max-active live in the embedded ConcurrencyStats.
const (
	// ExecutionPathEngine marks runs executed by the pkg/reflow engine.
	ExecutionPathEngine = "engine"
	// ExecutionPathCLIPool marks runs executed by the CLI worker pool.
	ExecutionPathCLIPool = "cli-pool"
)

// RunRecord is the payload for gonimbus.reflow.run.v1 JSONL records.
type RunRecord struct {
	DestURI        string `json:"dest_uri"`
	CheckpointPath string `json:"checkpoint_path"`
	DryRun         bool   `json:"dry_run"`
	Resume         bool   `json:"resume"`
	Parallel       int    `json:"parallel"`
	ExecutionPath  string `json:"execution_path"`
	ConcurrencyStats
	Provenance *ProvenanceRunConfig `json:"provenance,omitempty"`
	Metadata   *MetadataRunConfig   `json:"metadata,omitempty"`
}

// SummaryRecord is the payload for gonimbus.reflow.summary.v1 JSONL records.
type SummaryRecord struct {
	DestURI       string `json:"dest_uri"`
	DryRun        bool   `json:"dry_run"`
	OnCollision   string `json:"on_collision"`
	ExecutionPath string `json:"execution_path"`
	ConcurrencyStats
	DestIfAbsentHonored     *bool            `json:"dest_ifabsent_honored"`
	DestIfAbsentProbeStatus string           `json:"dest_ifabsent_probe_status,omitempty"`
	FallbackActive          bool             `json:"fallback_active"`
	IfAbsentFallbackObjects int64            `json:"ifabsent_fallback_objects"`
	Statuses                map[string]int64 `json:"statuses,omitempty"`
	Collisions              map[string]int64 `json:"collisions,omitempty"`
	InvalidInputs           int64            `json:"invalid_inputs,omitempty"`
	Errors                  int64            `json:"errors,omitempty"`
}

// CheckpointWriterStatsRecord is the payload for
// gonimbus.reflow.checkpoint_writer_stats.v1. It mirrors pkg/reflowstate
// WriterStats aggregates without importing that package (dependency boundary).
// Fields are counts and durations only — no paths, SQL, keys, URIs, or auth.
//
// BatchDuration* is wall time of the full batch write path (BeginTx through
// per-request SQL/savepoints and tx.Commit), not storage COMMIT alone.
// Barrier* outcomes are waiter-side observations, not a durable commit ledger.
// Queue depth samples are approximate.
//
// Experimental: may change with an in-release note.
type CheckpointWriterStatsRecord struct {
	MaxBatch int `json:"max_batch"`

	QueueDepthSamples int64 `json:"queue_depth_samples"`
	QueueDepthSum     int64 `json:"queue_depth_sum"`
	QueueDepthPeak    int64 `json:"queue_depth_peak"`

	Admissions            int64 `json:"admissions"`
	AdmissionWaitNanos    int64 `json:"admission_wait_nanos"`
	AdmissionWaitMaxNanos int64 `json:"admission_wait_max_nanos"`
	AdmissionBlocked      int64 `json:"admission_blocked"`

	Barriers            int64 `json:"barriers"`
	BarrierWaitNanos    int64 `json:"barrier_wait_nanos"`
	BarrierWaitMaxNanos int64 `json:"barrier_wait_max_nanos"`
	BarrierOK           int64 `json:"barrier_ok"`
	BarrierRefusal      int64 `json:"barrier_refusal"`
	BarrierWriterFailed int64 `json:"barrier_writer_failed"`
	BarrierWriterClosed int64 `json:"barrier_writer_closed"`
	BarrierCanceled     int64 `json:"barrier_canceled"`

	Batches          int64 `json:"batches"`
	BatchSizeSum     int64 `json:"batch_size_sum"`
	BatchSizeMax     int64 `json:"batch_size_max"`
	BatchSize1       int64 `json:"batch_size_1"`
	BatchSize2To8    int64 `json:"batch_size_2_to_8"`
	BatchSize9To32   int64 `json:"batch_size_9_to_32"`
	BatchSize33To128 int64 `json:"batch_size_33_to_128"`
	BatchSize129Plus int64 `json:"batch_size_129_plus"`

	Commits               int64 `json:"commits"`
	BatchDurationNanos    int64 `json:"batch_duration_nanos"`
	BatchDurationMaxNanos int64 `json:"batch_duration_max_nanos"`
	CommitFatals          int64 `json:"commit_fatals"`
	RequestRefusals       int64 `json:"request_refusals"`

	// Experimental savepoint elision counters (Phase A measure).
	SavepointsCreated int64 `json:"savepoints_created,omitempty"`
	SavepointsElided  int64 `json:"savepoints_elided,omitempty"`
}

// SourceRunRecord is the payload for gonimbus.reflow.source.v1 JSONL records.
type SourceRunRecord struct {
	Provider   string `json:"provider"`
	Bucket     string `json:"source_bucket,omitempty"`
	Root       string `json:"source_root,omitempty"`
	URI        string `json:"source_uri"`
	OutputOnly bool   `json:"source_uri_output_only,omitempty"`
}

// Warning is the payload for gonimbus.warning.v1 records emitted by reflow.
type Warning struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Key     string         `json:"key,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

// CollisionInfo describes the destination collision decision for a reflow item.
type CollisionInfo struct {
	Kind                     string     `json:"kind"`
	DestETagObserved         string     `json:"dest_etag_observed,omitempty"`
	DestSizeObserved         *int64     `json:"dest_size_observed,omitempty"`
	SrcLastModified          *time.Time `json:"src_last_modified,omitempty"`
	DestLastModifiedObserved *time.Time `json:"dest_last_modified_observed,omitempty"`
	DecisionReason           string     `json:"decision_reason,omitempty"`
	DecisionPath             string     `json:"decision_path"`
}

// ProvenanceRef identifies a provenance sidecar emitted for a reflow item.
type ProvenanceRef struct {
	Written bool   `json:"written"`
	Key     string `json:"key"`
	URI     string `json:"uri,omitempty"`
}

// MetadataRunConfig describes destination metadata behavior for a reflow run.
type MetadataRunConfig struct {
	Policy                  string            `json:"policy"`
	SetKeys                 []string          `json:"set_keys,omitempty"`
	SourceKeyRuleKeys       []string          `json:"source_key_rule_keys,omitempty"`
	DerivedRuleKeys         []string          `json:"derived_rule_keys,omitempty"`
	OnMissingSource         string            `json:"on_missing_source,omitempty"`
	PreserveContentType     bool              `json:"preserve_content_type,omitempty"`
	DestinationStorageClass string            `json:"destination_storage_class,omitempty"`
	MetadataSidecarSuffix   string            `json:"metadata_sidecar_suffix,omitempty"`
	Set                     map[string]string `json:"set,omitempty"`
}

// ProvenanceRunConfig describes provenance behavior for a reflow run.
type ProvenanceRunConfig struct {
	Mode         string                     `json:"mode"`
	Suffix       string                     `json:"suffix,omitempty"`
	OnWriteError string                     `json:"on_write_error,omitempty"`
	Placement    ProvenancePlacementContext `json:"placement"`
}

// ProvenancePlacementContext describes where provenance sidecars are written.
type ProvenancePlacementContext struct {
	Mode        string `json:"mode"`
	SidecarRoot string `json:"sidecar_root,omitempty"`
}
