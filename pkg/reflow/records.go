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

// RunRecord is the payload for gonimbus.reflow.run.v1 JSONL records.
type RunRecord struct {
	DestURI        string `json:"dest_uri"`
	CheckpointPath string `json:"checkpoint_path"`
	DryRun         bool   `json:"dry_run"`
	Resume         bool   `json:"resume"`
	Parallel       int    `json:"parallel"`
	ConcurrencyStats
	Provenance *ProvenanceRunConfig `json:"provenance,omitempty"`
	Metadata   *MetadataRunConfig   `json:"metadata,omitempty"`
}

// SummaryRecord is the payload for gonimbus.reflow.summary.v1 JSONL records.
type SummaryRecord struct {
	DestURI     string `json:"dest_uri"`
	DryRun      bool   `json:"dry_run"`
	OnCollision string `json:"on_collision"`
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
