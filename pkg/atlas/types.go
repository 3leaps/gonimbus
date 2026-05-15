package atlas

import "time"

const (
	SchemaVersion = "gonimbus.atlas.v1"

	HashProfileSHA256 = "sha256"

	DimensionTemporalDay     = "temporal-day"
	DimensionTemporalInstant = "temporal-instant"
	DimensionCategorical     = "categorical"

	ClassificationUnknown      = "unknown"
	ClassificationPublic       = "0-public"
	ClassificationConfidential = "1-confidential"
	ClassificationBlinded      = "2-blinded"
	ClassificationProprietary  = "3-proprietary"
	ClassificationPersonal     = "4-personal"
	ClassificationPrivileged   = "5-privileged"
	ClassificationEyesOnly     = "6-eyes-only"

	CoverageFull   = "full"
	CoverageScoped = "scoped"
)

type DimensionDeclaration struct {
	Name           string `json:"name"`
	Kind           string `json:"kind"`
	Classification string `json:"classification"`
}

type SystemFieldDeclaration struct {
	Name           string `json:"name"`
	Classification string `json:"classification"`
}

type Header struct {
	SchemaVersion    string                   `json:"schema_version"`
	AtlasID          string                   `json:"atlas_id"`
	CreatedAt        time.Time                `json:"created_at"`
	SourceIndexSetID string                   `json:"source_index_set_id"`
	SourceRunID      string                   `json:"source_run_id"`
	BaseURI          string                   `json:"base_uri"`
	ScopeDigest      string                   `json:"scope_digest"`
	RecipeDigest     string                   `json:"recipe_digest"`
	HashProfile      string                   `json:"hash_profile"`
	Coverage         string                   `json:"coverage"`
	ShardBy          []string                 `json:"shard_by"`
	Dimensions       []DimensionDeclaration   `json:"dimensions"`
	SystemFields     []SystemFieldDeclaration `json:"system_fields"`
	Counts           Counts                   `json:"counts"`
}

type Counts struct {
	ObjectsScanned       int64 `json:"objects_scanned"`
	RowsWritten          int64 `json:"rows_written"`
	Diagnostics          int64 `json:"diagnostics"`
	ReadFailures         int64 `json:"read_failures"`
	ExtractionFailures   int64 `json:"extraction_failures"`
	ValidationFailures   int64 `json:"validation_failures"`
	ArtifactWriteFailure int64 `json:"artifact_write_failures,omitempty"`
}

type ObjectRow struct {
	SchemaVersion    string            `json:"schema_version"`
	SourceIndexSetID string            `json:"source_index_set_id"`
	SourceRunID      string            `json:"source_run_id"`
	StorageKey       string            `json:"storage_key"`
	RelKey           string            `json:"rel_key"`
	SourceURI        string            `json:"source_uri"`
	ContentHash      string            `json:"content_hash"`
	HashProfile      string            `json:"hash_profile"`
	Dimensions       map[string]string `json:"dimensions"`
	Shard            map[string]string `json:"shard"`
	SizeBytes        int64             `json:"size_bytes"`
	ETag             string            `json:"etag,omitempty"`
	FirstSeenRunID   string            `json:"first_seen_run_id"`
	FirstSeenAt      time.Time         `json:"first_seen_at"`
}

type DiagnosticRow struct {
	SchemaVersion    string    `json:"schema_version"`
	SourceIndexSetID string    `json:"source_index_set_id"`
	SourceRunID      string    `json:"source_run_id"`
	StorageKey       string    `json:"storage_key"`
	RelKey           string    `json:"rel_key"`
	Stage            string    `json:"stage"`
	Code             string    `json:"code"`
	Message          string    `json:"message"`
	OccurredAt       time.Time `json:"occurred_at"`
}

type SourceObject struct {
	RelKey        string
	SizeBytes     int64
	ETag          string
	LastSeenRunID string
	LastSeenAt    time.Time
}

type SourceRun struct {
	IndexSetID  string
	RunID       string
	BaseURI     string
	ScopeDigest string
	Coverage    string
	Objects     []SourceObject
}

func DefaultSystemFields() []SystemFieldDeclaration {
	return []SystemFieldDeclaration{
		{Name: "content_hash", Classification: ClassificationProprietary},
		{Name: "storage_key", Classification: ClassificationConfidential},
		{Name: "source_uri", Classification: ClassificationConfidential},
		{Name: "source_index_set_id", Classification: ClassificationConfidential},
		{Name: "source_run_id", Classification: ClassificationConfidential},
		{Name: "first_seen_at", Classification: ClassificationPublic},
		{Name: "run_timestamp", Classification: ClassificationPublic},
		{Name: "coverage", Classification: ClassificationPublic},
		{Name: "scope_digest", Classification: ClassificationPublic},
		{Name: "recipe_digest", Classification: ClassificationPublic},
		{Name: "hash_profile", Classification: ClassificationPublic},
	}
}
