package reflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// ReflowInputRecordType is the JSONL type for a preselected reflow-input record,
// the line format a RecordStreamSource carries.
const ReflowInputRecordType = "gonimbus.reflow.input.v1"

// ErrCodeInvalidInput is the error code for a reflow-input record the engine
// cannot accept. It mirrors the command path's INVALID_INPUT wire value.
const ErrCodeInvalidInput = "INVALID_INPUT"

// reflowInput is the parsed, engine-internal form of a reflow-input line.
//
// A RecordStreamSource carries object-store gonimbus.reflow.input.v1 records (the
// `crawl --emit reflow-input` form). Local filesystem sources are modeled by
// FileTreeSource, so a file:// record is out of this contract and surfaces as
// invalid input rather than being planned here.
type reflowInput struct {
	SourceProvider   string
	SourceBucket     string
	SourceURI        string
	SourceKey        string
	SourceETag       string
	SourceSize       int64
	SourceLastMod    time.Time
	Vars             map[string]string
	DestRelKey       string
	RoutingClass     string
	QuarantinePrefix string
}

// parseReflowInputLine parses a single reflow-input JSONL line into the engine's
// internal form. It accepts the migrated S3 reflow-input record plus existing
// exact-object stdin forms the command path already supports (S3 index-object
// records and exact S3 URI lines). Anything else returns an error; the executor
// reports it as a per-record INVALID_INPUT event (CLI-equivalent) and continues
// streaming.
func parseReflowInputLine(line string) (reflowInput, error) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return parseRawS3ObjectLine(line)
	}
	var env struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		return reflowInput{}, err
	}
	switch env.Type {
	case ReflowInputRecordType:
		return parseReflowInputData(env.Data)
	case "gonimbus.index.object.v1":
		return parseIndexObjectInputData(env.Data)
	default:
		return reflowInput{}, fmt.Errorf("unsupported record type %q; RecordStreamSource carries %s records", env.Type, ReflowInputRecordType)
	}
}

func parseReflowInputData(raw json.RawMessage) (reflowInput, error) {
	var data struct {
		SourceURI        string            `json:"source_uri"`
		SourceKey        string            `json:"source_key"`
		SourceETag       string            `json:"source_etag"`
		SourceSize       int64             `json:"source_size_bytes"`
		SourceLastMod    time.Time         `json:"source_last_modified"`
		Vars             map[string]string `json:"vars"`
		DestRelKey       string            `json:"dest_rel_key"`
		RoutingClass     string            `json:"routing_class"`
		QuarantinePrefix string            `json:"quarantine_prefix"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return reflowInput{}, err
	}
	if strings.TrimSpace(data.SourceURI) == "" {
		return reflowInput{}, fmt.Errorf("missing data.source_uri")
	}
	u, err := uri.ParseURI(data.SourceURI)
	if err != nil {
		return reflowInput{}, err
	}
	if u.Provider != string(provider.ProviderS3) {
		return reflowInput{}, fmt.Errorf("RecordStreamSource source_uri must be an s3:// object URI; %q sources use FileTreeSource or another Source form", u.Provider)
	}
	if u.IsPrefix() || u.IsPattern() {
		return reflowInput{}, fmt.Errorf("reflow input source_uri must be an exact object URI")
	}
	routingClass := strings.TrimSpace(data.RoutingClass)
	if routingClass == "" {
		routingClass = "normal"
	}
	quarantinePrefix := strings.Trim(strings.TrimSpace(data.QuarantinePrefix), "/")
	switch routingClass {
	case "normal":
	case "quarantine":
		if quarantinePrefix == "" {
			return reflowInput{}, fmt.Errorf("quarantine_prefix is required when routing_class=quarantine")
		}
		if !IsRelativeQuarantinePrefix(data.QuarantinePrefix) {
			return reflowInput{}, fmt.Errorf("quarantine_prefix must be a relative destination prefix")
		}
	default:
		return reflowInput{}, fmt.Errorf("unsupported routing_class %q", data.RoutingClass)
	}
	key := u.Key
	if sk := strings.TrimPrefix(strings.TrimSpace(data.SourceKey), "/"); sk != "" {
		key = sk
	}
	return reflowInput{
		SourceProvider:   u.Provider,
		SourceBucket:     u.Bucket,
		SourceURI:        fmt.Sprintf("%s://%s/%s", u.Provider, u.Bucket, key),
		SourceKey:        key,
		SourceETag:       data.SourceETag,
		SourceSize:       data.SourceSize,
		SourceLastMod:    data.SourceLastMod,
		Vars:             data.Vars,
		DestRelKey:       strings.Trim(strings.TrimSpace(data.DestRelKey), "/"),
		RoutingClass:     routingClass,
		QuarantinePrefix: quarantinePrefix,
	}, nil
}

func parseIndexObjectInputData(raw json.RawMessage) (reflowInput, error) {
	var data struct {
		BaseURI      string    `json:"base_uri"`
		Key          string    `json:"key"`
		ETag         string    `json:"etag"`
		SizeBytes    int64     `json:"size_bytes"`
		LastModified time.Time `json:"last_modified"`
		RelKey       string    `json:"rel_key"`
		DeletedAt    *string   `json:"deleted_at"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return reflowInput{}, err
	}
	if data.DeletedAt != nil {
		return reflowInput{}, fmt.Errorf("deleted objects are not supported in reflow input")
	}
	base, err := uri.ParseURI(data.BaseURI)
	if err != nil {
		return reflowInput{}, fmt.Errorf("invalid base_uri: %w", err)
	}
	if base.Provider != string(provider.ProviderS3) {
		return reflowInput{}, fmt.Errorf("RecordStreamSource base_uri must be an s3:// URI; %q sources use FileTreeSource or another Source form", base.Provider)
	}
	key := strings.TrimPrefix(strings.TrimSpace(data.Key), "/")
	if key == "" {
		key = strings.TrimPrefix(strings.TrimSpace(data.RelKey), "/")
	}
	if key == "" {
		return reflowInput{}, fmt.Errorf("missing key in index record")
	}
	return reflowInput{
		SourceProvider: string(provider.ProviderS3),
		SourceBucket:   base.Bucket,
		SourceURI:      fmt.Sprintf("s3://%s/%s", base.Bucket, key),
		SourceKey:      key,
		SourceETag:     data.ETag,
		SourceSize:     data.SizeBytes,
		SourceLastMod:  data.LastModified,
		RoutingClass:   "normal",
	}, nil
}

func parseRawS3ObjectLine(line string) (reflowInput, error) {
	parsed, err := uri.ParseURI(line)
	if err != nil {
		return reflowInput{}, err
	}
	if parsed.Provider != string(provider.ProviderS3) {
		return reflowInput{}, fmt.Errorf("unsupported provider %q", parsed.Provider)
	}
	if parsed.IsPrefix() || parsed.IsPattern() {
		return reflowInput{}, fmt.Errorf("RecordStreamSource raw URI input must be an exact object URI")
	}
	key := strings.TrimPrefix(strings.TrimSpace(parsed.Key), "/")
	if key == "" {
		return reflowInput{}, fmt.Errorf("missing source key")
	}
	return reflowInput{
		SourceProvider: string(provider.ProviderS3),
		SourceBucket:   parsed.Bucket,
		SourceURI:      fmt.Sprintf("s3://%s/%s", parsed.Bucket, key),
		SourceKey:      key,
		RoutingClass:   "normal",
	}, nil
}

func (in reflowInput) sourceIdentity() string {
	if in.SourceProvider == "" || in.SourceBucket == "" {
		return ""
	}
	return in.SourceProvider + ":" + in.SourceBucket
}

// record builds the per-object reflow Record for this input at the given
// destination and status. The source URI is sanitized (presigned-URL query
// material removed) before it crosses the event boundary.
func (in reflowInput) record(destURI, destKey, status string) Record {
	return Record{
		SourceURI:    sanitizeSourceURI(in.SourceURI),
		SourceBucket: in.SourceBucket,
		SourceKey:    in.SourceKey,
		SourceETag:   in.SourceETag,
		SourceSize:   in.SourceSize,
		DestURI:      destURI,
		DestKey:      destKey,
		Status:       status,
	}
}
