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
// internal form. Any line that is not an acceptable S3 gonimbus.reflow.input.v1
// record returns an error; the executor reports it as a per-record INVALID_INPUT
// event (CLI-equivalent) and continues streaming.
func parseReflowInputLine(line string) (reflowInput, error) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return reflowInput{}, fmt.Errorf("reflow input must be a %s JSON record", ReflowInputRecordType)
	}
	var env struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		return reflowInput{}, err
	}
	if env.Type != ReflowInputRecordType {
		return reflowInput{}, fmt.Errorf("unsupported record type %q; RecordStreamSource carries %s records", env.Type, ReflowInputRecordType)
	}
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
	if err := json.Unmarshal(env.Data, &data); err != nil {
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
