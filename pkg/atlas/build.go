package atlas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
)

const (
	HeaderFile      = "atlas.json"
	DiagnosticsFile = "diagnostics.jsonl"
	ShardsDir       = "shards"
)

type BuildOptions struct {
	Source    SourceRun
	Recipe    Recipe
	Reader    provider.ObjectGetter
	OutputDir string
	Now       func() time.Time
}

type BuildResult struct {
	Header    Header
	OutputDir string
}

type recordEnvelope struct {
	Type string          `json:"type"`
	TS   time.Time       `json:"ts"`
	Data json.RawMessage `json:"data"`
}

func Build(ctx context.Context, opts BuildOptions) (*BuildResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Reader == nil {
		return nil, fmt.Errorf("atlas object reader is required")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		return nil, fmt.Errorf("atlas output directory is required")
	}
	if strings.TrimSpace(opts.Source.IndexSetID) == "" {
		return nil, fmt.Errorf("source index set id is required")
	}
	if strings.TrimSpace(opts.Source.RunID) == "" {
		return nil, fmt.Errorf("source run id is required")
	}
	if strings.TrimSpace(opts.Source.BaseURI) == "" {
		return nil, fmt.Errorf("source base uri is required")
	}
	if err := opts.Recipe.Validate(); err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	createdAt := now().UTC()
	recipeDigest, err := opts.Recipe.Digest()
	if err != nil {
		return nil, err
	}
	atlasID := computeAtlasID(opts.Source.IndexSetID, opts.Source.RunID, recipeDigest)
	coverage := strings.TrimSpace(opts.Source.Coverage)
	if coverage == "" {
		coverage = opts.Recipe.Coverage
	}

	if err := prepareOutputDir(opts.OutputDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(opts.OutputDir, ShardsDir), 0700); err != nil {
		return nil, fmt.Errorf("create atlas shards directory: %w", err)
	}

	prober, err := probe.New(opts.Recipe.ProbeConfig())
	if err != nil {
		return nil, fmt.Errorf("create atlas prober: %w", err)
	}

	header := Header{
		SchemaVersion:    SchemaVersion,
		AtlasID:          atlasID,
		CreatedAt:        createdAt,
		SourceIndexSetID: opts.Source.IndexSetID,
		SourceRunID:      opts.Source.RunID,
		BaseURI:          opts.Source.BaseURI,
		ScopeDigest:      opts.Source.ScopeDigest,
		RecipeDigest:     recipeDigest,
		HashProfile:      HashProfileSHA256,
		Coverage:         coverage,
		ShardBy:          append([]string(nil), opts.Recipe.ShardBy...),
		Dimensions:       opts.Recipe.DimensionDeclarations(),
		SystemFields:     opts.Recipe.SystemFieldDeclarations(),
	}

	diagFile, err := os.OpenFile(filepath.Join(opts.OutputDir, DiagnosticsFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) // #nosec G304 -- output path is the operator-selected local atlas artifact directory.
	if err != nil {
		return nil, fmt.Errorf("create atlas diagnostics: %w", err)
	}
	defer func() { _ = diagFile.Close() }()
	diagEnc := json.NewEncoder(diagFile)

	shardFiles := map[string]*os.File{}
	defer func() {
		for _, f := range shardFiles {
			_ = f.Close()
		}
	}()

	for _, obj := range opts.Source.Objects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		header.Counts.ObjectsScanned++
		storageKey, sourceURI, keyErr := objectKeyAndURI(opts.Source.BaseURI, obj.RelKey)
		if keyErr != nil {
			header.Counts.Diagnostics++
			header.Counts.ValidationFailures++
			if err := writeDiagnostic(diagEnc, now(), opts.Source, obj, "", "validation", "invalid_key", keyErr.Error()); err != nil {
				return nil, err
			}
			continue
		}

		body, _, err := opts.Reader.GetObject(ctx, storageKey)
		if err != nil {
			header.Counts.Diagnostics++
			header.Counts.ReadFailures++
			if err := writeDiagnostic(diagEnc, now(), opts.Source, obj, storageKey, "read", "get_object_failed", err.Error()); err != nil {
				return nil, err
			}
			continue
		}
		data, readErr := io.ReadAll(body)
		closeErr := body.Close()
		if readErr != nil || closeErr != nil {
			header.Counts.Diagnostics++
			header.Counts.ReadFailures++
			msg := firstErr(readErr, closeErr).Error()
			if err := writeDiagnostic(diagEnc, now(), opts.Source, obj, storageKey, "read", "read_object_failed", msg); err != nil {
				return nil, err
			}
			continue
		}

		sum := sha256.Sum256(data)
		contentHash := hex.EncodeToString(sum[:])
		res, err := prober.ProbeDetailed(data, int64(len(data)), probe.TerminationStreamExhausted)
		if err != nil {
			header.Counts.Diagnostics++
			header.Counts.ExtractionFailures++
			if err := writeDiagnostic(diagEnc, now(), opts.Source, obj, storageKey, "extract", "extract_failed", err.Error()); err != nil {
				return nil, err
			}
			continue
		}
		_, requiredFailed := prober.ApplyMissingPolicies(res.Vars, &res.Audit)
		if requiredFailed {
			header.Counts.Diagnostics++
			header.Counts.ExtractionFailures++
			if err := writeDiagnostic(diagEnc, now(), opts.Source, obj, storageKey, "extract", "required_dimension_unresolved", "one or more required dimensions were unresolved"); err != nil {
				return nil, err
			}
			continue
		}
		if err := validateDimensionValues(opts.Recipe, res.Vars); err != nil {
			header.Counts.Diagnostics++
			header.Counts.ValidationFailures++
			if err := writeDiagnostic(diagEnc, now(), opts.Source, obj, storageKey, "validate", "invalid_dimension_value", err.Error()); err != nil {
				return nil, err
			}
			continue
		}

		shard := map[string]string{opts.Recipe.ShardBy[0]: res.Vars[opts.Recipe.ShardBy[0]]}
		row := ObjectRow{
			SchemaVersion:    SchemaVersion,
			SourceIndexSetID: opts.Source.IndexSetID,
			SourceRunID:      opts.Source.RunID,
			StorageKey:       storageKey,
			RelKey:           obj.RelKey,
			SourceURI:        sourceURI,
			ContentHash:      contentHash,
			HashProfile:      HashProfileSHA256,
			Dimensions:       res.Vars,
			Shard:            shard,
			SizeBytes:        obj.SizeBytes,
			ETag:             obj.ETag,
			FirstSeenRunID:   obj.LastSeenRunID,
			FirstSeenAt:      obj.LastSeenAt,
		}
		shardName := sanitizeShardValue(shard[opts.Recipe.ShardBy[0]]) + ".jsonl"
		f, ok := shardFiles[shardName]
		if !ok {
			f, err = os.OpenFile(filepath.Join(opts.OutputDir, ShardsDir, shardName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) // #nosec G304 -- shardName is sanitized and rooted under the operator-selected atlas directory.
			if err != nil {
				header.Counts.ArtifactWriteFailure++
				return nil, fmt.Errorf("create atlas shard %s: %w", shardName, err)
			}
			shardFiles[shardName] = f
		}
		if err := writeObjectRow(json.NewEncoder(f), now(), row); err != nil {
			header.Counts.ArtifactWriteFailure++
			return nil, err
		}
		header.Counts.RowsWritten++
	}

	for _, f := range shardFiles {
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close atlas shard: %w", err)
		}
	}
	shardFiles = map[string]*os.File{}
	if err := diagFile.Close(); err != nil {
		return nil, fmt.Errorf("close atlas diagnostics: %w", err)
	}

	if err := writeHeader(opts.OutputDir, header); err != nil {
		return nil, err
	}
	return &BuildResult{Header: header, OutputDir: opts.OutputDir}, nil
}

func prepareOutputDir(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0700)
	}
	if err != nil {
		return fmt.Errorf("stat atlas output directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("atlas output path exists and is not a directory: %s", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read atlas output directory: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("atlas output directory must be empty: %s", path)
	}
	return nil
}

func writeHeader(dir string, header Header) error {
	data, err := json.MarshalIndent(header, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal atlas header: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(dir, HeaderFile), data, 0600); err != nil {
		return fmt.Errorf("write atlas header: %w", err)
	}
	return nil
}

func writeObjectRow(enc *json.Encoder, ts time.Time, row ObjectRow) error {
	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshal atlas object row: %w", err)
	}
	return enc.Encode(recordEnvelope{Type: "gonimbus.atlas.object.v1", TS: ts.UTC(), Data: data})
}

func writeDiagnostic(enc *json.Encoder, ts time.Time, source SourceRun, obj SourceObject, storageKey, stage, code, msg string) error {
	row := DiagnosticRow{
		SchemaVersion:    SchemaVersion,
		SourceIndexSetID: source.IndexSetID,
		SourceRunID:      source.RunID,
		StorageKey:       storageKey,
		RelKey:           obj.RelKey,
		Stage:            stage,
		Code:             code,
		Message:          msg,
		OccurredAt:       ts.UTC(),
	}
	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("marshal atlas diagnostic row: %w", err)
	}
	return enc.Encode(recordEnvelope{Type: "gonimbus.atlas.diagnostic.v1", TS: ts.UTC(), Data: data})
}

func validateDimensionValues(recipe Recipe, vars map[string]string) error {
	for _, dim := range recipe.Dimensions {
		value := strings.TrimSpace(vars[dim.Name])
		if value == "" {
			return fmt.Errorf("dimension %s is empty", dim.Name)
		}
		switch dim.Kind {
		case DimensionTemporalDay:
			if _, err := time.Parse("2006-01-02", value); err != nil {
				return fmt.Errorf("dimension %s must be YYYY-MM-DD: %w", dim.Name, err)
			}
		case DimensionTemporalInstant:
			if _, err := time.Parse(time.RFC3339, value); err != nil {
				return fmt.Errorf("dimension %s must be RFC3339 timestamp: %w", dim.Name, err)
			}
		}
	}
	return nil
}

func computeAtlasID(indexSetID, runID, recipeDigest string) string {
	sum := sha256.Sum256([]byte(indexSetID + "\n" + runID + "\n" + recipeDigest))
	return "atlas_" + hex.EncodeToString(sum[:])
}

func objectKeyAndURI(baseURI, relKey string) (string, string, error) {
	parsed, err := url.Parse(baseURI)
	if err != nil {
		return "", "", fmt.Errorf("parse base uri: %w", err)
	}
	if parsed.Scheme != "s3" {
		return "", "", fmt.Errorf("atlas Phase A supports s3 base_uri only, got %q", parsed.Scheme)
	}
	basePrefix := strings.TrimPrefix(parsed.Path, "/")
	basePrefix = strings.TrimSuffix(basePrefix, "/")
	rel := strings.TrimPrefix(relKey, "/")
	key := rel
	if basePrefix != "" {
		key = basePrefix + "/" + rel
	}
	sourceURI := "s3://" + parsed.Host + "/" + key
	return key, sourceURI, nil
}

var shardValuePattern = regexp.MustCompile(`[^A-Za-z0-9._=-]+`)

func sanitizeShardValue(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "_empty"
	}
	v = shardValuePattern.ReplaceAllString(v, "_")
	v = strings.Trim(v, "._-")
	if v == "" {
		return "_empty"
	}
	return v
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
