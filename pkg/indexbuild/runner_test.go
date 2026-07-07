package indexbuild

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/provider"
)

func TestRunnerBuildPublishesDeterministicSnapshotAndRetryParity(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t, "direct")

	summary, err := NewRunner(cfg).Build(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), summary.ObjectsObserved)
	require.Len(t, summary.Manifest.Segments, 2)

	manifestBefore, err := os.ReadFile(cfg.Paths.ManifestPath)
	require.NoError(t, err)
	_, rows, err := ReadLatest(cfg.Paths.LatestPath)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "a.xml", rows[0].RelKey)
	require.Equal(t, "b.xml", rows[1].RelKey)

	retrySummary, err := Retry(ctx, RetryConfig{
		IndexSetID:           cfg.IndexSetID,
		RunID:                cfg.RunID,
		Paths:                cfg.Paths,
		JournalPaths:         summary.JournalPaths,
		Coverage:             cfg.Coverage,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		Clock:                cfg.Clock,
		TargetRowsPerSegment: cfg.TargetRowsPerSegment,
	})
	require.NoError(t, err)
	require.Equal(t, summary.Manifest, retrySummary.Manifest)
	manifestAfter, err := os.ReadFile(cfg.Paths.ManifestPath)
	require.NoError(t, err)
	require.Equal(t, manifestBefore, manifestAfter)
}

func TestRunnerNormalizesProviderCoverageToRelKeyTombstones(t *testing.T) {
	cfg := testConfig(t, "coverage-relkey")
	priorSeen := cfg.RunStartedAt.Add(-24 * time.Hour)
	cfg.PriorRows = []ObjectState{{
		IndexSetID:       cfg.IndexSetID,
		RelKey:           "missing.xml",
		SizeBytes:        9,
		ETag:             `"old"`,
		FirstSeenRunID:   "run_old",
		FirstSeenAt:      priorSeen,
		LastChangedRunID: "run_old",
		LastChangedAt:    priorSeen,
		LastSeenRunID:    "run_old",
		LastSeenAt:       priorSeen,
	}}

	summary, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), summary.ObjectsObserved)

	_, rows, err := ReadLatest(cfg.Paths.LatestPath)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	byKey := map[string]ObjectState{}
	for _, row := range rows {
		byKey[row.RelKey] = row
	}
	require.NotNil(t, byKey["missing.xml"].DeletedAt)
	require.Equal(t, cfg.RunStartedAt, *byKey["missing.xml"].DeletedAt)
}

func TestPartialCrawlJournalIsNotPublishableByRetry(t *testing.T) {
	cfg := testConfig(t, "partial-journal")
	cfg.Source.Provider = partialListProvider{
		firstPage: []provider.ObjectSummary{testObjects(cfg.RunStartedAt)[0]},
		err:       provider.ErrAccessDenied,
	}

	summary, err := NewRunner(cfg).Build(context.Background())
	require.ErrorContains(t, err, "snapshot not published")
	require.Empty(t, summary.JournalPaths)
	require.NoFileExists(t, cfg.Paths.LatestPath)

	journalPath := filepath.Join(cfg.Paths.JournalDir, "shard-0001.jsonl")
	retrySummary, retryErr := Retry(context.Background(), RetryConfig{
		IndexSetID:           cfg.IndexSetID,
		RunID:                cfg.RunID,
		BaseURI:              cfg.BaseURI,
		Paths:                cfg.Paths,
		JournalPaths:         []string{journalPath},
		Coverage:             cfg.Coverage,
		RunStartedAt:         cfg.RunStartedAt,
		CreatedAt:            cfg.CreatedAt,
		Clock:                cfg.Clock,
		TargetRowsPerSegment: cfg.TargetRowsPerSegment,
	})
	require.ErrorIs(t, retryErr, indexsubstrate.ErrIncompleteJournal)
	require.Empty(t, retrySummary.Manifest.Segments)
	require.NoFileExists(t, cfg.Paths.LatestPath)
}

func TestEventSanitizationRedactsProviderErrors(t *testing.T) {
	cfg := testConfig(t, "redaction")
	sink := &captureSink{}
	cfg.Events = sink
	cfg.Source.Provider = fakeProvider{
		listErr: fmt.Errorf("GET https://storage.example.invalid/data/?X-Amz-Signature=SECRET&debug=keep failed: token=SECRET: %w", provider.ErrAccessDenied),
	}

	_, err := NewRunner(cfg).Build(context.Background())
	require.ErrorContains(t, err, "snapshot not published")
	require.NotEmpty(t, sink.events)

	blob, marshalErr := json.Marshal(sink.events)
	require.NoError(t, marshalErr)
	text := string(blob)
	require.NotContains(t, text, "SECRET")
	require.NotContains(t, text, "debug=keep")
	require.Contains(t, sink.events[1].Message, "token=<redacted>")
}

func TestConfigFormattingRedactsProviderAndPaths(t *testing.T) {
	cfg := testConfig(t, "format")
	rendered := fmt.Sprintf("%#v", cfg)
	require.Contains(t, rendered, "Provider:<set>")
	require.Contains(t, rendered, "JournalDir:<set>")
	require.NotContains(t, rendered, cfg.Paths.JournalDir)
	require.NotContains(t, rendered, "fakeProvider")
}

func TestPathValidationRejectsEngineStateUnderIndexRoot(t *testing.T) {
	root := t.TempDir()
	cfg := testConfig(t, "paths")
	cfg.Paths.IndexDBDir = filepath.Join(root, "indexes", "idx_test")
	cfg.Paths.JournalDir = filepath.Join(cfg.Paths.IndexDBDir, "journals")

	_, err := NewRunner(cfg).Build(context.Background())
	require.ErrorContains(t, err, "journal directory must not be inside index db directory")
}

func TestReadLatestUsesEngineSideSegmentTraversalGuard(t *testing.T) {
	cfg := testConfig(t, "traversal")
	_, err := NewRunner(cfg).Build(context.Background())
	require.NoError(t, err)

	var manifest map[string]any
	manifestBytes, err := os.ReadFile(cfg.Paths.ManifestPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	segments := manifest["segments"].([]any)
	first := segments[0].(map[string]any)
	first["path"] = "../escape.parquet"
	tamperedManifest, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	tamperedManifest = append(tamperedManifest, '\n')
	require.NoError(t, os.WriteFile(cfg.Paths.ManifestPath, tamperedManifest, 0o600))
	digest := sha256.Sum256(tamperedManifest)

	var complete map[string]any
	completeBytes, err := os.ReadFile(cfg.Paths.CompletePath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(completeBytes, &complete))
	complete["manifest_sha256"] = hex.EncodeToString(digest[:])
	tamperedComplete, err := json.MarshalIndent(complete, "", "  ")
	require.NoError(t, err)
	tamperedComplete = append(tamperedComplete, '\n')
	require.NoError(t, os.WriteFile(cfg.Paths.CompletePath, tamperedComplete, 0o600))

	_, rows, err := ReadLatest(cfg.Paths.LatestPath)
	require.Error(t, err)
	require.Nil(t, rows)
}

func TestNoBypassAPISurface(t *testing.T) {
	denied := map[string]bool{
		"CreateJournal":   true,
		"JournalWriter":   true,
		"WriteSegmentSet": true,
		"WriteManifest":   true,
		"PublishSnapshot": true,
		"AdvanceLatest":   true,
		"WriteLatest":     true,
	}
	exported := exportedPackageNames(t)
	for _, name := range exported {
		require.False(t, denied[name], "exported bypass primitive %q", name)
	}

	runnerType := reflect.TypeOf(&Runner{})
	for i := 0; i < runnerType.NumMethod(); i++ {
		name := runnerType.Method(i).Name
		require.False(t, denied[name], "exported Runner bypass method %q", name)
	}
}

func TestDependencyBoundary(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	cmd.Env = append(os.Environ(), "GOCACHE="+filepath.Join(t.TempDir(), "go-build"))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	denied := []string{
		"github.com/3leaps/gonimbus/internal/cmd",
		"github.com/3leaps/gonimbus/internal/providerdispatch",
		"github.com/3leaps/gonimbus/pkg/indexstore",
		"github.com/3leaps/gonimbus/pkg/provider/s3",
		"github.com/3leaps/gonimbus/pkg/provider/gcs",
		"modernc.org/sqlite",
		"modernc.org/libc",
		"github.com/aws/aws-sdk-go-v2",
		"cloud.google.com/go/storage",
		"google.golang.org/api",
		"github.com/spf13/cobra",
		"github.com/spf13/viper",
		"github.com/fulmenhq/gofulmen",
	}
	deps := strings.Fields(string(out))
	for _, dep := range deps {
		for _, prefix := range denied {
			if dep == prefix || strings.HasPrefix(dep, prefix+"/") {
				t.Fatalf("pkg/indexbuild dependency graph includes denied dependency %q via %q", prefix, dep)
			}
		}
	}
}

func testConfig(t *testing.T, name string) Config {
	t.Helper()
	root := filepath.Join(t.TempDir(), name)
	base := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	return Config{
		IndexSetID: "idx_test",
		RunID:      "run_test",
		BaseURI:    "s3://bucket/data/",
		Source: Source{
			Provider:     fakeProvider{objects: testObjects(base)},
			ProviderName: "s3",
		},
		Match: MatchConfig{Includes: []string{"**"}},
		Paths: PathConfig{
			JournalDir:   filepath.Join(root, "journals"),
			SegmentDir:   filepath.Join(root, "segments"),
			ManifestPath: filepath.Join(root, "manifest.json"),
			CompletePath: filepath.Join(root, "complete.json"),
			LatestPath:   filepath.Join(root, "latest.json"),
			IndexDBDir:   filepath.Join(root, "indexes", "idx_test"),
		},
		Coverage: []CoverageAttestation{{
			Scope:    &Scope{Prefix: "data/"},
			Basis:    CoverageBasisConfirmed,
			Complete: true,
		}},
		RunStartedAt:         base,
		CreatedAt:            base.Add(time.Minute),
		Clock:                func() time.Time { return base.Add(2 * time.Minute) },
		TargetRowsPerSegment: 1,
	}
}

func testObjects(base time.Time) []provider.ObjectSummary {
	return []provider.ObjectSummary{
		{Key: "data/a.xml", Size: 10, ETag: `"a"`, LastModified: base.Add(-time.Hour), StorageClass: "STANDARD"},
		{Key: "data/b.xml", Size: 11, ETag: `"b"`, LastModified: base.Add(-time.Minute), StorageClass: "STANDARD"},
		{Key: "other/c.xml", Size: 12, ETag: `"c"`, LastModified: base},
	}
}

type fakeProvider struct {
	objects []provider.ObjectSummary
	listErr error
}

func (p fakeProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	if p.listErr != nil {
		return nil, p.listErr
	}
	var out []provider.ObjectSummary
	for _, obj := range p.objects {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out}, nil
}

func (fakeProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (fakeProvider) Close() error { return nil }

type partialListProvider struct {
	firstPage []provider.ObjectSummary
	err       error
}

func (p partialListProvider) List(_ context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	if opts.ContinuationToken != "" {
		return nil, p.err
	}
	var out []provider.ObjectSummary
	for _, obj := range p.firstPage {
		if strings.HasPrefix(obj.Key, opts.Prefix) {
			out = append(out, obj)
		}
	}
	return &provider.ListResult{Objects: out, IsTruncated: true, ContinuationToken: "next"}, nil
}

func (partialListProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, provider.ErrNotFound
}

func (partialListProvider) Close() error { return nil }

type captureSink struct {
	events []Event
}

func (s *captureSink) OnEvent(_ context.Context, event Event) error {
	s.events = append(s.events, event)
	return nil
}

func exportedPackageNames(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	var names []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						if s.Name.IsExported() {
							names = append(names, s.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range s.Names {
							if name.IsExported() {
								names = append(names, name.Name)
							}
						}
					}
				}
			case *ast.FuncDecl:
				if d.Name.IsExported() {
					names = append(names, d.Name.Name)
				}
			}
		}
	}
	return names
}
