package scope_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/scope"
)

const testGonimbusVersion = "0.4.1-test-migration"

// Pure test identity computer: hashes config-affecting fields without importing
// storage packages. Proves match/scope change alters identity under fixed version.
func testComputeIdentity(m *manifest.IndexManifest, gonimbusVersion string) (*scope.ComputedIdentityEvidence, error) {
	includes := []string{manifest.DefaultIndexIncludes}
	var scopeHash string
	if m != nil && m.Build != nil {
		if m.Build.Match != nil && len(m.Build.Match.Includes) > 0 {
			includes = append([]string(nil), m.Build.Match.Includes...)
		}
		if m.Build.Scope != nil {
			h, err := scope.HashConfig(m.Build.Scope)
			if err != nil {
				return nil, err
			}
			scopeHash = h
		}
	}
	payload := gonimbusVersion + "|" + scopeHash + "|" + strings.Join(includes, ",")
	if m != nil {
		payload += "|" + m.Connection.BaseURI + "|" + m.Connection.Provider
	}
	sum := sha256.Sum256([]byte(payload))
	hexDigest := hex.EncodeToString(sum[:])
	return &scope.ComputedIdentityEvidence{
		Kind:            "computed",
		IndexSetID:      "idx_" + hexDigest,
		CanonicalSHA256: hexDigest,
		ScopeHash:       scopeHash,
		Includes:        includes,
		GonimbusVersion: gonimbusVersion,
	}, nil
}

func testManifestYAML(includes string, extras string) []byte {
	body := fmt.Sprintf(`version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
  region: us-east-1
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
%s
%s
  crawl:
    concurrency: 4
`, includes, extras)
	return []byte(body)
}

func includeLines(patterns ...string) string {
	var b strings.Builder
	for _, p := range patterns {
		b.WriteString("      - \"")
		b.WriteString(p)
		b.WriteString("\"\n")
	}
	return b.String()
}

func TestPlanMatchPrefixMigration_ConvertibleSingle(t *testing.T) {
	raw := testManifestYAML(includeLines("cohort-a/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{GonimbusVersion: testGonimbusVersion, ComputeIdentity: testComputeIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
	}
	if plan.ReasonCode != scope.ReasonConvertible {
		t.Fatalf("reason=%s", plan.ReasonCode)
	}
	wantLegacy := []string{"data/cohort-a/"}
	if !equalStrings(plan.LegacyProviderPrefixes, wantLegacy) {
		t.Fatalf("legacy plan=%v want %v", plan.LegacyProviderPrefixes, wantLegacy)
	}
	if !equalStrings(plan.ProposedProviderPrefixes, wantLegacy) {
		t.Fatalf("proposed plan=%v want %v", plan.ProposedProviderPrefixes, wantLegacy)
	}
	if plan.ProposedScope == nil || plan.ProposedScope.Type != "prefix_list" {
		t.Fatalf("proposed scope=%+v", plan.ProposedScope)
	}
	if !equalStrings(plan.ProposedScope.Prefixes, []string{"cohort-a/"}) {
		t.Fatalf("scope prefixes=%v", plan.ProposedScope.Prefixes)
	}
	if plan.ProposedManifestYAML == "" {
		t.Fatal("expected proposed manifest yaml")
	}
	if plan.LegacyConfigIdentity == nil || plan.ProposedConfigIdentity == nil {
		t.Fatal("expected identity evidence")
	}
	if plan.LegacyConfigIdentity.IndexSetID == plan.ProposedConfigIdentity.IndexSetID {
		t.Fatal("expected identity change")
	}
	if plan.LegacyConfigIdentity.Kind != "computed" {
		t.Fatalf("kind=%s", plan.LegacyConfigIdentity.Kind)
	}

	// Idempotence: re-run on emitted manifest → already_migrated.
	plan2, err := scope.PlanMatchPrefixMigration([]byte(plan.ProposedManifestYAML), scope.PlanOptions{GonimbusVersion: testGonimbusVersion, ComputeIdentity: testComputeIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if plan2.Classification != scope.ClassificationAlreadyMigrated {
		t.Fatalf("round-trip classification=%s reason=%s detail=%s", plan2.Classification, plan2.ReasonCode, plan2.Detail)
	}
	if plan2.ProposedScopeHash != plan.ProposedScopeHash {
		t.Fatalf("scope hash drift: %s vs %s", plan2.ProposedScopeHash, plan.ProposedScopeHash)
	}
}

func TestPlanMatchPrefixMigration_BasePrefixedInclude(t *testing.T) {
	// Include already carries the base key; must not double-prefix in scope.
	raw := testManifestYAML(includeLines("data/cohort-a/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{GonimbusVersion: testGonimbusVersion, ComputeIdentity: testComputeIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
	}
	if !equalStrings(plan.LegacyProviderPrefixes, []string{"data/cohort-a/"}) {
		t.Fatalf("legacy=%v", plan.LegacyProviderPrefixes)
	}
	if !equalStrings(plan.ProposedScope.Prefixes, []string{"cohort-a/"}) {
		t.Fatalf("scope prefixes=%v (must be relative, not data/cohort-a/)", plan.ProposedScope.Prefixes)
	}
}

func TestPlanMatchPrefixMigration_ParentSubsumption(t *testing.T) {
	raw := testManifestYAML(includeLines("a/**", "a/b/**", "a/b/c/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
	}
	if !equalStrings(plan.LegacyProviderPrefixes, []string{"data/a/"}) {
		t.Fatalf("legacy=%v", plan.LegacyProviderPrefixes)
	}
	if !equalStrings(plan.ProposedScope.Prefixes, []string{"a/"}) {
		t.Fatalf("scope=%v", plan.ProposedScope.Prefixes)
	}
}

func TestPlanMatchPrefixMigration_DuplicatesAndOrder(t *testing.T) {
	raw1 := testManifestYAML(includeLines("z/**", "a/**"), "")
	raw2 := testManifestYAML(includeLines("a/**", "z/**", "a/**"), "")
	p1, err := scope.PlanMatchPrefixMigration(raw1, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := scope.PlanMatchPrefixMigration(raw2, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if p1.Classification != scope.ClassificationConvertible || p2.Classification != scope.ClassificationConvertible {
		t.Fatalf("p1=%s p2=%s", p1.Classification, p2.Classification)
	}
	if p1.ProposedScopeHash != p2.ProposedScopeHash {
		t.Fatalf("scope hash not stable across permutation: %s vs %s", p1.ProposedScopeHash, p2.ProposedScopeHash)
	}
	if p1.ProposedPlanDigest != p2.ProposedPlanDigest {
		t.Fatalf("plan digest drift")
	}
}

func TestPlanMatchPrefixMigration_PostDateLiteralEntity(t *testing.T) {
	// Compiler caveat: post-date entity as exact prefix_list, never discover.
	raw := testManifestYAML(includeLines("date/2024-01-01/entity=store-1/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
	}
	if plan.ProposedScope.Type != "prefix_list" {
		t.Fatalf("type=%s", plan.ProposedScope.Type)
	}
	if plan.ProposedScope.Discover != nil || plan.ProposedScope.Date != nil {
		t.Fatal("must not emit date_partitions/discover")
	}
	want := []string{"date/2024-01-01/entity=store-1/"}
	if !equalStrings(plan.ProposedScope.Prefixes, want) {
		t.Fatalf("prefixes=%v want %v", plan.ProposedScope.Prefixes, want)
	}
}

func TestPlanMatchPrefixMigration_AlreadyCompatible(t *testing.T) {
	raw := testManifestYAML(includeLines("**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationAlreadyCompatible {
		t.Fatalf("classification=%s reason=%s", plan.Classification, plan.ReasonCode)
	}
}

func TestPlanMatchPrefixMigration_RefuseMatrix(t *testing.T) {
	cases := []struct {
		name   string
		raw    []byte
		reason string
	}{
		{
			name:   "suffix_glob",
			raw:    testManifestYAML(includeLines("**/*.parquet"), ""),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name:   "mid_glob",
			raw:    testManifestYAML(includeLines("cohort-*/day/**"), ""),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name:   "question",
			raw:    testManifestYAML(includeLines("cohort-?/day/**"), ""),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name:   "char_class",
			raw:    testManifestYAML(includeLines("cohort-[ab]/**"), ""),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name:   "braces",
			raw:    testManifestYAML(includeLines("cohort-{a,b}/**"), ""),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name: "escape",
			raw: []byte(`version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
      - 'cohort-\*/**'
`),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name:   "root_slash_star",
			raw:    testManifestYAML(includeLines("/**"), ""),
			reason: scope.ReasonEmptyOrRootPrefix,
		},
		{
			name: "excludes",
			raw: testManifestYAML(includeLines("cohort-a/**"), `    excludes:
      - "**/tmp/**"
`),
			reason: scope.ReasonHasExcludes,
		},
		{
			name: "filters_empty_object",
			raw: testManifestYAML(includeLines("cohort-a/**"), `    filters: {}
`),
			reason: scope.ReasonHasFilters,
		},
		{
			name: "include_hidden",
			raw: testManifestYAML(includeLines("cohort-a/**"), `    include_hidden: true
`),
			reason: scope.ReasonIncludeHidden,
		},
		{
			name: "existing_scope_with_includes",
			raw: []byte(`version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  scope:
    type: prefix_list
    prefixes:
      - "other/"
  match:
    includes:
      - "cohort-a/**"
`),
			reason: scope.ReasonExistingScope,
		},
		{
			name: "backslash_separator",
			raw: []byte(`version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/data/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
      - 'cohort\day/**'
`),
			reason: scope.ReasonNonPrefixInclude,
		},
		{
			name:   "mixed_default",
			raw:    testManifestYAML(includeLines("**", "cohort-a/**"), ""),
			reason: scope.ReasonMixedDefaultAndPrefix,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := scope.PlanMatchPrefixMigration(tc.raw, scope.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Classification != scope.ClassificationRefused {
				t.Fatalf("want refused, got %s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
			}
			if plan.ReasonCode != tc.reason {
				t.Fatalf("reason=%s want %s detail=%s", plan.ReasonCode, tc.reason, plan.Detail)
			}
			if plan.ProposedManifestYAML != "" {
				t.Fatal("refused plan must not emit proposed manifest")
			}
		})
	}
}

func TestPlanMatchPrefixMigration_NestedBaseURI(t *testing.T) {
	raw := []byte(`version: "1.0"
connection:
  provider: s3
  bucket: example-bucket
  base_uri: "s3://example-bucket/org/team/project/"
identity:
  storage_provider: aws_s3
  cloud_provider: aws
  region_kind: aws
  region: us-east-1
build:
  source: crawl
  match:
    includes:
      - "site-1/day/**"
      - "site-2/day/**"
`)
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{GonimbusVersion: testGonimbusVersion, ComputeIdentity: testComputeIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s reason=%s detail=%s", plan.Classification, plan.ReasonCode, plan.Detail)
	}
	wantLegacy := []string{"org/team/project/site-1/day/", "org/team/project/site-2/day/"}
	if !equalStrings(plan.LegacyProviderPrefixes, wantLegacy) {
		t.Fatalf("legacy=%v", plan.LegacyProviderPrefixes)
	}
	if !equalStrings(plan.ProposedScope.Prefixes, []string{"site-1/day/", "site-2/day/"}) {
		t.Fatalf("scope=%v", plan.ProposedScope.Prefixes)
	}
}

func TestPlanMatchPrefixMigration_IndependentProjectionOracle(t *testing.T) {
	raw := testManifestYAML(includeLines("a/**", "b/c/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s detail=%s", plan.Classification, plan.Detail)
	}

	baseKey := "data/"
	// Common population includes the bare prefix key and near-collisions.
	// LIST prefixes end with "/"; both independent oracles apply LIST reachability
	// before ingest match, so "data/a" is excluded from both projections.
	population := []sterileObject{
		{Key: "data/a", Size: 1},
		{Key: "data/a/x", Size: 10},
		{Key: "data/a/y/z", Size: 20},
		{Key: "data/ab/x", Size: 30},
		{Key: "data/b/c/1", Size: 40},
		{Key: "data/b/d/1", Size: 50},
		{Key: "data/a/.hidden/file", Size: 60},
		{Key: "data/other/x", Size: 70},
	}

	legacyIncludes := []string{"a/**", "b/c/**"}
	legacy, err := independentLegacyProjection(baseKey, legacyIncludes, population)
	if err != nil {
		t.Fatal(err)
	}
	proposed, err := independentProposedProjection(baseKey, plan.ProposedScope, population)
	if err != nil {
		t.Fatal(err)
	}
	if !projectionEqual(legacy, proposed) {
		t.Fatalf("projection mismatch\nlegacy=%+v\nproposed=%+v", legacy, proposed)
	}
	if legacy.Rows != 3 || legacy.TotalBytes != 70 {
		t.Fatalf("unexpected legacy totals rows=%d bytes=%d", legacy.Rows, legacy.TotalBytes)
	}
	if legacy.Digest != proposed.Digest {
		t.Fatal("projection digests differ")
	}
	// Bare key excluded by LIST reachability on both sides.
	for _, row := range legacy.RowsDetail {
		if row.Key == "data/a" {
			t.Fatal("bare prefix key must not be LIST-reachable under data/a/")
		}
	}
}

// sterileObject is a LIST-visible object with size for independent oracles.
type sterileObject struct {
	Key  string
	Size int64
}

type projectionResult struct {
	RowsDetail []sterileObject
	Rows       int
	TotalBytes int64
	Digest     string
}

// independentLegacyProjection models build semantics without converter helpers:
// anchor includes (build prefixPatterns), DerivePrefixes for LIST, filter by LIST
// reachability, then match.New on full keys.
func independentLegacyProjection(baseKey string, includes []string, pop []sterileObject) (*projectionResult, error) {
	anchored := testPrefixPatterns(baseKey, includes)
	listPrefixes := match.DerivePrefixes(anchored)
	reachable := filterLISTReachable(listPrefixes, pop)
	matcher, err := match.New(match.Config{Includes: anchored, IncludeHidden: false})
	if err != nil {
		return nil, err
	}
	var rows []sterileObject
	var total int64
	for _, o := range reachable {
		if matcher.Match(o.Key) {
			rows = append(rows, o)
			total += o.Size
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return &projectionResult{
		RowsDetail: rows,
		Rows:       len(rows),
		TotalBytes: total,
		Digest:     projectionRowsDigest(rows),
	}, nil
}

// independentProposedProjection models post-migration durable ingest: compile
// scope.prefix_list to LIST prefixes, filter reachability, default match **.
func independentProposedProjection(baseKey string, scopeCfg *manifest.IndexScopeConfig, pop []sterileObject) (*projectionResult, error) {
	plan, err := scope.Compile(context.Background(), scopeCfg, baseKey, nil)
	if err != nil {
		return nil, err
	}
	var listPrefixes []string
	if plan != nil {
		listPrefixes = plan.Prefixes
	}
	reachable := filterLISTReachable(listPrefixes, pop)
	anchored := testPrefixPatterns(baseKey, []string{manifest.DefaultIndexIncludes})
	matcher, err := match.New(match.Config{Includes: anchored, IncludeHidden: false})
	if err != nil {
		return nil, err
	}
	var rows []sterileObject
	var total int64
	for _, o := range reachable {
		if matcher.Match(o.Key) {
			rows = append(rows, o)
			total += o.Size
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return &projectionResult{
		RowsDetail: rows,
		Rows:       len(rows),
		TotalBytes: total,
		Digest:     projectionRowsDigest(rows),
	}, nil
}

// testPrefixPatterns is a test-local copy of index-build anchoring (not converter internals).
func testPrefixPatterns(basePrefix string, patterns []string) []string {
	if len(patterns) == 0 {
		return nil
	}
	if basePrefix == "" {
		out := make([]string, 0, len(patterns))
		for _, raw := range patterns {
			p := strings.TrimSpace(raw)
			if p == "" {
				continue
			}
			out = append(out, match.NormalizePattern(strings.TrimPrefix(p, "/")))
		}
		return out
	}
	if !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	out := make([]string, 0, len(patterns))
	for _, raw := range patterns {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		p = match.NormalizePattern(strings.TrimPrefix(p, "/"))
		if strings.HasPrefix(p, basePrefix) {
			out = append(out, p)
			continue
		}
		out = append(out, basePrefix+p)
	}
	return out
}

func filterLISTReachable(prefixes []string, pop []sterileObject) []sterileObject {
	var out []sterileObject
	for _, o := range pop {
		for _, p := range prefixes {
			if p == "" || strings.HasPrefix(o.Key, p) {
				out = append(out, o)
				break
			}
		}
	}
	return out
}

func projectionEqual(a, b *projectionResult) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Rows != b.Rows || a.TotalBytes != b.TotalBytes || a.Digest != b.Digest {
		return false
	}
	if len(a.RowsDetail) != len(b.RowsDetail) {
		return false
	}
	for i := range a.RowsDetail {
		if a.RowsDetail[i].Key != b.RowsDetail[i].Key || a.RowsDetail[i].Size != b.RowsDetail[i].Size {
			return false
		}
	}
	return true
}

func projectionRowsDigest(rows []sterileObject) string {
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "%s\t%d\n", r.Key, r.Size)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func TestPlanMatchPrefixMigration_SourceDigest(t *testing.T) {
	raw := testManifestYAML(includeLines("cohort/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if plan.SourceManifestDigest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest mismatch")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
