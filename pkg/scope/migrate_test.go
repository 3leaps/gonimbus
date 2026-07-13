package scope_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/scope"
)

const testGonimbusVersion = "0.4.1-test-slot13"

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
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{GonimbusVersion: testGonimbusVersion})
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
	plan2, err := scope.PlanMatchPrefixMigration([]byte(plan.ProposedManifestYAML), scope.PlanOptions{GonimbusVersion: testGonimbusVersion})
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
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{GonimbusVersion: testGonimbusVersion})
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
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{GonimbusVersion: testGonimbusVersion})
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

func TestPlanMatchPrefixMigration_ProjectionEquivalence(t *testing.T) {
	raw := testManifestYAML(includeLines("a/**", "b/c/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("classification=%s detail=%s", plan.Classification, plan.Detail)
	}

	baseKey := "data/"
	// Sterile population with near-collisions and hidden segments.
	// Note: bare key "data/a" is intentionally omitted. doublestar may treat
	// pattern "data/a/**" as matching the exact key "data/a", while S3 LIST
	// with prefix "data/a/" does not return that key. Slot 1.3 equivalence is
	// defined for objects under the listed prefix (see TestBarePrefixKeyEdge).
	population := []string{
		"data/a/x",
		"data/a/y/z",
		"data/ab/x", // near-collision: not under a/
		"data/b/c/1",
		"data/b/d/1", // under b/ but not b/c/
		"data/a/.hidden/file",
		"data/other/x",
	}

	src, err := manifest.LoadIndexManifestFromBytes(raw, "src")
	if err != nil {
		t.Fatal(err)
	}
	legacyKeys, err := scope.ProjectKeys(baseKey, src.Build.Match, population)
	if err != nil {
		t.Fatal(err)
	}
	proposedKeys, err := scope.ProjectKeysUnderScope(baseKey, plan.ProposedScope, population)
	if err != nil {
		t.Fatal(err)
	}
	if !equalStrings(legacyKeys, proposedKeys) {
		t.Fatalf("projection mismatch\nlegacy=%v\nproposed=%v", legacyKeys, proposedKeys)
	}

	want := []string{"data/a/x", "data/a/y/z", "data/b/c/1"}
	if !equalStrings(legacyKeys, want) {
		t.Fatalf("legacy projection=%v want %v", legacyKeys, want)
	}
	if projectionDigest(legacyKeys) != projectionDigest(proposedKeys) {
		t.Fatal("projection digests differ")
	}
}

// TestBarePrefixKeyEdge documents that exact keys equal to the LIST prefix
// path without a trailing child segment can match legacy doublestar includes
// but are not returned by a trailing-slash LIST prefix. Migration does not
// invent a special case for that boundary; operators comparing projections
// should use LIST-reachable object keys.
func TestBarePrefixKeyEdge(t *testing.T) {
	raw := testManifestYAML(includeLines("a/**"), "")
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Classification != scope.ClassificationConvertible {
		t.Fatalf("%s %s", plan.Classification, plan.Detail)
	}
	population := []string{"data/a", "data/a/x"}
	src, err := manifest.LoadIndexManifestFromBytes(raw, "src")
	if err != nil {
		t.Fatal(err)
	}
	legacyKeys, err := scope.ProjectKeys("data/", src.Build.Match, population)
	if err != nil {
		t.Fatal(err)
	}
	proposedKeys, err := scope.ProjectKeysUnderScope("data/", plan.ProposedScope, population)
	if err != nil {
		t.Fatal(err)
	}
	// Document current behavior rather than force artificial equality.
	if !contains(legacyKeys, "data/a/x") || !contains(proposedKeys, "data/a/x") {
		t.Fatalf("child key must project on both sides: legacy=%v proposed=%v", legacyKeys, proposedKeys)
	}
	// LIST plan remains equal regardless of the bare-key doublestar edge.
	if !equalStrings(plan.LegacyProviderPrefixes, plan.ProposedProviderPrefixes) {
		t.Fatalf("LIST plans must still be equal: %v vs %v", plan.LegacyProviderPrefixes, plan.ProposedProviderPrefixes)
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
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

func projectionDigest(keys []string) string {
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}
