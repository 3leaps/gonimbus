package atlas

import (
	"testing"

	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/stretchr/testify/require"
)

func TestRecipeValidateDefaultsAndDeclarations(t *testing.T) {
	recipe := Recipe{
		Dimensions: []DimensionRecipe{
			{
				Name:           "event_date",
				Kind:           DimensionTemporalDay,
				Classification: ClassificationConfidential,
				Extractor: probe.ExtractorConfig{
					Type:     "json_path",
					JSONPath: "$.event_date",
				},
			},
		},
		ShardBy: []string{"event_date"},
		SystemFields: map[string]FieldPolicy{
			"content_hash": {Classification: ClassificationPersonal},
		},
	}

	require.NoError(t, recipe.Validate())
	require.Equal(t, "1.0", recipe.Version)
	require.Equal(t, HashProfileSHA256, recipe.Hash)
	require.Equal(t, CoverageScoped, recipe.Coverage)
	require.True(t, recipe.Dimensions[0].Extractor.Required)

	dims := recipe.DimensionDeclarations()
	require.Equal(t, []DimensionDeclaration{{
		Name:           "event_date",
		Kind:           DimensionTemporalDay,
		Classification: ClassificationConfidential,
	}}, dims)

	fields := recipe.SystemFieldDeclarations()
	require.Equal(t, ClassificationPersonal, systemFieldClassification(t, fields, "content_hash"))
	require.Equal(t, ClassificationConfidential, systemFieldClassification(t, fields, "storage_key"))
}

func TestRecipeValidateRejectsUnsupportedV1Surface(t *testing.T) {
	base := Recipe{
		Dimensions: []DimensionRecipe{
			{Name: "event_date", Kind: DimensionTemporalDay, Extractor: probe.ExtractorConfig{Type: "json_path", JSONPath: "$.event_date"}},
			{Name: "tenant", Kind: DimensionCategorical, Extractor: probe.ExtractorConfig{Type: "json_path", JSONPath: "$.tenant"}},
		},
		ShardBy: []string{"event_date"},
	}

	unknownKind := base
	unknownKind.Dimensions = append([]DimensionRecipe(nil), base.Dimensions...)
	unknownKind.Dimensions[0].Kind = "hierarchical"
	require.ErrorContains(t, unknownKind.Validate(), "not supported by gonimbus.atlas.v1")

	compositeShard := base
	compositeShard.ShardBy = []string{"event_date", "tenant"}
	require.ErrorContains(t, compositeShard.Validate(), "supports exactly one shard_by")

	unknownClassification := base
	unknownClassification.Dimensions = append([]DimensionRecipe(nil), base.Dimensions...)
	unknownClassification.Dimensions[0].Classification = "secret"
	require.ErrorContains(t, unknownClassification.Validate(), "classification")
}

func TestRecipeValidateSystemFieldOverridesAreRaiseOnly(t *testing.T) {
	recipe := baseRecipe()
	recipe.SystemFields = map[string]FieldPolicy{
		"content_hash": {Classification: ClassificationPublic},
	}

	require.ErrorContains(t, recipe.Validate(), "cannot lower default classification")
}

func TestRecipeDigestNormalizesSystemFieldPolicies(t *testing.T) {
	left := baseRecipe()
	left.SystemFields = map[string]FieldPolicy{
		"content_hash": {Classification: " 4-PERSONAL "},
	}
	right := baseRecipe()
	right.SystemFields = map[string]FieldPolicy{
		"content_hash": {Classification: ClassificationPersonal},
	}

	leftDigest, err := left.Digest()
	require.NoError(t, err)
	rightDigest, err := right.Digest()
	require.NoError(t, err)
	require.Equal(t, rightDigest, leftDigest)
}

func baseRecipe() Recipe {
	return Recipe{
		Dimensions: []DimensionRecipe{
			{
				Name:           "event_date",
				Kind:           DimensionTemporalDay,
				Classification: ClassificationConfidential,
				Extractor:      probe.ExtractorConfig{Type: "json_path", JSONPath: "$.event_date"},
			},
		},
		ShardBy: []string{"event_date"},
	}
}

func systemFieldClassification(t *testing.T, fields []SystemFieldDeclaration, name string) string {
	t.Helper()
	for _, field := range fields {
		if field.Name == name {
			return field.Classification
		}
	}
	t.Fatalf("system field %q not found", name)
	return ""
}
