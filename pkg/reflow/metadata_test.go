package reflow

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func sourceMeta() *provider.ObjectMeta {
	return &provider.ObjectMeta{
		ObjectSummary: provider.ObjectSummary{
			Key:          "src/object.xml",
			Size:         42,
			ETag:         "etag-123",
			LastModified: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		ContentType:  "application/xml",
		StorageClass: "STANDARD_IA",
		Metadata: map[string]string{
			"Owner":   "team-blue",
			"payload": `{"site":"S-9","nested":{"x":1}}`,
			"encoded": "a%20b",
		},
	}
}

func TestMetadataPlanPutOptionsPolicies(t *testing.T) {
	t.Run("clear policy ignores source metadata", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyClear}.PutOptions(nil)
		require.NoError(t, err)
		require.Nil(t, opts.UserMetadata)
	})

	t.Run("preserve canonicalizes source user metadata", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyPreserve}.PutOptions(sourceMeta())
		require.NoError(t, err)
		require.Equal(t, "team-blue", opts.UserMetadata["owner"], "keys are lower-cased")
		require.NotContains(t, opts.UserMetadata, "Owner")
	})

	t.Run("preserve requires source", func(t *testing.T) {
		_, err := MetadataPlan{Policy: MetadataPolicyPreserve}.PutOptions(nil)
		require.Error(t, err)
	})

	t.Run("set overrides win over preserved keys", func(t *testing.T) {
		plan := MetadataPlan{Policy: MetadataPolicyMerge, Set: map[string]string{"owner": "override"}}
		opts, err := plan.PutOptions(sourceMeta())
		require.NoError(t, err)
		require.Equal(t, "override", opts.UserMetadata["owner"])
	})

	t.Run("preserve content type", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyClear, PreserveContentType: true}.PutOptions(sourceMeta())
		require.NoError(t, err)
		require.Equal(t, "application/xml", opts.ContentType)
	})

	t.Run("fixed storage class is upper-cased", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyClear, DestinationStorageClass: "glacier_ir"}.PutOptions(nil)
		require.NoError(t, err)
		require.Equal(t, "GLACIER_IR", opts.StorageClass)
	})

	t.Run("propagate storage class from source", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyClear, DestinationStorageClass: MetadataStorageClassPropagate}.PutOptions(sourceMeta())
		require.NoError(t, err)
		require.Equal(t, "STANDARD_IA", opts.StorageClass)
	})

	t.Run("propagate requires source", func(t *testing.T) {
		_, err := MetadataPlan{Policy: MetadataPolicyClear, DestinationStorageClass: MetadataStorageClassPropagate}.PutOptions(nil)
		require.Error(t, err)
	})
}

func TestMetadataPlanPerObjectRules(t *testing.T) {
	srcRules, err := ParseMetadataSourceKeyRules([]string{"dest-owner=owner"})
	require.NoError(t, err)
	derived, err := ParseMetadataDerivedRules([]string{
		`label="site:" + meta.payload.site`,
		"src_etag=system.etag",
	})
	require.NoError(t, err)

	plan := MetadataPlan{
		Policy:          MetadataPolicyClear,
		SourceKeyRules:  srcRules,
		DerivedRules:    derived,
		OnMissingSource: MetadataOnMissingFail,
	}
	require.True(t, plan.HasPerObjectRules())
	require.True(t, plan.NeedsSourceHead())
	require.True(t, plan.RequiresCapability())

	opts, err := plan.PutOptions(sourceMeta())
	require.NoError(t, err)
	require.Equal(t, "team-blue", opts.UserMetadata["dest-owner"])
	require.Equal(t, "site:S-9", opts.UserMetadata["label"])
	require.Equal(t, "etag-123", opts.UserMetadata["src_etag"])
}

func TestMetadataPlanOnMissingPolicies(t *testing.T) {
	rules, err := ParseMetadataDerivedRules([]string{"missing=meta.absent.field"})
	require.NoError(t, err)

	t.Run("skip omits the key", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyClear, DerivedRules: rules, OnMissingSource: MetadataOnMissingSkip}.PutOptions(sourceMeta())
		require.NoError(t, err)
		require.NotContains(t, opts.UserMetadata, "missing")
	})

	t.Run("empty writes empty string", func(t *testing.T) {
		opts, err := MetadataPlan{Policy: MetadataPolicyClear, DerivedRules: rules, OnMissingSource: MetadataOnMissingEmpty}.PutOptions(sourceMeta())
		require.NoError(t, err)
		require.Equal(t, "", opts.UserMetadata["missing"])
	})

	t.Run("fail returns derivation error", func(t *testing.T) {
		_, err := MetadataPlan{Policy: MetadataPolicyClear, DerivedRules: rules, OnMissingSource: MetadataOnMissingFail}.PutOptions(sourceMeta())
		var derivErr *MetadataDerivationError
		require.ErrorAs(t, err, &derivErr)
		require.Equal(t, "missing", derivErr.DestKey)
		require.Contains(t, derivErr.Details(), "metadata_dest_key")
	})
}

func TestMetadataPlanSourceCanonicalCollision(t *testing.T) {
	src := sourceMeta()
	// Two keys collide after lower-casing — preserve must surface a collision.
	src.Metadata = map[string]string{"Owner": "a", "owner": "b"}
	_, err := MetadataPlan{Policy: MetadataPolicyPreserve}.PutOptions(src)
	var collision *SourceMetadataCollisionError
	require.ErrorAs(t, err, &collision)
	require.ElementsMatch(t, []string{"Owner", "owner"}, collision.Keys)
}

func TestParseMetadataRulesValidation(t *testing.T) {
	_, err := ParseMetadataSourceKeyRules([]string{"no-equals"})
	require.ErrorContains(t, err, "dest=source")

	// "*" is not a valid metadata token, so it is rejected before the dedicated
	// wildcard guard; the derived-rule path exercises the ".*" wildcard guard.
	_, err = ParseMetadataSourceKeyRules([]string{"dest=*"})
	require.ErrorContains(t, err, "non-empty keys without whitespace")

	_, err = ParseMetadataDerivedRules([]string{"dest=meta.payload.*"})
	require.ErrorContains(t, err, "wildcard")

	_, err = ParseMetadataDerivedRules([]string{"dest=unknownfunc(x)"})
	require.ErrorContains(t, err, "invalid")

	src, err := ParseMetadataSourceKeyRules([]string{"dup=a"})
	require.NoError(t, err)
	der, err := ParseMetadataDerivedRules([]string{"dup=system.etag"})
	require.NoError(t, err)
	require.ErrorContains(t, ValidatePerObjectMetadataRules(src, der), "duplicate per-object metadata destination key")
}

func TestValidateMetadataBudget(t *testing.T) {
	require.NoError(t, ValidateMetadataBudget(map[string]string{"k": "v"}))

	over := map[string]string{"big": strings.Repeat("x", 9*1024)}
	err := ValidateMetadataBudget(over)
	var budgetErr *MetadataBudgetError
	require.ErrorAs(t, err, &budgetErr)
	require.Contains(t, budgetErr.OverLimitKeys, "big")
	require.Contains(t, budgetErr.Details(), "metadata_total_bytes")
	require.True(t, errors.As(err, &budgetErr))
}

func TestMetadataPlanValidate(t *testing.T) {
	dupSrc, err := ParseMetadataSourceKeyRules([]string{"dup=a"})
	require.NoError(t, err)
	dupDerived, err := ParseMetadataDerivedRules([]string{"dup=system.etag"})
	require.NoError(t, err)

	cases := []struct {
		name    string
		plan    MetadataPlan
		wantErr string
	}{
		{"valid clear", MetadataPlan{Policy: MetadataPolicyClear}, ""},
		{"valid propagate", MetadataPlan{Policy: MetadataPolicyMerge, DestinationStorageClass: MetadataStorageClassPropagate}, ""},
		{"valid fixed class", MetadataPlan{Policy: MetadataPolicyClear, DestinationStorageClass: "standard_ia"}, ""},
		{"unknown policy", MetadataPlan{Policy: "copy"}, "metadata-policy must be one of"},
		{"unknown on-missing", MetadataPlan{Policy: MetadataPolicyClear, OnMissingSource: "warn"}, "metadata-on-missing-source must be one of"},
		{"invalid fixed class", MetadataPlan{Policy: MetadataPolicyClear, DestinationStorageClass: "GLACIER"}, "not a valid PUT target"},
		{"empty set key", MetadataPlan{Policy: MetadataPolicyClear, Set: map[string]string{"": ""}}, "non-empty key=value"},
		{"whitespace set key", MetadataPlan{Policy: MetadataPolicyClear, Set: map[string]string{"a b": "v"}}, "without whitespace"},
		{"duplicate rule dest key", MetadataPlan{Policy: MetadataPolicyClear, SourceKeyRules: dupSrc, DerivedRules: dupDerived}, "duplicate per-object metadata destination key"},
		{"malformed source rule", MetadataPlan{Policy: MetadataPolicyClear, SourceKeyRules: []MetadataSourceKeyRule{{DestKey: "", SourceKey: "x"}}}, "invalid dest/source key"},
		{"derived rule missing compiled expr", MetadataPlan{Policy: MetadataPolicyClear, DerivedRules: []MetadataDerivedRule{{DestKey: "x", Expression: "system.etag"}}}, "no compiled expression"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.plan.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestMetadataPlanPutOptionsFailsClosed(t *testing.T) {
	// A direct library caller that skips Validate must not get silent-clear or an
	// unvalidated storage class handed to a provider.
	_, err := MetadataPlan{Policy: "copy"}.PutOptions(sourceMeta())
	require.ErrorContains(t, err, "metadata-policy must be one of")

	_, err = MetadataPlan{Policy: MetadataPolicyClear, DestinationStorageClass: "GLACIER"}.PutOptions(nil)
	require.ErrorContains(t, err, "not a valid PUT target")

	// A hand-built derived rule (no compiled expression) must error, not panic,
	// even when the caller never ran Validate.
	require.NotPanics(t, func() {
		plan := MetadataPlan{
			Policy:          MetadataPolicyClear,
			OnMissingSource: MetadataOnMissingSkip,
			DerivedRules:    []MetadataDerivedRule{{DestKey: "x", Expression: "system.etag"}},
		}
		_, err = plan.PutOptions(sourceMeta())
	})
	require.ErrorContains(t, err, "no compiled expression")
}

func TestIsValidPutStorageClass(t *testing.T) {
	require.True(t, IsValidPutStorageClass("standard"))
	require.True(t, IsValidPutStorageClass("GLACIER_IR"))
	require.False(t, IsValidPutStorageClass("GLACIER"))
	require.False(t, IsValidPutStorageClass(""))
}
