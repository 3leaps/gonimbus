package scope

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/uri"
)

// Migration plan schema identity for Slot 1.3 prefix-equivalent match→scope conversion.
const (
	MatchPrefixMigrationSchema  = "gonimbus.scope.match_prefix_migration.v1"
	MatchPrefixMigrationVersion = 1
)

// Classification outcomes for a match→scope migration audit.
const (
	ClassificationConvertible       = "convertible"
	ClassificationAlreadyCompatible = "already_compatible"
	ClassificationAlreadyMigrated   = "already_migrated"
	ClassificationRefused           = "refused"
)

// Stable reason codes for classification (allowlist + refuse matrix).
const (
	ReasonConvertible           = "prefix_includes_convertible"
	ReasonAlreadyCompatible     = "default_match_no_scope"
	ReasonAlreadyMigrated       = "proposed_form_present"
	ReasonExistingScope         = "existing_scope"
	ReasonNonPrefixInclude      = "non_prefix_include"
	ReasonHasExcludes           = "has_excludes"
	ReasonHasFilters            = "has_filters"
	ReasonIncludeHidden         = "include_hidden"
	ReasonEmptyOrRootPrefix     = "empty_or_root_prefix"
	ReasonBaseAnchorMismatch    = "base_anchor_mismatch"
	ReasonEmptyLegacyPlan       = "empty_legacy_plan"
	ReasonPlanInequality        = "plan_inequality"
	ReasonInvalidBaseURI        = "invalid_base_uri"
	ReasonInvalidManifest       = "invalid_manifest"
	ReasonEmitValidateFailed    = "emit_validate_failed"
	ReasonUnsupportedScopeType  = "unsupported_scope_type"
	ReasonIdentityComputeFailed = "identity_compute_failed"
	ReasonNoIncludes            = "no_includes"
	ReasonMixedDefaultAndPrefix = "mixed_default_and_prefix"
)

// MigrationPlan is the immutable audit record for one match→scope classification.
//
// Experimental: Slot 1.3 G11 prefix-equivalent migration. Pure classification and
// conversion; no provider calls, authority, or filesystem side effects.
type MigrationPlan struct {
	Schema  string `json:"schema"`
	Version int    `json:"version"`

	Classification string `json:"classification"`
	ReasonCode     string `json:"reason_code"`
	Detail         string `json:"detail,omitempty"`

	SourceManifestDigest string `json:"source_manifest_digest"`

	CanonicalLegacyIncludes []string `json:"canonical_legacy_includes,omitempty"`

	LegacyProviderPrefixes []string `json:"legacy_provider_prefixes,omitempty"`
	LegacyPlanDigest       string   `json:"legacy_plan_digest,omitempty"`
	LegacyPlanCount        int      `json:"legacy_plan_count,omitempty"`

	ProposedScope            *manifest.IndexScopeConfig `json:"proposed_scope,omitempty"`
	ProposedManifestYAML     string                     `json:"proposed_manifest_yaml,omitempty"`
	ProposedManifestDigest   string                     `json:"proposed_manifest_digest,omitempty"`
	ProposedProviderPrefixes []string                   `json:"proposed_provider_prefixes,omitempty"`
	ProposedPlanDigest       string                     `json:"proposed_plan_digest,omitempty"`
	ProposedPlanCount        int                        `json:"proposed_plan_count,omitempty"`
	ProposedScopeHash        string                     `json:"proposed_scope_hash,omitempty"`
	LegacyConfigIdentity     *ComputedIdentityEvidence  `json:"legacy_config_identity_under_current_binary,omitempty"`
	ProposedConfigIdentity   *ComputedIdentityEvidence  `json:"proposed_config_identity_under_current_binary,omitempty"`
}

// ComputedIdentityEvidence is an identity calculation under the current binary.
// It is not authoritative for a live on-disk set unless bound to a marker/receipt.
type ComputedIdentityEvidence struct {
	Kind            string   `json:"kind"` // always "computed"
	IndexSetID      string   `json:"index_set_id"`
	CanonicalSHA256 string   `json:"canonical_sha256"`
	ScopeHash       string   `json:"scope_hash,omitempty"`
	Includes        []string `json:"includes,omitempty"`
	GonimbusVersion string   `json:"gonimbus_version,omitempty"`
}

// PlanOptions configures PlanMatchPrefixMigration.
type PlanOptions struct {
	// GonimbusVersion is injected into identity evidence so tests can hold version constant.
	// When empty, identity evidence fields are omitted (plan/list equality still runs).
	GonimbusVersion string
}

// PlanMatchPrefixMigration classifies and, when convertible, converts prefix-shaped
// match.includes into an explicit build.scope prefix_list. It never mutates disk or
// contacts a provider.
func PlanMatchPrefixMigration(raw []byte, opts PlanOptions) (*MigrationPlan, error) {
	if len(raw) == 0 {
		return refusedPlan("", ReasonInvalidManifest, "manifest bytes are empty"), nil
	}
	sourceDigest := sha256Hex(raw)

	m, err := manifest.LoadIndexManifestFromBytes(raw, "migration-input")
	if err != nil {
		return refusedPlan(sourceDigest, ReasonInvalidManifest, err.Error()), nil
	}

	plan := &MigrationPlan{
		Schema:               MatchPrefixMigrationSchema,
		Version:              MatchPrefixMigrationVersion,
		SourceManifestDigest: sourceDigest,
	}

	if m.Build != nil && m.Build.Match != nil {
		plan.CanonicalLegacyIncludes = append([]string(nil), m.Build.Match.Includes...)
	}

	// Existing scope: either already migrated (default match) or refuse combine.
	if m.Build != nil && m.Build.Scope != nil {
		if isDefaultDurableMatch(m.Build.Match) {
			if strings.TrimSpace(m.Build.Scope.Type) != "prefix_list" {
				plan.Classification = ClassificationRefused
				plan.ReasonCode = ReasonUnsupportedScopeType
				plan.Detail = fmt.Sprintf("existing scope.type %q is not prefix_list", m.Build.Scope.Type)
				return plan, nil
			}
			plan.Classification = ClassificationAlreadyMigrated
			plan.ReasonCode = ReasonAlreadyMigrated
			plan.ProposedScope = cloneScope(m.Build.Scope)
			if hash, err := HashConfig(m.Build.Scope); err == nil {
				plan.ProposedScopeHash = hash
			}
			if err := attachIdentityEvidence(plan, m, m, opts.GonimbusVersion); err != nil {
				plan.Classification = ClassificationRefused
				plan.ReasonCode = ReasonIdentityComputeFailed
				plan.Detail = err.Error()
			}
			return plan, nil
		}
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonExistingScope
		plan.Detail = "existing build.scope cannot be combined with residual match includes (intersection ≠ union)"
		return plan, nil
	}

	if refuse, code, detail := refuseResidualPredicates(m.Build); refuse {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = code
		plan.Detail = detail
		return plan, nil
	}

	includes := []string{}
	if m.Build != nil && m.Build.Match != nil {
		includes = append([]string(nil), m.Build.Match.Includes...)
	}
	if len(includes) == 0 {
		// ApplyDefaults should have filled this; treat as default.
		includes = []string{manifest.DefaultIndexIncludes}
	}

	if isDefaultIncludesOnly(includes) {
		plan.Classification = ClassificationAlreadyCompatible
		plan.ReasonCode = ReasonAlreadyCompatible
		plan.Detail = "default match includes are already durable-compatible; no scope migration required"
		if err := attachIdentityEvidence(plan, m, m, opts.GonimbusVersion); err != nil {
			plan.Classification = ClassificationRefused
			plan.ReasonCode = ReasonIdentityComputeFailed
			plan.Detail = err.Error()
		}
		return plan, nil
	}

	// Reject mix of default ** with other includes.
	for _, inc := range includes {
		if strings.TrimSpace(inc) == manifest.DefaultIndexIncludes || strings.TrimSpace(inc) == "**" {
			if !isDefaultIncludesOnly(includes) {
				plan.Classification = ClassificationRefused
				plan.ReasonCode = ReasonMixedDefaultAndPrefix
				plan.Detail = "cannot mix default ** with other includes"
				return plan, nil
			}
		}
	}

	// Grammar check: every include must be convertible prefix/**.
	for _, inc := range includes {
		if _, ok := parseConvertibleInclude(inc); !ok {
			if isEmptyOrRootConvertibleAttempt(inc) {
				plan.Classification = ClassificationRefused
				plan.ReasonCode = ReasonEmptyOrRootPrefix
				plan.Detail = fmt.Sprintf("include %q is empty or root-relative after conversion", inc)
				return plan, nil
			}
			plan.Classification = ClassificationRefused
			plan.ReasonCode = ReasonNonPrefixInclude
			plan.Detail = fmt.Sprintf("include %q is not a literal non-root prefix with terminal /**", inc)
			return plan, nil
		}
	}

	baseKey, err := baseKeyFromManifest(m)
	if err != nil {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonInvalidBaseURI
		plan.Detail = err.Error()
		return plan, nil
	}

	// Legacy provider-key plan: anchor then DerivePrefixes (parent subsumption).
	anchored := anchorIncludePatterns(baseKey, includes)
	legacyPlan := match.DerivePrefixes(anchored)
	if len(legacyPlan) == 0 {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonEmptyLegacyPlan
		plan.Detail = "legacy provider-prefix plan is empty"
		return plan, nil
	}
	// Empty string plan means full-bucket listing — not a bounded prefix migration.
	if len(legacyPlan) == 1 && legacyPlan[0] == "" {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonEmptyOrRootPrefix
		plan.Detail = "legacy plan requires a full-bucket listing (empty prefix)"
		return plan, nil
	}

	relativePrefixes, err := relativizeProviderPrefixes(baseKey, legacyPlan)
	if err != nil {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonBaseAnchorMismatch
		plan.Detail = err.Error()
		return plan, nil
	}

	proposedScope := &manifest.IndexScopeConfig{
		Type:     "prefix_list",
		Prefixes: relativePrefixes,
	}

	// Construct proposed manifest: default match + proposed scope; everything else cloned.
	proposed := cloneManifestForEmit(m)
	proposed.Build.Scope = proposedScope
	proposed.Build.Match = &manifest.IndexMatchConfig{
		Includes:      []string{manifest.DefaultIndexIncludes},
		IncludeHidden: false,
	}

	emittedYAML, err := yaml.Marshal(proposed)
	if err != nil {
		return nil, fmt.Errorf("marshal proposed manifest: %w", err)
	}

	// Serialize → reparse → re-default → revalidate (schema).
	reparsed, err := manifest.LoadIndexManifestFromBytes(emittedYAML, "migration-proposed")
	if err != nil {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonEmitValidateFailed
		plan.Detail = err.Error()
		return plan, nil
	}

	// Compile proposed plan and prove equality with legacy provider-prefix set.
	// prefix_list does not use the context or lister; TODO is for SA1012 only.
	compiled, err := Compile(context.TODO(), reparsed.Build.Scope, baseKey, nil)
	if err != nil {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonEmitValidateFailed
		plan.Detail = fmt.Sprintf("compile proposed scope: %v", err)
		return plan, nil
	}
	proposedPlan := []string(nil)
	if compiled != nil {
		proposedPlan = append([]string(nil), compiled.Prefixes...)
	}
	if !prefixSetsEqual(legacyPlan, proposedPlan) {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonPlanInequality
		plan.Detail = fmt.Sprintf("legacy plan %v != proposed plan %v", legacyPlan, proposedPlan)
		plan.LegacyProviderPrefixes = append([]string(nil), legacyPlan...)
		plan.ProposedProviderPrefixes = append([]string(nil), proposedPlan...)
		return plan, nil
	}

	scopeHash, err := HashConfig(reparsed.Build.Scope)
	if err != nil {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonEmitValidateFailed
		plan.Detail = err.Error()
		return plan, nil
	}

	plan.Classification = ClassificationConvertible
	plan.ReasonCode = ReasonConvertible
	plan.LegacyProviderPrefixes = append([]string(nil), legacyPlan...)
	plan.LegacyPlanDigest = planDigest(legacyPlan)
	plan.LegacyPlanCount = len(legacyPlan)
	plan.ProposedScope = cloneScope(reparsed.Build.Scope)
	plan.ProposedManifestYAML = string(emittedYAML)
	plan.ProposedManifestDigest = sha256Hex(emittedYAML)
	plan.ProposedProviderPrefixes = append([]string(nil), proposedPlan...)
	plan.ProposedPlanDigest = planDigest(proposedPlan)
	plan.ProposedPlanCount = len(proposedPlan)
	plan.ProposedScopeHash = scopeHash

	if err := attachIdentityEvidence(plan, m, reparsed, opts.GonimbusVersion); err != nil {
		plan.Classification = ClassificationRefused
		plan.ReasonCode = ReasonIdentityComputeFailed
		plan.Detail = err.Error()
		// Clear emit fields on identity failure so operators do not consume a partial plan.
		plan.ProposedManifestYAML = ""
		return plan, nil
	}

	// Identity must differ when version is held constant.
	if plan.LegacyConfigIdentity != nil && plan.ProposedConfigIdentity != nil {
		if plan.LegacyConfigIdentity.IndexSetID == plan.ProposedConfigIdentity.IndexSetID {
			plan.Classification = ClassificationRefused
			plan.ReasonCode = ReasonIdentityComputeFailed
			plan.Detail = "legacy and proposed identities are equal; migration must change index-set identity"
			plan.ProposedManifestYAML = ""
			return plan, nil
		}
	}

	return plan, nil
}

// ProjectKeys applies match configuration as an ingest predicate against full
// provider keys (anchored includes, same as index build). Used by independent
// equivalence oracles — not by the converter's LIST-plan proof.
func ProjectKeys(baseKey string, matchCfg *manifest.IndexMatchConfig, providerKeys []string) ([]string, error) {
	if matchCfg == nil {
		matchCfg = &manifest.IndexMatchConfig{Includes: []string{manifest.DefaultIndexIncludes}}
	}
	includes := matchCfg.Includes
	if len(includes) == 0 {
		includes = []string{manifest.DefaultIndexIncludes}
	}
	cfg := match.Config{
		Includes:      anchorIncludePatterns(baseKey, includes),
		Excludes:      anchorIncludePatterns(baseKey, matchCfg.Excludes),
		IncludeHidden: matchCfg.IncludeHidden,
	}
	matcher, err := match.New(cfg)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, key := range providerKeys {
		if matcher.Match(key) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ProjectKeysUnderScope models post-migration durable ingest: LIST is bounded by
// compiled scope prefixes and default match (base-anchored ** , include_hidden=false)
// is applied to full provider keys.
func ProjectKeysUnderScope(baseKey string, scopeCfg *manifest.IndexScopeConfig, providerKeys []string) ([]string, error) {
	plan, err := Compile(context.TODO(), scopeCfg, baseKey, nil)
	if err != nil {
		return nil, err
	}
	var prefixes []string
	if plan != nil {
		prefixes = plan.Prefixes
	}
	defaultMatch, err := match.New(match.Config{
		Includes:      anchorIncludePatterns(baseKey, []string{manifest.DefaultIndexIncludes}),
		IncludeHidden: false,
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, key := range providerKeys {
		if !keyUnderAnyPrefix(key, prefixes) {
			continue
		}
		if defaultMatch.Match(key) {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out, nil
}

func keyUnderAnyPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if p == "" {
			return true
		}
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

func refusedPlan(sourceDigest, code, detail string) *MigrationPlan {
	return &MigrationPlan{
		Schema:               MatchPrefixMigrationSchema,
		Version:              MatchPrefixMigrationVersion,
		Classification:       ClassificationRefused,
		ReasonCode:           code,
		Detail:               detail,
		SourceManifestDigest: sourceDigest,
	}
}

func refuseResidualPredicates(build *manifest.IndexBuildConfig) (bool, string, string) {
	if build == nil || build.Match == nil {
		return false, "", ""
	}
	mc := build.Match
	if len(mc.Excludes) > 0 {
		return true, ReasonHasExcludes, "build.match.excludes is not convertible in Slot 1.3"
	}
	if mc.Filters != nil {
		return true, ReasonHasFilters, "build.match.filters is not convertible in Slot 1.3 (including empty filter objects)"
	}
	if mc.IncludeHidden {
		return true, ReasonIncludeHidden, "build.match.include_hidden=true is not convertible in Slot 1.3"
	}
	return false, "", ""
}

func isDefaultDurableMatch(mc *manifest.IndexMatchConfig) bool {
	if mc == nil {
		return true
	}
	if len(mc.Excludes) > 0 || mc.Filters != nil || mc.IncludeHidden {
		return false
	}
	return isDefaultIncludesOnly(mc.Includes)
}

func isDefaultIncludesOnly(includes []string) bool {
	if len(includes) == 0 {
		return true
	}
	if len(includes) != 1 {
		return false
	}
	trimmed := strings.TrimSpace(includes[0])
	return trimmed == manifest.DefaultIndexIncludes || trimmed == "**"
}

// parseConvertibleInclude accepts only literal non-root prefix + terminal /**.
// Returns the literal relative prefix without trailing slash and without leading slash.
func parseConvertibleInclude(raw string) (literal string, ok bool) {
	normalized := match.NormalizePattern(strings.TrimSpace(raw))
	if normalized == "" {
		return "", false
	}
	if !strings.HasSuffix(normalized, "/**") {
		return "", false
	}
	body := strings.TrimSuffix(normalized, "/**")
	body = strings.TrimPrefix(body, "/")
	if body == "" {
		return "", false
	}
	// Refuse any glob / escape metacharacters in the literal body.
	if match.IsGlobPattern(body) {
		return "", false
	}
	if strings.ContainsAny(body, `*?[{}\`) {
		return "", false
	}
	// No path cleanup: cloud keys are opaque; only reject empty segments that are
	// clearly not a usable prefix marker? Guidance: no filepath.Clean. Allow // as
	// opaque — but empty segment-only bodies already rejected.
	return body, true
}

func isEmptyOrRootConvertibleAttempt(raw string) bool {
	normalized := match.NormalizePattern(strings.TrimSpace(raw))
	return normalized == "/**" || normalized == "**/" || normalized == "/"
}

func baseKeyFromManifest(m *manifest.IndexManifest) (string, error) {
	if m == nil {
		return "", errors.New("manifest is nil")
	}
	baseURI := strings.TrimSpace(m.Connection.BaseURI)
	if baseURI == "" {
		return "", errors.New("connection.base_uri is required")
	}
	parsed, err := uri.ParseURI(baseURI)
	if err != nil {
		return "", fmt.Errorf("parse base_uri: %w", err)
	}
	if !parsed.IsPrefix() {
		return "", fmt.Errorf("base_uri path must end with '/': %s", baseURI)
	}
	key := parsed.Key
	if key != "" && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	return key, nil
}

// anchorIncludePatterns mirrors index build prefixPatterns: if an include already
// begins with the base key prefix, do not double-prefix.
func anchorIncludePatterns(basePrefix string, patterns []string) []string {
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

func relativizeProviderPrefixes(baseKey string, providerPrefixes []string) ([]string, error) {
	if baseKey != "" && !strings.HasSuffix(baseKey, "/") {
		baseKey += "/"
	}
	out := make([]string, 0, len(providerPrefixes))
	for _, p := range providerPrefixes {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, errors.New("empty provider prefix cannot be relativized under base")
		}
		if baseKey != "" {
			if !strings.HasPrefix(p, baseKey) {
				return nil, fmt.Errorf("provider prefix %q is not under base key %q", p, baseKey)
			}
			rel := strings.TrimPrefix(p, baseKey)
			if rel == "" {
				return nil, fmt.Errorf("provider prefix %q relativizes to empty/root under base %q", p, baseKey)
			}
			p = rel
		}
		p = strings.TrimPrefix(p, "/")
		if p == "" {
			return nil, errors.New("relative prefix is empty/root")
		}
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		out = append(out, p)
	}
	// Exact-dedupe + sort for stable scope hash (subsumption already applied by DerivePrefixes).
	return normalizePrefixes(out), nil
}

func prefixSetsEqual(a, b []string) bool {
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

func planDigest(prefixes []string) string {
	sorted := append([]string(nil), prefixes...)
	sort.Strings(sorted)
	payload, _ := json.Marshal(sorted)
	return sha256Hex(payload)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func cloneScope(cfg *manifest.IndexScopeConfig) *manifest.IndexScopeConfig {
	if cfg == nil {
		return nil
	}
	out := *cfg
	if cfg.Prefixes != nil {
		out.Prefixes = append([]string(nil), cfg.Prefixes...)
	}
	if cfg.Scopes != nil {
		out.Scopes = make([]manifest.IndexScopeConfig, len(cfg.Scopes))
		for i := range cfg.Scopes {
			cloned := cloneScope(&cfg.Scopes[i])
			if cloned != nil {
				out.Scopes[i] = *cloned
			}
		}
	}
	return &out
}

func cloneManifestForEmit(m *manifest.IndexManifest) *manifest.IndexManifest {
	// Round-trip via YAML to deep-copy without sharing pointers, then adjust build.
	b, err := yaml.Marshal(m)
	if err != nil {
		// Fallback shallow structure.
		out := *m
		if m.Build != nil {
			bc := *m.Build
			out.Build = &bc
		}
		return &out
	}
	var out manifest.IndexManifest
	if err := yaml.Unmarshal(b, &out); err != nil {
		cp := *m
		return &cp
	}
	if out.Build == nil {
		out.Build = &manifest.IndexBuildConfig{}
	}
	return &out
}

func attachIdentityEvidence(plan *MigrationPlan, legacy, proposed *manifest.IndexManifest, gonimbusVersion string) error {
	if strings.TrimSpace(gonimbusVersion) == "" {
		return nil
	}
	legacyID, err := computeConfigIdentity(legacy, gonimbusVersion)
	if err != nil {
		return fmt.Errorf("legacy identity: %w", err)
	}
	proposedID, err := computeConfigIdentity(proposed, gonimbusVersion)
	if err != nil {
		return fmt.Errorf("proposed identity: %w", err)
	}
	plan.LegacyConfigIdentity = legacyID
	plan.ProposedConfigIdentity = proposedID
	return nil
}

func computeConfigIdentity(m *manifest.IndexManifest, gonimbusVersion string) (*ComputedIdentityEvidence, error) {
	if m == nil {
		return nil, errors.New("manifest is nil")
	}
	var scopeHash string
	var includes []string
	includeHidden := false
	var excludes []string
	sourceType := manifest.DefaultIndexSource
	if m.Build != nil {
		if m.Build.Source != "" {
			sourceType = m.Build.Source
		}
		if m.Build.Scope != nil {
			h, err := HashConfig(m.Build.Scope)
			if err != nil {
				return nil, err
			}
			scopeHash = h
		}
		if m.Build.Match != nil {
			includes = append([]string(nil), m.Build.Match.Includes...)
			excludes = append([]string(nil), m.Build.Match.Excludes...)
			includeHidden = m.Build.Match.IncludeHidden
		}
	}
	if len(includes) == 0 {
		includes = []string{manifest.DefaultIndexIncludes}
	}

	params := indexstore.IndexSetParams{
		BaseURI:  m.Connection.BaseURI,
		Provider: m.Connection.Provider,
		Endpoint: m.Connection.Endpoint,
		BuildParams: indexstore.BuildParams{
			SourceType:      sourceType,
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: gonimbusVersion,
			Includes:        includes,
			Excludes:        excludes,
			IncludeHidden:   includeHidden,
			ScopeHash:       scopeHash,
		},
	}
	if m.Identity != nil {
		params.StorageProvider = m.Identity.StorageProvider
		params.CloudProvider = m.Identity.CloudProvider
		params.RegionKind = m.Identity.RegionKind
		params.Region = m.Identity.Region
		params.EndpointHost = m.Identity.EndpointHost
	}
	if m.PathDate != nil {
		params.BuildParams.PathDateExtraction = &indexstore.PathDateExtraction{
			Method:       m.PathDate.Method,
			Regex:        m.PathDate.Regex,
			SegmentIndex: m.PathDate.SegmentIndex,
		}
	}

	result, err := indexstore.ComputeIndexSetID(params)
	if err != nil {
		return nil, err
	}
	return &ComputedIdentityEvidence{
		Kind:            "computed",
		IndexSetID:      result.IndexSetID,
		CanonicalSHA256: result.CanonicalSHA256,
		ScopeHash:       scopeHash,
		Includes:        includes,
		GonimbusVersion: gonimbusVersion,
	}, nil
}
