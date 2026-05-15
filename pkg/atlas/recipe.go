package atlas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/3leaps/gonimbus/pkg/probe"
)

type Recipe struct {
	Schema       string                 `json:"$schema,omitempty" yaml:"$schema,omitempty"`
	Version      string                 `json:"version,omitempty" yaml:"version,omitempty"`
	Hash         string                 `json:"hash,omitempty" yaml:"hash,omitempty"`
	Coverage     string                 `json:"coverage,omitempty" yaml:"coverage,omitempty"`
	Dimensions   []DimensionRecipe      `json:"dimensions" yaml:"dimensions"`
	ShardBy      []string               `json:"shard_by" yaml:"shard_by"`
	SystemFields map[string]FieldPolicy `json:"system_fields,omitempty" yaml:"system_fields,omitempty"`
}

type DimensionRecipe struct {
	Name           string                `json:"name" yaml:"name"`
	Kind           string                `json:"kind" yaml:"kind"`
	From           string                `json:"from,omitempty" yaml:"from,omitempty"`
	Classification string                `json:"classification,omitempty" yaml:"classification,omitempty"`
	Extractor      probe.ExtractorConfig `json:"extractor" yaml:"extractor"`
}

type FieldPolicy struct {
	Classification string `json:"classification" yaml:"classification"`
}

var dimensionNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func LoadRecipeFile(path string) (*Recipe, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- recipe path is an explicit operator CLI input.
	if err != nil {
		return nil, fmt.Errorf("read atlas recipe: %w", err)
	}
	var recipe Recipe
	if err := yaml.Unmarshal(data, &recipe); err != nil {
		return nil, fmt.Errorf("parse atlas recipe: %w", err)
	}
	if err := recipe.Validate(); err != nil {
		return nil, err
	}
	return &recipe, nil
}

func (r *Recipe) Validate() error {
	if r == nil {
		return fmt.Errorf("atlas recipe is required")
	}
	r.Version = strings.TrimSpace(r.Version)
	if r.Version == "" {
		r.Version = "1.0"
	}
	if r.Version != "1.0" {
		return fmt.Errorf("atlas recipe version %q is not supported", r.Version)
	}
	r.Hash = strings.TrimSpace(strings.ToLower(r.Hash))
	if r.Hash == "" {
		r.Hash = HashProfileSHA256
	}
	if r.Hash != HashProfileSHA256 {
		return fmt.Errorf("atlas hash profile %q is not supported", r.Hash)
	}
	r.Coverage = strings.TrimSpace(strings.ToLower(r.Coverage))
	if r.Coverage == "" {
		r.Coverage = CoverageScoped
	}
	switch r.Coverage {
	case CoverageFull, CoverageScoped:
	default:
		return fmt.Errorf("atlas coverage %q is not supported", r.Coverage)
	}
	if len(r.Dimensions) == 0 {
		return fmt.Errorf("atlas recipe requires at least one dimension")
	}
	if len(r.ShardBy) != 1 {
		return fmt.Errorf("atlas Phase A supports exactly one shard_by dimension")
	}

	seen := map[string]struct{}{}
	for i := range r.Dimensions {
		d := &r.Dimensions[i]
		d.Name = strings.TrimSpace(d.Name)
		d.Kind = strings.TrimSpace(strings.ToLower(d.Kind))
		d.Classification = strings.TrimSpace(strings.ToLower(d.Classification))
		if d.Classification == "" {
			d.Classification = ClassificationUnknown
		}
		if d.Name == "" {
			return fmt.Errorf("dimensions[%d].name is required", i)
		}
		if !dimensionNamePattern.MatchString(d.Name) {
			return fmt.Errorf("dimensions[%d].name %q must match %s", i, d.Name, dimensionNamePattern.String())
		}
		if _, ok := seen[d.Name]; ok {
			return fmt.Errorf("dimensions[%d].name %q is duplicated", i, d.Name)
		}
		seen[d.Name] = struct{}{}
		if !validDimensionKind(d.Kind) {
			return fmt.Errorf("dimensions[%d].kind %q is not supported by gonimbus.atlas.v1", i, d.Kind)
		}
		if !validClassification(d.Classification) {
			return fmt.Errorf("dimensions[%d].classification %q is not supported", i, d.Classification)
		}
		d.Extractor.Name = d.Name
		d.Extractor.Required = true
		if d.Extractor.OnMissing == "" {
			d.Extractor.OnMissing = probe.OnMissingFail
		}
		cfg := probe.Config{Extract: []probe.ExtractorConfig{d.Extractor}}
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("dimensions[%d].extractor invalid: %w", i, err)
		}
		d.Extractor = cfg.Extract[0]
	}
	for i := range r.ShardBy {
		r.ShardBy[i] = strings.TrimSpace(r.ShardBy[i])
		if _, ok := seen[r.ShardBy[i]]; !ok {
			return fmt.Errorf("shard_by[%d] references unknown dimension %q", i, r.ShardBy[i])
		}
	}
	for name, policy := range r.SystemFields {
		if !validSystemField(name) {
			return fmt.Errorf("system_fields.%s is not a supported atlas system field", name)
		}
		normalized := strings.TrimSpace(strings.ToLower(policy.Classification))
		if !validClassification(normalized) {
			return fmt.Errorf("system_fields.%s.classification %q is not supported", name, policy.Classification)
		}
		defaultClassification, ok := defaultSystemFieldClassification(name)
		if !ok {
			return fmt.Errorf("system_fields.%s is not a supported atlas system field", name)
		}
		if classificationOrdinal(normalized) < classificationOrdinal(defaultClassification) {
			return fmt.Errorf("system_fields.%s.classification %q cannot lower default classification %q", name, normalized, defaultClassification)
		}
		if r.SystemFields == nil {
			r.SystemFields = map[string]FieldPolicy{}
		}
		r.SystemFields[name] = FieldPolicy{Classification: normalized}
	}
	return nil
}

func (r Recipe) Digest() (string, error) {
	normalized := r
	if err := normalized.Validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("marshal canonical recipe: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (r Recipe) ProbeConfig() probe.Config {
	extract := make([]probe.ExtractorConfig, 0, len(r.Dimensions))
	for _, d := range r.Dimensions {
		extract = append(extract, d.Extractor)
	}
	return probe.Config{Extract: extract}
}

func (r Recipe) DimensionDeclarations() []DimensionDeclaration {
	out := make([]DimensionDeclaration, 0, len(r.Dimensions))
	for _, d := range r.Dimensions {
		out = append(out, DimensionDeclaration{Name: d.Name, Kind: d.Kind, Classification: d.Classification})
	}
	return out
}

func (r Recipe) SystemFieldDeclarations() []SystemFieldDeclaration {
	fields := DefaultSystemFields()
	overrides := map[string]string{}
	for name, policy := range r.SystemFields {
		overrides[name] = strings.TrimSpace(strings.ToLower(policy.Classification))
	}
	for i := range fields {
		if c := overrides[fields[i].Name]; c != "" {
			fields[i].Classification = c
		}
	}
	return fields
}

func validDimensionKind(v string) bool {
	switch v {
	case DimensionTemporalDay, DimensionTemporalInstant, DimensionCategorical:
		return true
	default:
		return false
	}
}

func validClassification(v string) bool {
	switch v {
	case ClassificationUnknown, ClassificationPublic, ClassificationConfidential, ClassificationBlinded,
		ClassificationProprietary, ClassificationPersonal, ClassificationPrivileged, ClassificationEyesOnly:
		return true
	default:
		return false
	}
}

func validSystemField(name string) bool {
	for _, f := range DefaultSystemFields() {
		if f.Name == name {
			return true
		}
	}
	return false
}

func defaultSystemFieldClassification(name string) (string, bool) {
	for _, f := range DefaultSystemFields() {
		if f.Name == name {
			return f.Classification, true
		}
	}
	return "", false
}

func classificationOrdinal(v string) int {
	switch v {
	case ClassificationUnknown:
		return -1
	case ClassificationPublic:
		return 0
	case ClassificationConfidential:
		return 1
	case ClassificationBlinded:
		return 2
	case ClassificationProprietary:
		return 3
	case ClassificationPersonal:
		return 4
	case ClassificationPrivileged:
		return 5
	case ClassificationEyesOnly:
		return 6
	default:
		return -2
	}
}
