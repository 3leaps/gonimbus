package probe

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/pkg/match"
)

const (
	ReadStrategyFixedWindow   = "fixed_window"
	ReadStrategyUntilResolved = "until_resolved"

	DefaultChunkBytes int64 = 64 * 1024

	OnMissingFail       = "fail"
	OnMissingQuarantine = "quarantine"

	TransformSubstring    = "substring"
	TransformRegexCapture = "regex_capture"
	TransformFormat       = "format"
	TransformPad          = "pad"
	TransformLowercase    = "lowercase"
	TransformUppercase    = "uppercase"
	TransformLookup       = "lookup"

	LookupMatchModeRegex  = "regex"
	LookupMatchModePrefix = "prefix"
	LookupMatchModeExact  = "exact"

	MaxPadWidth = 1024
)

// Config controls content probing and derived field extraction.
//
// The intent is to support generic extraction without embedding dataset-specific logic.
// Extractors are applied to the provided byte window (usually a header read).
type Config struct {
	ReadStrategy     ReadStrategyConfig `json:"read_strategy,omitempty" yaml:"read_strategy,omitempty"`
	QuarantinePrefix string             `json:"quarantine_prefix,omitempty" yaml:"quarantine_prefix,omitempty"`
	Extract          []ExtractorConfig  `json:"extract" yaml:"extract"`
	Derived          []DerivedConfig    `json:"derived,omitempty" yaml:"derived,omitempty"`
}

type ReadStrategyConfig struct {
	Mode       string `json:"mode,omitempty" yaml:"mode,omitempty"`
	MaxBytes   string `json:"max_bytes,omitempty" yaml:"max_bytes,omitempty"`
	ChunkBytes string `json:"chunk_bytes,omitempty" yaml:"chunk_bytes,omitempty"`

	MaxBytesValue   int64 `json:"-" yaml:"-"`
	ChunkBytesValue int64 `json:"-" yaml:"-"`
}

type ExtractorConfig struct {
	Name      string `json:"name" yaml:"name"`
	Type      string `json:"type" yaml:"type"`
	Required  bool   `json:"required,omitempty" yaml:"required,omitempty"`
	OnMissing string `json:"on_missing,omitempty" yaml:"on_missing,omitempty"`

	// For type=xml_xpath.
	XPath         string   `json:"xpath" yaml:"xpath"`
	XPathPriority []string `json:"xpath_priority,omitempty" yaml:"xpath_priority,omitempty"`

	// For type=regex.
	Pattern string `json:"pattern" yaml:"pattern"`
	Group   int    `json:"group" yaml:"group"`

	// For type=json_path.
	JSONPath string `json:"json_path" yaml:"json_path"`
}

type DerivedConfig struct {
	Name      string         `json:"name" yaml:"name"`
	From      string         `json:"from" yaml:"from"`
	Transform string         `json:"transform" yaml:"transform"`
	Args      map[string]any `json:"args,omitempty" yaml:"args,omitempty"`
	Required  *bool          `json:"required,omitempty" yaml:"required,omitempty"`
	OnMissing string         `json:"on_missing,omitempty" yaml:"on_missing,omitempty"`

	lookup *configuredLookup
}

func (d DerivedConfig) RequiredValue() bool {
	if d.Required == nil {
		return true
	}
	return *d.Required
}

func (c *Config) Validate() error {
	return c.ValidateWithRewriteCaptures(nil)
}

func (c *Config) ValidateWithRewriteCaptures(rewriteCaptures []string) error {
	c.ReadStrategy.Mode = strings.TrimSpace(strings.ToLower(c.ReadStrategy.Mode))
	if c.ReadStrategy.Mode == "" {
		c.ReadStrategy.Mode = ReadStrategyFixedWindow
	}
	switch c.ReadStrategy.Mode {
	case ReadStrategyFixedWindow:
		// ok
	case ReadStrategyUntilResolved:
		if strings.TrimSpace(c.ReadStrategy.MaxBytes) == "" {
			return fmt.Errorf("read_strategy.max_bytes is required when mode=until_resolved")
		}
		maxBytes, err := match.ParseSize(c.ReadStrategy.MaxBytes)
		if err != nil {
			return fmt.Errorf("read_strategy.max_bytes invalid: %w", err)
		}
		if maxBytes <= 0 {
			return fmt.Errorf("read_strategy.max_bytes must be > 0")
		}
		c.ReadStrategy.MaxBytesValue = maxBytes
		if strings.TrimSpace(c.ReadStrategy.ChunkBytes) == "" {
			c.ReadStrategy.ChunkBytesValue = DefaultChunkBytes
		} else {
			chunkBytes, err := match.ParseSize(c.ReadStrategy.ChunkBytes)
			if err != nil {
				return fmt.Errorf("read_strategy.chunk_bytes invalid: %w", err)
			}
			if chunkBytes <= 0 {
				return fmt.Errorf("read_strategy.chunk_bytes must be > 0")
			}
			c.ReadStrategy.ChunkBytesValue = chunkBytes
		}
	default:
		return fmt.Errorf("read_strategy.mode %q is not supported", c.ReadStrategy.Mode)
	}

	needsQuarantine := false
	quarantineReason := ""
	extractNames := map[string]int{}
	seen := map[string]string{}
	rewriteCaptureNames := map[string]int{}
	for i, capture := range rewriteCaptures {
		name := strings.TrimSpace(capture)
		if name == "" || name == "_" {
			continue
		}
		if _, ok := rewriteCaptureNames[name]; ok {
			continue
		}
		rewriteCaptureNames[name] = i
		seen[name] = "rewriteFrom capture"
	}
	allDerivedNames := map[string]int{}
	for i := range c.Derived {
		name := strings.TrimSpace(c.Derived[i].Name)
		if name != "" {
			allDerivedNames[name] = i
		}
	}
	for i := range c.Extract {
		e := c.Extract[i]
		e.Name = strings.TrimSpace(e.Name)
		e.Type = strings.TrimSpace(strings.ToLower(e.Type))
		e.OnMissing = strings.TrimSpace(strings.ToLower(e.OnMissing))
		if e.Required && e.OnMissing == "" {
			e.OnMissing = OnMissingFail
		}
		c.Extract[i] = e

		if e.Name == "" {
			return fmt.Errorf("extract[%d].name is required", i)
		}
		if previous, ok := seen[e.Name]; ok {
			if previous == "rewriteFrom capture" {
				return fmt.Errorf("name %q conflicts between extract[%d] and rewriteFrom capture", e.Name, i)
			}
			if strings.HasPrefix(previous, "derived") {
				return fmt.Errorf("extract[%d].name %q conflicts with %s", i, e.Name, previous)
			}
			return fmt.Errorf("extract[%d].name %q is duplicated", i, e.Name)
		}
		seen[e.Name] = fmt.Sprintf("extract[%d]", i)
		extractNames[e.Name] = i

		switch e.Type {
		case "xml_xpath":
			hasXPath := strings.TrimSpace(e.XPath) != ""
			hasPriority := len(e.XPathPriority) > 0
			if hasXPath && hasPriority {
				return fmt.Errorf("extract[%d] must set exactly one of xpath or xpath_priority for type=xml_xpath", i)
			}
			if !hasXPath && !hasPriority {
				return fmt.Errorf("extract[%d].xpath is required for type=xml_xpath", i)
			}
			if hasXPath {
				if _, err := CompileXMLXPath(e.XPath); err != nil {
					return fmt.Errorf("extract[%d].xpath invalid: %w", i, err)
				}
			}
			if hasPriority {
				for j := range e.XPathPriority {
					candidate := strings.TrimSpace(e.XPathPriority[j])
					if candidate == "" {
						return fmt.Errorf("extract[%d].xpath_priority[%d] is required for type=xml_xpath", i, j)
					}
					e.XPathPriority[j] = candidate
					if _, err := CompileXMLXPath(candidate); err != nil {
						return fmt.Errorf("extract[%d].xpath_priority[%d] invalid: %w", i, j, err)
					}
				}
				c.Extract[i] = e
				if e.Required {
					needsQuarantine = true
					if quarantineReason == "" {
						quarantineReason = "required xpath_priority extractors can quarantine truncated fallbacks"
					}
				}
			}
		case "regex":
			if strings.TrimSpace(e.Pattern) == "" {
				return fmt.Errorf("extract[%d].pattern is required for type=regex", i)
			}
			if e.Group < 0 {
				return fmt.Errorf("extract[%d].group must be >= 0", i)
			}
			if _, err := regexp.Compile(e.Pattern); err != nil {
				return fmt.Errorf("extract[%d].pattern invalid: %w", i, err)
			}
		case "json_path":
			if c.ReadStrategy.Mode == ReadStrategyUntilResolved {
				return fmt.Errorf("extract[%d].json_path streaming not yet supported under until_resolved", i)
			}
			if strings.TrimSpace(e.JSONPath) == "" {
				return fmt.Errorf("extract[%d].json_path is required for type=json_path", i)
			}
			if _, err := CompileJSONPath(e.JSONPath); err != nil {
				return fmt.Errorf("extract[%d].json_path invalid: %w", i, err)
			}
		default:
			return fmt.Errorf("extract[%d].type %q is not supported", i, e.Type)
		}

		switch e.OnMissing {
		case "", OnMissingFail:
			// ok
		case OnMissingQuarantine:
			if !e.Required {
				return fmt.Errorf("extract[%d].on_missing=quarantine requires required=true", i)
			}
			needsQuarantine = true
			if quarantineReason == "" {
				quarantineReason = "a field uses on_missing=quarantine"
			}
		default:
			return fmt.Errorf("extract[%d].on_missing %q is not supported", i, e.OnMissing)
		}
	}
	for i := range c.Derived {
		d := c.Derived[i]
		d.Name = strings.TrimSpace(d.Name)
		d.From = strings.TrimSpace(d.From)
		d.Transform = strings.TrimSpace(strings.ToLower(d.Transform))
		d.OnMissing = strings.TrimSpace(strings.ToLower(d.OnMissing))
		if d.RequiredValue() && d.OnMissing == "" {
			d.OnMissing = OnMissingFail
		}
		c.Derived[i] = d

		if d.Name == "" {
			return fmt.Errorf("derived[%d].name is required", i)
		}
		if previous, ok := seen[d.Name]; ok {
			if previous == "rewriteFrom capture" {
				return fmt.Errorf("name %q conflicts between derived[%d] and rewriteFrom capture", d.Name, i)
			}
			return fmt.Errorf("derived[%d].name %q conflicts with %s", i, d.Name, previous)
		}
		seen[d.Name] = fmt.Sprintf("derived[%d]", i)

		if d.From == "" {
			return fmt.Errorf("derived[%d].from is required", i)
		}
		if j, ok := allDerivedNames[d.From]; ok {
			return fmt.Errorf("derived[%d].from = %q references derived[%d]; chaining is not supported; available source names: %s", i, d.From, j, formatAvailableNames(mergeAvailableNames(extractNames, rewriteCaptureNames)))
		}
		if _, ok := extractNames[d.From]; !ok {
			if _, ok := rewriteCaptureNames[d.From]; !ok {
				return fmt.Errorf("derived[%d].from = %q is unknown; available source names: %s", i, d.From, formatAvailableNames(mergeAvailableNames(extractNames, rewriteCaptureNames)))
			}
		}

		lookup, err := validateDerivedTransform(i, d)
		if err != nil {
			return err
		}
		d.lookup = lookup
		c.Derived[i] = d

		switch d.OnMissing {
		case "", OnMissingFail:
			// ok
		case OnMissingQuarantine:
			if !d.RequiredValue() {
				return fmt.Errorf("derived[%d].on_missing=quarantine requires required=true", i)
			}
			needsQuarantine = true
			if quarantineReason == "" {
				quarantineReason = "a field uses on_missing=quarantine"
			}
		default:
			return fmt.Errorf("derived[%d].on_missing %q is not supported", i, d.OnMissing)
		}
	}
	c.QuarantinePrefix = strings.TrimSpace(c.QuarantinePrefix)
	if needsQuarantine {
		if c.QuarantinePrefix == "" {
			return fmt.Errorf("quarantine_prefix is required when %s", quarantineReason)
		}
		if strings.HasPrefix(c.QuarantinePrefix, "/") {
			return fmt.Errorf("quarantine_prefix must be a relative destination prefix")
		}
		if u, err := url.Parse(c.QuarantinePrefix); err == nil && u.Scheme != "" {
			return fmt.Errorf("quarantine_prefix must be a relative destination prefix")
		}
	}
	return nil
}
