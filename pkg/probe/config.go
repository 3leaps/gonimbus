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
)

// Config controls content probing and derived field extraction.
//
// The intent is to support generic extraction without embedding dataset-specific logic.
// Extractors are applied to the provided byte window (usually a header read).
type Config struct {
	ReadStrategy     ReadStrategyConfig `json:"read_strategy,omitempty" yaml:"read_strategy,omitempty"`
	QuarantinePrefix string             `json:"quarantine_prefix,omitempty" yaml:"quarantine_prefix,omitempty"`
	Extract          []ExtractorConfig  `json:"extract" yaml:"extract"`
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
	XPath string `json:"xpath" yaml:"xpath"`

	// For type=regex.
	Pattern string `json:"pattern" yaml:"pattern"`
	Group   int    `json:"group" yaml:"group"`

	// For type=json_path.
	JSONPath string `json:"json_path" yaml:"json_path"`
}

func (c *Config) Validate() error {
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
	seen := map[string]struct{}{}
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
		if _, ok := seen[e.Name]; ok {
			return fmt.Errorf("extract[%d].name %q is duplicated", i, e.Name)
		}
		seen[e.Name] = struct{}{}

		switch e.Type {
		case "xml_xpath":
			if strings.TrimSpace(e.XPath) == "" {
				return fmt.Errorf("extract[%d].xpath is required for type=xml_xpath", i)
			}
			if _, err := CompileXMLXPath(e.XPath); err != nil {
				return fmt.Errorf("extract[%d].xpath invalid: %w", i, err)
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
		default:
			return fmt.Errorf("extract[%d].on_missing %q is not supported", i, e.OnMissing)
		}
	}
	c.QuarantinePrefix = strings.TrimSpace(c.QuarantinePrefix)
	if needsQuarantine {
		if c.QuarantinePrefix == "" {
			return fmt.Errorf("quarantine_prefix is required when any extractor uses on_missing=quarantine")
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
