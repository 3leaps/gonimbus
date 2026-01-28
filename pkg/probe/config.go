package probe

import (
	"fmt"
	"regexp"
	"strings"
)

// Config controls content probing and derived field extraction.
//
// The intent is to support generic extraction without embedding dataset-specific logic.
// Extractors are applied to the provided byte window (usually a header read).
type Config struct {
	Extract []ExtractorConfig `json:"extract" yaml:"extract"`
}

type ExtractorConfig struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`

	// For type=xml_xpath.
	XPath string `json:"xpath" yaml:"xpath"`

	// For type=regex.
	Pattern string `json:"pattern" yaml:"pattern"`
	Group   int    `json:"group" yaml:"group"`

	// For type=json_path.
	JSONPath string `json:"json_path" yaml:"json_path"`
}

func (c *Config) Validate() error {
	seen := map[string]struct{}{}
	for i := range c.Extract {
		e := c.Extract[i]
		e.Name = strings.TrimSpace(e.Name)
		e.Type = strings.TrimSpace(strings.ToLower(e.Type))
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
			if strings.TrimSpace(e.JSONPath) == "" {
				return fmt.Errorf("extract[%d].json_path is required for type=json_path", i)
			}
			if _, err := CompileJSONPath(e.JSONPath); err != nil {
				return fmt.Errorf("extract[%d].json_path invalid: %w", i, err)
			}
		default:
			return fmt.Errorf("extract[%d].type %q is not supported", i, e.Type)
		}
	}
	return nil
}
