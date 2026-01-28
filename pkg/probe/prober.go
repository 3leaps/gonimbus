package probe

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Prober executes configured extractors against a byte window.
type Prober struct {
	extractors []extractor
}

type extractor interface {
	Name() string
	Extract(data []byte) (string, bool, error)
}

func New(cfg Config) (*Prober, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	extractors := make([]extractor, 0, len(cfg.Extract))
	for _, e := range cfg.Extract {
		switch e.Type {
		case "xml_xpath":
			x, err := CompileXMLXPath(e.XPath)
			if err != nil {
				return nil, err
			}
			extractors = append(extractors, &xmlXPathExtractor{name: e.Name, xpath: x})
		case "regex":
			re, err := regexp.Compile(e.Pattern)
			if err != nil {
				return nil, err
			}
			extractors = append(extractors, &regexExtractor{name: e.Name, re: re, group: e.Group})
		case "json_path":
			p, err := CompileJSONPath(e.JSONPath)
			if err != nil {
				return nil, err
			}
			extractors = append(extractors, &jsonPathExtractor{name: e.Name, path: p})
		default:
			return nil, fmt.Errorf("unsupported extractor type %q", e.Type)
		}
	}

	return &Prober{extractors: extractors}, nil
}

// Probe returns derived fields. Missing fields are omitted.
func (p *Prober) Probe(data []byte) (map[string]string, error) {
	out := map[string]string{}
	for _, ex := range p.extractors {
		v, ok, err := ex.Extract(data)
		if err != nil {
			return nil, fmt.Errorf("extract %s: %w", ex.Name(), err)
		}
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		out[ex.Name()] = v
	}
	return out, nil
}

type xmlXPathExtractor struct {
	name  string
	xpath *XMLXPath
}

func (e *xmlXPathExtractor) Name() string { return e.name }

func (e *xmlXPathExtractor) Extract(data []byte) (string, bool, error) {
	if e.xpath == nil {
		return "", false, fmt.Errorf("xpath is nil")
	}
	v, ok, err := e.xpath.FindFirstText(data)
	return v, ok, err
}

type regexExtractor struct {
	name  string
	re    *regexp.Regexp
	group int
}

func (e *regexExtractor) Name() string { return e.name }

func (e *regexExtractor) Extract(data []byte) (string, bool, error) {
	if e.re == nil {
		return "", false, fmt.Errorf("regex is nil")
	}
	m := e.re.FindSubmatch(data)
	if len(m) == 0 {
		return "", false, nil
	}
	if e.group < 0 || e.group >= len(m) {
		return "", false, fmt.Errorf("group %d out of range", e.group)
	}
	return string(m[e.group]), true, nil
}

type jsonPathExtractor struct {
	name string
	path *JSONPath
}

func (e *jsonPathExtractor) Name() string { return e.name }

func (e *jsonPathExtractor) Extract(data []byte) (string, bool, error) {
	if e.path == nil {
		return "", false, fmt.Errorf("json path is nil")
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return "", false, err
	}
	got, ok := e.path.Eval(v)
	if !ok {
		return "", false, nil
	}
	// Convert scalar-ish values.
	switch x := got.(type) {
	case string:
		return x, true, nil
	case float64, bool:
		b, err := json.Marshal(x)
		if err != nil {
			return "", false, err
		}
		return string(b), true, nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return "", false, err
		}
		return string(b), true, nil
	}
}
