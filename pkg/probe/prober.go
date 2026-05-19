package probe

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

const (
	TerminationAllRequiredResolved = "all_required_resolved"
	TerminationMaxBytesReached     = "max_bytes_reached"
	TerminationStreamExhausted     = "stream_exhausted"
	TerminationParseError          = "parse_error"
)

// Prober executes configured extractors against a byte window.
type Prober struct {
	extractors []configuredExtractor
	derived    []configuredDerived
}

type configuredExtractor struct {
	cfg       ExtractorConfig
	extractor extractor
}

type extractor interface {
	Name() string
	Extract(data []byte) (string, bool, error)
}

type ExtractorAudit struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	Resolved          bool   `json:"resolved"`
	Required          bool   `json:"required"`
	OnMissing         string `json:"on_missing,omitempty"`
	BytesAtResolution *int64 `json:"bytes_at_resolution"`
}

type ProbeAudit struct {
	Extractors        []ExtractorAudit `json:"extractors"`
	BytesRead         int64            `json:"bytes_read"`
	TerminationReason string           `json:"termination_reason"`
}

type Result struct {
	Vars     map[string]string
	Audit    ProbeAudit
	Failures map[string]error
}

func New(cfg Config) (*Prober, error) {
	return NewWithRewriteCaptures(cfg, nil)
}

func NewWithRewriteCaptures(cfg Config, rewriteCaptures []string) (*Prober, error) {
	if err := cfg.ValidateWithRewriteCaptures(rewriteCaptures); err != nil {
		return nil, err
	}
	return newValidatedProber(cfg)
}

// NewNormalizedWithRewriteCaptures validates cfg in place and builds a prober
// from the normalized config. Callers that need config-derived runtime values
// after construction, such as read_strategy byte limits, should use this form.
func NewNormalizedWithRewriteCaptures(cfg *Config, rewriteCaptures []string) (*Prober, error) {
	if cfg == nil {
		return nil, fmt.Errorf("probe config is nil")
	}
	if err := cfg.ValidateWithRewriteCaptures(rewriteCaptures); err != nil {
		return nil, err
	}
	return newValidatedProber(*cfg)
}

func newValidatedProber(cfg Config) (*Prober, error) {
	extractors := make([]configuredExtractor, 0, len(cfg.Extract))
	for _, e := range cfg.Extract {
		var ex extractor
		switch e.Type {
		case "xml_xpath":
			x, err := CompileXMLXPath(e.XPath)
			if err != nil {
				return nil, err
			}
			ex = &xmlXPathExtractor{name: e.Name, xpath: x}
		case "regex":
			re, err := regexp.Compile(e.Pattern)
			if err != nil {
				return nil, err
			}
			ex = &regexExtractor{name: e.Name, re: re, group: e.Group}
		case "json_path":
			p, err := CompileJSONPath(e.JSONPath)
			if err != nil {
				return nil, err
			}
			ex = &jsonPathExtractor{name: e.Name, path: p}
		default:
			return nil, fmt.Errorf("unsupported extractor type %q", e.Type)
		}
		extractors = append(extractors, configuredExtractor{cfg: e, extractor: ex})
	}

	derived := make([]configuredDerived, 0, len(cfg.Derived))
	for _, d := range cfg.Derived {
		cd, err := newConfiguredDerived(d)
		if err != nil {
			return nil, err
		}
		derived = append(derived, cd)
	}

	return &Prober{extractors: extractors, derived: derived}, nil
}

// Probe returns derived fields. Missing fields are omitted.
func (p *Prober) Probe(data []byte) (map[string]string, error) {
	res, err := p.ProbeDetailed(data, int64(len(data)), "")
	if err != nil {
		return nil, err
	}
	return res.Vars, nil
}

func (p *Prober) ProbeDetailed(data []byte, bytesRead int64, terminationReason string) (*Result, error) {
	return p.ProbeDetailedWithVars(data, bytesRead, terminationReason, nil)
}

func (p *Prober) ProbeDetailedWithVars(data []byte, bytesRead int64, terminationReason string, initialVars map[string]string) (*Result, error) {
	return p.probeDetailed(data, bytesRead, terminationReason, initialVars, nil)
}

func (p *Prober) ProbeDetailedAllowIncomplete(data []byte, bytesRead int64, terminationReason string, incomplete func(error) bool) (*Result, error) {
	return p.ProbeDetailedAllowIncompleteWithVars(data, bytesRead, terminationReason, nil, incomplete)
}

func (p *Prober) ProbeDetailedAllowIncompleteWithVars(data []byte, bytesRead int64, terminationReason string, initialVars map[string]string, incomplete func(error) bool) (*Result, error) {
	return p.probeDetailed(data, bytesRead, terminationReason, initialVars, incomplete)
}

func (p *Prober) probeDetailed(data []byte, bytesRead int64, terminationReason string, initialVars map[string]string, incomplete func(error) bool) (*Result, error) {
	out := map[string]string{}
	for k, v := range initialVars {
		if strings.TrimSpace(v) != "" {
			out[k] = strings.TrimSpace(v)
		}
	}
	failures := map[string]error{}
	audit := make([]ExtractorAudit, 0, len(p.extractors))
	for _, entry := range p.extractors {
		ex := entry.extractor
		v, ok, err := ex.Extract(data)
		if err != nil {
			if incomplete != nil && incomplete(err) {
				audit = append(audit, ExtractorAudit{
					Name:      entry.cfg.Name,
					Type:      entry.cfg.Type,
					Required:  entry.cfg.Required,
					OnMissing: entry.cfg.OnMissing,
				})
				continue
			}
			return nil, fmt.Errorf("extract %s: %w", ex.Name(), err)
		}
		item := ExtractorAudit{
			Name:      entry.cfg.Name,
			Type:      entry.cfg.Type,
			Required:  entry.cfg.Required,
			OnMissing: entry.cfg.OnMissing,
		}
		if !ok {
			audit = append(audit, item)
			continue
		}
		v = strings.TrimSpace(v)
		if v == "" {
			audit = append(audit, item)
			continue
		}
		out[ex.Name()] = v
		at := bytesRead
		item.Resolved = true
		item.BytesAtResolution = &at
		audit = append(audit, item)
	}
	for _, d := range p.derived {
		v, ok, err := d.derive(out)
		if err != nil {
			failures[d.cfg.Name] = err
			continue
		}
		if !ok || strings.TrimSpace(v) == "" {
			continue
		}
		out[d.cfg.Name] = strings.TrimSpace(v)
	}
	return &Result{
		Vars:     out,
		Failures: failures,
		Audit: ProbeAudit{
			Extractors:        audit,
			BytesRead:         bytesRead,
			TerminationReason: terminationReason,
		},
	}, nil
}

func (p *Prober) AllRequiredResolved(vars map[string]string) bool {
	for _, entry := range p.extractors {
		if entry.cfg.Required && strings.TrimSpace(vars[entry.cfg.Name]) == "" {
			return false
		}
	}
	for _, entry := range p.derived {
		if entry.cfg.RequiredValue() && strings.TrimSpace(vars[entry.cfg.Name]) == "" {
			return false
		}
	}
	return true
}

func (p *Prober) ApplyMissingPolicies(vars map[string]string, audit *ProbeAudit) (routingClass string, requiredFailed bool) {
	routingClass, requiredFailed, _ = p.ApplyMissingPoliciesDetailed(vars, audit, nil)
	return routingClass, requiredFailed
}

func (p *Prober) ApplyMissingPoliciesDetailed(vars map[string]string, audit *ProbeAudit, failures map[string]error) (routingClass string, requiredFailed bool, failureErr error) {
	routingClass = "normal"
	if vars == nil {
		vars = map[string]string{}
	}
	for _, entry := range p.extractors {
		if !entry.cfg.Required {
			continue
		}
		if strings.TrimSpace(vars[entry.cfg.Name]) != "" {
			continue
		}
		switch entry.cfg.OnMissing {
		case OnMissingQuarantine:
			routingClass = "quarantine"
			vars[entry.cfg.Name] = "_unresolved"
		default:
			requiredFailed = true
		}
		if audit != nil {
			for j := range audit.Extractors {
				if audit.Extractors[j].Name == entry.cfg.Name {
					if entry.cfg.OnMissing == OnMissingQuarantine {
						audit.Extractors[j].OnMissing = OnMissingQuarantine
					}
					break
				}
			}
		}
	}
	for _, entry := range p.derived {
		if !entry.cfg.RequiredValue() {
			continue
		}
		if strings.TrimSpace(vars[entry.cfg.Name]) != "" {
			continue
		}
		if err := failures[entry.cfg.Name]; err != nil && failureErr == nil {
			failureErr = err
		}
		switch entry.cfg.OnMissing {
		case OnMissingQuarantine:
			routingClass = "quarantine"
			vars[entry.cfg.Name] = "_unresolved"
		default:
			requiredFailed = true
		}
	}
	return routingClass, requiredFailed, failureErr
}

func (p *Prober) UnresolvedResult(bytesRead int64, terminationReason string) *Result {
	audit := make([]ExtractorAudit, 0, len(p.extractors))
	for _, entry := range p.extractors {
		audit = append(audit, ExtractorAudit{
			Name:      entry.cfg.Name,
			Type:      entry.cfg.Type,
			Required:  entry.cfg.Required,
			OnMissing: entry.cfg.OnMissing,
		})
	}
	return &Result{
		Vars:     map[string]string{},
		Failures: map[string]error{},
		Audit: ProbeAudit{
			Extractors:        audit,
			BytesRead:         bytesRead,
			TerminationReason: terminationReason,
		},
	}
}

func (p *Prober) HasRequiredExtractors() bool {
	for _, entry := range p.extractors {
		if entry.cfg.Required {
			return true
		}
	}
	for _, entry := range p.derived {
		if entry.cfg.RequiredValue() {
			return true
		}
	}
	return false
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
