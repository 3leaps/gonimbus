package transfer

import (
	"fmt"
	"strings"
)

// ReflowRewrite maps a source key to a destination relative key.
//
// The rewrite is a simple segment matcher/renderer:
// - Templates are split on '/'.
// - In the from template, literal segments must match exactly.
// - Placeholders in the from template capture one segment: {var}
// - The special placeholder {_} captures and ignores a segment.
// - In the to template, {var} renders the captured value.
//
// This stays deliberately simple for v0.1.x; more complex derivations
// (content probes, filename parsing) are layered above this primitive.
type ReflowRewrite struct {
	from []reflowPart
	to   []reflowPart
}

type reflowPart struct {
	Lit string
	Var string
}

func CompileReflowRewrite(fromTemplate, toTemplate string) (*ReflowRewrite, error) {
	from, err := parseReflowTemplate(fromTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse from template: %w", err)
	}
	to, err := parseReflowTemplate(toTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse to template: %w", err)
	}
	if len(from) == 0 {
		return nil, fmt.Errorf("from template must not be empty")
	}
	if len(to) == 0 {
		return nil, fmt.Errorf("to template must not be empty")
	}
	return &ReflowRewrite{from: from, to: to}, nil
}

func (r *ReflowRewrite) Apply(sourceKey string) (destRelKey string, vars map[string]string, err error) {
	return r.ApplyWithVars(sourceKey, nil)
}

// ApplyWithVars applies the rewrite and allows callers to provide additional variables.
//
// extraVars override captured variables when a key is present in both maps.
func (r *ReflowRewrite) ApplyWithVars(sourceKey string, extraVars map[string]string) (destRelKey string, vars map[string]string, err error) {
	key := strings.Trim(sourceKey, "/")
	if key == "" {
		return "", nil, fmt.Errorf("empty source key")
	}

	srcSegs := splitKeySegments(key)
	if len(srcSegs) != len(r.from) {
		return "", nil, fmt.Errorf("source key %q does not match from template (segments=%d expected=%d)", sourceKey, len(srcSegs), len(r.from))
	}

	vars = map[string]string{}
	for i, p := range r.from {
		seg := srcSegs[i]
		if p.Lit != "" {
			if seg != p.Lit {
				return "", nil, fmt.Errorf("source key %q does not match from template at segment %d: got %q expected %q", sourceKey, i, seg, p.Lit)
			}
			continue
		}
		if p.Var == "" {
			return "", nil, fmt.Errorf("invalid template part")
		}
		if p.Var == "_" {
			continue
		}
		vars[p.Var] = seg
	}
	for k, v := range extraVars {
		vars[k] = v
	}

	outSegs := make([]string, 0, len(r.to))
	for _, p := range r.to {
		if p.Lit != "" {
			outSegs = append(outSegs, p.Lit)
			continue
		}
		if p.Var == "" {
			return "", nil, fmt.Errorf("invalid template part")
		}
		v, ok := vars[p.Var]
		if !ok {
			return "", nil, fmt.Errorf("missing variable %q", p.Var)
		}
		outSegs = append(outSegs, v)
	}

	out := strings.Join(outSegs, "/")
	out = strings.ReplaceAll(out, "//", "/")
	out = strings.Trim(out, "/")
	if out == "" {
		return "", nil, fmt.Errorf("rewrite produced empty destination key")
	}
	return out, vars, nil
}

func parseReflowTemplate(tpl string) ([]reflowPart, error) {
	tpl = strings.TrimSpace(tpl)
	tpl = strings.Trim(tpl, "/")
	if tpl == "" {
		return nil, nil
	}

	segs := splitKeySegments(tpl)
	parts := make([]reflowPart, 0, len(segs))
	for _, seg := range segs {
		if seg == "" {
			continue
		}
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			name := strings.TrimSuffix(strings.TrimPrefix(seg, "{"), "}")
			name = strings.TrimSpace(name)
			if name == "" {
				return nil, fmt.Errorf("empty placeholder")
			}
			if strings.Contains(name, "/") {
				return nil, fmt.Errorf("invalid placeholder %q", name)
			}
			parts = append(parts, reflowPart{Var: name})
			continue
		}
		if strings.Contains(seg, "{") || strings.Contains(seg, "}") {
			return nil, fmt.Errorf("placeholders must occupy a full path segment: %q", seg)
		}
		parts = append(parts, reflowPart{Lit: seg})
	}
	return parts, nil
}

func splitKeySegments(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, "/")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
