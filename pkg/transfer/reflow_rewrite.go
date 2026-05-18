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
// - Placeholders in the from template capture one segment or one segment substring: {var}, prefix-{var}-suffix
// - The special placeholder {_} captures and ignores a segment.
// - In the to template, {var} renders the captured value, optionally with a literal prefix/suffix in the same segment.
//
// Each segment may contain at most one placeholder.
type ReflowRewrite struct {
	from []reflowPart
	to   []reflowPart
}

type reflowPart struct {
	Lit    string
	Prefix string
	Var    string
	Suffix string
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
		if p.Var == "" {
			if seg != p.Lit {
				return "", nil, fmt.Errorf("source key %q does not match from template at segment %d: got %q expected %q", sourceKey, i, seg, p.Lit)
			}
			continue
		}
		captured, ok := captureReflowSegment(seg, p)
		if !ok {
			return "", nil, fmt.Errorf("source key %q does not match from template at segment %d", sourceKey, i)
		}
		if p.Var == "_" {
			continue
		}
		vars[p.Var] = captured
	}
	for k, v := range extraVars {
		vars[k] = v
	}

	outSegs := make([]string, 0, len(r.to))
	for _, p := range r.to {
		if p.Var == "" {
			outSegs = append(outSegs, p.Lit)
			continue
		}
		v, ok := vars[p.Var]
		if !ok {
			return "", nil, fmt.Errorf("missing variable %q", p.Var)
		}
		outSegs = append(outSegs, p.Prefix+v+p.Suffix)
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
		part, err := parseReflowSegment(seg)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func parseReflowSegment(seg string) (reflowPart, error) {
	open := strings.Index(seg, "{")
	close := strings.Index(seg, "}")
	if open == -1 && close == -1 {
		return reflowPart{Lit: seg}, nil
	}
	if close != -1 && (open == -1 || close < open) {
		return reflowPart{}, fmt.Errorf("unmatched } in template segment %q", seg)
	}
	if open != -1 && close == -1 {
		return reflowPart{}, fmt.Errorf("unmatched { in template segment %q", seg)
	}
	close = open + strings.Index(seg[open:], "}")
	prefix := seg[:open]
	name := strings.TrimSpace(seg[open+1 : close])
	suffix := seg[close+1:]
	if strings.Contains(name, "{") || strings.Contains(name, "}") || strings.Contains(suffix, "{") {
		return reflowPart{}, fmt.Errorf("multiple placeholders in segment %q are not supported in this release", seg)
	}
	if strings.Contains(suffix, "}") {
		return reflowPart{}, fmt.Errorf("unmatched } in template segment %q", seg)
	}
	if name == "" {
		return reflowPart{}, fmt.Errorf("empty placeholder")
	}
	if strings.Contains(name, "/") {
		return reflowPart{}, fmt.Errorf("invalid placeholder %q", name)
	}
	return reflowPart{Prefix: prefix, Var: name, Suffix: suffix}, nil
}

func captureReflowSegment(seg string, part reflowPart) (string, bool) {
	if part.Prefix != "" && !strings.HasPrefix(seg, part.Prefix) {
		return "", false
	}
	if part.Suffix != "" && !strings.HasSuffix(seg, part.Suffix) {
		return "", false
	}
	start := len(part.Prefix)
	end := len(seg) - len(part.Suffix)
	if end <= start {
		return "", false
	}
	return seg[start:end], true
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
