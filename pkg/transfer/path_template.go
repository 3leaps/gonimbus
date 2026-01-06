package transfer

import (
	"fmt"
	"strconv"
	"strings"
)

type pathTemplatePart interface {
	append(dst *strings.Builder, srcKey string) error
}

type literalPart string

type filenamePart struct{}

type keyPart struct{}

type dirPart struct{ idx int }

func (p literalPart) append(dst *strings.Builder, _ string) error {
	dst.WriteString(string(p))
	return nil
}

func (p filenamePart) append(dst *strings.Builder, srcKey string) error {
	dirs, filename := splitKey(srcKey)
	_ = dirs
	dst.WriteString(filename)
	return nil
}

func (p keyPart) append(dst *strings.Builder, srcKey string) error {
	dst.WriteString(srcKey)
	return nil
}

func (p dirPart) append(dst *strings.Builder, srcKey string) error {
	dirs, _ := splitKey(srcKey)
	if p.idx < 0 || p.idx >= len(dirs) {
		return fmt.Errorf("dir[%d] out of range for %q", p.idx, srcKey)
	}
	dst.WriteString(dirs[p.idx])
	return nil
}

// PathTemplate is a minimal key mapping template.
//
// Supported placeholders:
// - `{filename}`: final path segment
// - `{dir[n]}`: nth directory component (0-based)
// - `{key}`: full source key
//
// This intentionally stays simple for v0.1.x.
type PathTemplate struct {
	parts []pathTemplatePart
}

func (t *PathTemplate) Apply(sourceKey string) (string, error) {
	var b strings.Builder
	for _, part := range t.parts {
		if err := part.append(&b, sourceKey); err != nil {
			return "", err
		}
	}

	out := b.String()
	out = strings.ReplaceAll(out, "//", "/")
	out = strings.TrimPrefix(out, "/")
	if out == "" {
		return "", fmt.Errorf("path_template produced empty key for %q", sourceKey)
	}
	return out, nil
}

// CompilePathTemplate parses a template string into a PathTemplate.
func CompilePathTemplate(template string) (*PathTemplate, error) {
	if template == "" {
		return &PathTemplate{parts: []pathTemplatePart{keyPart{}}}, nil
	}

	var parts []pathTemplatePart
	s := template
	for len(s) > 0 {
		open := strings.IndexByte(s, '{')
		if open == -1 {
			parts = append(parts, literalPart(s))
			break
		}
		if open > 0 {
			parts = append(parts, literalPart(s[:open]))
			s = s[open:]
		}

		closeIdx := strings.IndexByte(s, '}')
		if closeIdx == -1 {
			return nil, fmt.Errorf("unclosed placeholder in %q", template)
		}

		placeholder := s[1:closeIdx]
		s = s[closeIdx+1:]

		part, err := parsePlaceholder(placeholder)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}

	return &PathTemplate{parts: parts}, nil
}

func parsePlaceholder(p string) (pathTemplatePart, error) {
	switch {
	case p == "filename":
		return filenamePart{}, nil
	case p == "key":
		return keyPart{}, nil
	case strings.HasPrefix(p, "dir[") && strings.HasSuffix(p, "]"):
		nStr := strings.TrimSuffix(strings.TrimPrefix(p, "dir["), "]")
		idx, err := strconv.Atoi(nStr)
		if err != nil {
			return nil, fmt.Errorf("invalid dir index %q", nStr)
		}
		return dirPart{idx: idx}, nil
	default:
		return nil, fmt.Errorf("unsupported placeholder {%s}", p)
	}
}

func splitKey(key string) (dirs []string, filename string) {
	trimmed := strings.TrimSuffix(key, "/")
	if trimmed == "" {
		return nil, ""
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 1 {
		return nil, parts[0]
	}
	return parts[:len(parts)-1], parts[len(parts)-1]
}
