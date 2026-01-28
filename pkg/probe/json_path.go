package probe

import (
	"fmt"
	"strconv"
	"strings"
)

// JSONPath is a small JSON path selector.
//
// Supported forms:
// - $.a.b.c
// - a.b.c
// - a[0].b
//
// This is intentionally minimal for v0.1.x.
type JSONPath struct {
	steps []jsonStep
}

type jsonStep struct {
	Key   string
	Index *int
}

func CompileJSONPath(expr string) (*JSONPath, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("json_path is empty")
	}
	expr = strings.TrimPrefix(expr, "$")
	expr = strings.TrimPrefix(expr, ".")
	expr = strings.TrimPrefix(expr, "/")

	var steps []jsonStep
	for len(expr) > 0 {
		// Consume up to next '.'
		seg := expr
		nextDot := strings.IndexByte(expr, '.')
		if nextDot >= 0 {
			seg = expr[:nextDot]
			expr = expr[nextDot+1:]
		} else {
			expr = ""
		}
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}

		step, err := parseJSONSegment(seg)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("json_path has no steps")
	}
	return &JSONPath{steps: steps}, nil
}

func parseJSONSegment(seg string) (jsonStep, error) {
	// key or key[idx]
	open := strings.IndexByte(seg, '[')
	if open == -1 {
		return jsonStep{Key: seg}, nil
	}
	if !strings.HasSuffix(seg, "]") {
		return jsonStep{}, fmt.Errorf("invalid json_path segment %q", seg)
	}
	key := strings.TrimSpace(seg[:open])
	idxStr := strings.TrimSpace(strings.TrimSuffix(seg[open+1:], "]"))
	if idxStr == "" {
		return jsonStep{}, fmt.Errorf("empty index in json_path segment %q", seg)
	}
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return jsonStep{}, fmt.Errorf("invalid index %q", idxStr)
	}
	if idx < 0 {
		return jsonStep{}, fmt.Errorf("index must be >= 0")
	}
	return jsonStep{Key: key, Index: &idx}, nil
}

func (p *JSONPath) Eval(v any) (any, bool) {
	cur := v
	for _, step := range p.steps {
		if step.Key != "" {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			next, ok := m[step.Key]
			if !ok {
				return nil, false
			}
			cur = next
		}
		if step.Index != nil {
			arr, ok := cur.([]any)
			if !ok {
				return nil, false
			}
			idx := *step.Index
			if idx < 0 || idx >= len(arr) {
				return nil, false
			}
			cur = arr[idx]
		}
	}
	return cur, true
}
