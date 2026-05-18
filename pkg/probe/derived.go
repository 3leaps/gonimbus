package probe

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

type configuredDerived struct {
	cfg DerivedConfig
	re  *regexp.Regexp
}

type DerivationError struct {
	Field     string
	From      string
	Transform string
	Detail    string
}

func (e *DerivationError) Error() string {
	return fmt.Sprintf("derive %q from %q using %s failed: %s", e.Field, e.From, e.Transform, e.Detail)
}

func validateDerivedTransform(i int, d DerivedConfig) error {
	switch d.Transform {
	case TransformSubstring:
		start, err := requiredIntArg(d.Args, "start")
		if err != nil {
			return fmt.Errorf("derived[%d].args.start %s", i, err)
		}
		end, err := requiredIntArg(d.Args, "end")
		if err != nil {
			return fmt.Errorf("derived[%d].args.end %s", i, err)
		}
		if start < 0 {
			return fmt.Errorf("derived[%d].args.start must be >= 0", i)
		}
		if end != -1 && end < start {
			return fmt.Errorf("derived[%d].args.end must be -1 or >= start", i)
		}
	case TransformRegexCapture:
		pattern, err := requiredStringArg(d.Args, "pattern")
		if err != nil {
			return fmt.Errorf("derived[%d].args.pattern %s", i, err)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("derived[%d].args.pattern invalid: %w", i, err)
		}
		group, err := requiredIntArg(d.Args, "group")
		if err != nil {
			return fmt.Errorf("derived[%d].args.group %s", i, err)
		}
		if group < 0 {
			return fmt.Errorf("derived[%d].args.group must be >= 0", i)
		}
		if group > re.NumSubexp() {
			return fmt.Errorf("derived[%d].args.group %d out of range for pattern with %d capture groups", i, group, re.NumSubexp())
		}
	case TransformFormat:
		if _, err := requiredStringArg(d.Args, "input_layout"); err != nil {
			return fmt.Errorf("derived[%d].args.input_layout %s", i, err)
		}
		if _, err := requiredStringArg(d.Args, "output_layout"); err != nil {
			return fmt.Errorf("derived[%d].args.output_layout %s", i, err)
		}
	case TransformPad:
		width, err := requiredIntArg(d.Args, "width")
		if err != nil {
			return fmt.Errorf("derived[%d].args.width %s", i, err)
		}
		if width < 1 || width > MaxPadWidth {
			return fmt.Errorf("derived[%d].args.width must be in [1, %d]", i, MaxPadWidth)
		}
		char := "0"
		if raw, ok := d.Args["char"]; ok {
			got, ok := raw.(string)
			if !ok {
				return fmt.Errorf("derived[%d].args.char must be a string", i)
			}
			char = strings.TrimSpace(got)
		}
		switch utf8.RuneCountInString(char) {
		case 0:
			return fmt.Errorf("derived[%d].args.char must be exactly one Unicode scalar; got empty string", i)
		case 1:
			// ok
		default:
			return fmt.Errorf("derived[%d].args.char must be exactly one Unicode scalar; got %q (%d runes)", i, char, utf8.RuneCountInString(char))
		}
		side := stringArgDefault(d.Args, "side", "left")
		switch side {
		case "left", "right":
			// ok
		default:
			return fmt.Errorf("derived[%d].args.side must be left or right", i)
		}
	case TransformLowercase, TransformUppercase:
		// no args required
	default:
		return fmt.Errorf("derived[%d].transform %q is not supported; available transforms: %s", i, d.Transform, strings.Join(availableTransforms(), ", "))
	}
	return nil
}

func newConfiguredDerived(d DerivedConfig) (configuredDerived, error) {
	cd := configuredDerived{cfg: d}
	if d.Transform == TransformRegexCapture {
		pattern, _ := requiredStringArg(d.Args, "pattern")
		re, err := regexp.Compile(pattern)
		if err != nil {
			return cd, err
		}
		cd.re = re
	}
	return cd, nil
}

func (d configuredDerived) derive(vars map[string]string) (string, bool, error) {
	input, ok := vars[d.cfg.From]
	if !ok || strings.TrimSpace(input) == "" {
		return "", false, d.failure("source field is unresolved")
	}

	switch d.cfg.Transform {
	case TransformSubstring:
		start, _ := requiredIntArg(d.cfg.Args, "start")
		end, _ := requiredIntArg(d.cfg.Args, "end")
		runes := []rune(input)
		if end == -1 {
			end = len(runes)
		}
		if start > len(runes) || end > len(runes) {
			return "", false, d.failure(fmt.Sprintf("substring bounds start=%d end=%d exceed input length=%d", start, end, len(runes)))
		}
		return string(runes[start:end]), true, nil
	case TransformRegexCapture:
		group, _ := requiredIntArg(d.cfg.Args, "group")
		m := d.re.FindStringSubmatch(input)
		if len(m) == 0 {
			pattern, _ := requiredStringArg(d.cfg.Args, "pattern")
			return "", false, d.failure(fmt.Sprintf("pattern %q did not match input length=%d", pattern, utf8.RuneCountInString(input)))
		}
		if group >= len(m) {
			return "", false, d.failure(fmt.Sprintf("group=%d out of range for matched group count=%d", group, len(m)-1))
		}
		return m[group], true, nil
	case TransformFormat:
		inputLayout, _ := requiredStringArg(d.cfg.Args, "input_layout")
		outputLayout, _ := requiredStringArg(d.cfg.Args, "output_layout")
		t, err := time.Parse(inputLayout, input)
		if err != nil {
			return "", false, d.failure(fmt.Sprintf("input did not parse with expected layout %q; input length=%d", inputLayout, utf8.RuneCountInString(input)))
		}
		return t.Format(outputLayout), true, nil
	case TransformPad:
		width, _ := requiredIntArg(d.cfg.Args, "width")
		char := stringArgDefault(d.cfg.Args, "char", "0")
		side := stringArgDefault(d.cfg.Args, "side", "left")
		runeCount := utf8.RuneCountInString(input)
		if runeCount >= width {
			return input, true, nil
		}
		padding := strings.Repeat(char, width-runeCount)
		if side == "right" {
			return input + padding, true, nil
		}
		return padding + input, true, nil
	case TransformLowercase:
		return strings.ToLower(input), true, nil
	case TransformUppercase:
		return strings.ToUpper(input), true, nil
	default:
		return "", false, d.failure("unsupported transform")
	}
}

func (d configuredDerived) failure(detail string) error {
	return &DerivationError{
		Field:     d.cfg.Name,
		From:      d.cfg.From,
		Transform: d.cfg.Transform,
		Detail:    detail,
	}
}

func requiredStringArg(args map[string]any, name string) (string, error) {
	v, ok := args[name]
	if !ok {
		return "", fmt.Errorf("is required")
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("must be a string")
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("must not be empty")
	}
	return s, nil
}

func stringArgDefault(args map[string]any, name, def string) string {
	v, ok := args[name]
	if !ok {
		return def
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return def
	}
	return strings.TrimSpace(s)
}

func requiredIntArg(args map[string]any, name string) (int, error) {
	v, ok := args[name]
	if !ok {
		return 0, fmt.Errorf("is required")
	}
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case int32:
		return int(x), nil
	case float64:
		if x != float64(int(x)) {
			return 0, fmt.Errorf("must be an integer")
		}
		return int(x), nil
	case float32:
		if x != float32(int(x)) {
			return 0, fmt.Errorf("must be an integer")
		}
		return int(x), nil
	default:
		return 0, fmt.Errorf("must be an integer")
	}
}

func availableTransforms() []string {
	return []string{
		TransformSubstring,
		TransformRegexCapture,
		TransformFormat,
		TransformPad,
		TransformLowercase,
		TransformUppercase,
	}
}

func formatAvailableNames(names map[string]int) string {
	if len(names) == 0 {
		return "[]"
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return "[" + strings.Join(out, ", ") + "]"
}
