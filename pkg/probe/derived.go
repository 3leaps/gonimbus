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
	cfg    DerivedConfig
	re     *regexp.Regexp
	lookup *configuredLookup
}

type configuredLookup struct {
	matchMode  string
	table      []configuredLookupEntry
	defaultVal *string
}

type configuredLookupEntry struct {
	match string
	value string
	re    *regexp.Regexp
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

func validateDerivedTransform(i int, d DerivedConfig) (*configuredLookup, error) {
	switch d.Transform {
	case TransformSubstring:
		start, err := requiredIntArg(d.Args, "start")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.start %s", i, err)
		}
		end, err := requiredIntArg(d.Args, "end")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.end %s", i, err)
		}
		if start < 0 {
			return nil, fmt.Errorf("derived[%d].args.start must be >= 0", i)
		}
		if end != -1 && end < start {
			return nil, fmt.Errorf("derived[%d].args.end must be -1 or >= start", i)
		}
	case TransformRegexCapture:
		pattern, err := requiredStringArg(d.Args, "pattern")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.pattern %s", i, err)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.pattern invalid: %w", i, err)
		}
		group, err := requiredIntArg(d.Args, "group")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.group %s", i, err)
		}
		if group < 0 {
			return nil, fmt.Errorf("derived[%d].args.group must be >= 0", i)
		}
		if group > re.NumSubexp() {
			return nil, fmt.Errorf("derived[%d].args.group %d out of range for pattern with %d capture groups", i, group, re.NumSubexp())
		}
	case TransformFormat:
		if _, err := requiredStringArg(d.Args, "input_layout"); err != nil {
			return nil, fmt.Errorf("derived[%d].args.input_layout %s", i, err)
		}
		if _, err := requiredStringArg(d.Args, "output_layout"); err != nil {
			return nil, fmt.Errorf("derived[%d].args.output_layout %s", i, err)
		}
	case TransformPad:
		width, err := requiredIntArg(d.Args, "width")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.width %s", i, err)
		}
		if width < 1 || width > MaxPadWidth {
			return nil, fmt.Errorf("derived[%d].args.width must be in [1, %d]", i, MaxPadWidth)
		}
		char, err := stringArgDefault(d.Args, "char", "0")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.char %s", i, err)
		}
		switch utf8.RuneCountInString(char) {
		case 0:
			return nil, fmt.Errorf("derived[%d].args.char must be exactly one non-whitespace Unicode scalar; got empty string", i)
		case 1:
			if strings.TrimSpace(char) == "" {
				return nil, fmt.Errorf("derived[%d].args.char must be exactly one non-whitespace Unicode scalar; got whitespace", i)
			}
		default:
			return nil, fmt.Errorf("derived[%d].args.char must be exactly one non-whitespace Unicode scalar; got %q (%d runes)", i, char, utf8.RuneCountInString(char))
		}
		side, err := stringArgDefault(d.Args, "side", "left")
		if err != nil {
			return nil, fmt.Errorf("derived[%d].args.side %s", i, err)
		}
		switch side {
		case "left", "right":
			// ok
		default:
			return nil, fmt.Errorf("derived[%d].args.side must be left or right", i)
		}
	case TransformLookup:
		lookup, err := parseLookupConfig(i, d.Args, compileLookupRegex)
		if err != nil {
			return nil, err
		}
		return lookup, nil
	case TransformLowercase, TransformUppercase:
		// no args required
	default:
		return nil, fmt.Errorf("derived[%d].transform %q is not supported; available transforms: %s", i, d.Transform, strings.Join(availableTransforms(), ", "))
	}
	return nil, nil
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
	if d.Transform == TransformLookup {
		if d.lookup != nil {
			cd.lookup = d.lookup
		} else {
			lookup, err := parseLookupConfig(-1, d.Args, compileLookupRegex)
			if err != nil {
				return cd, err
			}
			cd.lookup = lookup
		}
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
		char, _ := stringArgDefault(d.cfg.Args, "char", "0")
		side, _ := stringArgDefault(d.cfg.Args, "side", "left")
		runeCount := utf8.RuneCountInString(input)
		if runeCount >= width {
			return input, true, nil
		}
		padding := strings.Repeat(char, width-runeCount)
		if side == "right" {
			return input + padding, true, nil
		}
		return padding + input, true, nil
	case TransformLookup:
		if d.lookup == nil {
			return "", false, d.failure("lookup configuration is missing")
		}
		for _, entry := range d.lookup.table {
			if lookupEntryMatches(d.lookup.matchMode, entry, input) {
				return entry.value, true, nil
			}
		}
		if d.lookup.defaultVal != nil {
			return *d.lookup.defaultVal, true, nil
		}
		return "", false, d.failure(fmt.Sprintf("no lookup entry matched; match_mode=%s table_entries=%d default_set=false on_missing=%s", d.lookup.matchMode, len(d.lookup.table), d.cfg.OnMissing))
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

func lookupEntryMatches(mode string, entry configuredLookupEntry, input string) bool {
	switch mode {
	case LookupMatchModeExact:
		return input == entry.match
	case LookupMatchModePrefix:
		return strings.HasPrefix(input, entry.match)
	case LookupMatchModeRegex:
		return entry.re != nil && entry.re.MatchString(input)
	default:
		return false
	}
}

func parseLookupConfig(i int, args map[string]any, compileRegex func(string) (*regexp.Regexp, error)) (*configuredLookup, error) {
	matchMode, err := requiredStringArg(args, "match_mode")
	if err != nil {
		return nil, derivedArgError(i, "match_mode", err)
	}
	matchMode = strings.ToLower(matchMode)
	switch matchMode {
	case LookupMatchModeRegex, LookupMatchModePrefix, LookupMatchModeExact:
		// ok
	default:
		return nil, derivedArgError(i, "match_mode", fmt.Errorf("unknown match_mode %q; valid: [regex, prefix, exact]", matchMode))
	}

	tableRaw, ok := args["table"]
	if !ok {
		return nil, derivedArgError(i, "table", fmt.Errorf("is required"))
	}
	tableList, ok := tableRaw.([]any)
	if !ok {
		return nil, derivedArgError(i, "table", fmt.Errorf("must be an array"))
	}
	if len(tableList) == 0 {
		return nil, derivedArgError(i, "table", fmt.Errorf("must contain at least one entry"))
	}

	lookup := &configuredLookup{matchMode: matchMode, table: make([]configuredLookupEntry, 0, len(tableList))}
	for j, raw := range tableList {
		entryMap, ok := raw.(map[string]any)
		if !ok {
			return nil, derivedArgError(i, fmt.Sprintf("table[%d]", j), fmt.Errorf("must be an object"))
		}
		match, err := requiredStringArg(entryMap, "match")
		if err != nil {
			return nil, derivedArgError(i, fmt.Sprintf("table[%d].match", j), err)
		}
		value, err := requiredStringArg(entryMap, "value")
		if err != nil {
			return nil, derivedArgError(i, fmt.Sprintf("table[%d].value", j), err)
		}
		entry := configuredLookupEntry{match: match, value: value}
		if matchMode == LookupMatchModeRegex {
			re, err := compileRegex(match)
			if err != nil {
				return nil, derivedArgError(i, fmt.Sprintf("table[%d].match", j), fmt.Errorf("invalid regex: %w", err))
			}
			entry.re = re
		}
		lookup.table = append(lookup.table, entry)
	}

	if raw, ok := args["default"]; ok {
		def, ok := raw.(string)
		if !ok {
			return nil, derivedArgError(i, "default", fmt.Errorf("must be a string"))
		}
		def = strings.TrimSpace(def)
		if def == "" {
			return nil, derivedArgError(i, "default", fmt.Errorf("must not be empty"))
		}
		lookup.defaultVal = &def
	}
	return lookup, nil
}

var compileLookupRegex = regexp.Compile

func derivedArgError(i int, name string, err error) error {
	if i < 0 {
		return fmt.Errorf("args.%s %s", name, err)
	}
	return fmt.Errorf("derived[%d].args.%s %s", i, name, err)
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

func stringArgDefault(args map[string]any, name, def string) (string, error) {
	v, ok := args[name]
	if !ok {
		return def, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("must be a string")
	}
	return s, nil
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
		TransformLookup,
	}
}

func mergeAvailableNames(left, right map[string]int) map[string]int {
	out := make(map[string]int, len(left)+len(right))
	for name, idx := range left {
		out[name] = idx
	}
	for name, idx := range right {
		out[name] = idx
	}
	return out
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
