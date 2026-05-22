package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

type metadataSourceKeyRule struct {
	DestKey   string
	SourceKey string
	Raw       string
}

type metadataDerivedRule struct {
	DestKey    string
	Expression string
	Expr       metadataExpr
	Raw        string
}

type metadataExpr interface {
	eval(metadataEvalContext) (string, error)
	usesUserMetadata() bool
}

type metadataEvalContext struct {
	Source       *provider.ObjectMeta
	UserMetadata map[string]string
}

type metadataDerivationError struct {
	DestKey       string
	Expression    string
	Reason        string
	ResultKind    string
	CollidingKeys []string
}

func (e *metadataDerivationError) Error() string {
	parts := []string{fmt.Sprintf("metadata derivation failed for dest key %q", e.DestKey)}
	if e.Expression != "" {
		parts = append(parts, fmt.Sprintf("expression %q", e.Expression))
	}
	if e.Reason != "" {
		parts = append(parts, e.Reason)
	}
	return strings.Join(parts, ": ")
}

func (e *metadataDerivationError) details() map[string]any {
	details := map[string]any{
		"metadata_dest_key": e.DestKey,
		"metadata_reason":   e.Reason,
	}
	if e.Expression != "" {
		details["metadata_expression"] = e.Expression
	}
	if e.ResultKind != "" {
		details["metadata_result_kind"] = e.ResultKind
	}
	if len(e.CollidingKeys) > 0 {
		details["metadata_colliding_keys"] = append([]string(nil), e.CollidingKeys...)
	}
	return details
}

type sourceMetadataCollisionError struct {
	Keys []string
}

func (e *sourceMetadataCollisionError) Error() string {
	return fmt.Sprintf("source metadata contains duplicate keys after normalization: %s", strings.Join(e.Keys, ","))
}

type metadataMissingError struct {
	Reason string
	Kind   string
}

func (e *metadataMissingError) Error() string { return e.Reason }

type metadataSourceValueExpr struct {
	SourceKey string
}

func (e metadataSourceValueExpr) eval(ctx metadataEvalContext) (string, error) {
	value, ok := ctx.UserMetadata[e.SourceKey]
	if !ok {
		return "", &metadataMissingError{Reason: "source metadata key missing"}
	}
	return value, nil
}

func (e metadataSourceValueExpr) usesUserMetadata() bool { return true }

type metadataJSONSubfieldExpr struct {
	SourceKey string
	Subfield  string
	DecodeURL bool
}

func (e metadataJSONSubfieldExpr) eval(ctx metadataEvalContext) (string, error) {
	raw, ok := ctx.UserMetadata[e.SourceKey]
	if !ok {
		return "", &metadataMissingError{Reason: "source metadata key missing"}
	}
	if e.DecodeURL {
		decoded, err := url.QueryUnescape(raw)
		if err != nil {
			return "", &metadataMissingError{Reason: "url decode failed"}
		}
		raw = decoded
	}
	var obj map[string]any
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil {
		return "", &metadataMissingError{Reason: "json parse failed"}
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return "", &metadataMissingError{Reason: "json parse failed"}
	}
	value, ok := obj[e.Subfield]
	if !ok {
		return "", &metadataMissingError{Reason: "json subfield missing"}
	}
	return renderMetadataScalar(value)
}

func (e metadataJSONSubfieldExpr) usesUserMetadata() bool { return true }

type metadataSystemFieldExpr struct {
	Field string
}

func (e metadataSystemFieldExpr) eval(ctx metadataEvalContext) (string, error) {
	if ctx.Source == nil {
		return "", &metadataMissingError{Reason: "source metadata missing"}
	}
	switch e.Field {
	case "etag":
		return ctx.Source.ETag, nil
	case "last_modified":
		if ctx.Source.LastModified.IsZero() {
			return "", &metadataMissingError{Reason: "system field missing"}
		}
		return ctx.Source.LastModified.UTC().Format(time.RFC3339Nano), nil
	case "content_length":
		return strconv.FormatInt(ctx.Source.Size, 10), nil
	case "content_type":
		if ctx.Source.ContentType == "" {
			return "", &metadataMissingError{Reason: "system field missing"}
		}
		return ctx.Source.ContentType, nil
	case "storage_class":
		storageClass := ctx.Source.StorageClass
		if storageClass == "" {
			storageClass = "STANDARD"
		}
		return storageClass, nil
	default:
		return "", &metadataMissingError{Reason: "unknown system field"}
	}
}

func (e metadataSystemFieldExpr) usesUserMetadata() bool { return false }

type metadataLiteralExpr struct {
	Value string
}

func (e metadataLiteralExpr) eval(metadataEvalContext) (string, error) { return e.Value, nil }
func (e metadataLiteralExpr) usesUserMetadata() bool                   { return false }

type metadataConcatExpr struct {
	Left  metadataExpr
	Right metadataExpr
}

func (e metadataConcatExpr) eval(ctx metadataEvalContext) (string, error) {
	left, err := e.Left.eval(ctx)
	if err != nil {
		return "", err
	}
	right, err := e.Right.eval(ctx)
	if err != nil {
		return "", err
	}
	return left + right, nil
}

func (e metadataConcatExpr) usesUserMetadata() bool {
	return e.Left.usesUserMetadata() || e.Right.usesUserMetadata()
}

func renderMetadataScalar(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		return strconv.FormatBool(v), nil
	case nil:
		return "", &metadataMissingError{Reason: "json value is null", Kind: "null"}
	case []any:
		return "", &metadataMissingError{Reason: "json value is non-scalar", Kind: "array"}
	case map[string]any:
		return "", &metadataMissingError{Reason: "json value is non-scalar", Kind: "object"}
	default:
		return "", &metadataMissingError{Reason: "json value is unsupported"}
	}
}

func parseMetadataSourceKeyRules(raw []string) ([]metadataSourceKeyRule, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	rules := make([]metadataSourceKeyRule, 0, len(raw))
	for _, entry := range raw {
		dest, source, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("metadata-set-from-source-key entries must use dest=source syntax")
		}
		dest = strings.ToLower(strings.TrimSpace(dest))
		source = strings.ToLower(strings.TrimSpace(source))
		if !isMetadataToken(dest) || !isMetadataToken(source) {
			return nil, fmt.Errorf("metadata-set-from-source-key entries require non-empty keys without whitespace or '='")
		}
		if dest == "*" || source == "*" {
			return nil, fmt.Errorf("metadata-set-from-source-key rejects wildcard metadata projection")
		}
		rules = append(rules, metadataSourceKeyRule{DestKey: dest, SourceKey: source, Raw: entry})
	}
	return rules, nil
}

func parseMetadataDerivedRules(raw []string) ([]metadataDerivedRule, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	rules := make([]metadataDerivedRule, 0, len(raw))
	for _, entry := range raw {
		dest, expression, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("metadata-set-from-source-derived entries must use dest=expression syntax")
		}
		dest = strings.ToLower(strings.TrimSpace(dest))
		expression = strings.TrimSpace(expression)
		if !isMetadataToken(dest) {
			return nil, fmt.Errorf("metadata-set-from-source-derived entries require non-empty destination keys without whitespace or '='")
		}
		if dest == "*" || strings.Contains(expression, ".*") {
			return nil, fmt.Errorf("metadata-set-from-source-derived rejects wildcard metadata projection in %q", expression)
		}
		expr, err := parseMetadataExpression(expression)
		if err != nil {
			return nil, fmt.Errorf("metadata-set-from-source-derived expression %q is invalid: %w", expression, err)
		}
		rules = append(rules, metadataDerivedRule{DestKey: dest, Expression: expression, Expr: expr, Raw: entry})
	}
	return rules, nil
}

func validatePerObjectMetadataRules(sourceRules []metadataSourceKeyRule, derivedRules []metadataDerivedRule) error {
	seen := map[string]string{}
	for _, rule := range sourceRules {
		if prev, ok := seen[rule.DestKey]; ok {
			return fmt.Errorf("duplicate per-object metadata destination key %q in %s and --metadata-set-from-source-key", rule.DestKey, prev)
		}
		seen[rule.DestKey] = "--metadata-set-from-source-key"
	}
	for _, rule := range derivedRules {
		if prev, ok := seen[rule.DestKey]; ok {
			return fmt.Errorf("duplicate per-object metadata destination key %q in %s and --metadata-set-from-source-derived", rule.DestKey, prev)
		}
		seen[rule.DestKey] = "--metadata-set-from-source-derived"
	}
	return nil
}

func parseMetadataExpression(expression string) (metadataExpr, error) {
	if expression == "" {
		return nil, fmt.Errorf("empty expression")
	}
	parts, err := splitMetadataConcat(expression)
	if err != nil {
		return nil, err
	}
	if len(parts) == 2 {
		left, err := parseMetadataOperand(parts[0])
		if err != nil {
			return nil, err
		}
		right, err := parseMetadataOperand(parts[1])
		if err != nil {
			return nil, err
		}
		return metadataConcatExpr{Left: left, Right: right}, nil
	}
	return parseMetadataOperand(parts[0])
}

func splitMetadataConcat(expression string) ([]string, error) {
	var parts []string
	inQuote := rune(0)
	start := 0
	for i, r := range expression {
		if inQuote != 0 {
			if r == inQuote {
				inQuote = 0
			}
			continue
		}
		if r == '"' {
			inQuote = r
			continue
		}
		if r == '+' {
			parts = append(parts, strings.TrimSpace(expression[start:i]))
			start = i + len(string(r))
		}
	}
	if inQuote != 0 {
		return nil, fmt.Errorf("unterminated string literal")
	}
	parts = append(parts, strings.TrimSpace(expression[start:]))
	if len(parts) > 2 {
		return nil, fmt.Errorf("only one + operator is supported")
	}
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("dangling + operator")
		}
	}
	return parts, nil
}

func parseMetadataOperand(operand string) (metadataExpr, error) {
	if operand == "" {
		return nil, fmt.Errorf("empty operand")
	}
	if isQuotedMetadataLiteral(operand) {
		value, err := strconv.Unquote(operand)
		if err != nil {
			return nil, fmt.Errorf("invalid string literal")
		}
		return metadataLiteralExpr{Value: value}, nil
	}
	if strings.HasPrefix(operand, "system.") {
		field := strings.TrimPrefix(operand, "system.")
		switch field {
		case "etag", "last_modified", "content_length", "content_type", "storage_class":
			return metadataSystemFieldExpr{Field: field}, nil
		default:
			return nil, fmt.Errorf("unknown system field %q", field)
		}
	}
	if strings.HasPrefix(operand, "meta.") {
		return parseMetadataSubfieldOperand(operand, false)
	}
	if strings.HasPrefix(operand, "urldecode(") {
		close := strings.Index(operand, ")")
		if close < 0 {
			return nil, fmt.Errorf("unclosed urldecode call")
		}
		if close != strings.LastIndex(operand, ")") {
			return nil, fmt.Errorf("invalid urldecode call")
		}
		inner := operand[len("urldecode("):close]
		if !strings.HasPrefix(inner, "meta.") {
			return nil, fmt.Errorf("urldecode only accepts meta.<key>")
		}
		suffix := strings.TrimPrefix(operand[close+1:], ".")
		if suffix == operand[close+1:] || suffix == "" {
			return nil, fmt.Errorf("urldecode expression requires a subfield")
		}
		return parseMetadataSubfieldParts(strings.TrimPrefix(inner, "meta."), suffix, true)
	}
	if strings.Contains(operand, "(") {
		name := operand[:strings.Index(operand, "(")]
		return nil, fmt.Errorf("unknown function %q", name)
	}
	return nil, fmt.Errorf("unknown expression operand")
}

func isQuotedMetadataLiteral(operand string) bool {
	return len(operand) >= 2 && operand[0] == '"' && operand[len(operand)-1] == '"'
}

func parseMetadataSubfieldOperand(operand string, decode bool) (metadataExpr, error) {
	trimmed := strings.TrimPrefix(operand, "meta.")
	sourceKey, subfield, ok := strings.Cut(trimmed, ".")
	if !ok || sourceKey == "" || subfield == "" {
		return nil, fmt.Errorf("meta expression requires meta.<key>.<subfield>")
	}
	return parseMetadataSubfieldParts(sourceKey, subfield, decode)
}

func parseMetadataSubfieldParts(sourceKey, subfield string, decode bool) (metadataExpr, error) {
	sourceKey = strings.ToLower(strings.TrimSpace(sourceKey))
	subfield = strings.TrimSpace(subfield)
	if !isMetadataToken(sourceKey) || !isMetadataToken(subfield) || strings.Contains(subfield, ".") {
		return nil, fmt.Errorf("meta expression contains invalid key or subfield")
	}
	return metadataJSONSubfieldExpr{SourceKey: sourceKey, Subfield: subfield, DecodeURL: decode}, nil
}

func isMetadataToken(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && !strings.ContainsAny(value, " \t\r\n=*")
}

func (c reflowMetadataConfig) hasPerObjectRules() bool {
	return len(c.SourceKeyRules) > 0 || len(c.DerivedRules) > 0
}

func metadataSourceRuleDestKeys(rules []metadataSourceKeyRule) []string {
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.DestKey)
	}
	sort.Strings(out)
	return out
}

func metadataDerivedRuleDestKeys(rules []metadataDerivedRule) []string {
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.DestKey)
	}
	sort.Strings(out)
	return out
}

func (c reflowMetadataConfig) applyPerObjectMetadata(opts *provider.PutOptions, source *provider.ObjectMeta) error {
	if !c.hasPerObjectRules() {
		return nil
	}
	if source == nil {
		return &metadataDerivationError{Reason: "source metadata missing"}
	}
	var userMeta map[string]string
	var sourceCollision *sourceMetadataCollisionError
	if c.perObjectUsesUserMetadata() {
		var err error
		userMeta, err = canonicalizeSourceMetadata(source)
		if err != nil {
			if !strings.Contains(err.Error(), "duplicate") {
				return &metadataDerivationError{Reason: err.Error()}
			}
			sourceCollision = sourceMetadataCollisionFromError(err)
		}
	}
	ctx := metadataEvalContext{Source: source, UserMetadata: userMeta}
	for _, rule := range c.SourceKeyRules {
		if sourceCollision != nil {
			if err := c.applyMissingMetadata(opts, rule.DestKey, rule.SourceKey, "source metadata canonical collision", "", sourceCollision.Keys); err != nil {
				return err
			}
			continue
		}
		value, err := (metadataSourceValueExpr{SourceKey: rule.SourceKey}).eval(ctx)
		if err != nil {
			if err := c.applyEvalError(opts, rule.DestKey, rule.SourceKey, err); err != nil {
				return err
			}
			continue
		}
		setOptionMetadata(opts, rule.DestKey, value)
	}
	for _, rule := range c.DerivedRules {
		if sourceCollision != nil && rule.Expr.usesUserMetadata() {
			if err := c.applyMissingMetadata(opts, rule.DestKey, rule.Expression, "source metadata canonical collision", "", sourceCollision.Keys); err != nil {
				return err
			}
			continue
		}
		value, err := rule.Expr.eval(ctx)
		if err != nil {
			if err := c.applyEvalError(opts, rule.DestKey, rule.Expression, err); err != nil {
				return err
			}
			continue
		}
		setOptionMetadata(opts, rule.DestKey, value)
	}
	return nil
}

func (c reflowMetadataConfig) perObjectUsesUserMetadata() bool {
	if len(c.SourceKeyRules) > 0 {
		return true
	}
	for _, rule := range c.DerivedRules {
		if rule.Expr.usesUserMetadata() {
			return true
		}
	}
	return false
}

func (c reflowMetadataConfig) applyEvalError(opts *provider.PutOptions, destKey, expression string, err error) error {
	reason := err.Error()
	kind := ""
	var missing *metadataMissingError
	if errors.As(err, &missing) {
		reason = missing.Reason
		kind = missing.Kind
	}
	return c.applyMissingMetadata(opts, destKey, expression, reason, kind, nil)
}

func (c reflowMetadataConfig) applyMissingMetadata(opts *provider.PutOptions, destKey, expression, reason, kind string, collidingKeys []string) error {
	switch c.OnMissingSource {
	case metadataMissingEmpty:
		setOptionMetadata(opts, destKey, "")
		return nil
	case metadataMissingFail:
		return &metadataDerivationError{DestKey: destKey, Expression: expression, Reason: reason, ResultKind: kind, CollidingKeys: collidingKeys}
	case metadataMissingSkip, "":
		return nil
	default:
		return &metadataDerivationError{DestKey: destKey, Expression: expression, Reason: reason, ResultKind: kind, CollidingKeys: collidingKeys}
	}
}

func setOptionMetadata(opts *provider.PutOptions, key, value string) {
	if opts.UserMetadata == nil {
		opts.UserMetadata = map[string]string{}
	}
	opts.UserMetadata[key] = value
}

func sourceMetadataCollisionFromError(err error) *sourceMetadataCollisionError {
	if collision, ok := err.(*sourceMetadataCollisionError); ok {
		return collision
	}
	return &sourceMetadataCollisionError{Keys: []string{err.Error()}}
}
