package reflow

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

// Destination user-metadata policy values. These are the resolved policy
// strings the engine acts on; the CLI maps its --metadata-policy flag onto them.
const (
	MetadataPolicyClear    = "clear"
	MetadataPolicyPreserve = "preserve"
	MetadataPolicyMerge    = "merge"

	// MetadataOnMissing* selects how a per-object rule reacts when its source
	// value is missing or unrepresentable.
	MetadataOnMissingSkip  = "skip"
	MetadataOnMissingFail  = "fail"
	MetadataOnMissingEmpty = "empty"

	// MetadataStorageClassPropagate copies the source object's storage class to
	// the destination rather than setting a fixed class.
	MetadataStorageClassPropagate = "propagate"

	// S3 user-metadata budget: per-pair and total byte ceilings enforced before
	// a conditional/unconditional PUT against an S3 destination.
	metadataMaxPairBytes  = 2 * 1024
	metadataMaxTotalBytes = 8 * 1024
)

// MetadataPlan is the resolved, flag-free destination-metadata decision plan.
// It carries the policy plus the compiled per-object rules and produces the
// provider.PutOptions for a source object deterministically — no provider I/O.
// The CLI resolves it from flags; the engine consumes it. Experimental.
type MetadataPlan struct {
	Policy                  string
	Set                     map[string]string
	SourceKeyRules          []MetadataSourceKeyRule
	DerivedRules            []MetadataDerivedRule
	OnMissingSource         string
	PreserveContentType     bool
	DestinationStorageClass string
	MetadataSidecarSuffix   string
}

// String returns a value-free summary suitable for debug output. Metadata values
// and derived expression text may reflect operator-sensitive configuration, so
// they are never formatted by value.
func (c MetadataPlan) String() string {
	return fmt.Sprintf("reflow.MetadataPlan{Policy:%q, Set:%d, SourceKeyRules:%d, DerivedRules:%d, OnMissingSource:%q, PreserveContentType:%t, DestinationStorageClass:%s, MetadataSidecarSuffix:%s}",
		c.Policy,
		len(c.Set),
		len(c.SourceKeyRules),
		len(c.DerivedRules),
		c.OnMissingSource,
		c.PreserveContentType,
		fieldPresence(c.DestinationStorageClass == ""),
		fieldPresence(c.MetadataSidecarSuffix == ""))
}

// GoString implements fmt %#v with the same redaction as String.
func (c MetadataPlan) GoString() string { return c.String() }

// MetadataSourceKeyRule projects a single source user-metadata key onto a
// destination user-metadata key.
type MetadataSourceKeyRule struct {
	DestKey   string
	SourceKey string
	Raw       string
}

// MetadataDerivedRule projects a compiled expression over a source object onto a
// destination user-metadata key. Construct it via ParseMetadataDerivedRules: the
// compiled expression is held in an unexported field, so a hand-built rule has no
// expression and is rejected by ValidatePerObjectMetadataRules (and fails closed
// in PutOptions) rather than panicking.
type MetadataDerivedRule struct {
	DestKey    string
	Expression string
	Raw        string
	expr       metadataExpr
}

// MetadataDerivationError describes a per-object metadata derivation failure
// under --metadata-on-missing-source=fail.
type MetadataDerivationError struct {
	DestKey       string
	Expression    string
	Reason        string
	ResultKind    string
	CollidingKeys []string
}

func (e *MetadataDerivationError) Error() string {
	parts := []string{fmt.Sprintf("metadata derivation failed for dest key %q", e.DestKey)}
	if e.Expression != "" {
		parts = append(parts, fmt.Sprintf("expression %q", e.Expression))
	}
	if e.Reason != "" {
		parts = append(parts, e.Reason)
	}
	return strings.Join(parts, ": ")
}

// Details renders the structured detail map for error reporting.
func (e *MetadataDerivationError) Details() map[string]any {
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

// SourceMetadataCollisionError reports source metadata keys that collide after
// canonicalization (lower-cased, trimmed).
type SourceMetadataCollisionError struct {
	Keys []string
}

func (e *SourceMetadataCollisionError) Error() string {
	return fmt.Sprintf("source metadata contains duplicate keys after normalization: %s", strings.Join(e.Keys, ","))
}

// MetadataBudgetError reports user metadata exceeding the S3 metadata budget.
type MetadataBudgetError struct {
	OverLimitKeys []string
	PairLimit     int
	TotalBytes    int
	TotalLimit    int
	Count         int
}

func (e *MetadataBudgetError) Error() string {
	return fmt.Sprintf("user metadata exceeds S3 metadata budget: keys=%v count=%d total_bytes=%d total_limit=%d pair_limit=%d", e.OverLimitKeys, e.Count, e.TotalBytes, e.TotalLimit, e.PairLimit)
}

// Details renders the structured detail map for error reporting.
func (e *MetadataBudgetError) Details() map[string]any {
	return map[string]any{
		"metadata_keys":        append([]string(nil), e.OverLimitKeys...),
		"metadata_count":       e.Count,
		"metadata_total_bytes": e.TotalBytes,
		"metadata_total_limit": e.TotalLimit,
		"metadata_pair_limit":  e.PairLimit,
	}
}

type metadataMissingError struct {
	Reason string
	Kind   string
}

func (e *metadataMissingError) Error() string { return e.Reason }

// NeedsSourceHead reports whether resolving the plan for an object requires a
// source HEAD (to read source metadata/content-type/storage-class).
func (c MetadataPlan) NeedsSourceHead() bool {
	return c.Policy == MetadataPolicyPreserve || c.Policy == MetadataPolicyMerge || c.PreserveContentType || c.DestinationStorageClass == MetadataStorageClassPropagate || c.HasPerObjectRules()
}

// RequiresCapability reports whether the plan requires a metadata-aware PUT.
func (c MetadataPlan) RequiresCapability() bool {
	return c.Policy == MetadataPolicyPreserve || c.Policy == MetadataPolicyMerge || len(c.Set) > 0 || c.HasPerObjectRules() || c.PreserveContentType || c.DestinationStorageClass != ""
}

// HasPerObjectRules reports whether any per-object metadata rule is configured.
func (c MetadataPlan) HasPerObjectRules() bool {
	return len(c.SourceKeyRules) > 0 || len(c.DerivedRules) > 0
}

// Validate checks the resolved plan's enum fields and per-object rules. It is the
// canonical resolved-plan validation: the CLI flag-resolution path delegates to
// it, and direct library callers should call it before handing the plan to the
// engine. PutOptions additionally fails closed on the policy and fixed
// storage-class fields as defense-in-depth, but Validate is the single place that
// rejects an inconsistent plan up front. It does not validate the file-sidecar
// suffix, which is a destination-provider concern carried by the CLI.
func (c MetadataPlan) Validate() error {
	switch c.Policy {
	case MetadataPolicyClear, MetadataPolicyPreserve, MetadataPolicyMerge:
		// ok
	default:
		return fmt.Errorf("metadata-policy must be one of: clear, preserve, merge")
	}
	if _, bad := c.Set[""]; bad {
		return fmt.Errorf("metadata-set entries must use non-empty key=value syntax")
	}
	for key := range c.Set {
		if strings.ContainsAny(key, " \t\r\n=") {
			return fmt.Errorf("metadata-set keys must be non-empty tokens without whitespace or '='")
		}
	}
	switch c.OnMissingSource {
	case "", MetadataOnMissingSkip, MetadataOnMissingFail, MetadataOnMissingEmpty:
		// ok
	default:
		return fmt.Errorf("metadata-on-missing-source must be one of: skip, fail, empty")
	}
	if err := ValidatePerObjectMetadataRules(c.SourceKeyRules, c.DerivedRules); err != nil {
		return err
	}
	if c.DestinationStorageClass == "" || strings.EqualFold(c.DestinationStorageClass, MetadataStorageClassPropagate) {
		return nil
	}
	if !isValidPutStorageClass(strings.ToUpper(c.DestinationStorageClass)) {
		return fmt.Errorf("destination-storage-class is not a valid PUT target")
	}
	return nil
}

type metadataExpr interface {
	eval(metadataEvalContext) (string, error)
	usesUserMetadata() bool
}

type metadataEvalContext struct {
	Source       *provider.ObjectMeta
	UserMetadata map[string]string
}

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

// ParseMetadataSourceKeyRules compiles dest=source projection entries.
func ParseMetadataSourceKeyRules(raw []string) ([]MetadataSourceKeyRule, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	rules := make([]MetadataSourceKeyRule, 0, len(raw))
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
		rules = append(rules, MetadataSourceKeyRule{DestKey: dest, SourceKey: source, Raw: entry})
	}
	return rules, nil
}

// ParseMetadataDerivedRules compiles dest=expression projection entries.
func ParseMetadataDerivedRules(raw []string) ([]MetadataDerivedRule, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	rules := make([]MetadataDerivedRule, 0, len(raw))
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
		rules = append(rules, MetadataDerivedRule{DestKey: dest, Expression: expression, Raw: entry, expr: expr})
	}
	return rules, nil
}

// ValidatePerObjectMetadataRules rejects duplicate destination keys across the
// source-key and derived rule sets.
func ValidatePerObjectMetadataRules(sourceRules []MetadataSourceKeyRule, derivedRules []MetadataDerivedRule) error {
	seen := map[string]string{}
	for _, rule := range sourceRules {
		if !isMetadataToken(rule.DestKey) || !isMetadataToken(rule.SourceKey) {
			return fmt.Errorf("metadata-set-from-source-key rule has invalid dest/source key: %q=%q", rule.DestKey, rule.SourceKey)
		}
		if prev, ok := seen[rule.DestKey]; ok {
			return fmt.Errorf("duplicate per-object metadata destination key %q in %s and --metadata-set-from-source-key", rule.DestKey, prev)
		}
		seen[rule.DestKey] = "--metadata-set-from-source-key"
	}
	for _, rule := range derivedRules {
		if !isMetadataToken(rule.DestKey) {
			return fmt.Errorf("metadata-set-from-source-derived rule has invalid destination key: %q", rule.DestKey)
		}
		// A hand-built rule (or one that skipped ParseMetadataDerivedRules) has no
		// compiled expression; reject it here so it never reaches eval.
		if strings.TrimSpace(rule.Expression) == "" || rule.expr == nil {
			return fmt.Errorf("metadata-set-from-source-derived rule for %q has no compiled expression", rule.DestKey)
		}
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

func (c MetadataPlan) applyPerObjectMetadata(opts *provider.PutOptions, source *provider.ObjectMeta) error {
	if !c.HasPerObjectRules() {
		return nil
	}
	if source == nil {
		return &MetadataDerivationError{Reason: "source metadata missing"}
	}
	var userMeta map[string]string
	var sourceCollision *SourceMetadataCollisionError
	if c.perObjectUsesUserMetadata() {
		var err error
		userMeta, err = canonicalizeSourceMetadata(source)
		if err != nil {
			if !strings.Contains(err.Error(), "duplicate") {
				return &MetadataDerivationError{Reason: err.Error()}
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
		if rule.expr == nil {
			// Fail closed: a rule with no compiled expression (built without
			// ParseMetadataDerivedRules) must error, never panic.
			return &MetadataDerivationError{DestKey: rule.DestKey, Expression: rule.Expression, Reason: "derived rule has no compiled expression"}
		}
		if sourceCollision != nil && rule.expr.usesUserMetadata() {
			if err := c.applyMissingMetadata(opts, rule.DestKey, rule.Expression, "source metadata canonical collision", "", sourceCollision.Keys); err != nil {
				return err
			}
			continue
		}
		value, err := rule.expr.eval(ctx)
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

func (c MetadataPlan) perObjectUsesUserMetadata() bool {
	if len(c.SourceKeyRules) > 0 {
		return true
	}
	for _, rule := range c.DerivedRules {
		if rule.expr != nil && rule.expr.usesUserMetadata() {
			return true
		}
	}
	return false
}

func (c MetadataPlan) applyEvalError(opts *provider.PutOptions, destKey, expression string, err error) error {
	reason := err.Error()
	kind := ""
	var missing *metadataMissingError
	if errors.As(err, &missing) {
		reason = missing.Reason
		kind = missing.Kind
	}
	return c.applyMissingMetadata(opts, destKey, expression, reason, kind, nil)
}

func (c MetadataPlan) applyMissingMetadata(opts *provider.PutOptions, destKey, expression, reason, kind string, collidingKeys []string) error {
	switch c.OnMissingSource {
	case MetadataOnMissingEmpty:
		setOptionMetadata(opts, destKey, "")
		return nil
	case MetadataOnMissingFail:
		return &MetadataDerivationError{DestKey: destKey, Expression: expression, Reason: reason, ResultKind: kind, CollidingKeys: collidingKeys}
	case MetadataOnMissingSkip, "":
		return nil
	default:
		return &MetadataDerivationError{DestKey: destKey, Expression: expression, Reason: reason, ResultKind: kind, CollidingKeys: collidingKeys}
	}
}

func setOptionMetadata(opts *provider.PutOptions, key, value string) {
	if opts.UserMetadata == nil {
		opts.UserMetadata = map[string]string{}
	}
	opts.UserMetadata[key] = value
}

func sourceMetadataCollisionFromError(err error) *SourceMetadataCollisionError {
	var collision *SourceMetadataCollisionError
	if errors.As(err, &collision) {
		return collision
	}
	return &SourceMetadataCollisionError{Keys: []string{err.Error()}}
}

// PutOptions builds the destination provider.PutOptions for a source object,
// applying the policy, per-object rules, content-type, and storage-class
// decisions deterministically. source may be nil only for policies that do not
// read source metadata.
func (c MetadataPlan) PutOptions(source *provider.ObjectMeta) (provider.PutOptions, error) {
	var opts provider.PutOptions
	switch c.Policy {
	case MetadataPolicyPreserve:
		userMeta, err := canonicalizeSourceMetadata(source)
		if err != nil {
			return opts, err
		}
		opts.UserMetadata = userMeta
	case MetadataPolicyMerge:
		userMeta, err := canonicalizeSourceMetadata(source)
		if err != nil {
			return opts, err
		}
		opts.UserMetadata = userMeta
	case MetadataPolicyClear:
	default:
		// Fail closed: an unknown policy must not silently behave like clear.
		return opts, fmt.Errorf("metadata-policy must be one of: clear, preserve, merge")
	}
	if err := c.applyPerObjectMetadata(&opts, source); err != nil {
		return opts, err
	}
	if len(c.Set) > 0 {
		if opts.UserMetadata == nil {
			opts.UserMetadata = map[string]string{}
		}
		for key, value := range c.Set {
			opts.UserMetadata[key] = value
		}
	}
	if c.PreserveContentType {
		if source == nil {
			return opts, fmt.Errorf("source metadata is required to preserve content type")
		}
		opts.ContentType = source.ContentType
	}
	if c.DestinationStorageClass != "" {
		if c.DestinationStorageClass == MetadataStorageClassPropagate {
			if source == nil {
				return opts, fmt.Errorf("source metadata is required to propagate storage class")
			}
			storageClass := source.StorageClass
			if storageClass == "" {
				storageClass = "STANDARD"
			}
			storageClass = strings.ToUpper(storageClass)
			if !isValidPutStorageClass(storageClass) {
				return opts, fmt.Errorf("source storage class is not a valid PUT target")
			}
			opts.StorageClass = storageClass
		} else {
			storageClass := strings.ToUpper(c.DestinationStorageClass)
			// Fail closed: never hand an invalid fixed storage class to a provider.
			if !isValidPutStorageClass(storageClass) {
				return opts, fmt.Errorf("destination-storage-class is not a valid PUT target")
			}
			opts.StorageClass = storageClass
		}
	}
	return opts, nil
}

func isValidPutStorageClass(storageClass string) bool {
	switch strings.ToUpper(strings.TrimSpace(storageClass)) {
	case "STANDARD", "INTELLIGENT_TIERING", "STANDARD_IA", "ONEZONE_IA", "GLACIER_IR", "REDUCED_REDUNDANCY":
		return true
	default:
		return false
	}
}

// IsValidPutStorageClass reports whether storageClass is a valid PUT target.
func IsValidPutStorageClass(storageClass string) bool {
	return isValidPutStorageClass(storageClass)
}

// ValidateMetadataBudget enforces the S3 per-pair and total user-metadata byte
// budget, returning a *MetadataBudgetError when exceeded.
func ValidateMetadataBudget(metadata map[string]string) error {
	if len(metadata) == 0 {
		return nil
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	total := 0
	overLimitKeys := make([]string, 0)
	for _, key := range keys {
		pairBytes := len([]byte(key)) + len([]byte(metadata[key]))
		total += pairBytes
		if pairBytes > metadataMaxPairBytes {
			overLimitKeys = append(overLimitKeys, key)
		}
	}
	if total > metadataMaxTotalBytes {
		overLimitKeys = append(overLimitKeys, keys...)
	}
	if len(overLimitKeys) == 0 {
		return nil
	}
	overLimitKeys = uniqueSortedMetadataKeys(overLimitKeys)
	return &MetadataBudgetError{
		OverLimitKeys: overLimitKeys,
		PairLimit:     metadataMaxPairBytes,
		TotalBytes:    total,
		TotalLimit:    metadataMaxTotalBytes,
		Count:         len(metadata),
	}
}

func uniqueSortedMetadataKeys(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func canonicalizeSourceMetadata(source *provider.ObjectMeta) (map[string]string, error) {
	if source == nil {
		return nil, fmt.Errorf("source metadata is required for metadata-policy")
	}
	out := make(map[string]string, len(source.Metadata))
	seenOriginal := make(map[string]string, len(source.Metadata))
	for key, value := range source.Metadata {
		canon := strings.ToLower(strings.TrimSpace(key))
		if canon == "" {
			continue
		}
		if first, ok := seenOriginal[canon]; ok && first != key {
			keys := []string{first, key}
			sort.Strings(keys)
			return nil, &SourceMetadataCollisionError{Keys: keys}
		}
		seenOriginal[canon] = key
		out[canon] = value
	}
	return out, nil
}
