package indexsubstrate

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Dark durable-lineage schema and bounded digest-verifying ancestry readers.
// Does not activate continuous-state publish, PriorRows loading, durable
// --since, or --since-run. See docs/architecture/durable-lineage.md.

const (
	// LineageVersionV1 is the only supported lineage.version.
	LineageVersionV1 = 1

	// LineageBaselineGeneration is the generation of the first continuous
	// lineage baseline publication (baseline:true). Later continuous children
	// must use parent.generation + 1.
	LineageBaselineGeneration = 1

	// Default ancestry resource budgets (graph-wide, not only per file).
	DefaultAncestryMaxDepth          = 64
	DefaultAncestryMaxNodes          = 64
	DefaultAncestryMaxAggregateBytes = 256 << 20
)

// Stable lineage reason codes (operator/product APIs; sanitized messages).
const (
	LineageCodeLegacy            = "lineage_legacy"
	LineageCodeUnknownVersion    = "lineage_unknown_version"
	LineageCodePartial           = "lineage_partial"
	LineageCodeInvalidTime       = "lineage_invalid_time"
	LineageCodeInvalidDigest     = "lineage_invalid_digest"
	LineageCodeCrossSet          = "lineage_cross_set"
	LineageCodeMissingParent     = "lineage_missing_parent"
	LineageCodeDigestMismatch    = "lineage_digest_mismatch"
	LineageCodeSetRunMismatch    = "lineage_set_run_mismatch"
	LineageCodeCycle             = "lineage_cycle"
	LineageCodeGeneration        = "lineage_generation"
	LineageCodeBaselineConflict  = "lineage_baseline_conflict"
	LineageCodeBudgetDepth       = "lineage_budget_depth"
	LineageCodeBudgetNodes       = "lineage_budget_nodes"
	LineageCodeBudgetBytes       = "lineage_budget_bytes"
	LineageCodeMalformed         = "lineage_malformed"
	LineageCodeLookupRequired    = "lineage_lookup_required"
	LineageCodeRequireContinuous = "lineage_require_continuous"
)

// sha256HexRE matches a lowercase hex-encoded SHA-256 digest.
var sha256HexRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// StateParent is the exact single continuous-state parent binding. Distinct
// from parent_manifests (reachability/enrich heritage; digest optional there).
type StateParent struct {
	IndexSetID     string `json:"index_set_id"`
	RunID          string `json:"run_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// LineageRecord is the all-or-nothing versioned lineage contract.
// Absence of the whole object means pre-continuity / no continuous ancestry —
// never "generation 0" inferred from timestamps or directory order.
type LineageRecord struct {
	Version    int  `json:"version"`
	Generation int  `json:"generation"`
	Baseline   bool `json:"baseline"`
}

// LineageError is a fail-closed ancestry/structure error with a stable code.
type LineageError struct {
	Code    string
	Message string
	// Cause is optional low-level detail (not for operator presentation).
	Cause error
}

func (e *LineageError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return e.Code
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

func (e *LineageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func lineageErrorf(code, format string, args ...any) *LineageError {
	return &LineageError{Code: code, Message: fmt.Sprintf(format, args...)}
}

func lineageError(code, message string) *LineageError {
	return &LineageError{Code: code, Message: message}
}

// IsLineageCode reports whether err is a LineageError with the given code.
func IsLineageCode(err error, code string) bool {
	var le *LineageError
	if !errors.As(err, &le) {
		return false
	}
	return le.Code == code
}

// LineageCodeOf returns the stable code when err is a LineageError.
func LineageCodeOf(err error) string {
	var le *LineageError
	if !errors.As(err, &le) {
		return ""
	}
	return le.Code
}

// HasContinuousLineage reports whether the manifest claims a continuous
// lineage record (not whether the graph has been digest-verified).
func HasContinuousLineage(manifest InternalManifest) bool {
	return manifest.Lineage != nil
}

// ValidateManifestLineageStructure checks optional lineage fields when present.
// All-absent is legal (pre-continuity). Does not walk parents or verify digests.
//
// When lineage is present, an authoritative non-zero UTC run_started_at is
// required so continuous nodes can serve as trustworthy delta boundaries later.
// Generation-1 baseline may optionally bind one exact state_parent as a verified
// pre-continuity state source (not a delta boundary).
func ValidateManifestLineageStructure(manifest InternalManifest) error {
	hasLineage := manifest.Lineage != nil
	hasParent := manifest.StateParent != nil
	hasRunStart := manifest.RunStartedAt != nil

	// Optional run_started_at alone (no lineage) is allowed and validated if present.
	if hasRunStart {
		if err := validateAuthoritativeRunStartedAt(*manifest.RunStartedAt); err != nil {
			return err
		}
	}

	if !hasLineage && !hasParent {
		return nil
	}
	if hasParent && !hasLineage {
		return lineageError(LineageCodePartial, "state_parent requires lineage (partial continuity claim refused)")
	}

	// Continuous lineage requires authoritative run start.
	if !hasRunStart {
		return lineageError(LineageCodeInvalidTime, "continuous lineage requires authoritative run_started_at")
	}

	// Own identity must be safe single path components when claiming continuity.
	if err := validateSafeRunComponent(manifest.IndexSetID, "index_set_id"); err != nil {
		return err
	}
	if err := validateSafeRunComponent(manifest.RunID, "run_id"); err != nil {
		return err
	}

	lin := *manifest.Lineage
	if lin.Version != LineageVersionV1 {
		if lin.Version == 0 {
			return lineageError(LineageCodeUnknownVersion, "lineage.version is required and must be 1")
		}
		return lineageErrorf(LineageCodeUnknownVersion, "lineage.version %d is not supported", lin.Version)
	}
	if lin.Generation < LineageBaselineGeneration {
		return lineageErrorf(LineageCodeGeneration, "lineage.generation must be >= %d", LineageBaselineGeneration)
	}

	if lin.Baseline {
		if lin.Generation != LineageBaselineGeneration {
			return lineageErrorf(LineageCodeGeneration, "baseline generation must be %d", LineageBaselineGeneration)
		}
		// Optional exact pre-continuity state parent (0 or 1).
		if hasParent {
			if err := validateStateParentShape(*manifest.StateParent); err != nil {
				return err
			}
			if strings.TrimSpace(manifest.StateParent.IndexSetID) != strings.TrimSpace(manifest.IndexSetID) {
				return lineageError(LineageCodeCrossSet, "state_parent index_set_id must match manifest index_set_id")
			}
			if strings.TrimSpace(manifest.StateParent.RunID) == strings.TrimSpace(manifest.RunID) {
				return lineageError(LineageCodeCycle, "state_parent run_id must not equal manifest run_id")
			}
		}
		return nil
	}

	// baseline:false — continuous child requires full continuous-state parent.
	if !hasParent {
		return lineageError(LineageCodePartial, "baseline:false requires state_parent")
	}
	if lin.Generation < LineageBaselineGeneration+1 {
		return lineageErrorf(LineageCodeGeneration, "non-baseline generation must be >= %d", LineageBaselineGeneration+1)
	}
	if err := validateStateParentShape(*manifest.StateParent); err != nil {
		return err
	}
	// Same-set rule for the claimed parent edge (graph walk re-checks opened bytes).
	if strings.TrimSpace(manifest.StateParent.IndexSetID) != strings.TrimSpace(manifest.IndexSetID) {
		return lineageError(LineageCodeCrossSet, "state_parent index_set_id must match manifest index_set_id")
	}
	if strings.TrimSpace(manifest.StateParent.RunID) == strings.TrimSpace(manifest.RunID) {
		return lineageError(LineageCodeCycle, "state_parent run_id must not equal manifest run_id")
	}
	return nil
}

func validateAuthoritativeRunStartedAt(t time.Time) error {
	if t.IsZero() {
		return lineageError(LineageCodeInvalidTime, "run_started_at must be a non-zero RFC3339 UTC timestamp")
	}
	// Wire rule: UTC only (offset must be zero). No wall-clock-relative checks —
	// artifact validity must not change with observer time.
	if _, offset := t.Zone(); offset != 0 {
		return lineageError(LineageCodeInvalidTime, "run_started_at must be UTC (non-zero offset refused)")
	}
	return nil
}

func validateStateParentShape(parent StateParent) error {
	parent.IndexSetID = strings.TrimSpace(parent.IndexSetID)
	parent.RunID = strings.TrimSpace(parent.RunID)
	parent.ManifestSHA256 = strings.TrimSpace(parent.ManifestSHA256)
	if parent.IndexSetID == "" || parent.RunID == "" {
		return lineageError(LineageCodePartial, "state_parent requires index_set_id and run_id")
	}
	if parent.ManifestSHA256 == "" {
		return lineageError(LineageCodeInvalidDigest, "state_parent.manifest_sha256 is required")
	}
	if !sha256HexRE.MatchString(parent.ManifestSHA256) {
		if sha256HexRE.MatchString(strings.ToLower(parent.ManifestSHA256)) && parent.ManifestSHA256 != strings.ToLower(parent.ManifestSHA256) {
			return lineageError(LineageCodeInvalidDigest, "state_parent.manifest_sha256 must be lowercase hex")
		}
		return lineageError(LineageCodeInvalidDigest, "state_parent.manifest_sha256 must be lowercase sha256 hex")
	}
	if err := validateSafeRunComponent(parent.IndexSetID, "index_set_id"); err != nil {
		return err
	}
	if err := validateSafeRunComponent(parent.RunID, "run_id"); err != nil {
		return err
	}
	return nil
}

func validateSafeRunComponent(value, field string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return lineageErrorf(LineageCodeMalformed, "%s is required", field)
	}
	if strings.Contains(value, "/") || strings.Contains(value, "\\") || strings.Contains(value, "..") {
		return lineageErrorf(LineageCodeMalformed, "%s contains unsafe path components", field)
	}
	if filepath.Base(value) != value {
		return lineageErrorf(LineageCodeMalformed, "%s must be a single path component", field)
	}
	return nil
}

// AncestryMode classifies continuous vs pre-continuity ancestry.
type AncestryMode string

const (
	// AncestryModeLegacy means no lineage object: verified current-state only;
	// not a trustworthy delta / --since-run boundary.
	AncestryModeLegacy AncestryMode = "legacy"
	// AncestryModeContinuous means lineage is present and the graph verified.
	AncestryModeContinuous AncestryMode = "continuous"
)

// AncestryNode is one verified hop in a continuous ancestry chain.
type AncestryNode struct {
	IndexSetID     string
	RunID          string
	ManifestSHA256 string
	Generation     int
	Baseline       bool
	RunStartedAt   time.Time
	// DeltaBoundary is true when this node is the earliest trustworthy
	// continuous boundary (baseline:true). History without lineage behind it
	// is not a delta boundary even if a prior-state source exists.
	DeltaBoundary bool
}

// AncestryResult is the outcome of ResolveAncestry.
type AncestryResult struct {
	Mode AncestryMode
	// Chain is root-first toward the baseline (root, parent, ..., baseline).
	Chain []AncestryNode
	// DeltaBoundary is set when Mode is continuous (the baseline node).
	DeltaBoundary *AncestryNode
	// AccountedBytes is the total marker+manifest bytes charged across the graph.
	AccountedBytes int64
}

// AncestryBudget bounds graph walk resource use.
type AncestryBudget struct {
	MaxDepth          int
	MaxNodes          int
	MaxMarkerBytes    int64
	MaxManifestBytes  int64
	MaxAggregateBytes int64
}

// DefaultAncestryBudget returns production defaults.
func DefaultAncestryBudget() AncestryBudget {
	return AncestryBudget{
		MaxDepth:          DefaultAncestryMaxDepth,
		MaxNodes:          DefaultAncestryMaxNodes,
		MaxMarkerBytes:    DefaultMaxPublishedMarkerBytes,
		MaxManifestBytes:  DefaultMaxPublishedManifestBytes,
		MaxAggregateBytes: DefaultAncestryMaxAggregateBytes,
	}
}

func (b AncestryBudget) normalize() AncestryBudget {
	if b.MaxDepth <= 0 {
		b.MaxDepth = DefaultAncestryMaxDepth
	}
	if b.MaxNodes <= 0 {
		b.MaxNodes = DefaultAncestryMaxNodes
	}
	if b.MaxMarkerBytes <= 0 {
		b.MaxMarkerBytes = DefaultMaxPublishedMarkerBytes
	}
	if b.MaxManifestBytes <= 0 {
		b.MaxManifestBytes = DefaultMaxPublishedManifestBytes
	}
	if b.MaxAggregateBytes <= 0 {
		b.MaxAggregateBytes = DefaultAncestryMaxAggregateBytes
	}
	return b
}

// PublishedRunLookup resolves a published complete.json path for a set/run.
// Callers must only return paths under trusted roots; the resolver does not
// follow ref-supplied arbitrary paths.
type PublishedRunLookup func(indexSetID, runID string) (completePath string, err error)

// AncestryResolveConfig configures ResolveAncestry.
type AncestryResolveConfig struct {
	// Lookup is required when the root claims a non-baseline continuous parent.
	Lookup PublishedRunLookup
	Budget AncestryBudget
	// RequireContinuous fails closed on legacy (no lineage) instead of
	// returning AncestryModeLegacy.
	RequireContinuous bool
}

// ResolveAncestry walks digest-bound same-set state parents from a verified
// snapshot. It does not materialize segment rows and does not mutate the FS.
//
// Legacy roots (no lineage) return Mode=legacy without error unless
// RequireContinuous is set. Continuous roots fail closed on cycle, cross-set,
// digest mismatch, generation discontinuity, missing parent, or budget exhaustion.
//
// Continuous roots require CompletePath and are always re-opened under the
// aggregate remaining-byte budget (self-asserted Accounted*Bytes alone are not
// provenance). Node budget is checked before parent lookup/open. Aggregate
// remaining is carried into the open seam so marker/manifest reads are capped.
//
// baseline:true may optionally verify one pre-continuity state_parent (legacy
// parent need not carry lineage); trusted delta ancestry still stops at baseline.
func ResolveAncestry(root PublishedSnapshot, cfg AncestryResolveConfig) (AncestryResult, error) {
	cfg.Budget = cfg.Budget.normalize()
	if err := ValidateManifestLineageStructure(root.Manifest); err != nil {
		return AncestryResult{}, err
	}

	if root.Manifest.Lineage == nil {
		if cfg.RequireContinuous {
			return AncestryResult{}, lineageError(LineageCodeRequireContinuous, "manifest has no continuous lineage (legacy latest is a verified state source only, not a delta boundary)")
		}
		return AncestryResult{Mode: AncestryModeLegacy}, nil
	}

	completePath := strings.TrimSpace(root.CompletePath)
	if completePath == "" {
		return AncestryResult{}, lineageError(LineageCodeMalformed, "continuous ancestry requires CompletePath for trusted root reopen")
	}

	remaining := cfg.Budget.MaxAggregateBytes
	current, err := openPublishedCompleteBudgeted(completePath, cfg.Budget.MaxMarkerBytes, cfg.Budget.MaxManifestBytes, remaining)
	if err != nil {
		return AncestryResult{}, mapOpenErrorToLineage(err, "root")
	}
	// Prefer caller's claimed identity when provided; otherwise bind to reopened bytes.
	if strings.TrimSpace(root.Complete.IndexSetID) != "" || strings.TrimSpace(root.Complete.RunID) != "" || strings.TrimSpace(root.Complete.ManifestSHA256) != "" {
		if root.Complete.IndexSetID != "" && current.Complete.IndexSetID != root.Complete.IndexSetID {
			return AncestryResult{}, lineageError(LineageCodeSetRunMismatch, "root complete path identity disagrees with snapshot")
		}
		if root.Complete.RunID != "" && current.Complete.RunID != root.Complete.RunID {
			return AncestryResult{}, lineageError(LineageCodeSetRunMismatch, "root complete path identity disagrees with snapshot")
		}
		if root.Complete.ManifestSHA256 != "" && current.Complete.ManifestSHA256 != root.Complete.ManifestSHA256 {
			return AncestryResult{}, lineageError(LineageCodeDigestMismatch, "root complete path digest disagrees with snapshot")
		}
	}
	aggBytes := current.AccountedBytes()
	remaining -= aggBytes
	if remaining < 0 {
		return AncestryResult{}, lineageErrorf(LineageCodeBudgetBytes, "ancestry exceeds aggregate byte budget %d", cfg.Budget.MaxAggregateBytes)
	}

	chain := make([]AncestryNode, 0, 4)
	visited := map[string]struct{}{}
	depth := 0

	for {
		key := manifestKey(current.Manifest.IndexSetID, current.Manifest.RunID)
		if _, seen := visited[key]; seen {
			return AncestryResult{}, lineageError(LineageCodeCycle, "lineage cycle detected")
		}
		// Prospective continuous node count before recording this node.
		if len(visited)+1 > cfg.Budget.MaxNodes {
			return AncestryResult{}, lineageErrorf(LineageCodeBudgetNodes, "ancestry exceeds max nodes %d", cfg.Budget.MaxNodes)
		}
		visited[key] = struct{}{}

		node := ancestryNodeFromSnapshot(current)
		chain = append(chain, node)

		if current.Manifest.Lineage == nil {
			return AncestryResult{}, lineageError(LineageCodeGeneration, "continuous lineage hop lacks lineage record")
		}

		if current.Manifest.Lineage.Baseline {
			// Optional pre-continuity state source: still a graph node/edge for
			// budgets, but not a trusted delta boundary and not on Chain.
			if current.Manifest.StateParent != nil {
				// Same contract as continuous parents: refuse identity already in
				// the walk as lineage_cycle before Lookup/FS I/O.
				preParentKey := manifestKey(
					strings.TrimSpace(current.Manifest.StateParent.IndexSetID),
					strings.TrimSpace(current.Manifest.StateParent.RunID),
				)
				if _, seen := visited[preParentKey]; seen {
					return AncestryResult{}, lineageError(LineageCodeCycle, "lineage cycle detected")
				}
				// Prospectively charge one node + one edge before Lookup/FS I/O.
				if len(visited)+1 > cfg.Budget.MaxNodes {
					return AncestryResult{}, lineageErrorf(LineageCodeBudgetNodes, "ancestry exceeds max nodes %d", cfg.Budget.MaxNodes)
				}
				if depth+1 > cfg.Budget.MaxDepth {
					return AncestryResult{}, lineageErrorf(LineageCodeBudgetDepth, "ancestry exceeds max depth %d", cfg.Budget.MaxDepth)
				}
				charged, err := verifyPreContinuityStateParent(current, cfg, remaining)
				if err != nil {
					return AncestryResult{}, err
				}
				// remaining was already enforced inside the open; only total charge is needed for the result.
				aggBytes += charged
			}
			boundary := chain[len(chain)-1]
			boundary.DeltaBoundary = true
			chain[len(chain)-1] = boundary
			return AncestryResult{
				Mode:           AncestryModeContinuous,
				Chain:          chain,
				DeltaBoundary:  &boundary,
				AccountedBytes: aggBytes,
			}, nil
		}

		if current.Manifest.StateParent == nil {
			return AncestryResult{}, lineageError(LineageCodePartial, "non-baseline lineage missing state_parent")
		}
		parentRef := *current.Manifest.StateParent
		parentRef.IndexSetID = strings.TrimSpace(parentRef.IndexSetID)
		parentRef.RunID = strings.TrimSpace(parentRef.RunID)
		parentRef.ManifestSHA256 = strings.TrimSpace(parentRef.ManifestSHA256)

		if parentRef.IndexSetID != strings.TrimSpace(current.Manifest.IndexSetID) {
			return AncestryResult{}, lineageError(LineageCodeCrossSet, "state_parent crosses index set boundary")
		}

		// Detect identity cycle before parent IO or generation comparison.
		parentKey := manifestKey(parentRef.IndexSetID, parentRef.RunID)
		if _, seen := visited[parentKey]; seen {
			return AncestryResult{}, lineageError(LineageCodeCycle, "lineage cycle detected")
		}

		// Prospective continuous parent node: refuse before lookup/open.
		if len(visited)+1 > cfg.Budget.MaxNodes {
			return AncestryResult{}, lineageErrorf(LineageCodeBudgetNodes, "ancestry exceeds max nodes %d", cfg.Budget.MaxNodes)
		}

		depth++
		if depth > cfg.Budget.MaxDepth {
			return AncestryResult{}, lineageErrorf(LineageCodeBudgetDepth, "ancestry exceeds max depth %d", cfg.Budget.MaxDepth)
		}

		if remaining <= 0 {
			return AncestryResult{}, lineageErrorf(LineageCodeBudgetBytes, "ancestry exceeds aggregate byte budget %d", cfg.Budget.MaxAggregateBytes)
		}

		if cfg.Lookup == nil {
			return AncestryResult{}, lineageError(LineageCodeLookupRequired, "published run lookup is required to verify parent")
		}
		parentPath, err := cfg.Lookup(parentRef.IndexSetID, parentRef.RunID)
		if err != nil {
			return AncestryResult{}, &LineageError{
				Code:    LineageCodeMissingParent,
				Message: "parent run is not resolvable",
				Cause:   err,
			}
		}
		parentPath = strings.TrimSpace(parentPath)
		if parentPath == "" {
			return AncestryResult{}, lineageError(LineageCodeMissingParent, "parent run lookup returned empty path")
		}

		parentSnap, err := openPublishedCompleteBudgeted(parentPath, cfg.Budget.MaxMarkerBytes, cfg.Budget.MaxManifestBytes, remaining)
		if err != nil {
			return AncestryResult{}, mapOpenErrorToLineage(err, "parent")
		}
		aggBytes += parentSnap.AccountedBytes()
		remaining -= parentSnap.AccountedBytes()
		if remaining < 0 {
			return AncestryResult{}, lineageErrorf(LineageCodeBudgetBytes, "ancestry exceeds aggregate byte budget %d", cfg.Budget.MaxAggregateBytes)
		}

		if parentSnap.Complete.IndexSetID != parentRef.IndexSetID || parentSnap.Complete.RunID != parentRef.RunID {
			return AncestryResult{}, lineageError(LineageCodeSetRunMismatch, "opened parent set/run does not match state_parent")
		}
		if parentSnap.Manifest.IndexSetID != parentRef.IndexSetID || parentSnap.Manifest.RunID != parentRef.RunID {
			return AncestryResult{}, lineageError(LineageCodeSetRunMismatch, "opened parent manifest set/run does not match state_parent")
		}
		if parentSnap.Complete.ManifestSHA256 != parentRef.ManifestSHA256 {
			return AncestryResult{}, lineageError(LineageCodeDigestMismatch, "state_parent manifest digest does not match complete marker")
		}
		// Continuous (non-baseline) parents must themselves carry continuous lineage.
		if parentSnap.Manifest.Lineage == nil {
			return AncestryResult{}, lineageError(LineageCodeGeneration, "continuous lineage parent lacks lineage record (no invented baseline)")
		}
		if current.Manifest.Lineage.Generation != parentSnap.Manifest.Lineage.Generation+1 {
			return AncestryResult{}, lineageErrorf(LineageCodeGeneration, "generation discontinuity: child %d parent %d",
				current.Manifest.Lineage.Generation, parentSnap.Manifest.Lineage.Generation)
		}

		current = parentSnap
	}
}

// verifyPreContinuityStateParent opens and digest-checks a baseline's optional
// state_parent. The parent is a verified prior-state source only — it must not
// already carry continuous lineage (no silent ancestry reset) and is not a
// trusted delta boundary. Callers must enforce node/depth budgets before Lookup.
func verifyPreContinuityStateParent(baseline PublishedSnapshot, cfg AncestryResolveConfig, remaining int64) (charged int64, err error) {
	parentRef := *baseline.Manifest.StateParent
	parentRef.IndexSetID = strings.TrimSpace(parentRef.IndexSetID)
	parentRef.RunID = strings.TrimSpace(parentRef.RunID)
	parentRef.ManifestSHA256 = strings.TrimSpace(parentRef.ManifestSHA256)

	if parentRef.IndexSetID != strings.TrimSpace(baseline.Manifest.IndexSetID) {
		return 0, lineageError(LineageCodeCrossSet, "baseline state_parent crosses index set boundary")
	}
	if remaining <= 0 {
		return 0, lineageErrorf(LineageCodeBudgetBytes, "ancestry exceeds aggregate byte budget %d", cfg.Budget.MaxAggregateBytes)
	}
	if cfg.Lookup == nil {
		return 0, lineageError(LineageCodeLookupRequired, "published run lookup is required to verify baseline state_parent")
	}
	parentPath, err := cfg.Lookup(parentRef.IndexSetID, parentRef.RunID)
	if err != nil {
		return 0, &LineageError{Code: LineageCodeMissingParent, Message: "baseline state_parent run is not resolvable", Cause: err}
	}
	parentPath = strings.TrimSpace(parentPath)
	if parentPath == "" {
		return 0, lineageError(LineageCodeMissingParent, "baseline state_parent lookup returned empty path")
	}

	// Legacy parent (no lineage fields) opens fine. Continuous-shaped parents
	// are refused after open with lineage_baseline_conflict (no silent reset).
	parentSnap, err := openPublishedCompleteBudgeted(parentPath, cfg.Budget.MaxMarkerBytes, cfg.Budget.MaxManifestBytes, remaining)
	if err != nil {
		return 0, mapOpenErrorToLineage(err, "baseline-parent")
	}
	if parentSnap.Complete.IndexSetID != parentRef.IndexSetID || parentSnap.Complete.RunID != parentRef.RunID {
		return 0, lineageError(LineageCodeSetRunMismatch, "baseline state_parent set/run does not match complete marker")
	}
	if parentSnap.Manifest.IndexSetID != parentRef.IndexSetID || parentSnap.Manifest.RunID != parentRef.RunID {
		return 0, lineageError(LineageCodeSetRunMismatch, "baseline state_parent set/run does not match manifest")
	}
	if parentSnap.Complete.ManifestSHA256 != parentRef.ManifestSHA256 {
		return 0, lineageError(LineageCodeDigestMismatch, "baseline state_parent digest does not match complete marker")
	}
	if parentSnap.Manifest.Lineage != nil {
		return 0, lineageError(LineageCodeBaselineConflict, "baseline state_parent must be pre-continuity (parent lineage present refuses silent ancestry reset)")
	}
	return parentSnap.AccountedBytes(), nil
}

// mapOpenErrorToLineage preserves LineageError codes from structural validation
// and maps known open failures to stable lineage codes.
func mapOpenErrorToLineage(err error, role string) error {
	if err == nil {
		return nil
	}
	var le *LineageError
	if errors.As(err, &le) {
		return le
	}
	msg := err.Error()
	switch {
	case errors.Is(err, ErrSnapshotNotPublished), os.IsNotExist(err):
		return &LineageError{Code: LineageCodeMissingParent, Message: role + " snapshot not found", Cause: err}
	case strings.Contains(msg, "aggregate byte budget exhausted"):
		return &LineageError{Code: LineageCodeBudgetBytes, Message: role + " open exceeds aggregate byte budget", Cause: err}
	case strings.Contains(msg, "manifest digest mismatch"):
		return &LineageError{Code: LineageCodeDigestMismatch, Message: role + " complete marker disagrees with manifest bytes", Cause: err}
	case strings.Contains(msg, "index_set_id mismatch"),
		strings.Contains(msg, "run_id mismatch"),
		strings.Contains(msg, "manifest index_set_id mismatch"),
		strings.Contains(msg, "manifest run_id mismatch"):
		return &LineageError{Code: LineageCodeSetRunMismatch, Message: role + " set/run identity mismatch", Cause: err}
	default:
		return &LineageError{Code: LineageCodeMalformed, Message: "open " + role + " snapshot", Cause: err}
	}
}

func ancestryNodeFromSnapshot(snap PublishedSnapshot) AncestryNode {
	node := AncestryNode{
		IndexSetID:     snap.Manifest.IndexSetID,
		RunID:          snap.Manifest.RunID,
		ManifestSHA256: snap.Complete.ManifestSHA256,
	}
	if snap.Manifest.RunStartedAt != nil {
		node.RunStartedAt = snap.Manifest.RunStartedAt.UTC()
	}
	if snap.Manifest.Lineage != nil {
		node.Generation = snap.Manifest.Lineage.Generation
		node.Baseline = snap.Manifest.Lineage.Baseline
	}
	return node
}

// cloneAcceptedRunStartedAt returns a UTC copy of an already-accepted
// authoritative run start, or nil when t is nil/zero. Emission normalization
// only — not a substitute for validateAuthoritativeRunStartedAt. Non-UTC
// offsets are refused (not rewritten) so a validation bypass cannot silently
// accept +01:00 (or any non-zero offset) on the writer seam.
func cloneAcceptedRunStartedAt(t *time.Time) (*time.Time, error) {
	if t == nil || t.IsZero() {
		return nil, nil
	}
	if err := validateAuthoritativeRunStartedAt(*t); err != nil {
		return nil, err
	}
	u := t.UTC()
	return &u, nil
}

// NormalizeStateParent returns a trimmed copy; empty input stays empty-shaped.
func NormalizeStateParent(parent *StateParent) *StateParent {
	if parent == nil {
		return nil
	}
	out := &StateParent{
		IndexSetID:     strings.TrimSpace(parent.IndexSetID),
		RunID:          strings.TrimSpace(parent.RunID),
		ManifestSHA256: strings.TrimSpace(parent.ManifestSHA256),
	}
	return out
}

// NormalizeLineage returns a copy of the lineage record or nil.
func NormalizeLineage(lin *LineageRecord) *LineageRecord {
	if lin == nil {
		return nil
	}
	out := *lin
	return &out
}

// ManifestSHA256Of returns the SHA-256 hex digest of raw manifest bytes.
func ManifestSHA256Of(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
