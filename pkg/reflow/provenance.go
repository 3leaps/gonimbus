package reflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// Provenance sidecar schema identity. These are the wire contract for the
// gonimbus.provenance.v1 audit object; do not change without a schema revision.
const (
	ProvenanceSchema        = "gonimbus.provenance.v1"
	ProvenanceSchemaVersion = "1.0.0"

	// ProvenanceWriteFailedWarningCode is the warning code emitted when a sidecar
	// write fails under the warn policy.
	ProvenanceWriteFailedWarningCode = "PROVENANCE_WRITE_FAILED"
)

// ProvenanceSidecarInput carries the per-object facts a provenance sidecar
// records, expressed independently of the CLI-pool or engine input types so a
// single builder serves both surfaces. The command adapter fills it from its
// reflowTask; the engine fills it from a reflowInput.
type ProvenanceSidecarInput struct {
	SourceURI     string
	SourceETag    string
	SourceSize    int64
	SourceLastMod time.Time
	DestURI       string
	DestETag      string
	DestSize      int64
	RoutingClass  string
	// RewriteTemplate is the rewrite "to" template, when one was used.
	RewriteTemplate string
	// QuarantinePrefix is set only for quarantine routing.
	QuarantinePrefix string
	Collision        *CollisionInfo
	Vars             map[string]string
	Probe            *probe.ProbeAudit
	// Action is the pool action string recorded in the sidecar (e.g. the landed
	// action, "skipped.duplicate", or "quarantined").
	Action string
}

// ProvenanceSidecarPayload is the gonimbus.provenance.v1 audit object. Its JSON
// serialization is the wire contract compared byte-for-byte across execution
// paths (modulo the intentionally time-varying run.ts).
type ProvenanceSidecarPayload struct {
	Schema        string                 `json:"schema"`
	SchemaVersion string                 `json:"schema_version"`
	Source        provenanceSourceBlock  `json:"source"`
	Destination   provenanceDestBlock    `json:"destination"`
	Run           provenanceRunBlock     `json:"run"`
	Routing       provenanceRoutingBlock `json:"routing"`
	Collision     *CollisionInfo         `json:"collision,omitempty"`
	Vars          map[string]string      `json:"vars,omitempty"`
	Probe         *probe.ProbeAudit      `json:"probe,omitempty"`
	Action        string                 `json:"action"`
}

type provenanceSourceBlock struct {
	URI          string     `json:"uri"`
	ETag         string     `json:"etag,omitempty"`
	Size         int64      `json:"size,omitempty"`
	LastModified *time.Time `json:"last_modified,omitempty"`
}

type provenanceDestBlock struct {
	URI  string `json:"uri"`
	ETag string `json:"etag,omitempty"`
	Size int64  `json:"size,omitempty"`
}

type provenanceRunBlock struct {
	RunID       string `json:"run_id"`
	TS          string `json:"ts"`
	ToolVersion string `json:"tool_version"`
}

type provenanceRoutingBlock struct {
	RoutingClass     string  `json:"routing_class"`
	RewriteTemplate  string  `json:"rewrite_template,omitempty"`
	QuarantinePrefix *string `json:"quarantine_prefix"`
}

// BuildProvenanceSidecar assembles the audit payload from neutral inputs. runID
// and toolVersion are supplied by the caller (the run identity/version are
// explicit inputs, never reached from process globals). ts is the run timestamp;
// callers pass a stable value so parity comparisons can normalize only this field.
func BuildProvenanceSidecar(runID, toolVersion, ts string, in ProvenanceSidecarInput) ProvenanceSidecarPayload {
	routingClass := in.RoutingClass
	if routingClass == "" {
		routingClass = "normal"
	}
	var lastModified *time.Time
	if !in.SourceLastMod.IsZero() {
		t := in.SourceLastMod.UTC()
		lastModified = &t
	}
	var quarantinePrefix *string
	if routingClass == "quarantine" {
		prefix := in.QuarantinePrefix
		quarantinePrefix = &prefix
	}
	return ProvenanceSidecarPayload{
		Schema:        ProvenanceSchema,
		SchemaVersion: ProvenanceSchemaVersion,
		Source: provenanceSourceBlock{
			URI:          in.SourceURI,
			ETag:         in.SourceETag,
			Size:         in.SourceSize,
			LastModified: lastModified,
		},
		Destination: provenanceDestBlock{
			URI:  in.DestURI,
			ETag: in.DestETag,
			Size: in.DestSize,
		},
		Run: provenanceRunBlock{
			RunID:       runID,
			TS:          ts,
			ToolVersion: toolVersion,
		},
		Routing: provenanceRoutingBlock{
			RoutingClass:     routingClass,
			RewriteTemplate:  in.RewriteTemplate,
			QuarantinePrefix: quarantinePrefix,
		},
		Collision: in.Collision,
		Vars:      in.Vars,
		Probe:     in.Probe,
		Action:    in.Action,
	}
}

// ProvenanceNow returns the RFC3339Nano UTC timestamp used for run.ts.
func ProvenanceNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// ProvenanceEmitter delivers the sidecar writer's warn/fail outcomes through the
// calling surface's native record path, so each surface keeps its own record
// shape (the command pool emits its error record; the engine emits an
// ErrorEvent through its EventSink) while sharing the content and policy.
type ProvenanceEmitter interface {
	// EmitProvenanceWarning reports a warn-policy sidecar write failure.
	EmitProvenanceWarning(ctx context.Context, w Warning) error
	// EmitProvenanceError reports a fail-policy sidecar write failure.
	EmitProvenanceError(ctx context.Context, key, message string, cause error, details map[string]any) error
}

// ProvenancePutFunc writes a marshaled sidecar payload to the sidecar key. It is
// the injected write transport so the shared writer stays transport-agnostic: the
// engine passes a function that routes the PUT through its concurrency/AIMD
// limiter (Acquire + ObserveProviderResult); the command pool passes the
// provider's PutObject directly (ProvenancePutViaProvider). A nil ProvenancePutFunc
// means the sidecar destination cannot accept a PUT and is reported through the
// on-write-error policy.
type ProvenancePutFunc func(ctx context.Context, key string, body io.Reader, size int64) error

// ProvenancePutViaProvider adapts a provider's ObjectPutter to a ProvenancePutFunc,
// returning nil when the provider does not support PutObject so the shared writer
// reports the unsupported-destination failure through the on-write-error policy.
func ProvenancePutViaProvider(p provider.Provider) ProvenancePutFunc {
	putter, ok := p.(provider.ObjectPutter)
	if !ok {
		return nil
	}
	return putter.PutObject
}

// WriteProvenanceSidecar builds the audit payload, writes it through put, and
// applies the on-write-error policy. It returns the reference (with Written set),
// whether the failure is fatal to the item (fail policy), and any event-sink
// delivery error. The sidecarKey/sidecarURI are resolved by the caller from its
// placement layout; mode labels the operational surface in the emitted details.
// Emission goes through emit so each surface keeps its native record shape.
//
// The event-sink delivery error is propagated (never swallowed): a warn- or
// fail-policy write failure whose warning/error event could not be delivered
// must not let a caller silently proceed to a success terminal, so the caller
// treats a non-nil emitErr as fatal to the item (fatal-to-item ordering).
func WriteProvenanceSidecar(ctx context.Context, put ProvenancePutFunc, runID, toolVersion, ts, mode, onWriteError, sidecarKey, sidecarURI, destURI string, in ProvenanceSidecarInput, emit ProvenanceEmitter) (ref *ProvenanceRef, sidecarFatal bool, emitErr error) {
	ref = &ProvenanceRef{Written: false, Key: sidecarKey, URI: sidecarURI}
	details := map[string]any{"sidecar_key": sidecarKey, "sidecar_uri": sidecarURI, "dest_uri": destURI, "mode": mode}

	if put == nil {
		fatal, err := provenanceWriteFailure(ctx, emit, onWriteError, sidecarKey, details, fmt.Errorf("destination provider does not support PutObject"))
		return ref, fatal, err
	}
	payload, err := json.Marshal(BuildProvenanceSidecar(runID, toolVersion, ts, in))
	if err != nil {
		fatal, eerr := provenanceWriteFailure(ctx, emit, onWriteError, sidecarKey, details, err)
		return ref, fatal, eerr
	}
	payload = append(payload, '\n')
	if err := put(ctx, sidecarKey, bytes.NewReader(payload), int64(len(payload))); err != nil {
		fatal, eerr := provenanceWriteFailure(ctx, emit, onWriteError, sidecarKey, details, err)
		return ref, fatal, eerr
	}
	ref.Written = true
	return ref, false, nil
}

// provenanceWriteFailure applies the on-write-error policy and returns whether
// the failure is fatal to the item plus the event-sink delivery error (nil when
// the warning/error was delivered). The caller must not silently succeed when the
// delivery error is non-nil.
func provenanceWriteFailure(ctx context.Context, emit ProvenanceEmitter, onWriteError, sidecarKey string, details map[string]any, cause error) (fatal bool, emitErr error) {
	if onWriteError == ProvenanceOnWriteErrorFail {
		return true, emit.EmitProvenanceError(ctx, sidecarKey, "provenance sidecar write failed", cause, details)
	}
	return false, emit.EmitProvenanceWarning(ctx, Warning{
		Code:    ProvenanceWriteFailedWarningCode,
		Message: FormatErrorMessage("provenance sidecar write failed", cause),
		Key:     sidecarKey,
		Details: details,
	})
}

// On-write-error policy values shared by both surfaces.
const (
	ProvenanceOnWriteErrorWarn = "warn"
	ProvenanceOnWriteErrorFail = "fail"
)

// Provenance mode and placement values shared by both surfaces.
const (
	ProvenanceModeNone         = "none"
	ProvenanceModeSidecar      = "sidecar"
	ProvenancePlacementSibling = "sibling"
	ProvenancePlacementMirror  = "mirrored-root"
)

// ProvenancePlan is the resolved, validated provenance-sidecar execution plan and
// the library-owned authority for a run. Validate is canonical: the command
// adapter delegates to it, and the runner calls it before any stream read, event
// emission, provider probe, or destination mutation. A plan carries explicit run
// identity/version (audit anchors, never reached from process globals) and the
// explicit unsafe-suffix confirmation, so an accidental data-extension suffix
// cannot pass as an approved one.
type ProvenancePlan struct {
	Mode              string
	Suffix            string
	AllowUnsafeSuffix bool
	OnWriteError      string
	Placement         ProvenancePlacementPlan
	RunID             string
	ToolVersion       string
}

// ProvenancePlacementPlan is the resolved sidecar placement. SidecarRoot is set
// only for mirrored-root placement.
type ProvenancePlacementPlan struct {
	Mode        string
	SidecarRoot *ProvenanceSidecarRoot
	// SidecarProvider is an injected second provider handle used only for a
	// file mirrored-root, whose sidecars land under a base directory distinct
	// from the destination provider's. Sibling and same-bucket object-store
	// mirrored placement write through the destination handle, so this stays
	// nil for them. It is redaction-safe: String/GoString expose presence only.
	SidecarProvider provider.Provider
}

// ProvenanceSidecarRoot is a resolved mirrored sidecar-root layout. For object
// stores it is constrained to the destination bucket (SameBucketAsDest true); a
// cross-bucket object-store root is refused at resolution, so a sidecar URI is
// never rendered from a bucket other than the one written through.
type ProvenanceSidecarRoot struct {
	Provider         string
	Bucket           string
	Prefix           string
	BaseDir          string
	BaseURI          string
	SameBucketAsDest bool
}

// Enabled reports whether the plan writes sidecars.
func (p ProvenancePlan) Enabled() bool { return p.Mode == ProvenanceModeSidecar }

// Validate enforces the provenance plan's internal consistency before any I/O.
func (p ProvenancePlan) Validate() error {
	// Mode and on-write-error rules delegate to the shared validator so the engine
	// and the CLI adapter enforce one canonical rule set. This runs before the
	// disabled-mode short-circuit so an invalid on-write-error value is rejected
	// even when provenance is disabled, matching the command-side helper rather
	// than silently accepting it.
	if err := ValidateProvenanceModeAndPolicy(p.Mode, p.OnWriteError); err != nil {
		return err
	}
	if !p.Enabled() {
		return nil
	}
	if err := ValidateProvenanceSuffix(p.Suffix, p.AllowUnsafeSuffix); err != nil {
		return err
	}
	if strings.TrimSpace(p.RunID) == "" {
		return fmt.Errorf("provenance run_id is required")
	}
	if strings.TrimSpace(p.ToolVersion) == "" {
		return fmt.Errorf("provenance tool_version is required")
	}
	switch p.Placement.Mode {
	case "", ProvenancePlacementSibling:
		// sibling writes through the destination authority; no root needed.
	case ProvenancePlacementMirror:
		root := p.Placement.SidecarRoot
		if root == nil {
			return fmt.Errorf("mirrored-root provenance placement requires a resolved sidecar root")
		}
		// The resolved sidecar authority must be internally consistent so the
		// rendered URI and the write authority describe one location: an
		// object-store root carries a bucket and no base dir; a file root carries a
		// base dir and no bucket; a file root additionally requires the injected
		// second provider (its authority handle).
		switch root.Provider {
		case string(provider.ProviderS3), string(provider.ProviderGCS):
			if strings.TrimSpace(root.Bucket) == "" {
				return fmt.Errorf("object-store provenance sidecar root requires a bucket")
			}
			if root.BaseDir != "" {
				return fmt.Errorf("object-store provenance sidecar root must not carry a file base dir")
			}
			// Must resolve to the destination bucket so URI and write authority
			// cannot diverge (proven against the destination in ValidateAgainstDestination).
			if !root.SameBucketAsDest {
				return fmt.Errorf("cross-bucket object-store provenance sidecar root is not supported; use a same-bucket mirrored root")
			}
		case string(provider.ProviderFile):
			if strings.TrimSpace(root.BaseDir) == "" {
				return fmt.Errorf("file provenance sidecar root requires a base dir")
			}
			if root.Bucket != "" {
				return fmt.Errorf("file provenance sidecar root must not carry a bucket")
			}
			if p.Placement.SidecarProvider == nil {
				return fmt.Errorf("file mirrored-root provenance placement requires an injected sidecar provider bound to the base dir")
			}
		default:
			return fmt.Errorf("provenance sidecar root provider must be one of: s3, gcs, file")
		}
		// Prove the declared BaseURI decomposes to the same provider/layout as the
		// structured fields, so a direct library caller cannot name one authority in
		// Provider/Bucket/Prefix/BaseDir while RunConfig publishes a different one in
		// the run echo's BaseURI. (An object-store bucket is additionally proven
		// against the live destination in ValidateAgainstDestination.)
		if err := validateSidecarRootURIConsistency(root); err != nil {
			return err
		}
	default:
		return fmt.Errorf("provenance placement must be one of: sibling, mirrored-root")
	}
	return nil
}

// validateSidecarRootURIConsistency proves a mirrored sidecar root's declared
// BaseURI decomposes to the same provider and layout as its structured
// Provider/Bucket/Prefix/BaseDir fields. It runs pre-I/O in Validate and reuses
// ParseDestLayout — the same decomposition the engine keys writes against — so a
// direct library caller cannot declare one authority in the struct fields while
// the run echo (RunConfig) publishes a different authority via BaseURI. The
// injected file handle itself remains caller-attested (a trusted handle like
// reflow.Destination): this proves the declared URI/layout agree, not that the
// handle writes where BaseDir claims.
func validateSidecarRootURIConsistency(root *ProvenanceSidecarRoot) error {
	derived, err := ParseDestLayout(root.BaseURI)
	if err != nil {
		return fmt.Errorf("provenance sidecar root base_uri %q is not a valid destination layout: %w", root.BaseURI, err)
	}
	if derived.ProviderID != root.Provider {
		return fmt.Errorf("provenance sidecar root base_uri provider %q must equal the declared root provider %q", derived.ProviderID, root.Provider)
	}
	switch root.Provider {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		if derived.Bucket != root.Bucket {
			return fmt.Errorf("provenance sidecar root base_uri bucket %q must equal the declared root bucket %q", derived.Bucket, root.Bucket)
		}
		if strings.Trim(derived.Prefix, "/") != strings.Trim(root.Prefix, "/") {
			return fmt.Errorf("provenance sidecar root base_uri prefix %q must equal the declared root prefix %q", derived.Prefix, root.Prefix)
		}
	case string(provider.ProviderFile):
		if derived.BaseDir != root.BaseDir {
			return fmt.Errorf("provenance sidecar root base_uri base dir %q must equal the declared root base dir %q", derived.BaseDir, root.BaseDir)
		}
	}
	return nil
}

func isObjectStoreProvenanceProvider(prov string) bool {
	return prov == string(provider.ProviderS3) || prov == string(provider.ProviderGCS)
}

// ValidateAgainstDestination verifies an object-store mirrored sidecar root against
// the run's actual resolved destination (provider id + bucket), so a caller's
// SameBucketAsDest assertion is proven rather than trusted. It runs at the runner
// boundary, before any I/O: a self-asserted same-bucket root that in fact names a
// different provider or bucket is refused, closing the fail-closed authority-
// integrity gap that ProvenancePlan.Validate (which has no destination) cannot see.
// The file mirrored root has no bucket; its authority is the injected provider,
// checked by the callable-ObjectPutter admission.
func (p ProvenancePlan) ValidateAgainstDestination(destProviderID, destBucket string) error {
	if !p.Enabled() || p.Placement.Mode != ProvenancePlacementMirror || p.Placement.SidecarRoot == nil {
		return nil
	}
	root := p.Placement.SidecarRoot
	if !isObjectStoreProvenanceProvider(root.Provider) {
		return nil
	}
	if root.Provider != destProviderID {
		return fmt.Errorf("provenance sidecar root provider %q must equal the destination provider %q", root.Provider, destProviderID)
	}
	if root.Bucket != destBucket {
		return fmt.Errorf("cross-bucket object-store provenance sidecar root is not supported; root bucket %q must equal the destination bucket %q", root.Bucket, destBucket)
	}
	return nil
}

// sidecarKeyFor resolves the sidecar object key for a destination, mirroring the
// command pool's buildProvenanceSidecarKey exactly so both surfaces write to the
// same key. Sibling placement writes next to the object (destKey+suffix); a
// mirrored object-store root writes prefix-relative (same bucket as the
// destination, enforced by Validate); a mirrored file root writes rel+suffix
// under its own base directory.
func (p ProvenancePlan) sidecarKeyFor(destRel, destKey string) string {
	if p.Placement.Mode != ProvenancePlacementMirror || p.Placement.SidecarRoot == nil {
		return destKey + p.Suffix
	}
	root := p.Placement.SidecarRoot
	rel := strings.Trim(destRel, "/")
	switch root.Provider {
	case string(provider.ProviderS3), string(provider.ProviderGCS):
		key := strings.TrimPrefix(root.Prefix+rel, "/")
		key = strings.ReplaceAll(key, "//", "/")
		return key + p.Suffix
	case string(provider.ProviderFile):
		return rel + p.Suffix
	default:
		return destKey + p.Suffix
	}
}

// isFileMirror reports whether the plan places sidecars under a mirrored file
// root (the only placement that writes through a distinct injected handle rather
// than the destination handle).
func (p ProvenancePlan) isFileMirror() bool {
	return p.Placement.Mode == ProvenancePlacementMirror && p.Placement.SidecarRoot != nil &&
		p.Placement.SidecarRoot.Provider == string(provider.ProviderFile)
}

// sidecarURIFor renders the sidecar URI for a resolved sidecar key from the same
// authority branch the sidecar is written through. A mirrored file root renders
// against its own injected base directory; every other case — sibling and
// same-bucket object-store mirrored — renders through the destination layout.
// Object-store mirrored roots deliberately render from the destination layout
// (not the root's own provider/bucket fields): the root is same-bucket by
// construction, so this yields the identical URI while making a bucket-A-URI /
// handle-B-write mismatch structurally impossible even for an inconsistently-
// populated root. For the file mirror the URI comes from the declared BaseDir and
// the bytes go through the injected handle: Validate proves the declared BaseURI/
// BaseDir agree, but that the injected handle actually writes under BaseDir is
// caller-attested (a trusted handle like reflow.Destination) and not provable on
// this API — so URI and write authority cannot diverge only insofar as the
// embedder honors that declared handle/BaseDir binding.
func (p ProvenancePlan) sidecarURIFor(layout DestLayout, sidecarKey string) string {
	if p.isFileMirror() {
		root := p.Placement.SidecarRoot
		return fileURI(filepath.Join(root.BaseDir, filepath.FromSlash(sidecarKey)))
	}
	return layout.DestURI(sidecarKey)
}

// sidecarProvider resolves the provider handle the sidecar is written through. A
// mirrored file root writes through its injected handle and NOTHING ELSE: it
// returns that handle verbatim (nil when it was not injected), so a missing
// handle is refused by the pre-I/O admission check rather than silently falling
// back to the destination handle and stranding the file:// URI. Every other
// placement writes through the destination handle. Paired with sidecarURIFor,
// the rendered URI and the write authority are sourced from one branch; for the
// file mirror they agree only insofar as the embedder bound the injected handle
// to the declared BaseDir, which this API cannot prove (caller-attested handle).
func (p ProvenancePlan) sidecarProvider(dst provider.Provider) provider.Provider {
	if p.isFileMirror() {
		return p.Placement.SidecarProvider
	}
	return dst
}

// RunConfig derives the gonimbus.reflow.run.v1 provenance echo from the validated
// plan. The engine emits this in its RunRecord so a direct library caller and the
// command share one authority (the command no longer overwrites it). Returns nil
// when provenance is disabled.
func (p ProvenancePlan) RunConfig() *ProvenanceRunConfig {
	if !p.Enabled() {
		return nil
	}
	placementMode := p.Placement.Mode
	if placementMode == "" {
		placementMode = ProvenancePlacementSibling
	}
	onWriteError := p.OnWriteError
	if onWriteError == "" {
		onWriteError = ProvenanceOnWriteErrorWarn
	}
	placement := ProvenancePlacementContext{Mode: placementMode}
	if p.Placement.Mode == ProvenancePlacementMirror && p.Placement.SidecarRoot != nil {
		placement.SidecarRoot = p.Placement.SidecarRoot.BaseURI
	}
	return &ProvenanceRunConfig{Mode: p.Mode, Suffix: p.Suffix, OnWriteError: onWriteError, Placement: placement}
}

// String renders a redacted summary: the injected sidecar provider handle is
// never dumped (presence only), while the non-secret run identity/version and
// placement remain visible for debugging.
func (p ProvenancePlan) String() string {
	root := "<nil>"
	if p.Placement.SidecarRoot != nil {
		root = p.Placement.SidecarRoot.BaseURI
	}
	return fmt.Sprintf("reflow.ProvenancePlan{Mode:%q, Suffix:%q, OnWriteError:%q, Placement:%q, SidecarRoot:%q, SidecarProvider:%s, RunID:%q, ToolVersion:%q}",
		p.Mode, p.Suffix, p.OnWriteError, p.Placement.Mode, root,
		providerPresence(p.Placement.SidecarProvider == nil), p.RunID, p.ToolVersion)
}

// GoString implements fmt %#v with the same redaction as String.
func (p ProvenancePlan) GoString() string { return p.String() }

// unsafeProvenanceSuffixes are common data extensions a provenance suffix must
// not collide with unless explicitly confirmed.
var unsafeProvenanceSuffixes = map[string]struct{}{
	".xml": {}, ".json": {}, ".jsonl": {}, ".csv": {}, ".parquet": {}, ".avro": {},
	".txt": {}, ".gz": {}, ".zst": {}, ".zip": {}, ".tar": {}, ".html": {}, ".pdf": {},
}

// ValidateProvenanceModeAndPolicy is the shared validator for the provenance mode
// and on-write-error policy values. The command adapter delegates to it so the
// engine and the CLI enforce one rule set.
func ValidateProvenanceModeAndPolicy(mode, onWriteError string) error {
	switch mode {
	case "", ProvenanceModeNone, ProvenanceModeSidecar:
	default:
		return fmt.Errorf("provenance mode must be one of: none, sidecar")
	}
	switch onWriteError {
	case "", ProvenanceOnWriteErrorWarn, ProvenanceOnWriteErrorFail:
	default:
		return fmt.Errorf("provenance on-write-error must be one of: warn, fail")
	}
	return nil
}

// ValidateProvenanceSuffix enforces the sidecar suffix rules: a leading dot, no
// path separator, no glob metacharacters, and no collision with a common data
// extension unless allowUnsafe confirms it.
func ValidateProvenanceSuffix(suffix string, allowUnsafe bool) error {
	if !strings.HasPrefix(suffix, ".") {
		return fmt.Errorf("provenance suffix must start with a leading dot")
	}
	if strings.Contains(suffix, "/") {
		return fmt.Errorf("provenance suffix must not contain '/'")
	}
	if strings.ContainsAny(suffix, "*?[") {
		return fmt.Errorf("provenance suffix must not look like a glob pattern")
	}
	if !allowUnsafe {
		if _, unsafe := unsafeProvenanceSuffixes[strings.ToLower(suffix)]; unsafe {
			return fmt.Errorf("provenance suffix %q collides with common data extensions; confirm with the unsafe-suffix option", suffix)
		}
	}
	return nil
}
