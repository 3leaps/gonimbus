package reflow

import (
	"context"
	"strings"

	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider"
)

// Collision mode values. These are the resolved --on-collision strings the
// engine acts on.
const (
	CollisionSkipIfDuplicate        = "skip-if-duplicate"
	CollisionFail                   = "fail"
	CollisionOverwrite              = "overwrite"
	CollisionQuarantine             = "quarantine"
	CollisionOverwriteIfSourceNewer = "overwrite-if-source-newer"
)

// ifAbsentFallbackWarningCode is the warning code emitted when a destination's
// IfAbsent support is unverified and the engine falls back to head-compare.
const ifAbsentFallbackWarningCode = "REFLOW_IFABSENT_FALLBACK_ACTIVE"

// IfAbsentCapability records what the engine knows about a destination's
// conditional-create (If-None-Match: *) support for the run, and whether the
// head-compare fallback is active. Experimental.
type IfAbsentCapability struct {
	ProbeStatus    preflight.IfAbsentProbeStatus
	Honored        *bool
	FallbackActive bool
	ProbeError     string
}

func isObjectStoreProviderID(providerID string) bool {
	return providerID == string(provider.ProviderS3) || providerID == string(provider.ProviderGCS)
}

// collisionModeDependsOnIfAbsent reports whether a collision mode relies on the
// destination honoring conditional create.
func collisionModeDependsOnIfAbsent(mode string) bool {
	switch mode {
	case CollisionSkipIfDuplicate, CollisionFail, CollisionQuarantine, CollisionOverwriteIfSourceNewer:
		return true
	default:
		return false
	}
}

// dryRunIfAbsentCapability is the capability the engine reports for a dry run:
// mutation is disabled, so the IfAbsent semantics cannot be probed and the
// head-compare fallback is reported as active for collision modes that depend on
// it. It performs no provider I/O, mirroring the CLI's dry-run/readonly branch.
func dryRunIfAbsentCapability(providerID, mode string) IfAbsentCapability {
	if !isObjectStoreProviderID(providerID) || !collisionModeDependsOnIfAbsent(mode) {
		return IfAbsentCapability{}
	}
	return IfAbsentCapability{
		ProbeStatus:    preflight.IfAbsentProbeInconclusive,
		FallbackActive: true,
		ProbeError:     "mutation disabled for dry-run or readonly mode",
	}
}

func liveIfAbsentCapability(ctx context.Context, dst provider.Provider, layout DestLayout, mode string, readOnly bool) IfAbsentCapability {
	if !isObjectStoreProviderID(layout.ProviderID) || !collisionModeDependsOnIfAbsent(mode) {
		return IfAbsentCapability{}
	}
	if readOnly {
		return IfAbsentCapability{
			ProbeStatus:    preflight.IfAbsentProbeInconclusive,
			FallbackActive: true,
			ProbeError:     "mutation disabled for dry-run or readonly mode",
		}
	}
	result := preflight.ProbeIfAbsentSemantics(ctx, dst, preflight.Spec{
		Mode:        preflight.ModeWriteProbe,
		ProbePrefix: reflowProbePrefix(layout),
	})
	capability := IfAbsentCapability{
		ProbeStatus: result.Status,
		Honored:     result.Honored(),
	}
	if result.Err != nil {
		capability.ProbeError = SanitizeOperationCauseMessage(result.Err)
	}
	capability.FallbackActive = result.Status != preflight.IfAbsentProbeHonored
	return capability
}

func reflowProbePrefix(layout DestLayout) string {
	prefix := strings.TrimPrefix(layout.Prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix + ".gonimbus-preflight/"
}

// fallbackWarning builds the IfAbsent head-compare fallback warning, or nil when
// the fallback is not active. Details are sanitized by the Runner before
// delivery.
func fallbackWarning(providerID, mode string, capability IfAbsentCapability) *Warning {
	if !capability.FallbackActive {
		return nil
	}
	details := map[string]any{
		"on_collision":                    mode,
		"fallback":                        "head_compare",
		"dest_ifabsent_probe_status":      string(capability.ProbeStatus),
		"dest_ifabsent_honored":           capability.Honored,
		"cross_process_atomicity_limited": true,
	}
	if providerID != "" {
		details["provider"] = providerID
	}
	if capability.ProbeError != "" {
		details["probe_error"] = capability.ProbeError
	}
	return &Warning{
		Code:    ifAbsentFallbackWarningCode,
		Message: "destination IfAbsent support was not verified; using head-compare fallback for non-overwrite collision handling",
		Details: details,
	}
}
