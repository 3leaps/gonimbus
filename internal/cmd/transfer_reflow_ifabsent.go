package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/preflight"
	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

type reflowIfAbsentCapability struct {
	ProbeStatus    preflight.IfAbsentProbeStatus
	Honored        *bool
	FallbackActive bool
	ProbeError     string
}

func runObjectStoreReflowDryRunPreflight(ctx context.Context, w output.Writer, dst provider.Provider, dest *reflowDestSpec) error {
	probePrefix := strings.TrimPrefix(dest.Prefix, "/")
	if probePrefix != "" && !strings.HasSuffix(probePrefix, "/") {
		probePrefix += "/"
	}
	probePrefix += ".gonimbus-preflight/"

	if IsReadOnly() {
		rec := &output.PreflightRecord{
			Mode:          string(preflight.ModeWriteProbe),
			ProbeStrategy: string(preflight.ProbePutDelete),
			ProbePrefix:   probePrefix,
			Results: []output.PreflightCheckResult{{
				Capability: preflight.CapTargetWrite,
				Allowed:    false,
				Method:     "PutObjectConditional(IfAbsent,0 bytes)+DeleteObject",
				ErrorCode:  "READONLY",
				Detail:     fmt.Sprintf("readonly mode enabled: refusing destination %s write probe", strings.ToUpper(dest.Provider)),
			}},
		}
		return w.WritePreflight(ctx, rec)
	}

	rec, err := preflight.WriteProbe(ctx, dst, preflight.Spec{
		Mode:          preflight.ModeWriteProbe,
		ProbeStrategy: preflight.ProbePutDelete,
		ProbePrefix:   probePrefix,
	})
	if writeErr := w.WritePreflight(ctx, rec); writeErr != nil {
		return writeErr
	}
	return err
}

func collisionModeDependsOnIfAbsent(mode string) bool {
	switch mode {
	case reflowCollisionSkip, reflowCollisionFail, reflowCollisionQuar, reflowCollisionSrcNew:
		return true
	default:
		return false
	}
}

func isObjectStoreProvider(providerName string) bool {
	return providerName == string(provider.ProviderS3) || providerName == string(provider.ProviderGCS)
}

func reflowProbePrefix(dest *reflowDestSpec) string {
	probePrefix := ""
	if dest != nil {
		probePrefix = strings.TrimPrefix(dest.Prefix, "/")
	}
	if probePrefix != "" && !strings.HasSuffix(probePrefix, "/") {
		probePrefix += "/"
	}
	return probePrefix + ".gonimbus-preflight/"
}

func detectReflowIfAbsentCapability(ctx context.Context, dst provider.Provider, dest *reflowDestSpec, collCfg collisionConfig, dryRun bool) reflowIfAbsentCapability {
	if dest == nil || !isObjectStoreProvider(dest.Provider) || !collisionModeDependsOnIfAbsent(collCfg.Mode) {
		return reflowIfAbsentCapability{}
	}
	if dryRun || IsReadOnly() {
		return reflowIfAbsentCapability{
			ProbeStatus:    preflight.IfAbsentProbeInconclusive,
			FallbackActive: true,
			ProbeError:     "mutation disabled for dry-run or readonly mode",
		}
	}
	result := preflight.ProbeIfAbsentSemantics(ctx, dst, preflight.Spec{
		Mode:        preflight.ModeWriteProbe,
		ProbePrefix: reflowProbePrefix(dest),
	})
	capability := reflowIfAbsentCapability{
		ProbeStatus: result.Status,
		Honored:     result.Honored(),
		ProbeError:  "",
	}
	if result.Err != nil {
		capability.ProbeError = result.Err.Error()
	}
	capability.FallbackActive = result.Status != preflight.IfAbsentProbeHonored
	return capability
}

func emitIfAbsentFallbackWarning(ctx context.Context, w *output.JSONLWriter, collCfg collisionConfig, dest *reflowDestSpec, capability reflowIfAbsentCapability) error {
	if !capability.FallbackActive {
		return nil
	}
	details := map[string]any{
		"on_collision":                    collCfg.Mode,
		"fallback":                        "head_compare",
		"dest_ifabsent_probe_status":      string(capability.ProbeStatus),
		"dest_ifabsent_honored":           capability.Honored,
		"cross_process_atomicity_limited": true,
	}
	if dest != nil {
		details["provider"] = dest.Provider
	}
	if capability.ProbeError != "" {
		details["probe_error"] = capability.ProbeError
	}
	return w.WriteAny(ctx, reflowpkg.WarningRecordType, reflowpkg.Warning{
		Code:    ifAbsentFallbackWarning,
		Message: "destination IfAbsent support was not verified; using head-compare fallback for non-overwrite collision handling",
		Details: details,
	})
}
