package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/viper"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/match"
)

// Operator surfaces for sizing the durable merge's scratch workspace. Precedence
// is CLI flag > environment > application config > built-in default. These are
// host/operator capacity configuration only: they never enter index-set
// identity, manifests, receipts, or committed artifact digests.
const (
	spillWorkspaceMaxFlagName = "--spill-workspace-max"
	spillWorkspaceMaxEnv      = "GONIMBUS_SPILL_WORKSPACE_MAX"
	spillWorkspaceMaxConfig   = "index.spill.workspace_max"
	spillRootFlagName         = "--spill-root"
	spillRootEnv              = "GONIMBUS_SPILL_ROOT"
	spillRootConfig           = "index.spill.root"

	spillSourceFlag    = "flag " + spillWorkspaceMaxFlagName
	spillSourceEnv     = "env " + spillWorkspaceMaxEnv
	spillSourceConfig  = "config " + spillWorkspaceMaxConfig
	spillSourceDefault = "built-in default"

	spillRootSourceFlag    = "flag " + spillRootFlagName
	spillRootSourceEnv     = "env " + spillRootEnv
	spillRootSourceConfig  = "config " + spillRootConfig
	spillRootSourceDefault = "default (beside run journals)"
)

// indexBuildSpillResolution is the resolved workspace configuration plus the
// source each value came from, for operator diagnostics.
type indexBuildSpillResolution struct {
	// WorkspaceBytes is 0 when no operator surface set it, so the library
	// zero-value selects the substrate default. Otherwise it is a finite value
	// >= 1 already validated.
	WorkspaceBytes  int64
	WorkspaceSource string
	// EffectiveBytes is the resolved ceiling including the default, for display.
	EffectiveBytes int64
	// Root is empty when unset (co-locate beside journals). Never rendered into
	// artifacts or sanitized errors; the source/presence is what diagnostics show.
	Root       string
	RootSource string
}

// firstOperatorValue returns the highest-precedence non-empty value among the
// CLI flag, environment variable, and application config key, with a label.
func firstOperatorValue(flagVal, flagSource, envKey, envSource, configKey, configSource string) (value, source string, set bool) {
	if v := strings.TrimSpace(flagVal); v != "" {
		return v, flagSource, true
	}
	if v, ok := os.LookupEnv(envKey); ok {
		if v = strings.TrimSpace(v); v != "" {
			return v, envSource, true
		}
	}
	if viper.IsSet(configKey) {
		if v := strings.TrimSpace(viper.GetString(configKey)); v != "" {
			return v, configSource, true
		}
	}
	return "", "", false
}

// resolveIndexBuildSpill resolves the workspace ceiling and scratch root across
// the operator surfaces. Explicit zero, negative, overflow, malformed, or any
// "unlimited" spelling refuse; omission selects the built-in default (the library
// zero-value, which maps to the substrate ceiling). Called before the crawl so a
// bad value fails fast.
func resolveIndexBuildSpill() (indexBuildSpillResolution, error) {
	res := indexBuildSpillResolution{
		WorkspaceSource: spillSourceDefault,
		EffectiveBytes:  indexsubstrate.DefaultSpillMergeBudget().MaxWorkspaceBytes,
		RootSource:      spillRootSourceDefault,
	}

	if raw, source, ok := firstOperatorValue(
		indexBuildSpillWorkspaceMax, spillSourceFlag,
		spillWorkspaceMaxEnv, spillSourceEnv,
		spillWorkspaceMaxConfig, spillSourceConfig,
	); ok {
		b, err := match.ParseSize(raw)
		if err != nil {
			return indexBuildSpillResolution{}, fmt.Errorf("spill workspace budget (%s): %w", source, err)
		}
		if b < 1 {
			return indexBuildSpillResolution{}, fmt.Errorf("spill workspace budget (%s) must be at least 1 byte", source)
		}
		res.WorkspaceBytes = b
		res.WorkspaceSource = source
		res.EffectiveBytes = b
	}

	if raw, source, ok := firstOperatorValue(
		indexBuildSpillRoot, spillRootSourceFlag,
		spillRootEnv, spillRootSourceEnv,
		spillRootConfig, spillRootSourceConfig,
	); ok {
		res.Root = raw
		res.RootSource = source
	}

	return res, nil
}

// emitIndexBuildSpillDiagnostics prints the effective workspace ceiling and the
// configuration source. It never echoes the host-absolute scratch root; only its
// presence and source are shown.
func emitIndexBuildSpillDiagnostics(w io.Writer, res indexBuildSpillResolution) {
	if w == nil {
		return
	}
	root := "default (beside run journals)"
	if res.Root != "" {
		root = "set (source: " + res.RootSource + ")"
	}
	_, _ = fmt.Fprintf(w, "durable merge workspace ceiling: %s (source: %s); scratch root: %s\n",
		match.FormatSize(res.EffectiveBytes), res.WorkspaceSource, root)
}

// emitIndexBuildSpillCompletion prints the observed peak workspace against the
// resolved ceiling after a durable build, capacity evidence for sizing
// successive builds. A zero peak (nothing spilled) is not reported.
func emitIndexBuildSpillCompletion(w io.Writer, res indexBuildSpillResolution, peakWorkspaceBytes int64) {
	if w == nil || peakWorkspaceBytes <= 0 {
		return
	}
	_, _ = fmt.Fprintf(w, "durable merge peak workspace: %s of %s ceiling (source: %s)\n",
		match.FormatSize(peakWorkspaceBytes), match.FormatSize(res.EffectiveBytes), res.WorkspaceSource)
}
