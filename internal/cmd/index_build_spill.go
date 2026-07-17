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

// Operator surfaces for sizing the durable merge's budgets. Precedence is CLI
// flag > environment > application config > built-in default. These are
// host/operator capacity configuration only: they never enter index-set
// identity, manifests, receipts, or committed artifact digests.
const (
	spillWorkspaceMaxFlagName = "--spill-workspace-max"
	spillWorkspaceMaxEnv      = "GONIMBUS_SPILL_WORKSPACE_MAX"
	spillWorkspaceMaxConfig   = "index.spill.workspace_max"
	spillRecordMaxFlagName    = "--spill-record-max"
	spillRecordMaxEnv         = "GONIMBUS_SPILL_RECORD_MAX"
	spillRecordMaxConfig      = "index.spill.record_max"
	spillRootFlagName         = "--spill-root"
	spillRootEnv              = "GONIMBUS_SPILL_ROOT"
	spillRootConfig           = "index.spill.root"

	spillSourceFlag    = "flag " + spillWorkspaceMaxFlagName
	spillSourceEnv     = "env " + spillWorkspaceMaxEnv
	spillSourceConfig  = "config " + spillWorkspaceMaxConfig
	spillSourceDefault = "built-in default"

	spillRecordSourceFlag    = "flag " + spillRecordMaxFlagName
	spillRecordSourceEnv     = "env " + spillRecordMaxEnv
	spillRecordSourceConfig  = "config " + spillRecordMaxConfig
	spillRecordSourceDefault = "built-in default"

	spillRootSourceFlag    = "flag " + spillRootFlagName
	spillRootSourceEnv     = "env " + spillRootEnv
	spillRootSourceConfig  = "config " + spillRootConfig
	spillRootSourceDefault = "default (beside run journals)"
)

// indexBuildSpillResolution is the resolved spill configuration plus the source
// each value came from, for operator diagnostics.
type indexBuildSpillResolution struct {
	// WorkspaceBytes / RecordBytes are 0 when no operator surface set them, so the
	// library zero-value selects the substrate default. Otherwise each is a finite
	// value >= 1 already validated. Effective* carry the resolved ceiling
	// (including the default) for display.
	WorkspaceBytes       int64
	WorkspaceSource      string
	EffectiveBytes       int64
	RecordBytes          int64
	RecordSource         string
	EffectiveRecordBytes int64
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

// resolveSpillByteBudget resolves one byte budget across the operator surfaces.
// explicit is true when a surface set it; a set value parses via the shared size
// parser and refuses explicit zero, negative, overflow, malformed, or unlimited.
func resolveSpillByteBudget(flagVal, flagSource, envKey, envSource, configKey, configSource, label string) (bytes int64, source string, explicit bool, err error) {
	raw, source, ok := firstOperatorValue(flagVal, flagSource, envKey, envSource, configKey, configSource)
	if !ok {
		return 0, "", false, nil
	}
	b, perr := match.ParseSize(raw)
	if perr != nil {
		return 0, "", true, fmt.Errorf("%s (%s): %w", label, source, perr)
	}
	if b < 1 {
		return 0, "", true, fmt.Errorf("%s (%s) must be at least 1 byte", label, source)
	}
	return b, source, true, nil
}

// resolveIndexBuildSpill resolves the workspace and record ceilings and the
// scratch root across the operator surfaces. Explicit zero, negative, overflow,
// malformed, or any "unlimited" spelling refuse; omission selects the built-in
// default (the library zero-value, which maps to the substrate ceiling). Called
// before the crawl so a bad value fails fast.
func resolveIndexBuildSpill() (indexBuildSpillResolution, error) {
	def := indexsubstrate.DefaultSpillMergeBudget()
	res := indexBuildSpillResolution{
		WorkspaceSource:      spillSourceDefault,
		EffectiveBytes:       def.MaxWorkspaceBytes,
		RecordSource:         spillRecordSourceDefault,
		EffectiveRecordBytes: def.MaxRecordBytes,
		RootSource:           spillRootSourceDefault,
	}

	if b, source, explicit, err := resolveSpillByteBudget(
		indexBuildSpillWorkspaceMax, spillSourceFlag,
		spillWorkspaceMaxEnv, spillSourceEnv,
		spillWorkspaceMaxConfig, spillSourceConfig,
		"spill workspace budget",
	); err != nil {
		return indexBuildSpillResolution{}, err
	} else if explicit {
		res.WorkspaceBytes = b
		res.WorkspaceSource = source
		res.EffectiveBytes = b
	}

	if b, source, explicit, err := resolveSpillByteBudget(
		indexBuildSpillRecordMax, spillRecordSourceFlag,
		spillRecordMaxEnv, spillRecordSourceEnv,
		spillRecordMaxConfig, spillRecordSourceConfig,
		"spill record budget",
	); err != nil {
		return indexBuildSpillResolution{}, err
	} else if explicit {
		res.RecordBytes = b
		res.RecordSource = source
		res.EffectiveRecordBytes = b
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

// emitIndexBuildSpillDiagnostics prints the effective workspace and record
// ceilings and their sources. It never echoes the host-absolute scratch root;
// only its presence and source are shown.
func emitIndexBuildSpillDiagnostics(w io.Writer, res indexBuildSpillResolution) {
	if w == nil {
		return
	}
	root := "default (beside run journals)"
	if res.Root != "" {
		root = "set (source: " + res.RootSource + ")"
	}
	_, _ = fmt.Fprintf(w, "durable merge workspace ceiling: %s (source: %s); record ceiling: %s (source: %s); scratch root: %s\n",
		match.FormatSize(res.EffectiveBytes), res.WorkspaceSource,
		match.FormatSize(res.EffectiveRecordBytes), res.RecordSource, root)
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
