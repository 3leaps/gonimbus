package cmd

import (
	"bytes"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

func TestIndexBuildSpillFlags_DefaultEmpty(t *testing.T) {
	for _, name := range []string{"spill-workspace-max", "spill-record-max", "spill-root"} {
		flag := indexBuildCmd.Flags().Lookup(name)
		require.NotNil(t, flag, "flag %q must be registered", name)
		require.Equal(t, "", flag.DefValue)
	}
}

// withSpillSurfaces sets the CLI flag vars and (via t.Setenv/viper) the env and
// config surfaces, restoring all of them on cleanup. Record-budget tests set
// indexBuildSpillRecordMax directly; it is reset here so cases stay isolated.
func withSpillSurfaces(t *testing.T, flagVal, rootFlagVal string) {
	t.Helper()
	oldMax := indexBuildSpillWorkspaceMax
	oldRecord := indexBuildSpillRecordMax
	oldRoot := indexBuildSpillRoot
	oldResolved := indexBuildSpillResolved
	indexBuildSpillWorkspaceMax = flagVal
	indexBuildSpillRecordMax = ""
	indexBuildSpillRoot = rootFlagVal
	t.Cleanup(func() {
		indexBuildSpillWorkspaceMax = oldMax
		indexBuildSpillRecordMax = oldRecord
		indexBuildSpillRoot = oldRoot
		indexBuildSpillResolved = oldResolved
	})
}

// setSpillConfig sets a viper config key for the test and clears it after.
func setSpillConfig(t *testing.T, key, val string) {
	t.Helper()
	viper.Set(key, val)
	t.Cleanup(func() { viper.Set(key, "") })
}

func TestResolveIndexBuildSpill_Default(t *testing.T) {
	withSpillSurfaces(t, "", "")
	res, err := resolveIndexBuildSpill()
	require.NoError(t, err)
	require.Equal(t, int64(0), res.WorkspaceBytes, "zero lets the library default apply")
	require.Equal(t, indexsubstrate.DefaultSpillMergeBudget().MaxWorkspaceBytes, res.EffectiveBytes)
	require.Equal(t, int64(16)<<30, res.EffectiveBytes, "16 GiB workspace default")
	require.Equal(t, spillSourceDefault, res.WorkspaceSource)
	// Record budget defaults likewise.
	require.Equal(t, int64(0), res.RecordBytes)
	require.Equal(t, indexsubstrate.DefaultSpillMergeBudget().MaxRecordBytes, res.EffectiveRecordBytes)
	require.Equal(t, int64(16)<<20, res.EffectiveRecordBytes, "16 MiB record default")
	require.Equal(t, spillRecordSourceDefault, res.RecordSource)
	require.Equal(t, "", res.Root)
}

func TestResolveIndexBuildSpill_RecordBudget(t *testing.T) {
	t.Run("flag", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		indexBuildSpillRecordMax = "32MiB"
		res, err := resolveIndexBuildSpill()
		require.NoError(t, err)
		require.Equal(t, int64(32)<<20, res.RecordBytes)
		require.Equal(t, int64(32)<<20, res.EffectiveRecordBytes)
		require.Equal(t, spillRecordSourceFlag, res.RecordSource)
	})
	t.Run("env", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		t.Setenv(spillRecordMaxEnv, "8MiB")
		res, err := resolveIndexBuildSpill()
		require.NoError(t, err)
		require.Equal(t, int64(8)<<20, res.RecordBytes)
		require.Equal(t, spillRecordSourceEnv, res.RecordSource)
	})
	t.Run("config", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		setSpillConfig(t, spillRecordMaxConfig, "4MiB")
		res, err := resolveIndexBuildSpill()
		require.NoError(t, err)
		require.Equal(t, int64(4)<<20, res.RecordBytes)
		require.Equal(t, spillRecordSourceConfig, res.RecordSource)
	})
	t.Run("refuse explicit zero", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		indexBuildSpillRecordMax = "0"
		_, err := resolveIndexBuildSpill()
		require.ErrorContains(t, err, "spill record budget")
		require.ErrorContains(t, err, "at least 1 byte")
	})
	t.Run("refuse malformed", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		indexBuildSpillRecordMax = "8XB"
		_, err := resolveIndexBuildSpill()
		require.ErrorContains(t, err, "spill record budget")
	})
}

func TestResolveIndexBuildSpill_ParseAndUnits(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int64
	}{
		{"gib", "16GiB", 16 << 30},
		{"gb base10", "16GB", 16_000_000_000},
		{"raw bytes", "1048576", 1 << 20},
		{"lower to below default", "2GiB", 2 << 30},
		{"raise above default", "32GiB", 32 << 30},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withSpillSurfaces(t, tc.in, "")
			res, err := resolveIndexBuildSpill()
			require.NoError(t, err)
			require.Equal(t, tc.want, res.WorkspaceBytes)
			require.Equal(t, tc.want, res.EffectiveBytes)
			require.Equal(t, spillSourceFlag, res.WorkspaceSource)
		})
	}
}

func TestResolveIndexBuildSpill_Refusals(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		errIs string
	}{
		{"malformed unit", "8XB", "spill workspace budget"},
		{"explicit zero", "0", "at least 1 byte"},
		{"negative", "-4GiB", "spill workspace budget"},
		{"overflow", "99999999999GiB", "spill workspace budget"},
		{"unlimited spelling", "unlimited", "spill workspace budget"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withSpillSurfaces(t, tc.in, "")
			_, err := resolveIndexBuildSpill()
			require.ErrorContains(t, err, tc.errIs)
		})
	}
}

func TestResolveIndexBuildSpill_EnvAndConfig(t *testing.T) {
	t.Run("env", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		t.Setenv(spillWorkspaceMaxEnv, "4GiB")
		res, err := resolveIndexBuildSpill()
		require.NoError(t, err)
		require.Equal(t, int64(4)<<30, res.WorkspaceBytes)
		require.Equal(t, spillSourceEnv, res.WorkspaceSource)
	})
	t.Run("config", func(t *testing.T) {
		withSpillSurfaces(t, "", "")
		setSpillConfig(t, spillWorkspaceMaxConfig, "2GiB")
		res, err := resolveIndexBuildSpill()
		require.NoError(t, err)
		require.Equal(t, int64(2)<<30, res.WorkspaceBytes)
		require.Equal(t, spillSourceConfig, res.WorkspaceSource)
	})
}

// TestResolveIndexBuildSpill_Precedence proves CLI flag > env > config > default.
func TestResolveIndexBuildSpill_Precedence(t *testing.T) {
	// All three set: flag wins.
	withSpillSurfaces(t, "16GiB", "")
	t.Setenv(spillWorkspaceMaxEnv, "4GiB")
	setSpillConfig(t, spillWorkspaceMaxConfig, "2GiB")
	res, err := resolveIndexBuildSpill()
	require.NoError(t, err)
	require.Equal(t, int64(16)<<30, res.WorkspaceBytes)
	require.Equal(t, spillSourceFlag, res.WorkspaceSource)

	// Flag cleared: env wins over config.
	indexBuildSpillWorkspaceMax = ""
	res, err = resolveIndexBuildSpill()
	require.NoError(t, err)
	require.Equal(t, int64(4)<<30, res.WorkspaceBytes)
	require.Equal(t, spillSourceEnv, res.WorkspaceSource)
}

func TestResolveIndexBuildSpill_Root(t *testing.T) {
	withSpillSurfaces(t, "", "/mnt/scratch")
	res, err := resolveIndexBuildSpill()
	require.NoError(t, err)
	require.Equal(t, "/mnt/scratch", res.Root)
	require.Equal(t, spillRootSourceFlag, res.RootSource)
}

// TestEmitIndexBuildSpillDiagnostics_NeverEchoesRootPath proves the operator
// diagnostic shows the effective ceiling and source but never the host path.
func TestEmitIndexBuildSpillDiagnostics_NeverEchoesRootPath(t *testing.T) {
	var buf bytes.Buffer
	emitIndexBuildSpillDiagnostics(&buf, indexBuildSpillResolution{
		WorkspaceBytes:       16 << 30,
		WorkspaceSource:      spillSourceFlag,
		EffectiveBytes:       16 << 30,
		RecordSource:         spillRecordSourceDefault,
		EffectiveRecordBytes: 16 << 20,
		Root:                 "/mnt/secret-scratch",
		RootSource:           spillRootSourceEnv,
	})
	out := buf.String()
	require.Contains(t, out, "16.0GiB")
	require.Contains(t, out, spillSourceFlag)
	require.Contains(t, out, "record ceiling")
	require.Contains(t, out, "16.0MiB")
	require.NotContains(t, out, "/mnt/secret-scratch", "host-absolute root must never be echoed")
	require.Contains(t, out, spillRootSourceEnv, "root source/presence is enough")
}

func TestEmitIndexBuildSpillCompletion(t *testing.T) {
	res := indexBuildSpillResolution{EffectiveBytes: 8 << 30, WorkspaceSource: spillSourceDefault}

	// Zero peak (nothing spilled) is not reported.
	var quiet bytes.Buffer
	emitIndexBuildSpillCompletion(&quiet, res, 0)
	require.Empty(t, quiet.String())

	// Non-zero peak is reported against the ceiling with its source.
	var buf bytes.Buffer
	emitIndexBuildSpillCompletion(&buf, res, 3<<30)
	out := buf.String()
	require.Contains(t, out, "peak workspace")
	require.Contains(t, out, "3.0GiB")
	require.Contains(t, out, "8.0GiB")
	require.Contains(t, out, spillSourceDefault)
}

func TestIndexBuildBackgroundRejectsSpillFlagButNotEnvConfig(t *testing.T) {
	t.Run("flag rejected", func(t *testing.T) {
		withIndexBuildModes(t, true, false, false)
		withSpillSurfaces(t, "8GiB", "")
		err := validateIndexBuildBackgroundFlags()
		require.ErrorContains(t, err, "not forwarded to --background")
		require.ErrorContains(t, err, "GONIMBUS_SPILL_")
	})
	t.Run("record flag rejected", func(t *testing.T) {
		withIndexBuildModes(t, true, false, false)
		withSpillSurfaces(t, "", "")
		indexBuildSpillRecordMax = "32MiB"
		require.Error(t, validateIndexBuildBackgroundFlags())
	})
	t.Run("root flag rejected", func(t *testing.T) {
		withIndexBuildModes(t, true, false, false)
		withSpillSurfaces(t, "", "/mnt/scratch")
		require.Error(t, validateIndexBuildBackgroundFlags())
	})
	t.Run("no flag accepted (env/config path)", func(t *testing.T) {
		withIndexBuildModes(t, true, false, false)
		withSpillSurfaces(t, "", "")
		require.NoError(t, validateIndexBuildBackgroundFlags())
	})
}
