package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/fulmenhq/gofulmen/crucible"
	"github.com/stretchr/testify/require"
)

func executeVersionCommand(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	origExtended := extended
	origJSON := versionJSON
	restoreSilence := snapshotSilenceUsage(rootCmd)
	rootCmd.SetOut(&stdout)
	rootCmd.SetErr(&stderr)
	rootCmd.SetArgs(append([]string{"version"}, args...))
	t.Cleanup(func() {
		extended = origExtended
		versionJSON = origJSON
		if f := versionCmd.Flags().Lookup("extended"); f != nil {
			_ = f.Value.Set("false")
			f.Changed = false
		}
		if f := versionCmd.Flags().Lookup("json"); f != nil {
			_ = f.Value.Set("false")
			f.Changed = false
		}
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
		restoreSilence()
	})
	err := rootCmd.Execute()
	return stdout.String(), stderr.String(), err
}

func TestVersionHumanOutputUnchanged(t *testing.T) {
	origVersion := versionInfo
	SetVersionInfo("0.4.7", "abc1234", "2026-11-09T18:00:00Z")
	t.Cleanup(func() { versionInfo = origVersion })

	stdout, stderr, err := executeVersionCommand(t)
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Equal(t, "gonimbus 0.4.7\n", stdout)

	for _, flag := range []string{"--extended", "-e"} {
		t.Run(flag, func(t *testing.T) {
			stdout, stderr, err := executeVersionCommand(t, flag)
			require.NoError(t, err)
			require.Empty(t, stderr)
			cv := crucible.GetVersion()
			expected := "gonimbus 0.4.7\n" +
				"Commit: abc1234\n" +
				"Built: 2026-11-09T18:00:00Z\n" +
				"Go: " + runtime.Version() + "\n\n" +
				"Gofulmen: " + cv.Gofulmen + "\n" +
				"Crucible: " + cv.Crucible + "\n"
			require.Equal(t, expected, stdout)
		})
	}
}

func TestVersionJSONOutput(t *testing.T) {
	origVersion := versionInfo
	SetVersionInfo("0.4.7", "abc1234", "2026-11-09T18:00:00Z")
	t.Cleanup(func() { versionInfo = origVersion })

	stdout, stderr, err := executeVersionCommand(t, "--json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.True(t, strings.HasSuffix(stdout, "\n"))
	require.Equal(t, 1, strings.Count(stdout, "\n"))

	var raw map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &raw))
	require.ElementsMatch(t, []string{
		"name",
		"version",
		"commit",
		"built_at",
		"go",
		"platform",
	}, mapKeys(raw))
	require.Equal(t, "gonimbus", raw["name"])
	require.Equal(t, "0.4.7", raw["version"])
	require.Equal(t, "abc1234", raw["commit"])
	require.Equal(t, "2026-11-09T18:00:00Z", raw["built_at"])
	require.Equal(t, runtime.Version(), raw["go"])
	require.Equal(t, runtime.GOOS+"/"+runtime.GOARCH, raw["platform"])
}

func TestVersionJSONExtendedOutput(t *testing.T) {
	origVersion := versionInfo
	origReadBuildInfo := readBuildInfo
	SetVersionInfo("0.4.7", "abc1234", "2026-11-09T18:00:00Z")
	readBuildInfo = func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{
			Main: debug.Module{
				Path: "github.com/3leaps/gonimbus",
			},
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "abc1234567890def"},
				{Key: "vcs.time", Value: "2026-11-09T18:00:00Z"},
				{Key: "vcs.modified", Value: "false"},
			},
		}, true
	}
	t.Cleanup(func() {
		versionInfo = origVersion
		readBuildInfo = origReadBuildInfo
	})

	for _, flag := range []string{"--extended", "-e"} {
		t.Run(flag, func(t *testing.T) {
			stdout, stderr, err := executeVersionCommand(t, "--json", flag)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.True(t, strings.HasSuffix(stdout, "\n"))
			require.Equal(t, 1, strings.Count(stdout, "\n"))

			var raw map[string]any
			require.NoError(t, json.Unmarshal([]byte(stdout), &raw))
			cv := crucible.GetVersion()
			require.Equal(t, "gonimbus", raw["name"])
			require.Equal(t, "0.4.7", raw["version"])
			require.Equal(t, "abc1234", raw["commit"])
			require.Equal(t, "2026-11-09T18:00:00Z", raw["built_at"])
			require.Equal(t, runtime.Version(), raw["go"])
			require.Equal(t, runtime.GOOS+"/"+runtime.GOARCH, raw["platform"])
			require.Equal(t, runtime.GOOS, raw["os"])
			require.Equal(t, runtime.GOARCH, raw["arch"])
			require.Equal(t, cv.Gofulmen, raw["gofulmen"])
			require.Equal(t, cv.Crucible, raw["crucible"])
			require.Equal(t, "github.com/3leaps/gonimbus", raw["module_path"])
			require.Equal(t, "abc1234567890def", raw["vcs_revision"])
			require.Equal(t, "2026-11-09T18:00:00Z", raw["vcs_time"])
			require.Equal(t, false, raw["vcs_modified"])
		})
	}
}

func TestVersionRejectsExtraPositionalArgs(t *testing.T) {
	_, _, err := executeVersionCommand(t, "extra-arg")
	require.Error(t, err)
	require.ErrorContains(t, err, `unknown command "extra-arg" for "gonimbus version"`)
}

func TestVersionJSONCoexistsWithIndexQueryLookalikeHint(t *testing.T) {
	stdout, stderr, err := executeVersionCommand(t, "--json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Contains(t, stdout, `"name":"gonimbus"`)

	out, queryErr := executeRootCapture(t, "index", "query", "s3://bucket/prefix/", "--json")
	require.Error(t, queryErr)
	require.ErrorContains(t, queryErr, "hint: 'index query' output is already JSONL by default; omit --json, or use --output-format receipt-jsonl-v1 for a verified query receipt")
	require.Contains(t, out, "Usage:")
}

func TestVersionSubprocessScrubbedEnvHelper(t *testing.T) {
	markerIdx := -1
	for i, arg := range os.Args {
		if arg == "gonimbus-scrubbed-child" {
			markerIdx = i
			break
		}
	}
	if markerIdx == -1 {
		return
	}

	cliArgs := os.Args[markerIdx+1:]
	rootCmd.SetArgs(cliArgs)
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
	os.Exit(0)
}

func TestVersionSubprocessScrubbedEnv(t *testing.T) {
	exe, err := os.Executable()
	require.NoError(t, err)

	cases := []struct {
		name     string
		cliArgs  []string
		extended bool
	}{
		{
			name:     "version --json",
			cliArgs:  []string{"version", "--json"},
			extended: false,
		},
		{
			name:     "version --json --extended",
			cliArgs:  []string{"version", "--json", "--extended"},
			extended: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmdArgs := append([]string{
				"-test.run=^TestVersionSubprocessScrubbedEnvHelper$",
				"--",
				"gonimbus-scrubbed-child",
			}, tc.cliArgs...)
			child := exec.Command(exe, cmdArgs...)
			child.Env = []string{}
			var stdout, stderr bytes.Buffer
			child.Stdout = &stdout
			child.Stderr = &stderr

			err := child.Run()
			require.NoError(t, err, "stderr=%q stdout=%q", stderr.String(), stdout.String())
			require.Empty(t, stderr.String())

			out := stdout.String()
			require.True(t, strings.HasSuffix(out, "\n"))
			require.Equal(t, 1, strings.Count(out, "\n"))

			var parsed map[string]any
			require.NoError(t, json.Unmarshal([]byte(out), &parsed))
			require.Equal(t, "gonimbus", parsed["name"])
			require.NotEmpty(t, parsed["version"])
			require.NotEmpty(t, parsed["commit"])
			require.NotEmpty(t, parsed["built_at"])
			require.Equal(t, runtime.Version(), parsed["go"])
			require.Equal(t, runtime.GOOS+"/"+runtime.GOARCH, parsed["platform"])
			if tc.extended {
				require.Equal(t, runtime.GOOS, parsed["os"])
				require.Equal(t, runtime.GOARCH, parsed["arch"])
				require.NotEmpty(t, parsed["gofulmen"])
				require.NotEmpty(t, parsed["crucible"])
			} else {
				require.NotContains(t, parsed, "os")
				require.NotContains(t, parsed, "arch")
			}
		})
	}

	t.Run("explicit missing --config remains fail-closed", func(t *testing.T) {
		missingCfg := t.TempDir() + "/missing-config.yaml"
		child := exec.Command(exe,
			"-test.run=^TestVersionSubprocessScrubbedEnvHelper$",
			"--",
			"gonimbus-scrubbed-child",
			"--config", missingCfg,
			"version", "--json",
		)
		child.Env = []string{}
		var stdout, stderr bytes.Buffer
		child.Stdout = &stdout
		child.Stderr = &stderr

		err := child.Run()
		require.Error(t, err)
		require.Empty(t, stdout.String())
	})
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
