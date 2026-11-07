package cmd

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// executeRootCapture runs the real root command and returns the error and
// everything cobra wrote. Cobra prints usage to its output writer and the error
// line to its error writer, so both go to one buffer.
func executeRootCapture(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	rootCmd.SetArgs(args)
	rootCmd.SetErr(&out)
	rootCmd.SetOut(&out)
	rootCmd.SetContext(context.Background())
	restore := snapshotSilenceUsage(rootCmd)
	savedProbeBytes := contentProbeBytes
	savedCrawlJob := crawlJobPath
	savedIndexBuildJob := indexBuildJobPath
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetOut(nil)
		restore()
		// Flag values persist on the package-level command tree.
		contentProbeConfigPath = ""
		contentProbeBytes = savedProbeBytes
		crawlJobPath = savedCrawlJob
		indexBuildJobPath = savedIndexBuildJob
		require.NoError(t, contentProbeCmd.Flags().Set("config", ""))
		contentProbeCmd.Flags().Lookup("config").Changed = false
		if f := contentProbeCmd.Flags().Lookup("bytes"); f != nil {
			f.Changed = false
		}
		if f := crawlCmd.Flags().Lookup("job"); f != nil {
			_ = f.Value.Set(savedCrawlJob)
			f.Changed = false
		}
		if f := indexBuildCmd.Flags().Lookup("job"); f != nil {
			_ = f.Value.Set(savedIndexBuildJob)
			f.Changed = false
		}
	})
	err := rootCmd.Execute()
	return out.String(), err
}

// snapshotSilenceUsage records SilenceUsage across the command tree, resets
// each command to its construction-time default (only streamPutCmd sets
// SilenceUsage at construction) so earlier package tests that called
// rootCmd.Execute() cannot leak SilenceUsage=true into usage assertions, and
// returns a function that puts every value back.
func snapshotSilenceUsage(root *cobra.Command) func() {
	saved := map[*cobra.Command]bool{}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		saved[c] = c.SilenceUsage
		c.SilenceUsage = c == streamPutCmd
		for _, sub := range c.Commands() {
			walk(sub)
		}
	}
	walk(root)
	return func() {
		for c, v := range saved {
			c.SilenceUsage = v
		}
	}
}

func TestRuntimeErrorDoesNotPrintUsage(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent-probe.yaml")
	stderr, err := executeRootCapture(t, "content", "probe", "--config", missing, "s3://bucket/key")
	require.Error(t, err)
	require.NotContains(t, stderr, "Usage:")
}

func TestFlagAndArgumentErrorsStillPrintUsage(t *testing.T) {
	cases := map[string][]string{
		"unknown flag":          {"content", "probe", "--no-such-flag", "s3://bucket/key"},
		"missing required flag": {"content", "probe", "s3://bucket/key"},
		"wrong argument count":  {"content", "probe", "--config", "probe.yaml"},
		"invalid flag value":    {"content", "probe", "--config", "probe.yaml", "--bytes", "many", "s3://bucket/key"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			stderr, err := executeRootCapture(t, args...)
			require.Error(t, err)
			require.Contains(t, stderr, "Usage:")
		})
	}
}

// Cobra runs only the nearest persistent pre-run hook, so a subcommand that
// defines its own would bypass the root hook that silences usage.
func TestNoSubcommandOverridesRootPersistentPreRun(t *testing.T) {
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		for _, sub := range c.Commands() {
			require.Nil(t, sub.PersistentPreRun, "%s defines PersistentPreRun", sub.CommandPath())
			require.Nil(t, sub.PersistentPreRunE, "%s defines PersistentPreRunE", sub.CommandPath())
			walk(sub)
		}
	}
	walk(rootCmd)
}

func TestLookalikeFlagHintsAndUsage(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantHint string
	}{
		{
			name:     "index build --profile",
			args:     []string{"index", "build", "--profile", "myprof", "--job", "index.yaml"},
			wantHint: "hint: 'index build' does not accept --profile; set AWS_PROFILE in the environment or connection.profile in the index manifest",
		},
		{
			name:     "index build -p",
			args:     []string{"index", "build", "-p", "myprof", "--job", "index.yaml"},
			wantHint: "hint: 'index build' does not accept --profile; set AWS_PROFILE in the environment or connection.profile in the index manifest",
		},
		{
			name:     "crawl --profile",
			args:     []string{"crawl", "--profile", "myprof", "--job", "crawl.yaml"},
			wantHint: "hint: 'crawl' does not accept --profile; set AWS_PROFILE in the environment or connection.profile in the job manifest",
		},
		{
			name:     "index query --profile",
			args:     []string{"index", "query", "s3://bucket/prefix/", "--profile", "myprof"},
			wantHint: "hint: 'index query' reads local index artifacts and does not accept --profile; use --output-profile when --output targets S3",
		},
		{
			name:     "index query --json",
			args:     []string{"index", "query", "s3://bucket/prefix/", "--json"},
			wantHint: "hint: 'index query' output is already JSONL by default; omit --json, or use --output-format receipt-jsonl-v1 for a verified query receipt",
		},
		{
			name:     "crawl --json",
			args:     []string{"crawl", "--job", "crawl.yaml", "--json"},
			wantHint: "hint: 'crawl' output is already JSONL by default; omit --json",
		},
		{
			name:     "content probe --json",
			args:     []string{"content", "probe", "--config", "probe.yaml", "--json", "s3://bucket/key"},
			wantHint: "hint: 'content probe' output is already JSONL by default; omit --json",
		},
		{
			name:     "stream get --json",
			args:     []string{"stream", "get", "s3://bucket/key", "--json"},
			wantHint: "hint: 'stream get' output is a framed stream by default; there is no --json flag",
		},
		{
			name:     "inspect --count",
			args:     []string{"inspect", "s3://bucket/prefix/", "--count"},
			wantHint: "hint: 'inspect' is a bounded spot-check (--limit, default 100) and does not accept --count; use 'index query --count' over a local index",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := executeRootCapture(t, tc.args...)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.wantHint)
			require.Contains(t, out, tc.wantHint)
			require.Contains(t, out, "Usage:")
		})
	}

	// transferReflowCmd is probed via formatLookalikeFlagError directly so
	// rootCmd.Execute() does not merge persistent flags (--config/--readonly/
	// --verbose/help) onto transferReflowCmd.Flags() ahead of
	// TestTransferReflowFlagCoverageParityMatrix under -shuffle=on.
	t.Run("transfer reflow --profile and --json", func(t *testing.T) {
		profErr := rootCmd.FlagErrorFunc()(transferReflowCmd, errors.New("unknown flag: --profile"))
		require.ErrorContains(t, profErr, "hint: 'transfer reflow' uses --src-profile and --dest-profile instead of --profile (or set AWS_PROFILE in the environment)")
		jsonErr := rootCmd.FlagErrorFunc()(transferReflowCmd, errors.New("unknown flag: --json"))
		require.ErrorContains(t, jsonErr, "hint: 'transfer reflow' output is already JSONL by default; omit --json")
	})
}

func TestInspectHelpDocumentsDefaultLimit(t *testing.T) {
	t.Cleanup(func() {
		if f := inspectCmd.Flags().Lookup("help"); f != nil {
			_ = f.Value.Set("false")
			f.Changed = false
		}
	})
	out, err := executeRootCapture(t, "inspect", "--help")
	require.NoError(t, err)
	require.Contains(t, out, "at most 100 matching objects")
	require.Contains(t, out, "capped by --limit")
}
