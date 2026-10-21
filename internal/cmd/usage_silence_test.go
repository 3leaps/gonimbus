package cmd

import (
	"bytes"
	"context"
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
	t.Cleanup(func() {
		rootCmd.SetArgs(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetOut(nil)
		restore()
		// Flag values persist on the package-level command tree.
		contentProbeConfigPath = ""
		require.NoError(t, contentProbeCmd.Flags().Set("config", ""))
		contentProbeCmd.Flags().Lookup("config").Changed = false
	})
	err := rootCmd.Execute()
	return out.String(), err
}

// snapshotSilenceUsage records SilenceUsage across the command tree and returns
// a function that puts every value back, so cases stay independent without
// clearing commands that set it at construction.
func snapshotSilenceUsage(root *cobra.Command) func() {
	saved := map[*cobra.Command]bool{}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		saved[c] = c.SilenceUsage
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
