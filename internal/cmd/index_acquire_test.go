package cmd

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/spf13/cobra"
)

type commandExactReader struct {
	reads int
}

func (r *commandExactReader) OpenExact(_ context.Context, _ string) (io.ReadCloser, int64, error) {
	r.reads++
	return nil, 0, indexreader.ErrHubExactRead
}

func TestRunIndexAcquire_SelectsOnlyNamedReadHandle(t *testing.T) {
	selected := &commandExactReader{}
	otherAccount := &commandExactReader{}
	outputAuthority := &commandExactReader{}
	old := configuredHubReadHandleResolver
	t.Cleanup(func() { configuredHubReadHandleResolver = old })
	configuredHubReadHandleResolver = func(_ context.Context, name string) (hubReadHandleResolution, error) {
		require.Equal(t, "selected-account", name)
		return hubReadHandleResolution{reader: selected}, nil
	}

	parent, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	cmd := newIndexAcquireCommandForTest()
	cmd.SetArgs([]string{
		"--hub-read-handle", "selected-account",
		"--index-set", "idx_" + string(bytes.Repeat([]byte{'a'}, 64)),
		"--run-id", "run_1788950000000000000",
		"--dest", filepath.Join(parent, "bundle"),
	})
	err = cmd.Execute()
	require.ErrorIs(t, err, indexreader.ErrHubExactRead)
	require.Equal(t, 1, selected.reads)
	require.Zero(t, otherAccount.reads)
	require.Zero(t, outputAuthority.reads)
}

func newIndexAcquireCommandForTest() *cobra.Command {
	cmd := &cobra.Command{Use: "acquire", RunE: runIndexAcquire}
	cmd.Flags().String("hub-read-handle", "", "")
	cmd.Flags().String("index-set", "", "")
	cmd.Flags().String("run-id", "", "")
	cmd.Flags().String("proof-through-run", "", "")
	cmd.Flags().String("dest", "", "")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}
