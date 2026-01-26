package cmd

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestReadURILines(t *testing.T) {
	in := "\n  s3://bucket/a.txt  \n\n\t s3://bucket/prefix/ \n"
	got, err := readURILines(bytes.NewBufferString(in))
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bucket/a.txt", "s3://bucket/prefix/"}, got)
}

func TestValidateContentHeadArgs(t *testing.T) {
	makeCmd := func(stdin bool) *cobra.Command {
		c := &cobra.Command{}
		c.Flags().Bool("stdin", stdin, "")
		return c
	}

	{
		err := validateContentHeadArgs(makeCmd(false), []string{})
		require.Error(t, err)
	}
	{
		err := validateContentHeadArgs(makeCmd(false), []string{"s3://bucket/a.txt"})
		require.NoError(t, err)
	}
	{
		err := validateContentHeadArgs(makeCmd(true), []string{})
		require.NoError(t, err)
	}
	{
		err := validateContentHeadArgs(makeCmd(true), []string{"s3://bucket/a.txt"})
		require.Error(t, err)
	}
}

func TestValidateContentHeadBytes(t *testing.T) {
	require.NoError(t, validateContentHeadBytes(0))
	require.NoError(t, validateContentHeadBytes(4096))
	require.Error(t, validateContentHeadBytes(-1))
	require.Error(t, validateContentHeadBytes(contentHeadMaxBytes+1))
}

func TestContentHeadURITypes(t *testing.T) {
	{
		u, err := ParseURI("s3://bucket/a.txt")
		require.NoError(t, err)
		require.False(t, u.IsPrefix())
		require.False(t, u.IsPattern())
	}
	{
		u, err := ParseURI("s3://bucket/prefix/")
		require.NoError(t, err)
		require.True(t, u.IsPrefix())
		require.False(t, u.IsPattern())
	}
	{
		u, err := ParseURI("s3://bucket/prefix/**/*.parquet")
		require.NoError(t, err)
		require.True(t, u.IsPattern())
		// For patterns, Key is the derived list prefix, which is a prefix URI.
		require.True(t, u.IsPrefix())
		// In pattern form, Key is the derived list prefix.
		require.Equal(t, "prefix/", u.Key)
	}
}
