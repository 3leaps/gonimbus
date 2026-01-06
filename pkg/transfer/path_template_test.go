package transfer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompilePathTemplate_Apply(t *testing.T) {
	tpl, err := CompilePathTemplate("{dir[0]}/{filename}")
	require.NoError(t, err)

	out, err := tpl.Apply("a/b/c.txt")
	require.NoError(t, err)
	assert.Equal(t, "a/c.txt", out)
}

func TestCompilePathTemplate_KeyDefault(t *testing.T) {
	tpl, err := CompilePathTemplate("")
	require.NoError(t, err)

	out, err := tpl.Apply("a/b/c.txt")
	require.NoError(t, err)
	assert.Equal(t, "a/b/c.txt", out)
}

func TestCompilePathTemplate_InvalidPlaceholder(t *testing.T) {
	_, err := CompilePathTemplate("{capture:.*}")
	require.Error(t, err)
}

func TestPathTemplate_DirOutOfRange(t *testing.T) {
	tpl, err := CompilePathTemplate("{dir[2]}/{filename}")
	require.NoError(t, err)

	_, err = tpl.Apply("a/b.txt")
	require.Error(t, err)
}
