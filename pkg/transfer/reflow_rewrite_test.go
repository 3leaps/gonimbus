package transfer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReflowRewrite_Apply(t *testing.T) {
	r, err := CompileReflowRewrite("{_}/{store}/{device}/{date}/{file}", "{date}/{store}/{file}")
	require.NoError(t, err)

	out, vars, err := r.Apply("raw/01003/pos/2025-12-01/RecordTypeAlpha01122025120000.xml")
	require.NoError(t, err)
	require.Equal(t, "2025-12-01/01003/RecordTypeAlpha01122025120000.xml", out)
	require.Equal(t, map[string]string{"store": "01003", "device": "pos", "date": "2025-12-01", "file": "RecordTypeAlpha01122025120000.xml"}, vars)
}

func TestReflowRewrite_RejectsPartialPlaceholders(t *testing.T) {
	_, err := CompileReflowRewrite("foo{bar}", "x")
	require.Error(t, err)
}

func TestReflowRewrite_MissingVar(t *testing.T) {
	r, err := CompileReflowRewrite("{a}/{b}", "{a}/{c}")
	require.NoError(t, err)
	_, _, err = r.Apply("1/2")
	require.Error(t, err)
}

func TestReflowRewrite_ApplyWithVars_Override(t *testing.T) {
	r, err := CompileReflowRewrite("{site}/{date}/{file}", "{date}/{site}/{file}")
	require.NoError(t, err)

	out, vars, err := r.ApplyWithVars("s1/arrival/RecordTypeAlpha.xml", map[string]string{"date": "business"})
	require.NoError(t, err)
	require.Equal(t, "business/s1/RecordTypeAlpha.xml", out)
	require.Equal(t, "business", vars["date"])
}
