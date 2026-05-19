package transfer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/probe"
)

func TestReflowRewrite_Apply(t *testing.T) {
	r, err := CompileReflowRewrite("{_}/{store}/{device}/{date}/{file}", "{date}/{store}/{file}")
	require.NoError(t, err)

	out, vars, err := r.Apply("raw/01003/pos/2025-12-01/RecordTypeAlpha01122025120000.xml")
	require.NoError(t, err)
	require.Equal(t, "2025-12-01/01003/RecordTypeAlpha01122025120000.xml", out)
	require.Equal(t, map[string]string{"store": "01003", "device": "pos", "date": "2025-12-01", "file": "RecordTypeAlpha01122025120000.xml"}, vars)
}

func TestReflowRewrite_MixedSegmentRender(t *testing.T) {
	r, err := CompileReflowRewrite("{store}/{date}/{file}", "store={store}/year={date}/{file}")
	require.NoError(t, err)

	out, vars, err := r.Apply("007/2026/record.xml")
	require.NoError(t, err)
	require.Equal(t, "store=007/year=2026/record.xml", out)
	require.Equal(t, "007", vars["store"])
}

func TestReflowRewrite_MixedSegmentCapture(t *testing.T) {
	r, err := CompileReflowRewrite("source/vendor-{store}-site/{file}", "stores/{store}/{file}")
	require.NoError(t, err)

	out, vars, err := r.Apply("source/vendor-007-site/record.xml")
	require.NoError(t, err)
	require.Equal(t, "stores/007/record.xml", out)
	require.Equal(t, "007", vars["store"])
}

func TestReflowCapture_ParityWithRewriteSourceKeyCaptures(t *testing.T) {
	capture, err := CompileReflowCapture("source/vendor-{store}-site/{file}")
	require.NoError(t, err)
	require.Equal(t, []string{"store", "file"}, capture.CaptureNames())

	captureVars, err := capture.Apply("source/vendor-007-site/RecordTypeAlpha20260218.xml")
	require.NoError(t, err)

	rewrite, err := CompileReflowRewrite("source/vendor-{store}-site/{file}", "dest/{store}/{file}")
	require.NoError(t, err)
	_, rewriteVars, err := rewrite.Apply("source/vendor-007-site/RecordTypeAlpha20260218.xml")
	require.NoError(t, err)

	require.Equal(t, rewriteVars, captureVars)
}

func TestReflowRewrite_MixedSegmentCaptureRejectsEmptyValue(t *testing.T) {
	r, err := CompileReflowRewrite("source/vendor-{store}-site/{file}", "stores/{store}/{file}")
	require.NoError(t, err)

	_, _, err = r.Apply("source/vendor--site/record.xml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match from template")
}

func TestReflowRewrite_RejectsMultiplePlaceholdersInSegment(t *testing.T) {
	tests := []string{
		"{a}-{b}/{file}",
		"{a}{b}/{file}",
	}
	for _, tpl := range tests {
		t.Run(tpl, func(t *testing.T) {
			_, err := CompileReflowRewrite(tpl, "{file}")
			require.Error(t, err)
			require.Contains(t, err.Error(), "multiple placeholders")
		})
	}
}

func TestReflowRewrite_RejectsUnmatchedBraces(t *testing.T) {
	tests := []struct {
		name    string
		tpl     string
		wantErr string
	}{
		{name: "left only", tpl: "prefix-{id/{file}", wantErr: "unmatched {"},
		{name: "right only", tpl: "prefix-id}/{file}", wantErr: "unmatched }"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CompileReflowRewrite(tt.tpl, "{file}")
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
			require.NotContains(t, err.Error(), "multiple placeholders")
		})
	}
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

func TestReflowRewrite_UsesDerivedProbeVars(t *testing.T) {
	p, err := probe.New(probe.Config{
		Extract: []probe.ExtractorConfig{{Name: "date", Type: "regex", Pattern: `date=([0-9-]+)`, Group: 1}},
		Derived: []probe.DerivedConfig{
			{Name: "year", From: "date", Transform: probe.TransformSubstring, Args: map[string]any{"start": 0, "end": 4}},
			{Name: "month", From: "date", Transform: probe.TransformSubstring, Args: map[string]any{"start": 5, "end": 7}},
			{Name: "day", From: "date", Transform: probe.TransformSubstring, Args: map[string]any{"start": 8, "end": 10}},
		},
	})
	require.NoError(t, err)
	vars, err := p.Probe([]byte(`date=2026-01-15`))
	require.NoError(t, err)

	r, err := CompileReflowRewrite("landing/{store}/{file}", "{year}/{month}/{day}/{store}/{file}")
	require.NoError(t, err)
	out, gotVars, err := r.ApplyWithVars("landing/007/record.xml", vars)
	require.NoError(t, err)

	require.Equal(t, "2026/01/15/007/record.xml", out)
	require.Equal(t, "2026", gotVars["year"])
	require.Equal(t, "01", gotVars["month"])
	require.Equal(t, "15", gotVars["day"])
}
