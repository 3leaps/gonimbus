package probe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProber_XMLXPath(t *testing.T) {
	cfg := Config{Extract: []ExtractorConfig{{Name: "business_date", Type: "xml_xpath", XPath: "//BusinessDate"}}}
	p, err := New(cfg)
	require.NoError(t, err)

	data := []byte(`<?xml version="1.0"?><Root><BusinessDate>2025-12-31</BusinessDate></Root>`)
	vars, err := p.Probe(data)
	require.NoError(t, err)
	require.Equal(t, "2025-12-31", vars["business_date"])
}

func TestProber_JSONPath(t *testing.T) {
	cfg := Config{Extract: []ExtractorConfig{{Name: "id", Type: "json_path", JSONPath: "$.a.b[0].id"}}}
	p, err := New(cfg)
	require.NoError(t, err)

	data := []byte(`{"a":{"b":[{"id":"x"}]}}`)
	vars, err := p.Probe(data)
	require.NoError(t, err)
	require.Equal(t, "x", vars["id"])
}

func TestProber_JSONPathRootArray(t *testing.T) {
	cfg := Config{Extract: []ExtractorConfig{{Name: "id", Type: "json_path", JSONPath: "[0].id"}}}
	p, err := New(cfg)
	require.NoError(t, err)

	data := []byte(`[{"id":"x"}]`)
	vars, err := p.Probe(data)
	require.NoError(t, err)
	require.Equal(t, "x", vars["id"])
}

func TestProber_Regex(t *testing.T) {
	cfg := Config{Extract: []ExtractorConfig{{Name: "v", Type: "regex", Pattern: `BusinessDate>([^<]+)<`, Group: 1}}}
	p, err := New(cfg)
	require.NoError(t, err)

	data := []byte(`<BusinessDate>2025-12-31</BusinessDate>`)
	vars, err := p.Probe(data)
	require.NoError(t, err)
	require.Equal(t, "2025-12-31", vars["v"])
}

func TestProber_RegexGroup0FullMatch(t *testing.T) {
	cfg := Config{Extract: []ExtractorConfig{{Name: "v", Type: "regex", Pattern: `BusinessDate>([^<]+)<`, Group: 0}}}
	p, err := New(cfg)
	require.NoError(t, err)

	data := []byte(`<BusinessDate>2025-12-31</BusinessDate>`)
	vars, err := p.Probe(data)
	require.NoError(t, err)
	require.Equal(t, "BusinessDate>2025-12-31<", vars["v"])
}

func TestCompileXMLXPath_RejectsNestedDescendant(t *testing.T) {
	_, err := CompileXMLXPath("/a//b")
	require.Error(t, err)
}
