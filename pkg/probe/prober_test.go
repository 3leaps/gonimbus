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

func TestConfigUntilResolvedValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "requires max bytes",
			cfg: Config{
				ReadStrategy: ReadStrategyConfig{Mode: ReadStrategyUntilResolved},
				Extract:      []ExtractorConfig{{Name: "d", Type: "xml_xpath", XPath: "//d", Required: true}},
			},
			wantErr: "read_strategy.max_bytes is required",
		},
		{
			name: "rejects json path",
			cfg: Config{
				ReadStrategy: ReadStrategyConfig{Mode: ReadStrategyUntilResolved, MaxBytes: "1MB"},
				Extract:      []ExtractorConfig{{Name: "d", Type: "json_path", JSONPath: "$.d", Required: true}},
			},
			wantErr: "json_path streaming not yet supported under until_resolved",
		},
		{
			name: "quarantine requires prefix",
			cfg: Config{
				ReadStrategy: ReadStrategyConfig{Mode: ReadStrategyUntilResolved, MaxBytes: "1MB"},
				Extract:      []ExtractorConfig{{Name: "d", Type: "xml_xpath", XPath: "//d", Required: true, OnMissing: OnMissingQuarantine}},
			},
			wantErr: "quarantine_prefix is required",
		},
		{
			name: "quarantine prefix rejects URI",
			cfg: Config{
				ReadStrategy:     ReadStrategyConfig{Mode: ReadStrategyUntilResolved, MaxBytes: "1MB"},
				QuarantinePrefix: "s3://bucket/q/",
				Extract:          []ExtractorConfig{{Name: "d", Type: "xml_xpath", XPath: "//d", Required: true, OnMissing: OnMissingQuarantine}},
			},
			wantErr: "quarantine_prefix must be a relative destination prefix",
		},
		{
			name: "valid until resolved",
			cfg: Config{
				ReadStrategy:     ReadStrategyConfig{Mode: ReadStrategyUntilResolved, MaxBytes: "1MB", ChunkBytes: "64KB"},
				QuarantinePrefix: "_unresolved/",
				Extract:          []ExtractorConfig{{Name: "d", Type: "xml_xpath", XPath: "//d", Required: true, OnMissing: OnMissingQuarantine}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(1_000_000), tt.cfg.ReadStrategy.MaxBytesValue)
			require.Equal(t, int64(64_000), tt.cfg.ReadStrategy.ChunkBytesValue)
		})
	}
}

func TestProbeDetailedAudit(t *testing.T) {
	cfg := Config{
		Extract: []ExtractorConfig{
			{Name: "business_date", Type: "xml_xpath", XPath: "//BusinessDate", Required: true, OnMissing: OnMissingFail},
			{Name: "tenant", Type: "regex", Pattern: `Tenant>([^<]+)<`, Group: 1},
		},
	}
	p, err := New(cfg)
	require.NoError(t, err)

	data := []byte(`<Root><BusinessDate>2025-12-31</BusinessDate><Tenant>abc</Tenant></Root>`)
	got, err := p.ProbeDetailed(data, int64(len(data)), TerminationAllRequiredResolved)
	require.NoError(t, err)

	require.Equal(t, map[string]string{"business_date": "2025-12-31", "tenant": "abc"}, got.Vars)
	require.Equal(t, int64(len(data)), got.Audit.BytesRead)
	require.Equal(t, TerminationAllRequiredResolved, got.Audit.TerminationReason)
	require.Len(t, got.Audit.Extractors, 2)
	require.True(t, got.Audit.Extractors[0].Resolved)
	require.True(t, got.Audit.Extractors[0].Required)
	require.NotNil(t, got.Audit.Extractors[0].BytesAtResolution)
	require.Equal(t, int64(len(data)), *got.Audit.Extractors[0].BytesAtResolution)
}

func TestUnresolvedResultIncludesExtractorAudit(t *testing.T) {
	cfg := Config{
		QuarantinePrefix: "_unresolved/",
		Extract: []ExtractorConfig{
			{Name: "business_date", Type: "xml_xpath", XPath: "//BusinessDate", Required: true, OnMissing: OnMissingQuarantine},
			{Name: "tenant", Type: "regex", Pattern: `Tenant>([^<]+)<`, Group: 1},
		},
	}
	p, err := New(cfg)
	require.NoError(t, err)

	got := p.UnresolvedResult(64, TerminationParseError)

	require.Empty(t, got.Vars)
	require.Equal(t, int64(64), got.Audit.BytesRead)
	require.Equal(t, TerminationParseError, got.Audit.TerminationReason)
	require.Len(t, got.Audit.Extractors, 2)
	require.Equal(t, "business_date", got.Audit.Extractors[0].Name)
	require.True(t, got.Audit.Extractors[0].Required)
	require.Equal(t, OnMissingQuarantine, got.Audit.Extractors[0].OnMissing)
	require.False(t, got.Audit.Extractors[0].Resolved)
}
