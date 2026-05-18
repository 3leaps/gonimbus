package probe

import (
	"strings"
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

func TestXMLXPathDeclaredCharsetISO88591(t *testing.T) {
	x, err := CompileXMLXPath("//name")
	require.NoError(t, err)
	data := append([]byte(`<?xml version="1.0" encoding="ISO-8859-1"?><root><name>Caf`), 0xe9)
	data = append(data, []byte(`</name></root>`)...)

	got, ok, err := x.FindFirstText(data)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "Caf\u00e9", got)
}

func TestXMLXPathDeclaredCharsetWindows1252(t *testing.T) {
	x, err := CompileXMLXPath("//quote")
	require.NoError(t, err)
	data := append([]byte(`<?xml version="1.0" encoding="Windows-1252"?><root><quote>`), 0x91)
	data = append(data, []byte(`hello`)...)
	data = append(data, 0x92)
	data = append(data, []byte(`</quote></root>`)...)

	got, ok, err := x.FindFirstText(data)

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "\u2018hello\u2019", got)
}

func TestXMLXPathDeclaredCharsetUnsupportedLabel(t *testing.T) {
	x, err := CompileXMLXPath("//name")
	require.NoError(t, err)
	data := []byte(`<?xml version="1.0" encoding="NOT-AN-ENCODING"?><root><name>value</name></root>`)

	got, ok, err := x.FindFirstText(data)

	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "not-an-encoding")
	require.False(t, ok)
	require.Empty(t, got)
}

func TestXMLXPathMalformedDeclarationError(t *testing.T) {
	x, err := CompileXMLXPath("//name")
	require.NoError(t, err)
	data := []byte(`<?xml version="1.0" encoding="UTF-8"<root><name>value</name></root>`)

	got, ok, err := x.FindFirstText(data)

	require.Error(t, err)
	require.NotContains(t, strings.ToLower(err.Error()), "unsupported charset")
	require.False(t, ok)
	require.Empty(t, got)
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

func TestProber_DerivedTransforms(t *testing.T) {
	cfg := Config{
		Extract: []ExtractorConfig{
			{Name: "date", Type: "regex", Pattern: `date=([0-9-]+)`, Group: 1},
			{Name: "subject", Type: "regex", Pattern: `subject=([A-Za-z0-9]+)`, Group: 1},
			{Name: "ident", Type: "regex", Pattern: `ident=([A-Z]+-[0-9]+)`, Group: 1},
		},
		Derived: []DerivedConfig{
			{Name: "year", From: "date", Transform: TransformSubstring, Args: map[string]any{"start": 0, "end": 4}},
			{Name: "date_compact", From: "date", Transform: TransformFormat, Args: map[string]any{"input_layout": "2006-01-02", "output_layout": "20060102"}},
			{Name: "subject_lower", From: "subject", Transform: TransformLowercase},
			{Name: "subject_upper", From: "subject", Transform: TransformUppercase},
			{Name: "kind", From: "ident", Transform: TransformRegexCapture, Args: map[string]any{"pattern": `^([A-Z]+)-`, "group": 1}},
			{Name: "subject_padded", From: "subject", Transform: TransformPad, Args: map[string]any{"char": "0", "side": "left", "width": 5}},
		},
	}
	p, err := New(cfg)
	require.NoError(t, err)

	vars, err := p.Probe([]byte(`date=2026-01-15 subject=7 ident=ALPHA-42`))
	require.NoError(t, err)
	require.Equal(t, "2026", vars["year"])
	require.Equal(t, "20260115", vars["date_compact"])
	require.Equal(t, "7", vars["subject_lower"])
	require.Equal(t, "7", vars["subject_upper"])
	require.Equal(t, "ALPHA", vars["kind"])
	require.Equal(t, "00007", vars["subject_padded"])
}

func TestProber_DerivedFailureSanitizesRawValue(t *testing.T) {
	cfg := Config{
		Extract: []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `date=([^ ]+)`, Group: 1}},
		Derived: []DerivedConfig{{
			Name:      "date_iso",
			From:      "date",
			Transform: TransformFormat,
			Args:      map[string]any{"input_layout": "2006-01-02", "output_layout": "20060102"},
		}},
	}
	p, err := New(cfg)
	require.NoError(t, err)

	res, err := p.ProbeDetailed([]byte(`date=SENSITIVE-MARKER-7f9a2c`), 29, TerminationAllRequiredResolved)
	require.NoError(t, err)
	routingClass, requiredFailed, failureErr := p.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)

	require.Equal(t, "normal", routingClass)
	require.True(t, requiredFailed)
	require.Error(t, failureErr)
	require.Contains(t, failureErr.Error(), `derive "date_iso" from "date" using format failed`)
	require.Contains(t, failureErr.Error(), `expected layout "2006-01-02"`)
	require.NotContains(t, failureErr.Error(), "SENSITIVE-MARKER-7f9a2c")
}

func TestProber_DerivedTransformFailurePaths(t *testing.T) {
	tests := []struct {
		name      string
		derived   DerivedConfig
		wantError string
	}{
		{
			name: "substring out of bounds",
			derived: DerivedConfig{
				Name:      "year",
				From:      "date",
				Transform: TransformSubstring,
				Args:      map[string]any{"start": 0, "end": 20},
			},
			wantError: "substring bounds",
		},
		{
			name: "regex no match",
			derived: DerivedConfig{
				Name:      "prefix",
				From:      "date",
				Transform: TransformRegexCapture,
				Args:      map[string]any{"pattern": `^([A-Z]+)-`, "group": 1},
			},
			wantError: "did not match",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Extract: []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `date=([^ ]+)`, Group: 1}},
				Derived: []DerivedConfig{tt.derived},
			}
			p, err := New(cfg)
			require.NoError(t, err)

			res, err := p.ProbeDetailed([]byte(`date=2026-01-15`), 15, TerminationAllRequiredResolved)
			require.NoError(t, err)
			_, requiredFailed, failureErr := p.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)

			require.True(t, requiredFailed)
			require.Error(t, failureErr)
			require.Contains(t, failureErr.Error(), tt.wantError)
			require.NotContains(t, failureErr.Error(), "2026-01-15")
		})
	}
}

func TestProber_DerivedOnMissingQuarantine(t *testing.T) {
	cfg := Config{
		QuarantinePrefix: "_unresolved/",
		Extract:          []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `date=([^ ]+)`, Group: 1}},
		Derived: []DerivedConfig{{
			Name:      "date_iso",
			From:      "date",
			Transform: TransformFormat,
			Args:      map[string]any{"input_layout": "2006-01-02", "output_layout": "20060102"},
			OnMissing: OnMissingQuarantine,
		}},
	}
	p, err := New(cfg)
	require.NoError(t, err)

	res, err := p.ProbeDetailed([]byte(`date=not-a-date`), 15, TerminationAllRequiredResolved)
	require.NoError(t, err)
	routingClass, requiredFailed, failureErr := p.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)

	require.Equal(t, "quarantine", routingClass)
	require.False(t, requiredFailed)
	require.Error(t, failureErr)
	require.Equal(t, "_unresolved", res.Vars["date_iso"])
	require.NotContains(t, failureErr.Error(), "not-a-date")
}

func TestProber_DerivedRequiredMatrix(t *testing.T) {
	requiredFalse := false
	tests := []struct {
		name           string
		data           []byte
		required       *bool
		wantRequired   bool
		wantDerivedVar bool
	}{
		{name: "required true upstream resolved", data: []byte(`date=2026-01-15`), wantDerivedVar: true},
		{name: "required true upstream unresolved", data: []byte(`missing=true`), wantRequired: true},
		{name: "required false upstream resolved", data: []byte(`date=2026-01-15`), required: &requiredFalse, wantDerivedVar: true},
		{name: "required false upstream unresolved", data: []byte(`missing=true`), required: &requiredFalse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Extract: []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `date=([0-9-]+)`, Group: 1}},
				Derived: []DerivedConfig{{
					Name:      "year",
					From:      "date",
					Transform: TransformSubstring,
					Args:      map[string]any{"start": 0, "end": 4},
					Required:  tt.required,
				}},
			}
			p, err := New(cfg)
			require.NoError(t, err)

			res, err := p.ProbeDetailed(tt.data, int64(len(tt.data)), TerminationAllRequiredResolved)
			require.NoError(t, err)
			_, requiredFailed, _ := p.ApplyMissingPoliciesDetailed(res.Vars, &res.Audit, res.Failures)

			require.Equal(t, tt.wantRequired, requiredFailed)
			if tt.wantDerivedVar {
				require.Equal(t, "2026", res.Vars["year"])
			} else {
				require.NotContains(t, res.Vars, "year")
			}
		})
	}
}

func TestConfigDerivedValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "duplicate extract derived",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "year", Type: "regex", Pattern: `x=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{
					Name: "year", From: "year", Transform: TransformLowercase,
				}},
			},
			wantErr: `derived[0].name "year" conflicts with extract[0]`,
		},
		{
			name: "derived from derived rejected",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `x=(.+)`, Group: 1}},
				Derived: []DerivedConfig{
					{Name: "year", From: "date", Transform: TransformSubstring, Args: map[string]any{"start": 0, "end": 4}},
					{Name: "yy", From: "year", Transform: TransformSubstring, Args: map[string]any{"start": 2, "end": 4}},
				},
			},
			wantErr: `chaining is not supported`,
		},
		{
			name: "unknown from",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `x=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{
					Name: "x", From: "missing", Transform: TransformLowercase,
				}},
			},
			wantErr: `derived[0].from = "missing" is unknown`,
		},
		{
			name: "unknown transform",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "date", Type: "regex", Pattern: `x=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{
					Name: "x", From: "date", Transform: "replace_all",
				}},
			},
			wantErr: `available transforms`,
		},
		{
			name: "pad width zero",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "id", Type: "regex", Pattern: `id=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{Name: "id_pad", From: "id", Transform: TransformPad, Args: map[string]any{"width": 0}}},
			},
			wantErr: `width must be in [1, 1024]`,
		},
		{
			name: "pad width over cap",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "id", Type: "regex", Pattern: `id=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{Name: "id_pad", From: "id", Transform: TransformPad, Args: map[string]any{"width": 5000}}},
			},
			wantErr: `width must be in [1, 1024]`,
		},
		{
			name: "pad width missing",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "id", Type: "regex", Pattern: `id=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{Name: "id_pad", From: "id", Transform: TransformPad}},
			},
			wantErr: `args.width is required`,
		},
		{
			name: "pad char empty",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "id", Type: "regex", Pattern: `id=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{Name: "id_pad", From: "id", Transform: TransformPad, Args: map[string]any{"width": 5, "char": ""}}},
			},
			wantErr: `char must be exactly one Unicode scalar`,
		},
		{
			name: "pad char multi rune",
			cfg: Config{
				Extract: []ExtractorConfig{{Name: "id", Type: "regex", Pattern: `id=(.+)`, Group: 1}},
				Derived: []DerivedConfig{{Name: "id_pad", From: "id", Transform: TransformPad, Args: map[string]any{"width": 5, "char": "00"}}},
			},
			wantErr: `got "00" (2 runes)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
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
