package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/output"
	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func TestRunContentProbeUntilResolvedReadsMonotonicRanges(t *testing.T) {
	data := []byte(`<root><pad>` + strings.Repeat("x", 40) + `</pad><date>2026-05-15</date></root>`)
	prov := newRangeProbeProvider("deep.xml", data)
	cfg := &probe.Config{
		ReadStrategy: probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "128", ChunkBytes: "16"},
		Extract:      []probe.ExtractorConfig{{Name: "date", Type: "xml_xpath", XPath: "//date", Required: true, OnMissing: probe.OnMissingFail}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "deep.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, map[string]string{"date": "2026-05-15"}, got.vars)
	require.Equal(t, "normal", got.routingClass)
	require.Equal(t, probe.TerminationAllRequiredResolved, got.audit.TerminationReason)
	require.LessOrEqual(t, got.bytesRead, cfg.ReadStrategy.MaxBytesValue)
	require.Len(t, prov.ranges, 5)
	require.Equal(t, []rangeCall{{0, 15}, {16, 31}, {32, 47}, {48, 63}, {64, 79}}, prov.ranges)
	require.NotNil(t, got.audit.Extractors[0].BytesAtResolution)
	require.Equal(t, got.bytesRead, *got.audit.Extractors[0].BytesAtResolution)
}

func TestRunContentProbeUntilResolvedTracksFirstResolutionBytes(t *testing.T) {
	data := []byte(`<root><id>alpha</id><pad>` + strings.Repeat("x", 48) + `</pad><date>2026-05-15</date></root>`)
	prov := newRangeProbeProvider("multi.xml", data)
	cfg := &probe.Config{
		ReadStrategy: probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "160", ChunkBytes: "32"},
		Extract: []probe.ExtractorConfig{
			{Name: "id", Type: "regex", Pattern: `<id>([^<]+)</id>`, Group: 1, Required: true, OnMissing: probe.OnMissingFail},
			{Name: "date", Type: "xml_xpath", XPath: "//date", Required: true, OnMissing: probe.OnMissingFail},
		},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "multi.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "alpha", got.vars["id"])
	require.Equal(t, "2026-05-15", got.vars["date"])
	require.Equal(t, probe.TerminationAllRequiredResolved, got.audit.TerminationReason)
	require.Len(t, got.audit.Extractors, 2)
	require.NotNil(t, got.audit.Extractors[0].BytesAtResolution)
	require.NotNil(t, got.audit.Extractors[1].BytesAtResolution)
	require.Less(t, *got.audit.Extractors[0].BytesAtResolution, *got.audit.Extractors[1].BytesAtResolution)
	require.Equal(t, got.bytesRead, *got.audit.Extractors[1].BytesAtResolution)
}

func TestRunContentProbeUntilResolvedMissingRequiredFail(t *testing.T) {
	data := []byte(`<root><other>value</other></root>`)
	prov := newRangeProbeProvider("missing.xml", data)
	cfg := &probe.Config{
		ReadStrategy: probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "128", ChunkBytes: "16"},
		Extract:      []probe.ExtractorConfig{{Name: "date", Type: "xml_xpath", XPath: "//date", Required: true, OnMissing: probe.OnMissingFail}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "missing.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Error(t, got.extractErr)
	require.Contains(t, got.extractErr.Error(), "required extractors unresolved")
	require.Equal(t, probe.TerminationStreamExhausted, got.audit.TerminationReason)
	require.Empty(t, got.vars)
}

func TestRunContentProbeUntilResolvedMissingRequiredQuarantine(t *testing.T) {
	data := []byte(`<root><other>value</other></root>`)
	prov := newRangeProbeProvider("missing.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "128", ChunkBytes: "16"},
		QuarantinePrefix: "_unresolved/",
		Extract:          []probe.ExtractorConfig{{Name: "date", Type: "xml_xpath", XPath: "//date", Required: true, OnMissing: probe.OnMissingQuarantine}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "missing.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, "_unresolved/", got.quarantinePrefix)
	require.Equal(t, "_unresolved", got.vars["date"])
	require.Equal(t, probe.TerminationStreamExhausted, got.audit.TerminationReason)
}

func TestRunContentProbeUntilResolvedDerivedFromMissingQuarantineSource(t *testing.T) {
	data := []byte(`<record></record>`)
	prov := newRangeProbeProvider("missing-date.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "128", ChunkBytes: "16"},
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:      "date",
			Type:      "xml_xpath",
			XPath:     "//date",
			Required:  true,
			OnMissing: probe.OnMissingQuarantine,
		}},
		Derived: []probe.DerivedConfig{
			{Name: "year", From: "date", Transform: probe.TransformSubstring, Args: map[string]any{"start": 0, "end": 4}},
			{Name: "month", From: "date", Transform: probe.TransformSubstring, Args: map[string]any{"start": 5, "end": 7}},
			{Name: "day", From: "date", Transform: probe.TransformSubstring, Args: map[string]any{"start": 8, "end": 10}},
		},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "missing-date.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, "_unresolved/", got.quarantinePrefix)
	require.Equal(t, "_unresolved", got.vars["date"])
	require.Equal(t, "_unresolved", got.vars["year"])
	require.Equal(t, "_unresolved", got.vars["month"])
	require.Equal(t, "_unresolved", got.vars["day"])
	require.Equal(t, probe.TerminationStreamExhausted, got.audit.TerminationReason)
}

func TestRunContentProbeUntilResolvedTargetPastMaxBytesQuarantine(t *testing.T) {
	data := []byte(`<root><pad>` + strings.Repeat("x", 80) + `</pad><date>2026-05-15</date></root>`)
	prov := newRangeProbeProvider("past-max.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "32", ChunkBytes: "16"},
		QuarantinePrefix: "_unresolved/",
		Extract:          []probe.ExtractorConfig{{Name: "date", Type: "xml_xpath", XPath: "//date", Required: true, OnMissing: probe.OnMissingQuarantine}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "past-max.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, probe.TerminationMaxBytesReached, got.audit.TerminationReason)
	require.Equal(t, int64(32), got.bytesRead)
	require.Equal(t, []rangeCall{{0, 15}, {16, 31}}, prov.ranges)
}

func TestRunContentProbeUntilResolvedPriorityWaitsForPrimaryAndDerivedUsesWinner(t *testing.T) {
	data := []byte(`<record><header><WindowStartDate>2026-05-01</WindowStartDate></header><pad>` + strings.Repeat("x", 80) + `</pad><entry><EntryDate>2027-06-15</EntryDate></entry></record>`)
	prov := newRangeProbeProvider("priority.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "256", ChunkBytes: "32"},
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:          "routing_date",
			Type:          "xml_xpath",
			XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
			Required:      true,
			OnMissing:     probe.OnMissingFail,
		}},
		Derived: []probe.DerivedConfig{{
			Name:      "year",
			From:      "routing_date",
			Transform: probe.TransformSubstring,
			Args:      map[string]any{"start": 0, "end": 4},
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "priority.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "normal", got.routingClass)
	require.Equal(t, "2027-06-15", got.vars["routing_date"])
	require.Equal(t, "2027", got.vars["year"])
	require.Equal(t, probe.TerminationAllRequiredResolved, got.audit.TerminationReason)
	require.Greater(t, len(prov.ranges), 3)
	item := got.audit.Extractors[0]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 1, *item.ResolvedPriority)
	require.Equal(t, "//EntryDate", item.ResolvedXPath)
	require.False(t, item.TruncatedFallback)
	require.Zero(t, got.audit.TruncatedFallbackCount)
}

func TestRunContentProbeUntilResolvedOptionalPriorityFallbackAtEarlyStopIsTruncated(t *testing.T) {
	data := []byte(`<record><id>alpha</id><header><WindowStartDate>2026-05-01</WindowStartDate></header><pad>` + strings.Repeat("x", 80) + `</pad><entry><EntryDate>2027-06-15</EntryDate></entry></record>`)
	prov := newRangeProbeProvider("optional-priority.xml", data)
	cfg := &probe.Config{
		ReadStrategy: probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "256", ChunkBytes: "96"},
		Extract: []probe.ExtractorConfig{
			{
				Name:      "id",
				Type:      "regex",
				Pattern:   `<id>([^<]+)</id>`,
				Group:     1,
				Required:  true,
				OnMissing: probe.OnMissingFail,
			},
			{
				Name:          "routing_date",
				Type:          "xml_xpath",
				XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
				Required:      false,
			},
		},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "optional-priority.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "normal", got.routingClass)
	require.Equal(t, "alpha", got.vars["id"])
	require.Equal(t, "2026-05-01", got.vars["routing_date"])
	require.Equal(t, probe.TerminationAllRequiredResolved, got.audit.TerminationReason)
	require.Len(t, prov.ranges, 1)
	item := got.audit.Extractors[1]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 2, *item.ResolvedPriority)
	require.Equal(t, "//WindowStartDate", item.ResolvedXPath)
	require.True(t, item.TruncatedFallback)
	require.Equal(t, 1, got.audit.TruncatedFallbackCount)
}

func TestRunContentProbeUntilResolvedPriorityFallbackOnlyAtEOF(t *testing.T) {
	data := []byte(`<record><header><WindowStartDate>2026-05-01</WindowStartDate></header></record>`)
	prov := newRangeProbeProvider("fallback-only.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "256", ChunkBytes: "32"},
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:          "routing_date",
			Type:          "xml_xpath",
			XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
			Required:      true,
			OnMissing:     probe.OnMissingFail,
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "fallback-only.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "normal", got.routingClass)
	require.Equal(t, "2026-05-01", got.vars["routing_date"])
	require.Equal(t, probe.TerminationStreamExhausted, got.audit.TerminationReason)
	item := got.audit.Extractors[0]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 2, *item.ResolvedPriority)
	require.Equal(t, "//WindowStartDate", item.ResolvedXPath)
	require.False(t, item.TruncatedFallback)
	require.Zero(t, got.audit.TruncatedFallbackCount)
}

func TestRunContentProbeUntilResolvedPriorityFallbackAtExactLimitIsEOF(t *testing.T) {
	data := []byte(`<record><header><WindowStartDate>2026-05-01</WindowStartDate></header></record>`)
	prov := newRangeProbeProvider("exact-limit.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "79", ChunkBytes: "32"},
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:          "routing_date",
			Type:          "xml_xpath",
			XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
			Required:      true,
			OnMissing:     probe.OnMissingFail,
		}},
	}
	require.Equal(t, int64(79), int64(len(data)))
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "exact-limit.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "normal", got.routingClass)
	require.Equal(t, probe.TerminationStreamExhausted, got.audit.TerminationReason)
	item := got.audit.Extractors[0]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 2, *item.ResolvedPriority)
	require.False(t, item.TruncatedFallback)
	require.Zero(t, got.audit.TruncatedFallbackCount)
}

func TestRunContentProbeUntilResolvedPriorityTruncatedFallbackQuarantines(t *testing.T) {
	data := []byte(`<record><header><WindowStartDate>2026-05-01</WindowStartDate></header><pad>` + strings.Repeat("x", 96) + `</pad><entry><EntryDate>2027-06-15</EntryDate></entry></record>`)
	prov := newRangeProbeProvider("truncated.xml", data)
	cfg := &probe.Config{
		ReadStrategy:     probe.ReadStrategyConfig{Mode: probe.ReadStrategyUntilResolved, MaxBytes: "96", ChunkBytes: "32"},
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:          "routing_date",
			Type:          "xml_xpath",
			XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
			Required:      true,
			OnMissing:     probe.OnMissingFail,
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeUntilResolved(context.Background(), prov, "truncated.xml", p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, "_unresolved/", got.quarantinePrefix)
	require.Equal(t, "2026-05-01", got.vars["routing_date"])
	require.Equal(t, probe.TerminationMaxBytesReached, got.audit.TerminationReason)
	item := got.audit.Extractors[0]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 2, *item.ResolvedPriority)
	require.True(t, item.TruncatedFallback)
	require.Equal(t, 1, got.audit.TruncatedFallbackCount)
}

func TestRunContentProbeFixedWindowPriorityFallbackOnlyAtEOF(t *testing.T) {
	data := []byte(`<record><header><WindowStartDate>2026-05-01</WindowStartDate></header></record>`)
	prov := newRangeProbeProvider("fixed-fallback-only.xml", data)
	cfg := &probe.Config{
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:          "routing_date",
			Type:          "xml_xpath",
			XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
			Required:      true,
			OnMissing:     probe.OnMissingFail,
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeTask(context.Background(), prov, probeTask{Key: "fixed-fallback-only.xml"}, p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "normal", got.routingClass)
	require.Equal(t, "2026-05-01", got.vars["routing_date"])
	require.Equal(t, probe.TerminationAllRequiredResolved, got.audit.TerminationReason)
	item := got.audit.Extractors[0]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 2, *item.ResolvedPriority)
	require.False(t, item.TruncatedFallback)
	require.Zero(t, got.audit.TruncatedFallbackCount)
}

func TestRunContentProbeFixedWindowPriorityTruncatedFallbackQuarantines(t *testing.T) {
	data := []byte(`<record><header><WindowStartDate>2026-05-01</WindowStartDate></header><pad>` + strings.Repeat("x", int(contentProbeMaxBytes)) + `</pad><entry><EntryDate>2027-06-15</EntryDate></entry></record>`)
	prov := newRangeProbeProvider("fixed-truncated.xml", data)
	cfg := &probe.Config{
		QuarantinePrefix: "_unresolved/",
		Extract: []probe.ExtractorConfig{{
			Name:          "routing_date",
			Type:          "xml_xpath",
			XPathPriority: []string{"//EntryDate", "//WindowStartDate"},
			Required:      true,
			OnMissing:     probe.OnMissingFail,
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeTask(context.Background(), prov, probeTask{Key: "fixed-truncated.xml"}, p, cfg, nil)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, "_unresolved/", got.quarantinePrefix)
	require.Equal(t, "2026-05-01", got.vars["routing_date"])
	require.Equal(t, probe.TerminationFixedWindow, got.audit.TerminationReason)
	item := got.audit.Extractors[0]
	require.NotNil(t, item.ResolvedPriority)
	require.Equal(t, 2, *item.ResolvedPriority)
	require.True(t, item.TruncatedFallback)
	require.Equal(t, 1, got.audit.TruncatedFallbackCount)
}

func TestContentProbeDerivedFailureErrorOutputRedactsRawValue(t *testing.T) {
	const marker = "SENSITIVE-MARKER-7f9a2c"
	data := []byte(`date=` + marker)
	prov := newRangeProbeProvider("bad-date.xml", data)
	cfg := &probe.Config{
		Extract: []probe.ExtractorConfig{{Name: "date", Type: "regex", Pattern: `date=([^ ]+)`, Group: 1}},
		Derived: []probe.DerivedConfig{{
			Name:      "date_iso",
			From:      "date",
			Transform: probe.TransformFormat,
			Args:      map[string]any{"input_layout": "2006-01-02", "output_layout": "20060102"},
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeTask(context.Background(), prov, probeTask{Key: "bad-date.xml"}, p, cfg, nil)
	require.NoError(t, err)
	require.Error(t, got.extractErr)
	require.NotContains(t, got.extractErr.Error(), marker)

	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "test-job", string(provider.ProviderS3))
	require.NoError(t, emitContentProbeError(context.Background(), w, "bad-date.xml", "content probe extract failed", got.extractErr, map[string]any{"probe": got.audit}))
	require.NoError(t, w.Close())
	require.NotContains(t, buf.String(), marker)
	require.Contains(t, buf.String(), `derive \"date_iso\" from \"date\" using format failed`)
}

func TestContentProbeRewriteFromSeedsSourceKeyCaptures(t *testing.T) {
	data := []byte(`<root/>`)
	prov := newRangeProbeProvider("source/RecordTypeBeta20260218.xml", data)
	cfg := &probe.Config{
		Extract: []probe.ExtractorConfig{},
		Derived: []probe.DerivedConfig{{
			Name:      "category",
			From:      "file",
			Transform: probe.TransformLookup,
			Args: map[string]any{
				"match_mode": "prefix",
				"table": []any{
					map[string]any{"match": "RecordTypeAlpha", "value": "category_alpha"},
					map[string]any{"match": "RecordTypeBeta", "value": "category_alpha"},
				},
			},
		}},
	}
	capture, err := compileContentProbeRewriteCapture("source/{file}")
	require.NoError(t, err)
	p, err := probe.NewWithRewriteCaptures(*cfg, capture.CaptureNames())
	require.NoError(t, err)

	got, err := runContentProbeTask(context.Background(), prov, probeTask{
		Key: "source/RecordTypeBeta20260218.xml",
		URI: "s3://bucket/source/RecordTypeBeta20260218.xml",
	}, p, cfg, capture)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "RecordTypeBeta20260218.xml", got.vars["file"])
	require.Equal(t, "category_alpha", got.vars["category"])
}

func TestContentProbeUntilResolvedLoadedConfigUsesNormalizedRuntimeValuesWithRewriteFrom(t *testing.T) {
	data := []byte(`<root><pad>` + strings.Repeat("x", 40) + `</pad><date>2026-05-15</date></root>`)
	prov := newRangeProbeProvider("source/RecordTypeBeta20260218.xml", data)
	cfg, err := loadProbeConfig([]byte(`
read_strategy:
  mode: until_resolved
  max_bytes: "128"
  chunk_bytes: "16"
extract:
  - name: date
    type: xml_xpath
    xpath: //date
    required: true
    on_missing: fail
derived:
  - name: category
    from: file
    transform: lookup
    args:
      match_mode: prefix
      table:
        - match: RecordTypeBeta
          value: category_alpha
`), "probe.yaml")
	require.NoError(t, err)
	capture, err := compileContentProbeRewriteCapture("source/{file}")
	require.NoError(t, err)
	p, err := newContentProbeProber(cfg, capture.CaptureNames())
	require.NoError(t, err)

	got, err := runContentProbeTask(context.Background(), prov, probeTask{
		Key: "source/RecordTypeBeta20260218.xml",
		URI: "s3://bucket/source/RecordTypeBeta20260218.xml",
	}, p, cfg, capture)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, int64(128), cfg.ReadStrategy.MaxBytesValue)
	require.Equal(t, int64(16), cfg.ReadStrategy.ChunkBytesValue)
	require.Equal(t, "RecordTypeBeta20260218.xml", got.vars["file"])
	require.Equal(t, "2026-05-15", got.vars["date"])
	require.Equal(t, "category_alpha", got.vars["category"])
	require.NotEmpty(t, prov.ranges)
}

func TestContentProbeJSONLInputUsesSourceKeyForRewriteCapture(t *testing.T) {
	line := `{"type":"gonimbus.index.object.v1","data":{"base_uri":"s3://bucket/source/","key":"source/RecordTypeAlpha20260218.xml","size_bytes":12}}`
	tasks := make(chan probeTask, 1)
	var invalidCount atomic.Int64
	var errorCount atomic.Int64
	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "test-job", string(provider.ProviderS3))

	err := enqueueContentProbeInput(context.Background(), line, tasks, w, func(*uri.ObjectURI) (contentProbeProvider, error) {
		t.Fatal("jsonl exact object input should not connect to provider")
		return nil, nil
	}, &invalidCount, &errorCount)
	require.NoError(t, err)
	close(tasks)
	task := <-tasks

	capture, err := compileContentProbeRewriteCapture("source/{file}")
	require.NoError(t, err)
	vars, err := contentProbeInitialVars(task, capture)
	require.NoError(t, err)
	require.Equal(t, "source/RecordTypeAlpha20260218.xml", task.Key)
	require.Equal(t, "RecordTypeAlpha20260218.xml", vars["file"])
	require.Zero(t, invalidCount.Load())
	require.Zero(t, errorCount.Load())
}

func TestContentProbeLookupNoMatchErrorOutputRedactsRawValue(t *testing.T) {
	const marker = "SENSITIVE-MARKER-7f9a2c"
	data := []byte(`file=` + marker)
	prov := newRangeProbeProvider("sensitive.xml", data)
	cfg := &probe.Config{
		Extract: []probe.ExtractorConfig{{Name: "file", Type: "regex", Pattern: `file=([^ ]+)`, Group: 1}},
		Derived: []probe.DerivedConfig{{
			Name:      "category",
			From:      "file",
			Transform: probe.TransformLookup,
			Args: map[string]any{
				"match_mode": "prefix",
				"table": []any{
					map[string]any{"match": "OtherPrefix", "value": "category_other"},
				},
			},
			OnMissing: probe.OnMissingFail,
		}},
	}
	require.NoError(t, cfg.Validate())
	p, err := probe.New(*cfg)
	require.NoError(t, err)

	got, err := runContentProbeTask(context.Background(), prov, probeTask{Key: "sensitive.xml"}, p, cfg, nil)
	require.NoError(t, err)
	require.Error(t, got.extractErr)
	require.NotContains(t, got.extractErr.Error(), marker)

	var buf bytes.Buffer
	w := output.NewJSONLWriter(&buf, "test-job", string(provider.ProviderS3))
	require.NoError(t, emitContentProbeError(context.Background(), w, "sensitive.xml", "content probe extract failed", got.extractErr, map[string]any{"probe": got.audit}))
	require.NoError(t, w.Close())
	require.NotContains(t, buf.String(), marker)
	require.Contains(t, buf.String(), `derive \"category\" from \"file\" using lookup failed`)
	require.Contains(t, buf.String(), `match_mode=prefix`)
}

type rangeCall struct {
	start int64
	end   int64
}

type rangeProbeProvider struct {
	key    string
	data   []byte
	ranges []rangeCall
}

func newRangeProbeProvider(key string, data []byte) *rangeProbeProvider {
	return &rangeProbeProvider{key: key, data: data}
}

func (p *rangeProbeProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p *rangeProbeProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: p.key, Size: int64(len(p.data)), LastModified: time.Now()}}, nil
}

func (p *rangeProbeProvider) Close() error { return nil }

func (p *rangeProbeProvider) GetRange(_ context.Context, _ string, start, endInclusive int64) (io.ReadCloser, int64, error) {
	p.ranges = append(p.ranges, rangeCall{start: start, end: endInclusive})
	if start >= int64(len(p.data)) {
		return io.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	if endInclusive >= int64(len(p.data)) {
		endInclusive = int64(len(p.data)) - 1
	}
	b := p.data[start : endInclusive+1]
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}
