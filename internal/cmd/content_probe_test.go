package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/probe"
	"github.com/3leaps/gonimbus/pkg/provider"
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

	got, err := runContentProbeUntilResolved(context.Background(), prov, "deep.xml", p, cfg)

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

	got, err := runContentProbeUntilResolved(context.Background(), prov, "multi.xml", p, cfg)

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

	got, err := runContentProbeUntilResolved(context.Background(), prov, "missing.xml", p, cfg)

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

	got, err := runContentProbeUntilResolved(context.Background(), prov, "missing.xml", p, cfg)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, "_unresolved/", got.quarantinePrefix)
	require.Equal(t, "_unresolved", got.vars["date"])
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

	got, err := runContentProbeUntilResolved(context.Background(), prov, "past-max.xml", p, cfg)

	require.NoError(t, err)
	require.Nil(t, got.extractErr)
	require.Equal(t, "quarantine", got.routingClass)
	require.Equal(t, probe.TerminationMaxBytesReached, got.audit.TerminationReason)
	require.Equal(t, int64(32), got.bytesRead)
	require.Equal(t, []rangeCall{{0, 15}, {16, 31}}, prov.ranges)
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
