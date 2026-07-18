package cmd

import (
	"context"
	"fmt"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexreader"
)

// indexReaderResolveOptions returns the standard local roots used by format-aware
// index commands (query, list, stats, doctor). Marker bounds match hub/export limits.
func indexReaderResolveOptions() (indexreader.ResolveOptions, error) {
	indexesRoot, err := indexRootDir()
	if err != nil {
		return indexreader.ResolveOptions{}, err
	}
	segmentRoot, err := appDataPath(appDataClassSegmentCache)
	if err != nil {
		return indexreader.ResolveOptions{}, err
	}
	return indexreader.ResolveOptions{
		IndexesRoot:      indexesRoot,
		SegmentCacheRoot: segmentRoot,
		MaxMarkerBytes:   int64(maxHubMarkerBytes),
		MaxManifestBytes: int64(maxDurableManifestBytes),
	}, nil
}

// openIndexReader resolves a format-aware local index reader (sqlite-v1 or durable-v2).
// When runID is set, opens a pinned durable-v2 snapshot and never consults latest.json.
func openIndexReader(ctx context.Context, baseURI, indexSetID, runID string) (indexreader.Reader, error) {
	return openIndexReaderWithAuthority(ctx, baseURI, indexSetID, runID, nil)
}

func openIndexReaderWithAuthority(ctx context.Context, baseURI, indexSetID, runID string, authority *indexcoord.Lease) (indexreader.Reader, error) {
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return nil, err
	}
	opts.Authority = authority
	return indexreader.ResolveIndexReader(ctx, opts, indexreader.ResolveTarget{
		BaseURI:    baseURI,
		IndexSetID: indexSetID,
		RunID:      runID,
	})
}

// preferListedIndexes keeps one entry per IndexSetID. When both sqlite-v1 and
// durable-v2 exist for the same set, prefer durable: it is only listed behind a
// verified latest trust chain, while a set-root index.db beside it may be a
// stale artifact from an earlier run. SQLite-only sets keep their entry.
func preferListedIndexes(listed []indexreader.ListedIndex) []indexreader.ListedIndex {
	if len(listed) == 0 {
		return nil
	}
	bySet := make(map[string]indexreader.ListedIndex, len(listed))
	order := make([]string, 0, len(listed))
	for _, item := range listed {
		id := item.Meta.IndexSetID
		if id == "" {
			// Keep path-only anomalies as unique keys.
			id = string(item.Meta.Format) + "|" + item.Meta.SourcePath
		}
		existing, ok := bySet[id]
		if !ok {
			bySet[id] = item
			order = append(order, id)
			continue
		}
		// Prefer durable when both formats exist for the same set.
		if existing.Meta.Format != indexreader.FormatDurableV2 && item.Meta.Format == indexreader.FormatDurableV2 {
			bySet[id] = item
		}
	}
	out := make([]indexreader.ListedIndex, 0, len(order))
	for _, id := range order {
		out = append(out, bySet[id])
	}
	return out
}

func formatLabel(f indexreader.Format) string {
	switch f {
	case indexreader.FormatSQLiteV1:
		return "sqlite-v1"
	case indexreader.FormatDurableV2:
		return "durable-v2"
	default:
		if f == "" {
			return "-"
		}
		return string(f)
	}
}

func errUnsupportedOnDurable(flag string) error {
	return fmt.Errorf("%s is not supported on durable-v2 indexes; use --format sqlite or both for SQLite run/prefix surfaces", flag)
}
