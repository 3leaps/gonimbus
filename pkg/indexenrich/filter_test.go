package indexenrich

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

func TestCandidatesFromParentRowsFilterMatrix(t *testing.T) {
	standard := "STANDARD"
	glacier := "GLACIER"
	deletedAt := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	enrichedAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	rows := []indexsubstrate.CurrentObjectRow{
		{RelKey: "hot/a.xml", SizeBytes: 10, StorageClass: &standard, ETag: `"a"`},
		{RelKey: "hot/b.xml", SizeBytes: 100, StorageClass: &glacier, ETag: `"b"`},
		{RelKey: "hot/c.JSON", SizeBytes: 50, StorageClass: &standard, ETag: `"c"`},
		{RelKey: "cold/d.xml", SizeBytes: 20, StorageClass: nil, ETag: `"d"`}, // nil storage class
		{RelKey: "hot/gone.xml", SizeBytes: 30, StorageClass: &standard, ETag: `"g"`, DeletedAt: &deletedAt},
		{RelKey: "hot/done.xml", SizeBytes: 40, StorageClass: &standard, ETag: `"e"`, HeadEnrichedAt: &enrichedAt},
		{RelKey: "hot/exact.xml", SizeBytes: 0, StorageClass: &standard, ETag: `"z"`},
	}

	tests := []struct {
		name    string
		opts    QueryOptions
		want    []string
		wantErr string
	}{
		{
			name: "default excludes deleted",
			opts: QueryOptions{},
			want: []string{"hot/a.xml", "hot/b.xml", "hot/c.JSON", "cold/d.xml", "hot/done.xml", "hot/exact.xml"},
		},
		{
			name: "include-deleted keeps tombstones",
			opts: QueryOptions{IncludeDeleted: true},
			want: []string{"hot/a.xml", "hot/b.xml", "hot/c.JSON", "cold/d.xml", "hot/gone.xml", "hot/done.xml", "hot/exact.xml"},
		},
		{
			name: "glob pattern",
			opts: QueryOptions{Pattern: "hot/*.xml"},
			want: []string{"hot/a.xml", "hot/b.xml", "hot/done.xml", "hot/exact.xml"},
		},
		{
			name: "key-regex case-sensitive",
			opts: QueryOptions{KeyRegex: `\.JSON$`},
			want: []string{"hot/c.JSON"},
		},
		{
			name: "key-regex no match for wrong case",
			opts: QueryOptions{KeyRegex: `\.json$`},
			want: []string{},
		},
		{
			name: "min-size boundary inclusive lower",
			opts: QueryOptions{MinSize: "50"},
			want: []string{"hot/b.xml", "hot/c.JSON"},
		},
		{
			name: "max-size boundary inclusive upper",
			opts: QueryOptions{MaxSize: "20"},
			want: []string{"hot/a.xml", "cold/d.xml", "hot/exact.xml"},
		},
		{
			name: "min and max size combination",
			opts: QueryOptions{MinSize: "20", MaxSize: "50"},
			want: []string{"hot/c.JSON", "cold/d.xml", "hot/done.xml"},
		},
		{
			name: "storage-class exact single",
			opts: QueryOptions{StorageClasses: []string{"GLACIER"}},
			want: []string{"hot/b.xml"},
		},
		{
			name: "storage-class multi-value",
			opts: QueryOptions{StorageClasses: []string{"STANDARD", "GLACIER"}},
			want: []string{"hot/a.xml", "hot/b.xml", "hot/c.JSON", "hot/done.xml", "hot/exact.xml"},
		},
		{
			name: "storage-class case-sensitive no match",
			opts: QueryOptions{StorageClasses: []string{"glacier"}},
			want: []string{},
		},
		{
			name: "storage-class excludes nil class when filter set",
			opts: QueryOptions{StorageClasses: []string{"STANDARD"}},
			want: []string{"hot/a.xml", "hot/c.JSON", "hot/done.xml", "hot/exact.xml"},
		},
		{
			name: "pattern plus storage class plus size",
			opts: QueryOptions{Pattern: "hot/**", StorageClasses: []string{"STANDARD"}, MinSize: "40"},
			want: []string{"hot/c.JSON", "hot/done.xml"},
		},
		{
			name: "include-deleted with storage class",
			opts: QueryOptions{IncludeDeleted: true, StorageClasses: []string{"STANDARD"}},
			want: []string{"hot/a.xml", "hot/c.JSON", "hot/gone.xml", "hot/done.xml", "hot/exact.xml"},
		},
		{
			name:    "invalid glob",
			opts:    QueryOptions{Pattern: "["},
			wantErr: "invalid glob pattern",
		},
		{
			name:    "invalid key regex",
			opts:    QueryOptions{KeyRegex: "("},
			wantErr: "invalid key regex",
		},
		{
			name:    "invalid min size",
			opts:    QueryOptions{MinSize: "not-a-size"},
			wantErr: "invalid min size",
		},
		{
			name:    "invalid max size",
			opts:    QueryOptions{MaxSize: "xx"},
			wantErr: "invalid max size",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := candidatesFromParentRows(rows, tc.opts)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			keys := make([]string, 0, len(got))
			for _, c := range got {
				keys = append(keys, c.RelKey)
			}
			require.Equal(t, tc.want, keys)
		})
	}
}

func TestCandidatesFromParentRowsResumeSkipsAlreadyEnriched(t *testing.T) {
	// Resume is applied in executeHeads after filtering; this proves filtering
	// still returns already-enriched rows so the worker can skip them.
	standard := "STANDARD"
	enrichedAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	rows := []indexsubstrate.CurrentObjectRow{
		{RelKey: "hot/a.xml", SizeBytes: 10, StorageClass: &standard},
		{RelKey: "hot/done.xml", SizeBytes: 10, StorageClass: &standard, HeadEnrichedAt: &enrichedAt},
	}
	got, err := candidatesFromParentRows(rows, QueryOptions{Pattern: "hot/**"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Nil(t, got[0].HeadEnrichedAt)
	require.NotNil(t, got[1].HeadEnrichedAt)
	require.Equal(t, "hot/done.xml", got[1].RelKey)
}
