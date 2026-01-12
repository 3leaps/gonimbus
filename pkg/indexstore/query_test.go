package indexstore

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestQueryObjects(t *testing.T) {
	ctx := context.Background()

	// Create in-memory database
	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Run migration
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Create test index set
	indexSetID := "test-set-1"
	_, err = db.ExecContext(ctx, `
		INSERT INTO index_sets (index_set_id, base_uri, provider, build_params_hash, created_at)
		VALUES (?, 's3://test-bucket/', 's3', 'hash123', datetime('now'))
	`, indexSetID)
	if err != nil {
		t.Fatalf("insert index set: %v", err)
	}

	// Create test run
	runID := "test-run-1"
	_, err = db.ExecContext(ctx, `
		INSERT INTO index_runs (run_id, index_set_id, started_at, acquired_at, source_type, status)
		VALUES (?, ?, datetime('now'), datetime('now'), 'crawl', 'success')
	`, runID, indexSetID)
	if err != nil {
		t.Fatalf("insert run: %v", err)
	}

	// Insert test objects
	testObjects := []struct {
		relKey       string
		sizeBytes    int64
		lastModified string
	}{
		{"data/2025/01/file1.json", 1024, "2025-01-01T10:00:00Z"},
		{"data/2025/01/file2.xml", 2048, "2025-01-02T10:00:00Z"},
		{"data/2025/02/file3.json", 512, "2025-02-01T10:00:00Z"},
		{"logs/app.log", 4096, "2025-01-15T10:00:00Z"},
		{"config/settings.yaml", 256, "2024-12-01T10:00:00Z"},
	}

	for _, obj := range testObjects {
		_, err = db.ExecContext(ctx, `
			INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at)
			VALUES (?, ?, ?, ?, ?, datetime('now'))
		`, indexSetID, obj.relKey, obj.sizeBytes, obj.lastModified, runID)
		if err != nil {
			t.Fatalf("insert object %s: %v", obj.relKey, err)
		}
	}

	// Also insert a deleted object
	_, err = db.ExecContext(ctx, `
		INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at, deleted_at)
		VALUES (?, 'deleted/old.txt', 100, '2024-01-01T00:00:00Z', ?, datetime('now'), datetime('now'))
	`, indexSetID, runID)
	if err != nil {
		t.Fatalf("insert deleted object: %v", err)
	}

	tests := []struct {
		name          string
		params        QueryParams
		expectedCount int
		expectedKeys  []string
	}{
		{
			name: "all objects",
			params: QueryParams{
				IndexSetID: indexSetID,
			},
			expectedCount: 5,
		},
		{
			name: "glob pattern json files",
			params: QueryParams{
				IndexSetID: indexSetID,
				Pattern:    "**/*.json",
			},
			expectedCount: 2,
			expectedKeys:  []string{"data/2025/01/file1.json", "data/2025/02/file3.json"},
		},
		{
			name: "glob pattern xml files",
			params: QueryParams{
				IndexSetID: indexSetID,
				Pattern:    "**/*.xml",
			},
			expectedCount: 1,
			expectedKeys:  []string{"data/2025/01/file2.xml"},
		},
		{
			name: "glob pattern data prefix",
			params: QueryParams{
				IndexSetID: indexSetID,
				Pattern:    "data/**",
			},
			expectedCount: 3,
		},
		{
			name: "regex filter",
			params: QueryParams{
				IndexSetID: indexSetID,
				KeyRegex:   "2025/01",
			},
			expectedCount: 2,
		},
		{
			name: "min size filter",
			params: QueryParams{
				IndexSetID: indexSetID,
				MinSize:    1000,
			},
			expectedCount: 3, // 1024, 2048, 4096
		},
		{
			name: "max size filter",
			params: QueryParams{
				IndexSetID: indexSetID,
				MaxSize:    1000,
			},
			expectedCount: 2, // 512, 256
		},
		{
			name: "date after filter",
			params: QueryParams{
				IndexSetID:    indexSetID,
				ModifiedAfter: time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC),
			},
			expectedCount: 2, // logs/app.log and data/2025/02/file3.json
		},
		{
			name: "include deleted",
			params: QueryParams{
				IndexSetID:     indexSetID,
				IncludeDeleted: true,
			},
			expectedCount: 6, // all 5 + deleted one
		},
		{
			name: "limit",
			params: QueryParams{
				IndexSetID: indexSetID,
				Limit:      2,
			},
			expectedCount: 2,
		},
		{
			name: "combined filters",
			params: QueryParams{
				IndexSetID: indexSetID,
				Pattern:    "data/**",
				MinSize:    500,
				MaxSize:    2000,
			},
			expectedCount: 2, // file1.json (1024) and file3.json (512) match size range
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := QueryObjects(ctx, db, tt.params)
			if err != nil {
				t.Fatalf("QueryObjects: %v", err)
			}

			if len(results) != tt.expectedCount {
				t.Errorf("expected %d results, got %d", tt.expectedCount, len(results))
				for _, r := range results {
					t.Logf("  got: %s (size=%d)", r.RelKey, r.SizeBytes)
				}
			}

			if len(tt.expectedKeys) > 0 {
				gotKeys := make(map[string]bool)
				for _, r := range results {
					gotKeys[r.RelKey] = true
				}
				for _, expectedKey := range tt.expectedKeys {
					if !gotKeys[expectedKey] {
						t.Errorf("expected key %s not found in results", expectedKey)
					}
				}
			}
		})
	}
}

func TestQueryObjects_Validation(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tests := []struct {
		name        string
		params      QueryParams
		expectError string
	}{
		{
			name:        "missing index_set_id",
			params:      QueryParams{},
			expectError: "index_set_id is required",
		},
		{
			name: "invalid pattern",
			params: QueryParams{
				IndexSetID: "test",
				Pattern:    "[invalid",
			},
			expectError: "invalid glob pattern",
		},
		{
			name: "invalid regex",
			params: QueryParams{
				IndexSetID: "test",
				KeyRegex:   "[invalid",
			},
			expectError: "invalid key regex",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := QueryObjects(ctx, db, tt.params)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.expectError != "" && !contains(err.Error(), tt.expectError) {
				t.Errorf("expected error containing %q, got %q", tt.expectError, err.Error())
			}
		})
	}
}

func TestGetIndexSetByBaseURI(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Insert test index sets
	_, err = db.ExecContext(ctx, `
		INSERT INTO index_sets (index_set_id, base_uri, provider, build_params_hash, created_at)
		VALUES
			('set1', 's3://bucket1/', 's3', 'hash1', '2025-01-01T00:00:00Z'),
			('set2', 's3://bucket1/', 's3', 'hash2', '2025-01-02T00:00:00Z'),
			('set3', 's3://bucket2/', 's3', 'hash3', '2025-01-01T00:00:00Z')
	`)
	if err != nil {
		t.Fatalf("insert index sets: %v", err)
	}

	t.Run("returns most recent for base_uri", func(t *testing.T) {
		set, err := GetIndexSetByBaseURI(ctx, db, "s3://bucket1/")
		if err != nil {
			t.Fatalf("GetIndexSetByBaseURI: %v", err)
		}
		if set == nil {
			t.Fatal("expected index set, got nil")
		}
		// Should return set2 (most recent)
		if set.IndexSetID != "set2" {
			t.Errorf("expected set2, got %s", set.IndexSetID)
		}
	})

	t.Run("returns nil for unknown base_uri", func(t *testing.T) {
		set, err := GetIndexSetByBaseURI(ctx, db, "s3://unknown/")
		if err != nil {
			t.Fatalf("GetIndexSetByBaseURI: %v", err)
		}
		if set != nil {
			t.Errorf("expected nil, got %+v", set)
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
