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
			results, _, err := QueryObjects(ctx, db, tt.params)
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
			_, _, err := QueryObjects(ctx, db, tt.params)
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

func TestQueryObjectCount(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Create test index set
	indexSetID := "count-test-set"
	_, err = db.ExecContext(ctx, `
		INSERT INTO index_sets (index_set_id, base_uri, provider, build_params_hash, created_at)
		VALUES (?, 's3://count-bucket/', 's3', 'hash123', datetime('now'))
	`, indexSetID)
	if err != nil {
		t.Fatalf("insert index set: %v", err)
	}

	// Create test run
	runID := "count-test-run"
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

	tests := []struct {
		name          string
		params        QueryParams
		expectedCount int64
		description   string
	}{
		{
			name: "fast path - count all",
			params: QueryParams{
				IndexSetID: indexSetID,
			},
			expectedCount: 5,
			description:   "COUNT(*) fast path, no client filtering",
		},
		{
			name: "fast path - with size filter",
			params: QueryParams{
				IndexSetID: indexSetID,
				MinSize:    1000,
			},
			expectedCount: 3,
			description:   "COUNT(*) fast path with SQL-pushed size filter",
		},
		{
			name: "streaming path - with pattern",
			params: QueryParams{
				IndexSetID: indexSetID,
				Pattern:    "**/*.json",
			},
			expectedCount: 2,
			description:   "streaming path required for glob pattern",
		},
		{
			name: "streaming path - with regex",
			params: QueryParams{
				IndexSetID: indexSetID,
				KeyRegex:   "2025/01",
			},
			expectedCount: 2,
			description:   "streaming path required for regex",
		},
		{
			name: "streaming path - pattern with prefix pushdown",
			params: QueryParams{
				IndexSetID: indexSetID,
				Pattern:    "data/2025/**",
			},
			expectedCount: 3,
			description:   "streaming but prefix 'data/2025' pushed to SQL",
		},
		{
			name: "limit ignored for count",
			params: QueryParams{
				IndexSetID: indexSetID,
				Limit:      2,
			},
			expectedCount: 5, // limit is ignored, returns total count
			description:   "limit is ignored for count queries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := QueryObjectCount(ctx, db, tt.params)
			if err != nil {
				t.Fatalf("QueryObjectCount: %v", err)
			}
			if count != tt.expectedCount {
				t.Errorf("expected count %d, got %d (%s)", tt.expectedCount, count, tt.description)
			}
		})
	}
}

func TestQueryObjectCount_MatchesQueryObjects(t *testing.T) {
	// Verify that QueryObjectCount returns the same count as len(QueryObjects)
	ctx := context.Background()

	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	indexSetID := "consistency-test"
	_, _ = db.ExecContext(ctx, `
		INSERT INTO index_sets (index_set_id, base_uri, provider, build_params_hash, created_at)
		VALUES (?, 's3://consistency-bucket/', 's3', 'hash', datetime('now'))
	`, indexSetID)

	runID := "consistency-run"
	_, _ = db.ExecContext(ctx, `
		INSERT INTO index_runs (run_id, index_set_id, started_at, acquired_at, source_type, status)
		VALUES (?, ?, datetime('now'), datetime('now'), 'crawl', 'success')
	`, runID, indexSetID)

	// Insert varied test data
	for i := 0; i < 50; i++ {
		relKey := "data/file" + string(rune('0'+i%10)) + ".json"
		if i%3 == 0 {
			relKey = "logs/app" + string(rune('0'+i%10)) + ".log"
		}
		_, _ = db.ExecContext(ctx, `
			INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at)
			VALUES (?, ?, ?, '2025-01-01T00:00:00Z', ?, datetime('now'))
		`, indexSetID, relKey, i*100, runID)
	}

	testParams := []QueryParams{
		{IndexSetID: indexSetID},
		{IndexSetID: indexSetID, Pattern: "data/**"},
		{IndexSetID: indexSetID, KeyRegex: "file[0-5]"},
		{IndexSetID: indexSetID, MinSize: 2000},
		{IndexSetID: indexSetID, Pattern: "**/*.json", MinSize: 1000},
	}

	for i, params := range testParams {
		results, _, err := QueryObjects(ctx, db, params)
		if err != nil {
			t.Fatalf("test %d QueryObjects: %v", i, err)
		}

		count, err := QueryObjectCount(ctx, db, params)
		if err != nil {
			t.Fatalf("test %d QueryObjectCount: %v", i, err)
		}

		if int64(len(results)) != count {
			t.Errorf("test %d: QueryObjects returned %d, QueryObjectCount returned %d",
				i, len(results), count)
		}
	}
}

func TestQueryObjects_PrefixPushdown(t *testing.T) {
	// Test that prefix pushdown correctly narrows results
	ctx := context.Background()

	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	indexSetID := "prefix-test"
	_, _ = db.ExecContext(ctx, `
		INSERT INTO index_sets (index_set_id, base_uri, provider, build_params_hash, created_at)
		VALUES (?, 's3://prefix-bucket/', 's3', 'hash', datetime('now'))
	`, indexSetID)

	runID := "prefix-run"
	_, _ = db.ExecContext(ctx, `
		INSERT INTO index_runs (run_id, index_set_id, started_at, acquired_at, source_type, status)
		VALUES (?, ?, datetime('now'), datetime('now'), 'crawl', 'success')
	`, runID, indexSetID)

	// Insert objects with varied prefixes
	testKeys := []string{
		"data/2025/01/file.json",
		"data/2025/02/file.json",
		"data/2024/12/file.json",
		"logs/2025/app.log",
		"config/settings.yaml",
	}

	for _, key := range testKeys {
		_, _ = db.ExecContext(ctx, `
			INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at)
			VALUES (?, ?, 100, '2025-01-01T00:00:00Z', ?, datetime('now'))
		`, indexSetID, key, runID)
	}

	tests := []struct {
		name          string
		pattern       string
		expectedCount int
		expectedKeys  []string
	}{
		{
			name:          "prefix data/2025",
			pattern:       "data/2025/**",
			expectedCount: 2,
			expectedKeys:  []string{"data/2025/01/file.json", "data/2025/02/file.json"},
		},
		{
			name:          "prefix data/2025/01",
			pattern:       "data/2025/01/**",
			expectedCount: 1,
			expectedKeys:  []string{"data/2025/01/file.json"},
		},
		{
			name:          "prefix logs",
			pattern:       "logs/**",
			expectedCount: 1,
			expectedKeys:  []string{"logs/2025/app.log"},
		},
		{
			name:          "no derivable prefix",
			pattern:       "**/*.json",
			expectedCount: 3, // all json files
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := QueryParams{
				IndexSetID: indexSetID,
				Pattern:    tt.pattern,
			}

			results, _, err := QueryObjects(ctx, db, params)
			if err != nil {
				t.Fatalf("QueryObjects: %v", err)
			}

			if len(results) != tt.expectedCount {
				t.Errorf("expected %d results, got %d", tt.expectedCount, len(results))
				for _, r := range results {
					t.Logf("  got: %s", r.RelKey)
				}
			}

			if len(tt.expectedKeys) > 0 {
				gotKeys := make(map[string]bool)
				for _, r := range results {
					gotKeys[r.RelKey] = true
				}
				for _, expectedKey := range tt.expectedKeys {
					if !gotKeys[expectedKey] {
						t.Errorf("expected key %s not found", expectedKey)
					}
				}
			}
		})
	}
}

func TestEscapeLikePrefix(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"data/2025/", "data/2025/"},
		{"data%special/", `data\%special/`},
		{"data_underscore/", `data\_underscore/`},
		{`data\backslash/`, `data\\backslash/`},
		{"normal/path/", "normal/path/"},
		{`mixed%_\chars/`, `mixed\%\_\\chars/`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeLikePrefix(tt.input)
			if result != tt.expected {
				t.Errorf("escapeLikePrefix(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestQueryObjects_TimestampParseErrors(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("libsql", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	indexSetID := "timestamp-test"
	_, _ = db.ExecContext(ctx, `
		INSERT INTO index_sets (index_set_id, base_uri, provider, build_params_hash, created_at)
		VALUES (?, 's3://ts-bucket/', 's3', 'hash', datetime('now'))
	`, indexSetID)

	runID := "ts-run"
	_, _ = db.ExecContext(ctx, `
		INSERT INTO index_runs (run_id, index_set_id, started_at, acquired_at, source_type, status)
		VALUES (?, ?, datetime('now'), datetime('now'), 'crawl', 'success')
	`, runID, indexSetID)

	// Insert objects with valid and invalid timestamps
	_, _ = db.ExecContext(ctx, `
		INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at)
		VALUES (?, 'valid.txt', 100, '2025-01-01T00:00:00Z', ?, datetime('now'))
	`, indexSetID, runID)

	_, _ = db.ExecContext(ctx, `
		INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at)
		VALUES (?, 'invalid.txt', 100, 'not-a-timestamp', ?, datetime('now'))
	`, indexSetID, runID)

	_, _ = db.ExecContext(ctx, `
		INSERT INTO objects_current (index_set_id, rel_key, size_bytes, last_modified, last_seen_run_id, last_seen_at)
		VALUES (?, 'also-valid.txt', 100, '2025-01-02T00:00:00Z', ?, datetime('now'))
	`, indexSetID, runID)

	results, stats, err := QueryObjects(ctx, db, QueryParams{IndexSetID: indexSetID})
	if err != nil {
		t.Fatalf("QueryObjects: %v", err)
	}

	// Should return all 3 rows
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Should report 1 timestamp parse error
	if stats.TimestampParseErrors != 1 {
		t.Errorf("expected 1 timestamp parse error, got %d", stats.TimestampParseErrors)
	}

	// The invalid timestamp row should have nil LastModified
	for _, r := range results {
		if r.RelKey == "invalid.txt" && r.LastModified != nil {
			t.Errorf("expected nil LastModified for invalid.txt, got %v", r.LastModified)
		}
		if r.RelKey == "valid.txt" && r.LastModified == nil {
			t.Error("expected non-nil LastModified for valid.txt")
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		{
			name:      "RFC3339",
			input:     "2025-01-15T10:30:00Z",
			expectErr: false,
		},
		{
			name:      "RFC3339Nano",
			input:     "2025-01-15T10:30:00.123456789Z",
			expectErr: false,
		},
		{
			name:      "RFC3339 with offset",
			input:     "2025-01-15T10:30:00+05:00",
			expectErr: false,
		},
		{
			name:      "date only via match.ParseDate",
			input:     "2025-01-15",
			expectErr: false,
		},
		{
			name:      "invalid format",
			input:     "not-a-date",
			expectErr: true,
		},
		{
			name:      "partial timestamp",
			input:     "2025-01",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseTimestamp(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error for input %q, got nil (result: %v)", tt.input, result)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for input %q: %v", tt.input, err)
				}
				if result.IsZero() {
					t.Errorf("expected non-zero time for input %q", tt.input)
				}
			}
		})
	}
}
