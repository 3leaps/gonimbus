package manifest

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validManifestYAML returns a minimal valid manifest in YAML format.
func validManifestYAML() string {
	return `version: "1.0"
connection:
  provider: s3
  bucket: test-bucket
match:
  includes:
    - "**/*.parquet"
`
}

// validManifestJSON returns a minimal valid manifest in JSON format.
func validManifestJSON() string {
	return `{
  "version": "1.0",
  "connection": {
    "provider": "s3",
    "bucket": "test-bucket"
  },
  "match": {
    "includes": ["**/*.parquet"]
  }
}`
}

// manifestWithSchemaYAML returns a manifest with the $schema field for editor support.
func manifestWithSchemaYAML() string {
	return `$schema: https://schemas.3leaps.dev/gonimbus/v1.0.0/job-manifest.schema.json
version: "1.0"
connection:
  provider: s3
  bucket: test-bucket
match:
  includes:
    - "**/*.parquet"
`
}

// fullManifestYAML returns a complete manifest with all optional fields.
func fullManifestYAML() string {
	return `version: "1.0"
connection:
  provider: s3
  bucket: my-data-bucket
  region: us-east-1
  endpoint: https://s3.wasabisys.com
  profile: production
match:
  includes:
    - "data/2024/**/*.parquet"
    - "data/2024/**/*.csv"
  excludes:
    - "**/_temporary/**"
    - "**/.spark-*"
  include_hidden: true
crawl:
  concurrency: 8
  rate_limit: 100.5
  progress_every: 500
output:
  destination: file:/tmp/output.jsonl
  progress: false
`
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		filename    string
		wantErr     bool
		errContains string
		validate    func(t *testing.T, m *Manifest)
	}{
		{
			name:     "valid YAML manifest",
			content:  validManifestYAML(),
			filename: "manifest.yaml",
			wantErr:  false,
			validate: func(t *testing.T, m *Manifest) {
				assert.Equal(t, "1.0", m.Version)
				assert.Equal(t, "s3", m.Connection.Provider)
				assert.Equal(t, "test-bucket", m.Connection.Bucket)
				assert.Equal(t, []string{"**/*.parquet"}, m.Match.Includes)
				// Check defaults were applied
				assert.Equal(t, DefaultConcurrency, m.Crawl.Concurrency)
				assert.Equal(t, DefaultPreflightMode, m.Crawl.Preflight.Mode)
				assert.Equal(t, DefaultProbeStrategy, m.Crawl.Preflight.ProbeStrategy)
				assert.Equal(t, DefaultProbePrefix, m.Crawl.Preflight.ProbePrefix)
				assert.Equal(t, DefaultDestination, m.Output.Destination)
				assert.True(t, m.Output.ProgressEnabled())
			},
		},
		{
			name:     "valid JSON manifest",
			content:  validManifestJSON(),
			filename: "manifest.json",
			wantErr:  false,
			validate: func(t *testing.T, m *Manifest) {
				assert.Equal(t, "1.0", m.Version)
				assert.Equal(t, "s3", m.Connection.Provider)
				assert.Equal(t, "test-bucket", m.Connection.Bucket)
			},
		},
		{
			name:     "manifest with $schema field",
			content:  manifestWithSchemaYAML(),
			filename: "with-schema.yaml",
			wantErr:  false,
			validate: func(t *testing.T, m *Manifest) {
				assert.Equal(t, "https://schemas.3leaps.dev/gonimbus/v1.0.0/job-manifest.schema.json", m.Schema)
				assert.Equal(t, "1.0", m.Version)
			},
		},
		{
			name:     "full manifest with all options",
			content:  fullManifestYAML(),
			filename: "full.yaml",
			wantErr:  false,
			validate: func(t *testing.T, m *Manifest) {
				// Connection
				assert.Equal(t, "s3", m.Connection.Provider)
				assert.Equal(t, "my-data-bucket", m.Connection.Bucket)
				assert.Equal(t, "us-east-1", m.Connection.Region)
				assert.Equal(t, "https://s3.wasabisys.com", m.Connection.Endpoint)
				assert.Equal(t, "production", m.Connection.Profile)
				// Match
				assert.Equal(t, []string{"data/2024/**/*.parquet", "data/2024/**/*.csv"}, m.Match.Includes)
				assert.Equal(t, []string{"**/_temporary/**", "**/.spark-*"}, m.Match.Excludes)
				assert.True(t, m.Match.IncludeHidden)
				// Crawl
				assert.Equal(t, 8, m.Crawl.Concurrency)
				assert.InDelta(t, 100.5, m.Crawl.RateLimit, 0.001)
				assert.Equal(t, 500, m.Crawl.ProgressEvery)
				// Output
				assert.Equal(t, "file:/tmp/output.jsonl", m.Output.Destination)
				assert.False(t, m.Output.ProgressEnabled())
			},
		},
		{
			name:     "yml extension works",
			content:  validManifestYAML(),
			filename: "manifest.yml",
			wantErr:  false,
		},
		{
			name:        "empty file",
			content:     "",
			filename:    "empty.yaml",
			wantErr:     true,
			errContains: "empty",
		},
		{
			name:        "invalid YAML syntax",
			content:     "version: [invalid yaml",
			filename:    "bad.yaml",
			wantErr:     true,
			errContains: "invalid YAML",
		},
		{
			name:        "invalid JSON syntax",
			content:     `{"version": "1.0"`,
			filename:    "bad.json",
			wantErr:     true,
			errContains: "invalid JSON",
		},
		{
			name: "missing version",
			content: `connection:
  provider: s3
  bucket: test
match:
  includes:
    - "**/*"
`,
			filename:    "no-version.yaml",
			wantErr:     true,
			errContains: "version",
		},
		{
			name: "wrong version",
			content: `version: "2.0"
connection:
  provider: s3
  bucket: test
match:
  includes:
    - "**/*"
`,
			filename:    "wrong-version.yaml",
			wantErr:     true,
			errContains: "version",
		},
		{
			name: "missing connection",
			content: `version: "1.0"
match:
  includes:
    - "**/*"
`,
			filename:    "no-connection.yaml",
			wantErr:     true,
			errContains: "connection",
		},
		{
			name: "missing bucket",
			content: `version: "1.0"
connection:
  provider: s3
match:
  includes:
    - "**/*"
`,
			filename:    "no-bucket.yaml",
			wantErr:     true,
			errContains: "bucket",
		},
		{
			name: "invalid provider",
			content: `version: "1.0"
connection:
  provider: azure
  bucket: test
match:
  includes:
    - "**/*"
`,
			filename:    "bad-provider.yaml",
			wantErr:     true,
			errContains: "provider",
		},
		{
			name: "missing includes",
			content: `version: "1.0"
connection:
  provider: s3
  bucket: test
match:
  excludes:
    - "**/_temp/**"
`,
			filename:    "no-includes.yaml",
			wantErr:     true,
			errContains: "includes",
		},
		{
			name: "empty includes array",
			content: `version: "1.0"
connection:
  provider: s3
  bucket: test
match:
  includes: []
`,
			filename:    "empty-includes.yaml",
			wantErr:     true,
			errContains: "includes",
		},
		{
			name: "concurrency too high",
			content: `version: "1.0"
connection:
  provider: s3
  bucket: test
match:
  includes:
    - "**/*"
crawl:
  concurrency: 100
`,
			filename:    "high-concurrency.yaml",
			wantErr:     true,
			errContains: "concurrency",
		},
		{
			name: "concurrency too low",
			content: `version: "1.0"
connection:
  provider: s3
  bucket: test
match:
  includes:
    - "**/*"
crawl:
  concurrency: 0
`,
			filename:    "zero-concurrency.yaml",
			wantErr:     true,
			errContains: "concurrency",
		},
		{
			name: "negative rate limit",
			content: `version: "1.0"
connection:
  provider: s3
  bucket: test
match:
  includes:
    - "**/*"
crawl:
  rate_limit: -1
`,
			filename:    "neg-rate.yaml",
			wantErr:     true,
			errContains: "rate_limit",
		},
		{
			name: "unknown field rejected",
			content: `version: "1.0"
connection:
  provider: s3
  bucket: test
  unknown_field: value
match:
  includes:
    - "**/*"
`,
			filename:    "unknown-field.yaml",
			wantErr:     true,
			errContains: "additional",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, tt.filename)
			err := os.WriteFile(path, []byte(tt.content), 0o644)
			require.NoError(t, err)

			// Load manifest
			m, err := Load(path)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(tt.errContains),
						"error should contain %q", tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, m)

			if tt.validate != nil {
				tt.validate(t, m)
			}
		})
	}
}

func TestLoad_FileErrors(t *testing.T) {
	t.Run("file not found", func(t *testing.T) {
		_, err := Load("/nonexistent/path/manifest.yaml")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("permission denied", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("skipping permission test when running as root")
		}

		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "noperm.yaml")
		err := os.WriteFile(path, []byte(validManifestYAML()), 0o000)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = os.Chmod(path, 0o644) // Restore permissions for cleanup
		})

		_, err = Load(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "permission")
	})
}

func TestLoadFromBytes(t *testing.T) {
	t.Run("YAML by extension", func(t *testing.T) {
		m, err := LoadFromBytes([]byte(validManifestYAML()), "test.yaml")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket", m.Connection.Bucket)
	})

	t.Run("JSON by extension", func(t *testing.T) {
		m, err := LoadFromBytes([]byte(validManifestJSON()), "test.json")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket", m.Connection.Bucket)
	})

	t.Run("auto-detect YAML", func(t *testing.T) {
		m, err := LoadFromBytes([]byte(validManifestYAML()), "")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket", m.Connection.Bucket)
	})

	t.Run("auto-detect JSON", func(t *testing.T) {
		m, err := LoadFromBytes([]byte(validManifestJSON()), "")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket", m.Connection.Bucket)
	})

	t.Run("unknown extension tries both", func(t *testing.T) {
		m, err := LoadFromBytes([]byte(validManifestYAML()), "test.txt")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket", m.Connection.Bucket)
	})
}

func TestLoadFromReader(t *testing.T) {
	t.Run("reads from reader", func(t *testing.T) {
		r := strings.NewReader(validManifestYAML())
		m, err := LoadFromReader(r, "test.yaml")
		require.NoError(t, err)
		assert.Equal(t, "test-bucket", m.Connection.Bucket)
	})
}

func TestApplyDefaults(t *testing.T) {
	t.Run("applies all defaults", func(t *testing.T) {
		m := &Manifest{
			Version: "1.0",
			Connection: ConnectionConfig{
				Provider: "s3",
				Bucket:   "test",
			},
			Match: MatchConfig{
				Includes: []string{"**/*"},
			},
		}

		m.ApplyDefaults()

		assert.Equal(t, DefaultConcurrency, m.Crawl.Concurrency)
		assert.Equal(t, DefaultProgressEvery, m.Crawl.ProgressEvery)
		assert.Equal(t, DefaultDestination, m.Output.Destination)
		assert.NotNil(t, m.Output.Progress)
		assert.True(t, *m.Output.Progress)
	})

	t.Run("preserves explicit values", func(t *testing.T) {
		progress := false
		m := &Manifest{
			Version: "1.0",
			Crawl: CrawlConfig{
				Concurrency:   8,
				ProgressEvery: 500,
			},
			Output: OutputConfig{
				Destination: "file:/tmp/out.jsonl",
				Progress:    &progress,
			},
		}

		m.ApplyDefaults()

		assert.Equal(t, 8, m.Crawl.Concurrency)
		assert.Equal(t, 500, m.Crawl.ProgressEvery)
		assert.Equal(t, "file:/tmp/out.jsonl", m.Output.Destination)
		assert.False(t, *m.Output.Progress)
	})

	t.Run("zero rate limit is valid", func(t *testing.T) {
		m := &Manifest{
			Crawl: CrawlConfig{
				RateLimit: 0, // Explicitly unlimited
			},
		}

		m.ApplyDefaults()

		// RateLimit should remain 0 (not defaulted to something else)
		assert.Equal(t, 0.0, m.Crawl.RateLimit)
	})
}

func TestProgressEnabled(t *testing.T) {
	t.Run("nil returns default true", func(t *testing.T) {
		o := OutputConfig{}
		assert.True(t, o.ProgressEnabled())
	})

	t.Run("explicit true", func(t *testing.T) {
		v := true
		o := OutputConfig{Progress: &v}
		assert.True(t, o.ProgressEnabled())
	})

	t.Run("explicit false", func(t *testing.T) {
		v := false
		o := OutputConfig{Progress: &v}
		assert.False(t, o.ProgressEnabled())
	})
}

func TestValidationErrors(t *testing.T) {
	t.Run("single error", func(t *testing.T) {
		errs := ValidationErrors{
			{Path: "/version", Message: "required"},
		}
		assert.Contains(t, errs.Error(), "/version")
		assert.Contains(t, errs.Error(), "required")
	})

	t.Run("multiple errors", func(t *testing.T) {
		errs := ValidationErrors{
			{Path: "/version", Message: "required"},
			{Path: "/connection/bucket", Message: "must not be empty"},
		}
		errStr := errs.Error()
		assert.Contains(t, errStr, "2 errors")
		assert.Contains(t, errStr, "/version")
		assert.Contains(t, errStr, "/connection/bucket")
	})

	t.Run("empty path", func(t *testing.T) {
		errs := ValidationErrors{
			{Path: "", Message: "root error"},
		}
		assert.Equal(t, "root error", errs.Error())
	})

	t.Run("unwrap returns ErrValidationFailed", func(t *testing.T) {
		errs := ValidationErrors{{Path: "/x", Message: "bad"}}
		assert.True(t, errors.Is(errs, ErrValidationFailed))
	})
}

func TestValidate(t *testing.T) {
	t.Run("valid manifest passes", func(t *testing.T) {
		m := &Manifest{
			Version: "1.0",
			Connection: ConnectionConfig{
				Provider: "s3",
				Bucket:   "test-bucket",
			},
			Match: MatchConfig{
				Includes: []string{"**/*.parquet"},
			},
		}
		err := Validate(m)
		assert.NoError(t, err)
	})

	t.Run("invalid manifest fails", func(t *testing.T) {
		m := &Manifest{
			Version: "1.0",
			Connection: ConnectionConfig{
				Provider: "invalid-provider",
				Bucket:   "test-bucket",
			},
			Match: MatchConfig{
				Includes: []string{"**/*"},
			},
		}
		err := Validate(m)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrValidationFailed))
	})
}

func TestValidationError_Error(t *testing.T) {
	t.Run("with path", func(t *testing.T) {
		e := ValidationError{Path: "/foo/bar", Message: "invalid"}
		assert.Equal(t, "/foo/bar: invalid", e.Error())
	})

	t.Run("without path", func(t *testing.T) {
		e := ValidationError{Path: "", Message: "something wrong"}
		assert.Equal(t, "something wrong", e.Error())
	})
}

func TestValidate_EmbeddedSchema(t *testing.T) {
	// This test verifies that validation works from any directory,
	// proving the embedded schema is being used (not disk-based lookup).
	t.Run("works from arbitrary directory", func(t *testing.T) {
		// Save current directory
		originalDir, err := os.Getwd()
		require.NoError(t, err)

		// Change to a temporary directory (outside repo)
		tmpDir := t.TempDir()
		err = os.Chdir(tmpDir)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = os.Chdir(originalDir)
		})

		// Validation should still work because schema is embedded
		m := &Manifest{
			Version: "1.0",
			Connection: ConnectionConfig{
				Provider: "s3",
				Bucket:   "test-bucket",
			},
			Match: MatchConfig{
				Includes: []string{"**/*.parquet"},
			},
		}
		err = Validate(m)
		assert.NoError(t, err, "validation should work from any directory using embedded schema")
	})

	t.Run("validation errors work from arbitrary directory", func(t *testing.T) {
		// Save current directory
		originalDir, err := os.Getwd()
		require.NoError(t, err)

		// Change to a temporary directory (outside repo)
		tmpDir := t.TempDir()
		err = os.Chdir(tmpDir)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = os.Chdir(originalDir)
		})

		// Invalid manifest should still be caught
		m := &Manifest{
			Version: "1.0",
			Connection: ConnectionConfig{
				Provider: "invalid-provider", // Not in enum
				Bucket:   "test-bucket",
			},
			Match: MatchConfig{
				Includes: []string{"**/*.parquet"},
			},
		}
		err = Validate(m)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrValidationFailed))
	})
}
