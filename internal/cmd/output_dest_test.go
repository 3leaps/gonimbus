package cmd

import (
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func TestParseOutputDest(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		wantErr  bool
		provider string
		bucket   string
		key      string
		baseDir  string
	}{
		{
			name:     "s3 simple key",
			uri:      "s3://bucket/key.jsonl",
			provider: string(provider.ProviderS3),
			bucket:   "bucket",
			key:      "key.jsonl",
		},
		{
			name:     "s3 deep path",
			uri:      "s3://bucket/deep/path/file.jsonl",
			provider: string(provider.ProviderS3),
			bucket:   "bucket",
			key:      "deep/path/file.jsonl",
		},
		{
			name:     "file absolute path",
			uri:      "file:///tmp/out.jsonl",
			provider: string(provider.ProviderFile),
			key:      "out.jsonl",
			baseDir:  "/tmp",
		},
		{
			name:     "file nested path",
			uri:      "file:///home/user/data/output.jsonl",
			provider: string(provider.ProviderFile),
			key:      "output.jsonl",
			baseDir:  "/home/user/data",
		},
		{
			name:    "empty uri",
			uri:     "",
			wantErr: true,
		},
		{
			name:    "unsupported scheme",
			uri:     "gcs://bucket/key.jsonl",
			wantErr: true,
		},
		{
			name:    "s3 missing bucket",
			uri:     "s3://",
			wantErr: true,
		},
		{
			name:    "s3 missing key (bucket only)",
			uri:     "s3://bucket",
			wantErr: true,
		},
		{
			name:    "s3 trailing slash (prefix not key)",
			uri:     "s3://bucket/prefix/",
			wantErr: true,
		},
		{
			name:    "s3 bucket with trailing slash only",
			uri:     "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "file empty path",
			uri:     "file://",
			wantErr: true,
		},
		{
			name:    "file directory only",
			uri:     "file:///tmp/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOutputDest(tt.uri)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (result: %+v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Provider != tt.provider {
				t.Errorf("provider = %q, want %q", got.Provider, tt.provider)
			}
			if got.Bucket != tt.bucket {
				t.Errorf("bucket = %q, want %q", got.Bucket, tt.bucket)
			}
			if got.Key != tt.key {
				t.Errorf("key = %q, want %q", got.Key, tt.key)
			}
			if got.BaseDir != tt.baseDir {
				t.Errorf("baseDir = %q, want %q", got.BaseDir, tt.baseDir)
			}
		})
	}
}
