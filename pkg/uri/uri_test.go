package uri

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantErr     error
		errContains string
		want        *ObjectURI
	}{
		{
			name: "simple bucket",
			uri:  "s3://my-bucket",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "",
			},
		},
		{
			name: "bucket with trailing slash",
			uri:  "s3://my-bucket/",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "",
			},
		},
		{
			name: "bucket with key",
			uri:  "s3://my-bucket/path/to/object.txt",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "path/to/object.txt",
			},
		},
		{
			name: "bucket with prefix",
			uri:  "s3://my-bucket/path/to/prefix/",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "path/to/prefix/",
			},
		},
		{
			name: "bucket with glob pattern",
			uri:  "s3://my-bucket/data/2024/**/*.parquet",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "data/2024/",
				Pattern:  "data/2024/**/*.parquet",
			},
		},
		{
			name: "bucket with star pattern at root",
			uri:  "s3://my-bucket/*.txt",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "",
				Pattern:  "*.txt",
			},
		},
		{
			name: "bucket with question mark pattern",
			uri:  "s3://my-bucket/data/file?.csv",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "data/",
				Pattern:  "data/file?.csv",
			},
		},
		{
			name: "bucket with bracket pattern",
			uri:  "s3://my-bucket/data/file[0-9].csv",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "data/",
				Pattern:  "data/file[0-9].csv",
			},
		},
		{
			name: "bucket with brace pattern",
			uri:  "s3://my-bucket/data/{a,b,c}.csv",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "data/",
				Pattern:  "data/{a,b,c}.csv",
			},
		},
		{
			name: "uppercase S3 scheme",
			uri:  "S3://my-bucket/path",
			want: &ObjectURI{
				Provider: "s3",
				Bucket:   "my-bucket",
				Key:      "path",
			},
		},
		{
			name: "file absolute path",
			uri:  "file:///foo/bar",
			want: &ObjectURI{
				Provider: "file",
				Bucket:   "local",
				Key:      "/foo/bar",
			},
		},
		{
			name: "file apple absolute path",
			uri:  "file:///Users/operator/tree",
			want: &ObjectURI{
				Provider: "file",
				Bucket:   "local",
				Key:      "/Users/operator/tree",
			},
		},
		{
			name: "file absolute prefix preserves trailing slash",
			uri:  "file:///foo/bar/",
			want: &ObjectURI{
				Provider: "file",
				Bucket:   "local",
				Key:      "/foo/bar/",
			},
		},
		{
			name:        "empty URI",
			uri:         "",
			wantErr:     ErrInvalidURI,
			errContains: "empty",
		},
		{
			name:        "missing scheme",
			uri:         "my-bucket/path",
			wantErr:     ErrInvalidURI,
			errContains: "missing scheme",
		},
		{
			name:        "unsupported scheme",
			uri:         "gcs://my-bucket/path",
			wantErr:     ErrUnsupportedProvider,
			errContains: "gcs",
		},
		{
			name:        "file relative path rejected",
			uri:         "file://relative/path",
			wantErr:     ErrInvalidFileURI,
			errContains: "absolute local path",
		},
		{
			name:        "file remote host rejected",
			uri:         "file://host.example/path",
			wantErr:     ErrInvalidFileURI,
			errContains: "absolute local path",
		},
		{
			name:        "file local audit form rejected",
			uri:         "file://local/path",
			wantErr:     ErrInvalidFileURI,
			errContains: "absolute local path",
		},
		{
			name:        "missing bucket",
			uri:         "s3:///path",
			wantErr:     ErrMissingBucket,
			errContains: "missing bucket",
		},
		{
			name:        "http scheme not supported",
			uri:         "http://example.com/bucket",
			wantErr:     ErrUnsupportedProvider,
			errContains: "http",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.uri)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr), "expected %v, got %v", tt.wantErr, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.want.Provider, got.Provider)
			assert.Equal(t, tt.want.Bucket, got.Bucket)
			assert.Equal(t, tt.want.Key, got.Key)
			assert.Equal(t, tt.want.Pattern, got.Pattern)
		})
	}
}

func TestObjectURI_String(t *testing.T) {
	tests := []struct {
		name string
		uri  *ObjectURI
		want string
	}{
		{
			name: "bucket only",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket"},
			want: "s3://bucket/",
		},
		{
			name: "bucket with key",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: "path/to/file.txt"},
			want: "s3://bucket/path/to/file.txt",
		},
		{
			name: "bucket with pattern",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: "data/", Pattern: "data/**/*.csv"},
			want: "s3://bucket/data/**/*.csv",
		},
		{
			name: "file absolute path",
			uri:  &ObjectURI{Provider: "file", Bucket: "local", Key: "/foo/bar"},
			want: "file:///foo/bar",
		},
		{
			name: "file absolute prefix",
			uri:  &ObjectURI{Provider: "file", Bucket: "local", Key: "/foo/bar/"},
			want: "file:///foo/bar/",
		},
		{
			name: "file root",
			uri:  &ObjectURI{Provider: "file", Bucket: "local", Key: "/"},
			want: "file:///",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.uri.String()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestObjectURI_IsPattern(t *testing.T) {
	tests := []struct {
		name string
		uri  *ObjectURI
		want bool
	}{
		{
			name: "no pattern",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: "path/"},
			want: false,
		},
		{
			name: "with pattern",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: "data/", Pattern: "data/**/*.csv"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.uri.IsPattern()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestObjectURI_IsPrefix(t *testing.T) {
	tests := []struct {
		name string
		uri  *ObjectURI
		want bool
	}{
		{
			name: "empty key is prefix",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: ""},
			want: true,
		},
		{
			name: "trailing slash is prefix",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: "path/"},
			want: true,
		},
		{
			name: "no trailing slash is not prefix",
			uri:  &ObjectURI{Provider: "s3", Bucket: "bucket", Key: "path/file.txt"},
			want: false,
		},
		{
			name: "file trailing slash is prefix",
			uri:  &ObjectURI{Provider: "file", Bucket: "local", Key: "/tmp/source/"},
			want: true,
		},
		{
			name: "file path without trailing slash is not prefix",
			uri:  &ObjectURI{Provider: "file", Bucket: "local", Key: "/tmp/source/file.txt"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.uri.IsPrefix()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseURI_FileRoundTrip(t *testing.T) {
	got, err := ParseURI("file:///foo/bar")
	require.NoError(t, err)
	require.Equal(t, "file", got.Provider)
	require.Equal(t, "local", got.Bucket)
	require.Equal(t, "/foo/bar", got.Key)
	require.Equal(t, "file:///foo/bar", got.String())
}

func TestParseURI_FilePrefixRoundTrip(t *testing.T) {
	got, err := ParseURI("file:///foo/bar/")
	require.NoError(t, err)
	require.True(t, got.IsPrefix())
	require.Equal(t, "/foo/bar/", got.Key)
	require.Equal(t, "file:///foo/bar/", got.String())
}

func TestParseURI_EscapeAware(t *testing.T) {
	// These tests verify escape-aware glob detection and unescaping
	tests := []struct {
		name    string
		uri     string
		wantKey string
		wantPat string
	}{
		{
			name:    "escaped asterisk is literal - unescaped for S3",
			uri:     `s3://bucket/data/file\*.txt`,
			wantKey: "data/file*.txt", // unescaped for S3 key lookup
			wantPat: "",               // not a pattern
		},
		{
			name:    "escaped question mark is literal - unescaped for S3",
			uri:     `s3://bucket/data/file\?.txt`,
			wantKey: "data/file?.txt", // unescaped for S3 key lookup
			wantPat: "",               // not a pattern
		},
		{
			name:    "escaped brackets are literal - unescaped for S3",
			uri:     `s3://bucket/data/\[backup\]/file.txt`,
			wantKey: "data/[backup]/file.txt", // unescaped for S3 key lookup
			wantPat: "",                       // not a pattern
		},
		{
			name:    "mixed escaped and unescaped glob",
			uri:     `s3://bucket/data/file\*/*.txt`,
			wantKey: "data/file*/", // prefix up to unescaped glob (unescaped by DerivePrefix)
			wantPat: `data/file\*/*.txt`,
		},
		{
			name:    "unescaped glob detected",
			uri:     "s3://bucket/data/**/*.parquet",
			wantKey: "data/",
			wantPat: "data/**/*.parquet",
		},
		{
			name:    "no escapes no glob - unchanged",
			uri:     "s3://bucket/data/file.txt",
			wantKey: "data/file.txt",
			wantPat: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.uri)
			require.NoError(t, err)
			assert.Equal(t, tt.wantKey, got.Key)
			assert.Equal(t, tt.wantPat, got.Pattern)
		})
	}
}
