package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// mockProvider implements provider.Provider for testing.
type mockProvider struct {
	// headCalls tracks calls to Head with the key argument
	headCalls []string
	// listCalls tracks calls to List with the prefix argument
	listCalls []string

	// headResult is returned by Head
	headResult *provider.ObjectMeta
	// headErr is returned by Head if set
	headErr error

	// listResult is returned by List
	listResult *provider.ListResult
	// listErr is returned by List if set
	listErr error
}

func (m *mockProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	m.headCalls = append(m.headCalls, key)
	if m.headErr != nil {
		return nil, m.headErr
	}
	return m.headResult, nil
}

func (m *mockProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	m.listCalls = append(m.listCalls, opts.Prefix)
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.listResult, nil
}

func (m *mockProvider) Close() error {
	return nil
}

func TestListObjects_UsesHeadForExactKey(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name         string
		uri          *ObjectURI
		headResult   *provider.ObjectMeta
		listResult   *provider.ListResult
		wantHeadCall bool
		wantListCall bool
		wantKey      string // expected key passed to Head
		wantPrefix   string // expected prefix passed to List
		wantCount    int    // expected number of objects returned
	}{
		{
			name: "exact key uses Head",
			uri: &ObjectURI{
				Provider: "s3",
				Bucket:   "bucket",
				Key:      "path/to/file.txt",
			},
			headResult: &provider.ObjectMeta{
				ObjectSummary: provider.ObjectSummary{
					Key:          "path/to/file.txt",
					Size:         1024,
					LastModified: now,
				},
			},
			wantHeadCall: true,
			wantListCall: false,
			wantKey:      "path/to/file.txt",
			wantCount:    1,
		},
		{
			name: "prefix uses List",
			uri: &ObjectURI{
				Provider: "s3",
				Bucket:   "bucket",
				Key:      "path/to/",
			},
			listResult: &provider.ListResult{
				Objects: []provider.ObjectSummary{
					{Key: "path/to/file1.txt", Size: 100, LastModified: now},
					{Key: "path/to/file2.txt", Size: 200, LastModified: now},
				},
				IsTruncated: false,
			},
			wantHeadCall: false,
			wantListCall: true,
			wantPrefix:   "path/to/",
			wantCount:    2,
		},
		{
			name: "empty key (bucket root) uses List",
			uri: &ObjectURI{
				Provider: "s3",
				Bucket:   "bucket",
				Key:      "",
			},
			listResult: &provider.ListResult{
				Objects: []provider.ObjectSummary{
					{Key: "file.txt", Size: 100, LastModified: now},
				},
				IsTruncated: false,
			},
			wantHeadCall: false,
			wantListCall: true,
			wantPrefix:   "",
			wantCount:    1,
		},
		{
			name: "glob pattern uses List with derived prefix",
			uri: &ObjectURI{
				Provider: "s3",
				Bucket:   "bucket",
				Key:      "data/2024/",
				Pattern:  "data/2024/**/*.parquet",
			},
			listResult: &provider.ListResult{
				Objects: []provider.ObjectSummary{
					{Key: "data/2024/01/file.parquet", Size: 100, LastModified: now},
					{Key: "data/2024/01/file.csv", Size: 200, LastModified: now}, // won't match pattern
				},
				IsTruncated: false,
			},
			wantHeadCall: false,
			wantListCall: true,
			wantPrefix:   "data/2024/",
			wantCount:    1, // only .parquet matches
		},
		{
			name: "unescaped key with literal asterisk uses Head",
			uri: &ObjectURI{
				Provider: "s3",
				Bucket:   "bucket",
				Key:      "data/file*.txt", // already unescaped by ParseURI
			},
			headResult: &provider.ObjectMeta{
				ObjectSummary: provider.ObjectSummary{
					Key:          "data/file*.txt",
					Size:         512,
					LastModified: now,
				},
			},
			wantHeadCall: true,
			wantListCall: false,
			wantKey:      "data/file*.txt",
			wantCount:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockProvider{
				headResult: tt.headResult,
				listResult: tt.listResult,
			}

			// Save and restore inspectLimit
			oldLimit := inspectLimit
			inspectLimit = 100
			defer func() { inspectLimit = oldLimit }()

			objects, err := listObjects(context.Background(), mock, tt.uri)
			require.NoError(t, err)

			// Verify correct method was called
			if tt.wantHeadCall {
				require.Len(t, mock.headCalls, 1, "expected Head to be called")
				assert.Equal(t, tt.wantKey, mock.headCalls[0])
				assert.Empty(t, mock.listCalls, "expected List not to be called")
			}
			if tt.wantListCall {
				require.Len(t, mock.listCalls, 1, "expected List to be called")
				assert.Equal(t, tt.wantPrefix, mock.listCalls[0])
				assert.Empty(t, mock.headCalls, "expected Head not to be called")
			}

			// Verify result count
			assert.Len(t, objects, tt.wantCount)
		})
	}
}
