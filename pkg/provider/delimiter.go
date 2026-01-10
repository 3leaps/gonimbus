package provider

import "context"

// DelimiterLister supports delimiter-based listing.
//
// This is used for safe, directory-like operations (tree/du) and prefix discovery.
// Delimiter listing returns:
//   - Objects directly under Prefix (no nested delimiter in the remainder)
//   - CommonPrefixes (immediate child prefixes)
//
// Implementations should map to provider-native delimiter listing when available
// (e.g., S3 ListObjectsV2 with Delimiter).
type DelimiterLister interface {
	ListWithDelimiter(ctx context.Context, opts ListWithDelimiterOptions) (*ListWithDelimiterResult, error)
}

// ListWithDelimiterOptions configures a delimiter listing operation.
type ListWithDelimiterOptions struct {
	// Prefix filters results to keys starting with this value.
	Prefix string

	// Delimiter groups keys (e.g., "/").
	Delimiter string

	// ContinuationToken resumes listing from a previous ListWithDelimiterResult.
	ContinuationToken string

	// MaxKeys limits the number of keys returned per page.
	MaxKeys int
}

// ListWithDelimiterResult contains a page of results from a delimiter listing.
type ListWithDelimiterResult struct {
	// Objects are object summaries directly under the requested Prefix.
	Objects []ObjectSummary

	// CommonPrefixes are the immediate child prefixes.
	CommonPrefixes []string

	// ContinuationToken is used to retrieve the next page.
	ContinuationToken string

	// IsTruncated indicates whether more results are available.
	IsTruncated bool
}
