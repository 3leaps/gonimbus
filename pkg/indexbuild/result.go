package indexbuild

type Summary struct {
	IndexSetID      string
	RunID           string
	JournalPaths    []string
	ObjectsObserved int64
	PrefixesCrawled []string
	Manifest        ManifestSummary
}

type ManifestSummary struct {
	Rows          int
	ActiveRows    int
	Tombstones    int
	DistinctETags int
	Segments      []SegmentSummary
}

type SegmentSummary struct {
	SegmentID  string
	Path       string
	Rows       int
	Tombstones int
	Digest     Digest
}

type Digest struct {
	Algorithm string
	Hex       string
}
