package indexbuild

type Summary struct {
	IndexSetID      string
	RunID           string
	JournalPaths    []string
	ObjectsObserved int64
	PrefixesCrawled []string
	// ManifestSHA256 is the digest written into the durable complete marker at
	// publish time. Prefer this over re-hashing the manifest path after commit.
	ManifestSHA256 string
	Manifest       ManifestSummary
	// PeakWorkspaceBytes is the high-water live on-disk spill workspace observed
	// during the durable merge (0 when nothing spilled or on the SQLite path).
	// Observational capacity evidence for sizing successive builds.
	PeakWorkspaceBytes int64
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
