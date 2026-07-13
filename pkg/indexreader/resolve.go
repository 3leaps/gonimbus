package indexreader

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var validHexPattern = regexp.MustCompile(`^[0-9a-f]{1,64}$`)

// indexSetHexMatches reports whether a stored hex id matches a user-provided
// prefix lookup or a full-hash reverse prefix match against a truncated dir name.
func indexSetHexMatches(storedHex, wantHex string) bool {
	if strings.HasPrefix(storedHex, wantHex) {
		return true
	}
	return len(wantHex) == 64 && strings.HasPrefix(wantHex, storedHex)
}

// ResolveIndexReader opens a format-aware reader for the target.
//
// Dispatch is marker-authoritative:
//   - index.db present → sqlite-v1 (preferred when both formats exist)
//   - else durable latest+complete+manifest trust chain → durable-v2
//   - absent/unknown/malformed markers → reject (no layout guessing)
//
// When target.RunID is set, dispatch is pin-mode: durable-v2 only, via the run
// complete marker. latest.json is never consulted (receipt-pinned snapshot).
func ResolveIndexReader(ctx context.Context, opts ResolveOptions, target ResolveTarget) (Reader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeResolveOptions(opts)
	target.BaseURI = strings.TrimSpace(target.BaseURI)
	target.IndexSetID = strings.TrimSpace(target.IndexSetID)
	target.RunID = strings.TrimSpace(target.RunID)
	// Pin mode is explicit: run_id without index_set_id is a hard reject even
	// when base_uri is also absent (clearer than the generic required-field error).
	if target.RunID != "" {
		if target.IndexSetID == "" {
			return nil, fmt.Errorf("run_id requires index_set_id")
		}
		return openPinnedDurableRun(opts, target)
	}
	if target.BaseURI == "" && target.IndexSetID == "" {
		return nil, fmt.Errorf("base_uri or index_set_id is required")
	}

	candidates, err := discoverCandidates(ctx, opts, target)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		if target.IndexSetID != "" {
			return nil, fmt.Errorf("no index found matching ID: %s", target.IndexSetID)
		}
		return nil, fmt.Errorf("no index found for base URI: %s", target.BaseURI)
	}
	if len(candidates) > 1 {
		// Prefer exact base URI matches that share the same index set when ambiguous.
		best := selectPreferredCandidate(candidates)
		if best == nil {
			ids := make([]string, 0, len(candidates))
			for _, c := range candidates {
				ids = append(ids, c.meta.IndexSetID+"/"+string(c.meta.Format))
			}
			return nil, fmt.Errorf("ambiguous index target matches %d candidates: %s", len(candidates), strings.Join(ids, ", "))
		}
		return openCandidate(ctx, opts, *best)
	}
	return openCandidate(ctx, opts, candidates[0])
}

// ListIndexReaders enumerates local indexes under the configured roots.
// SQLite entries require index.db; durable entries require a validated latest pointer.
func ListIndexReaders(ctx context.Context, opts ResolveOptions) ([]ListedIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeResolveOptions(opts)
	candidates, err := discoverCandidates(ctx, opts, ResolveTarget{})
	if err != nil {
		return nil, err
	}
	out := make([]ListedIndex, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, c := range candidates {
		key := c.meta.IndexSetID + "|" + string(c.meta.Format) + "|" + c.meta.SourcePath
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ListedIndex{
			Meta:               c.meta,
			IdentityStatus:     c.identityStatus,
			IdentityDiagnostic: c.identityDiagnostic,
		})
	}
	return out, nil
}

type candidate struct {
	meta               Meta
	dbPath             string
	latest             string
	identityStatus     IdentityStatus
	identityDiagnostic string
}

func discoverCandidates(ctx context.Context, opts ResolveOptions, target ResolveTarget) ([]candidate, error) {
	var out []candidate

	if opts.IndexesRoot != "" {
		fromIndexes, err := discoverIndexRootCandidates(ctx, opts, target)
		if err != nil {
			return nil, err
		}
		out = append(out, fromIndexes...)
	}
	// Durable-only discovery via segment cache when no identity dir, or for ID lookup.
	if opts.SegmentCacheRoot != "" {
		fromSegments, err := discoverSegmentCacheCandidates(opts, target)
		if err != nil {
			return nil, err
		}
		out = append(out, fromSegments...)
	}
	return dedupeCandidates(out), nil
}

func discoverIndexRootCandidates(ctx context.Context, opts ResolveOptions, target ResolveTarget) ([]candidate, error) {
	entries, err := os.ReadDir(opts.IndexesRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read indexes root: %w", err)
	}

	wantID := strings.TrimPrefix(target.IndexSetID, "idx_")
	var out []candidate
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirName := entry.Name()
		dirHex := strings.TrimPrefix(dirName, "idx_")
		if target.IndexSetID != "" {
			if !validHexPattern.MatchString(wantID) {
				return nil, fmt.Errorf("invalid index set ID: %s (must be hex characters, max 64)", target.IndexSetID)
			}
			if !indexSetHexMatches(dirHex, wantID) {
				continue
			}
		}

		dirPath := filepath.Join(opts.IndexesRoot, dirName)
		identity, identityErr := readIdentityMeta(dirPath, opts.MaxMarkerBytes)
		if identityErr != nil {
			// Canonical SQLite must not downgrade to an uncoordinated immutable
			// probe when its stable authority identity is unavailable.
			identity = identityMeta{}
		}
		if target.BaseURI != "" && identity.BaseURI != "" && identity.BaseURI != target.BaseURI {
			continue
		}

		dbPath := filepath.Join(dirPath, "index.db")
		if st, statErr := os.Stat(dbPath); statErr == nil && !st.IsDir() {
			if identityErr != nil {
				if target.IndexSetID != "" || target.BaseURI != "" {
					return nil, fmt.Errorf("open canonical SQLite candidate %s: canonical SQLite identity requires a valid full index_set_id: %w", dirName, identityErr)
				}
				status := IdentityStatusInvalid
				diagnostic := "identity.json is unreadable or invalid"
				if os.IsNotExist(identityErr) {
					status = IdentityStatusMissing
					diagnostic = "identity.json is missing"
				}
				out = append(out, untrustedSQLiteCandidate(dbPath, dirPath, status, diagnostic))
				continue
			}
			if !identityMatchesCanonicalDir(identity.IndexSetID, dirName) {
				if target.IndexSetID != "" || target.BaseURI != "" {
					return nil, fmt.Errorf("open canonical SQLite candidate %s: identity.json does not match canonical directory", dirName)
				}
				out = append(out, untrustedSQLiteCandidate(dbPath, dirPath, IdentityStatusMismatch, "identity.json does not match canonical directory"))
				continue
			}
			c, err := candidateFromSQLite(ctx, opts, dbPath, dirPath, target, identity)
			if err != nil {
				if target.IndexSetID != "" || target.BaseURI != "" {
					return nil, fmt.Errorf("open canonical SQLite candidate %s: %w", dirName, err)
				}
				if errors.Is(err, ErrSQLiteIdentityScope) {
					out = append(out, untrustedSQLiteCandidate(dbPath, dirPath, IdentityStatusMismatch, "identity.json does not match the database index set"))
				}
				continue
			}
			if identity.BaseURI != "" {
				c.meta.BaseURI = identity.BaseURI
			}
			if identity.Provider != "" {
				c.meta.Provider = identity.Provider
			}
			out = append(out, c)
			continue
		}

		// Durable-only identity dir: resolve via segment cache using full ID from identity.
		fullID := identity.IndexSetID
		if fullID == "" && len(dirHex) == 64 {
			fullID = "idx_" + dirHex
		}
		if fullID == "" && opts.SegmentCacheRoot != "" && dirHex != "" {
			// Dir names are typically 16-hex prefixes; resolve unique segment-cache match.
			matched, matchErr := matchSegmentCacheID(opts.SegmentCacheRoot, dirHex)
			if matchErr != nil {
				continue
			}
			fullID = matched
		}
		if fullID == "" {
			continue
		}
		if target.BaseURI != "" && identity.BaseURI == "" {
			// Cannot confirm base URI without identity or SQLite.
			continue
		}
		latest := filepath.Join(opts.SegmentCacheRoot, fullID, "latest.json")
		c, err := candidateFromDurable(opts, latest, dirPath, identity)
		if err != nil {
			continue
		}
		if target.BaseURI != "" && c.meta.BaseURI != "" && c.meta.BaseURI != target.BaseURI {
			continue
		}
		if target.IndexSetID != "" {
			setHex := strings.TrimPrefix(c.meta.IndexSetID, "idx_")
			if !indexSetHexMatches(setHex, wantID) {
				continue
			}
		}
		out = append(out, c)
	}
	return out, nil
}

func untrustedSQLiteCandidate(dbPath, identityDir string, status IdentityStatus, diagnostic string) candidate {
	return candidate{
		meta: Meta{
			Format:      FormatSQLiteV1,
			IdentityDir: identityDir,
			SourcePath:  dbPath,
		},
		dbPath:             dbPath,
		identityStatus:     status,
		identityDiagnostic: diagnostic,
	}
}

func identityMatchesCanonicalDir(indexSetID, dirName string) bool {
	if !isFullIndexSetID(indexSetID) || !strings.HasPrefix(dirName, "idx_") {
		return false
	}
	fullHex := strings.TrimPrefix(indexSetID, "idx_")
	dirHex := strings.TrimPrefix(dirName, "idx_")
	return dirHex != "" && len(dirHex) <= len(fullHex) && strings.HasPrefix(fullHex, dirHex)
}

func discoverSegmentCacheCandidates(opts ResolveOptions, target ResolveTarget) ([]candidate, error) {
	if target.BaseURI != "" && target.IndexSetID == "" {
		// Base-URI selection requires identity.json (or SQLite); segment cache alone
		// has no base_uri. Skip bare segment enumeration for base-uri targets.
		return nil, nil
	}
	if target.IndexSetID == "" {
		// List mode: enumerate durable snapshots with valid latest markers.
		entries, err := os.ReadDir(opts.SegmentCacheRoot)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("read segment cache root: %w", err)
		}
		var out []candidate
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			latest := filepath.Join(opts.SegmentCacheRoot, entry.Name(), "latest.json")
			c, err := candidateFromDurable(opts, latest, "", identityMeta{IndexSetID: entry.Name()})
			if err != nil {
				continue
			}
			out = append(out, c)
		}
		return out, nil
	}

	wantID := strings.TrimPrefix(target.IndexSetID, "idx_")
	if !validHexPattern.MatchString(wantID) {
		return nil, fmt.Errorf("invalid index set ID: %s (must be hex characters, max 64)", target.IndexSetID)
	}
	fullID, err := matchSegmentCacheID(opts.SegmentCacheRoot, wantID)
	if err != nil {
		return nil, nil
	}
	latest := filepath.Join(opts.SegmentCacheRoot, fullID, "latest.json")
	c, err := candidateFromDurable(opts, latest, "", identityMeta{IndexSetID: fullID})
	if err != nil {
		// Explicit target: surface marker/trust failures rather than "not found".
		return nil, fmt.Errorf("open durable snapshot for %s: %w", fullID, err)
	}
	return []candidate{c}, nil
}

func matchSegmentCacheID(segmentRoot, wantHex string) (string, error) {
	entries, err := os.ReadDir(segmentRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("segment cache not found")
		}
		return "", err
	}
	var matches []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirHex := strings.TrimPrefix(entry.Name(), "idx_")
		if strings.HasPrefix(dirHex, wantHex) || (len(wantHex) == 64 && strings.HasPrefix(wantHex, dirHex)) {
			if _, statErr := os.Stat(filepath.Join(segmentRoot, entry.Name(), "latest.json")); statErr == nil {
				matches = append(matches, entry.Name())
			}
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no durable snapshot matching %s", wantHex)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("ambiguous durable index set matches: %s", strings.Join(matches, ", "))
	}
	return matches[0], nil
}

func candidateFromSQLite(ctx context.Context, opts ResolveOptions, dbPath, identityDir string, target ResolveTarget, identity identityMeta) (candidate, error) {
	indexSetID := strings.TrimSpace(identity.IndexSetID)
	if !isFullIndexSetID(indexSetID) {
		return candidate{}, fmt.Errorf("canonical SQLite identity requires a valid full index_set_id")
	}
	if opts.SegmentCacheRoot == "" {
		return candidate{}, fmt.Errorf("segment cache root is required for canonical SQLite authority")
	}
	snapshot, err := OpenSQLiteSnapshot(ctx, SQLiteSnapshotOptions{
		Path:           dbPath,
		SegmentSetRoot: filepath.Join(opts.SegmentCacheRoot, indexSetID),
		IndexSetID:     indexSetID,
		Authority:      opts.Authority,
	})
	if err != nil {
		return candidate{}, err
	}
	db := snapshot.DB()
	set, err := selectSQLiteSet(ctx, db, target)
	closeErr := snapshot.Close()
	if err != nil {
		return candidate{}, err
	}
	if closeErr != nil {
		return candidate{}, closeErr
	}
	return candidate{
		meta: Meta{
			Format:      FormatSQLiteV1,
			IndexSetID:  set.IndexSetID,
			BaseURI:     set.BaseURI,
			Provider:    set.Provider,
			IdentityDir: identityDir,
			SourcePath:  dbPath,
		},
		dbPath:         dbPath,
		identityStatus: IdentityStatusOK,
	}, nil
}

func selectSQLiteSet(ctx context.Context, db *sql.DB, target ResolveTarget) (*indexstore.IndexSet, error) {
	var set *indexstore.IndexSet
	var err error
	if target.BaseURI != "" {
		set, err = indexstore.GetIndexSetByBaseURI(ctx, db, target.BaseURI)
		if err != nil {
			return nil, err
		}
		if set == nil {
			return nil, fmt.Errorf("base_uri not in db")
		}
	} else {
		sets, listErr := indexstore.ListIndexSets(ctx, db, "")
		if listErr != nil {
			return nil, listErr
		}
		if len(sets) == 0 {
			return nil, fmt.Errorf("no index sets")
		}
		if target.IndexSetID != "" {
			want := strings.TrimPrefix(target.IndexSetID, "idx_")
			for i := range sets {
				hexID := strings.TrimPrefix(sets[i].IndexSetID, "idx_")
				if strings.HasPrefix(hexID, want) || (len(want) == 64 && strings.HasPrefix(want, hexID)) {
					set = &sets[i]
					break
				}
			}
			if set == nil {
				set = &sets[0]
			}
		} else {
			set = &sets[0]
		}
	}

	return set, nil
}

func isFullIndexSetID(id string) bool {
	hexID := strings.TrimPrefix(strings.TrimSpace(id), "idx_")
	return strings.HasPrefix(strings.TrimSpace(id), "idx_") && len(hexID) == 64 && validHexPattern.MatchString(hexID)
}

func candidateFromDurable(opts ResolveOptions, latestPath, identityDir string, identity identityMeta) (candidate, error) {
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(latestPath, opts.MaxMarkerBytes, opts.MaxManifestBytes)
	if err != nil {
		return candidate{}, err
	}
	indexSetID := snap.Manifest.IndexSetID
	if identity.IndexSetID != "" && identity.IndexSetID != indexSetID {
		// Allow short identity dir names; require full id match when both full.
		idHex := strings.TrimPrefix(identity.IndexSetID, "idx_")
		setHex := strings.TrimPrefix(indexSetID, "idx_")
		if len(idHex) == 64 && idHex != setHex {
			return candidate{}, fmt.Errorf("identity and durable snapshot index_set_id disagree")
		}
		if len(idHex) < 64 && !strings.HasPrefix(setHex, idHex) {
			return candidate{}, fmt.Errorf("identity and durable snapshot index_set_id disagree")
		}
	}
	baseURI := identity.BaseURI
	provider := identity.Provider
	return candidate{
		meta: Meta{
			Format:      FormatDurableV2,
			IndexSetID:  indexSetID,
			BaseURI:     baseURI,
			Provider:    provider,
			IdentityDir: identityDir,
			SourcePath:  latestPath,
			RunID:       snap.Manifest.RunID,
		},
		latest: latestPath,
	}, nil
}

type identityMeta struct {
	IndexSetID string
	BaseURI    string
	Provider   string
}

func readIdentityMeta(dir string, maxBytes int64) (identityMeta, error) {
	path := filepath.Join(dir, "identity.json")
	file, err := ReadLocalIdentityFile(path, maxBytes)
	if err != nil {
		return identityMeta{}, err
	}
	return identityMeta{
		IndexSetID: file.IndexSetID,
		BaseURI:    file.Payload.BaseURI,
		Provider:   file.Payload.Provider,
	}, nil
}

func openCandidate(ctx context.Context, opts ResolveOptions, c candidate) (Reader, error) {
	switch c.meta.Format {
	case FormatSQLiteV1:
		return openSQLiteReader(ctx, opts, c)
	case FormatDurableV2:
		return openDurableReader(opts, c)
	default:
		return nil, fmt.Errorf("unsupported index format %q", c.meta.Format)
	}
}

func selectPreferredCandidate(candidates []candidate) *candidate {
	// Prefer sqlite when both formats for the same set are present.
	bySet := map[string][]candidate{}
	for _, c := range candidates {
		bySet[c.meta.IndexSetID] = append(bySet[c.meta.IndexSetID], c)
	}
	if len(bySet) != 1 {
		return nil
	}
	var only []candidate
	for _, list := range bySet {
		only = list
	}
	for i := range only {
		if only[i].meta.Format == FormatSQLiteV1 {
			return &only[i]
		}
	}
	if len(only) == 1 {
		return &only[0]
	}
	return nil
}

func dedupeCandidates(in []candidate) []candidate {
	seen := map[string]struct{}{}
	out := make([]candidate, 0, len(in))
	for _, c := range in {
		key := string(c.meta.Format) + "|" + c.meta.SourcePath + "|" + c.meta.IndexSetID + "|" + string(c.identityStatus)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, c)
	}
	return out
}

// ReadBoundedFile reads a local file with a single open and LimitReader(max+1).
// Oversized files fail closed. Default max is 1 MiB when maxBytes <= 0.
func ReadBoundedFile(path string, maxBytes int64) ([]byte, error) {
	return readBoundedFile(path, maxBytes)
}

func readBoundedFile(path string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	// Bind without following the final path component, then verify the opened
	// file is the same regular file still named at the read boundary.
	f, err := openSQLiteIdentityBinding(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	boundInfo, err := f.Stat()
	if err != nil || !boundInfo.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("bounded input is not a regular file"), err)
	}
	namedInfo, err := os.Lstat(path)
	if err != nil || namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() || !os.SameFile(boundInfo, namedInfo) {
		return nil, errors.Join(fmt.Errorf("bounded input binding changed before read"), err)
	}
	limited := io.LimitReader(f, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("file %s size exceeds limit %d", filepath.Base(path), maxBytes)
	}
	return data, nil
}

// LocalIdentityFile is identity.json loaded with bounded single-open semantics.
type LocalIdentityFile struct {
	// Raw is the trimmed file content used for presentation hashing.
	Raw []byte
	// Payload is the parsed identity document.
	Payload indexstore.IndexSetIdentityPayload
	// IndexSetID is the recomputed full idx_<64hex> when ComputeIndexSetID succeeds.
	IndexSetID string
}

// ReadLocalIdentityFile reads and parses identity.json with the same bounded
// single-open posture as durable marker discovery. Prefer this over unbounded
// os.ReadFile + ad-hoc ComputeIndexSetID assembly at call sites.
func ReadLocalIdentityFile(path string, maxBytes int64) (LocalIdentityFile, error) {
	data, err := readBoundedFile(path, maxBytes)
	if err != nil {
		return LocalIdentityFile{}, err
	}
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return LocalIdentityFile{}, fmt.Errorf("identity.json is empty")
	}
	var payload indexstore.IndexSetIdentityPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return LocalIdentityFile{}, fmt.Errorf("parse identity.json: %w", err)
	}
	out := LocalIdentityFile{
		Raw:     []byte(trimmed),
		Payload: payload,
	}
	params := indexstore.IndexSetParams{
		BaseURI:         payload.BaseURI,
		Provider:        payload.Provider,
		StorageProvider: payload.StorageProvider,
		CloudProvider:   payload.CloudProvider,
		RegionKind:      payload.RegionKind,
		Region:          payload.Region,
		EndpointHost:    payload.EndpointHost,
		BuildParams: indexstore.BuildParams{
			SourceType:      payload.Build.SourceType,
			SchemaVersion:   payload.Build.SchemaVersion,
			GonimbusVersion: payload.Build.GonimbusVersion,
			Includes:        payload.Build.Includes,
			Excludes:        payload.Build.Excludes,
			IncludeHidden:   payload.Build.IncludeHidden,
			FiltersHash:     payload.Build.FiltersHash,
			ScopeHash:       payload.Build.ScopeHash,
		},
	}
	if payload.PathDate != nil {
		params.BuildParams.PathDateExtraction = &indexstore.PathDateExtraction{
			Method:       payload.PathDate.Method,
			Regex:        payload.PathDate.Regex,
			SegmentIndex: payload.PathDate.SegmentIndex,
		}
	}
	if identity, err := indexstore.ComputeIndexSetID(params); err == nil {
		out.IndexSetID = identity.IndexSetID
	}
	return out, nil
}
