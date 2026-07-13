package cmd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var indexDoctorCmd = newIndexDoctorCommand()

func newIndexDoctorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "doctor [idx_<id-prefix>|path]",
		Aliases: []string{"show"},
		Short:   "Show and validate local index stores",
		Long: `Inspect local index stores and their identity metadata.

This is a read-only introspection command (it does not repair or modify indexes).

Format-aware: discovers sqlite-v1 (index.db) and durable-v2 (latest/complete/manifest
trust chain). Durable-only sets without index.db are included.

It helps map the per-index directory name (idx_<hash-prefix>) to a human-readable identity
(base URI, provider identity, build parameters) and checks for common issues:

- missing or invalid identity.json
- identity.json hash not matching the index_set_id
- DBs containing multiple index sets (unsupported)
- durable marker/manifest digest failures

Examples:
  # Show all local indexes (alias)
  gonimbus index show

  # Diagnose all local indexes
  gonimbus index doctor

  # Diagnose a specific local index by ID or prefix
  gonimbus index doctor idx_1234abcd

  # Diagnose a specific index directory
  gonimbus index doctor --db ~/.local/share/gonimbus/indexes/idx_1234abcd5678ef90/

  # Include object counts (sqlite: objects_current; durable: manifest counts)
  gonimbus index doctor --stats

  # Show detailed JSON for a single index
  gonimbus index doctor idx_1234abcd --detail

  # Dual-format set: select durable detail explicitly
  gonimbus index doctor idx_1234abcd --detail --format durable-v2

  # Machine-readable output
  gonimbus index doctor --json`,
		Args: cobra.MaximumNArgs(1),
		RunE: runIndexDoctor,
	}

	cmd.Flags().StringVar(&indexDoctorRootDir, "root", "", "Override index root directory (defaults to app data dir indexes)")
	cmd.Flags().StringVar(&indexDoctorDB, "db", "", "Inspect a specific index db path or index directory (optional)")
	cmd.Flags().StringVar(&indexDoctorFormat, "format", "", "Select substrate for multi-format sets: sqlite-v1 or durable-v2")
	cmd.Flags().Bool("json", false, "Output as JSON")
	cmd.Flags().Bool("verbose", false, "Include identity payload details")
	cmd.Flags().Bool("stats", false, "Include object counts (may be expensive on very large sqlite indexes)")
	cmd.Flags().Bool("detail", false, "Show detailed JSON report for a single index (includes identity and manifest when present)")

	return cmd
}

var (
	indexDoctorRootDir string
	indexDoctorDB      string
	indexDoctorFormat  string
)

func init() {
	indexCmd.AddCommand(indexDoctorCmd)
}

type indexDoctorEntry struct {
	Format  string `json:"format,omitempty"`
	DBPath  string `json:"db_path,omitempty"`
	Dir     string `json:"dir"`
	DirName string `json:"dir_name"`

	// From DB or durable markers
	IndexSetID      string    `json:"index_set_id,omitempty"`
	BaseURI         string    `json:"base_uri,omitempty"`
	Provider        string    `json:"provider,omitempty"`
	StorageProvider string    `json:"storage_provider,omitempty"`
	CloudProvider   string    `json:"cloud_provider,omitempty"`
	RegionKind      string    `json:"region_kind,omitempty"`
	Region          string    `json:"region,omitempty"`
	Endpoint        string    `json:"endpoint,omitempty"`
	EndpointHost    string    `json:"endpoint_host,omitempty"`
	CreatedAt       time.Time `json:"created_at,omitempty"`

	// Stats (optional)
	ActiveObjectCount  *int64 `json:"active_object_count,omitempty"`
	DeletedObjectCount *int64 `json:"deleted_object_count,omitempty"`

	// Latest run
	LatestRunAt  *time.Time `json:"latest_run_at,omitempty"`
	LatestStatus string     `json:"latest_status,omitempty"`
	LatestRunID  string     `json:"latest_run_id,omitempty"`

	// Identity.json
	IdentityPath          string                              `json:"identity_path,omitempty"`
	IdentityPresent       bool                                `json:"identity_present"`
	IdentityValidJSON     bool                                `json:"identity_valid_json"`
	IdentityHash          string                              `json:"identity_hash,omitempty"`
	IdentityIndexSetID    string                              `json:"identity_index_set_id,omitempty"`
	IdentityDirName       string                              `json:"identity_dir_name,omitempty"`
	IdentityPayload       *indexstore.IndexSetIdentityPayload `json:"identity_payload,omitempty"`
	IdentityHashMatchesDB bool                                `json:"identity_hash_matches_db"`
	IdentityDirMatches    bool                                `json:"identity_dir_matches"`
	IdentityBaseURIMatch  bool                                `json:"identity_base_uri_matches"`
	IdentityProviderMatch bool                                `json:"identity_provider_matches"`

	// Manifest (optional provenance file written by index build)
	ManifestPath      string          `json:"manifest_path,omitempty"`
	ManifestPresent   bool            `json:"manifest_present"`
	ManifestValidJSON bool            `json:"manifest_valid_json"`
	ManifestRaw       json.RawMessage `json:"manifest_raw,omitempty"`

	// Durable marker health (durable_marker_ok is always present for durable-v2 rows).
	DurableLatestPath   string `json:"durable_latest_path,omitempty"`
	DurableMarkerOK     *bool  `json:"durable_marker_ok,omitempty"`
	DurableManifestSHA  string `json:"durable_manifest_sha256,omitempty"`
	DurableSegmentCount int    `json:"durable_segment_count,omitempty"`

	// Overall health summary
	IdentityOK bool `json:"identity_ok"`

	// Warnings / notes
	Notes []string `json:"notes,omitempty"`
}

type indexDoctorOptions struct {
	IncludeIdentityPayload bool
	IncludeStats           bool
	IncludeManifest        bool
}

// indexDoctorTarget is a format-aware inspect target.
type indexDoctorTarget struct {
	Format indexreader.Format
	// DBPath is set for sqlite-v1 (path to index.db).
	DBPath string
	// Meta is set when discovery used the reader seam (both formats).
	Meta indexreader.Meta
}

func runIndexDoctor(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	jsonOutput, _ := cmd.Flags().GetBool("json")
	verbose, _ := cmd.Flags().GetBool("verbose")
	includeStats, _ := cmd.Flags().GetBool("stats")
	detail, _ := cmd.Flags().GetBool("detail")

	target := ""
	if len(args) > 0 {
		target = args[0]
	}

	targets, err := resolveIndexDoctorTargets(ctx, target)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No indexes found")
		return nil
	}
	targets, err = filterDoctorTargetsByFormat(targets, indexDoctorFormat)
	if err != nil {
		return err
	}

	opts := indexDoctorOptions{
		IncludeIdentityPayload: verbose || detail,
		IncludeStats:           includeStats || detail,
		IncludeManifest:        detail,
	}

	if detail {
		if len(targets) != 1 {
			return fmt.Errorf("--detail matched %d substrates; pass --format sqlite-v1|durable-v2 (or a path that selects one backend)", len(targets))
		}
		entry, err := inspectIndexForDoctor(ctx, targets[0], opts)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(entry)
	}

	entries := make([]indexDoctorEntry, 0, len(targets))
	for _, t := range targets {
		entry, err := inspectIndexForDoctor(ctx, t, opts)
		if err != nil {
			entries = append(entries, indexDoctorEntry{
				Format:  formatLabel(t.Format),
				DBPath:  t.DBPath,
				Dir:     filepath.Dir(firstNonEmpty(t.DBPath, t.Meta.SourcePath)),
				DirName: filepath.Base(filepath.Dir(firstNonEmpty(t.DBPath, t.Meta.SourcePath))),
				Notes:   []string{err.Error()},
			})
			continue
		}
		entries = append(entries, *entry)
	}

	if jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(entries)
	}

	return printIndexDoctorTable(entries)
}

func inspectIndexForDoctor(ctx context.Context, target indexDoctorTarget, opts indexDoctorOptions) (*indexDoctorEntry, error) {
	switch target.Format {
	case indexreader.FormatSQLiteV1:
		return inspectIndexDBForDoctor(ctx, target.DBPath, opts)
	case indexreader.FormatDurableV2:
		return inspectDurableForDoctor(target.Meta, opts)
	default:
		if target.DBPath != "" {
			return inspectIndexDBForDoctor(ctx, target.DBPath, opts)
		}
		return nil, fmt.Errorf("unsupported index format %q", target.Format)
	}
}

func filterDoctorTargetsByFormat(targets []indexDoctorTarget, formatFlag string) ([]indexDoctorTarget, error) {
	formatFlag = strings.TrimSpace(formatFlag)
	if formatFlag == "" {
		return targets, nil
	}
	var want indexreader.Format
	switch formatFlag {
	case string(indexreader.FormatSQLiteV1), "sqlite":
		want = indexreader.FormatSQLiteV1
	case string(indexreader.FormatDurableV2), "durable":
		want = indexreader.FormatDurableV2
	default:
		return nil, fmt.Errorf("--format must be sqlite-v1 or durable-v2")
	}
	var out []indexDoctorTarget
	for _, t := range targets {
		if t.Format == want {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s target matched", want)
	}
	return out, nil
}

func resolveIndexDoctorTargets(ctx context.Context, target string) ([]indexDoctorTarget, error) {
	_ = ctx
	target = strings.TrimSpace(target)
	explicitDB := strings.TrimSpace(indexDoctorDB)
	if target != "" && explicitDB != "" {
		return nil, fmt.Errorf("cannot use positional target with --db")
	}

	if explicitDB != "" {
		return resolveExplicitDoctorPath(explicitDB)
	}
	if target != "" {
		return resolveNamedDoctorTarget(target)
	}
	return discoverAllDoctorTargets()
}

// discoverAllDoctorTargets enumerates sqlite and durable backends independently
// of trust success. Unlike ListIndexReaders, broken durable markers remain
// discoverable so doctor can report durable_marker_ok=false.
// Unlike list, format-both sets yield BOTH backends (no preferListedIndexes).
func discoverAllDoctorTargets() ([]indexDoctorTarget, error) {
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return nil, err
	}
	if root := strings.TrimSpace(indexDoctorRootDir); root != "" {
		opts.IndexesRoot = root
	}
	return discoverDoctorTargets(opts)
}

func discoverDoctorTargets(opts indexreader.ResolveOptions) ([]indexDoctorTarget, error) {
	var out []indexDoctorTarget
	seen := map[string]struct{}{}

	// 1) Indexes root: sqlite DBs and identity dirs (with or without durable markers).
	if opts.IndexesRoot != "" {
		entries, err := os.ReadDir(opts.IndexesRoot)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read indexes root: %w", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dirName := entry.Name()
			if !strings.HasPrefix(dirName, "idx_") {
				continue
			}
			dirPath := filepath.Join(opts.IndexesRoot, dirName)
			dbPath := filepath.Join(dirPath, "index.db")
			if st, err := os.Stat(dbPath); err == nil && !st.IsDir() {
				key := "sqlite|" + dbPath
				if _, ok := seen[key]; !ok {
					seen[key] = struct{}{}
					out = append(out, indexDoctorTarget{
						Format: indexreader.FormatSQLiteV1,
						DBPath: dbPath,
						Meta: indexreader.Meta{
							Format:      indexreader.FormatSQLiteV1,
							IdentityDir: dirPath,
							SourcePath:  dbPath,
						},
					})
				}
			}
			// Durable candidate only when a real segment-set directory exists
			// for this identity (identity alone does not prove durable was built).
			if dt, ok := durableDoctorTargetFromIdentityDir(opts, dirPath); ok {
				key := durableDoctorTargetKey(dt)
				if _, exists := seen[key]; !exists {
					seen[key] = struct{}{}
					out = append(out, dt)
				}
			}
		}
	}

	// 2) Segment cache: every valid idx_* set directory is a durable candidate,
	// even when latest.json is missing or markers fail trust validation.
	if opts.SegmentCacheRoot != "" {
		entries, err := os.ReadDir(opts.SegmentCacheRoot)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read segment cache: %w", err)
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			setID := entry.Name()
			if !strings.HasPrefix(setID, "idx_") {
				continue
			}
			setDir := filepath.Join(opts.SegmentCacheRoot, setID)
			if st, err := os.Stat(setDir); err != nil || !st.IsDir() {
				continue
			}
			latest := filepath.Join(setDir, "latest.json")
			identityDir := findIdentityDirForSet(opts.IndexesRoot, setID)
			dt := indexDoctorTarget{
				Format: indexreader.FormatDurableV2,
				Meta: indexreader.Meta{
					Format:      indexreader.FormatDurableV2,
					IndexSetID:  setID,
					IdentityDir: identityDir,
					SourcePath:  latest, // may be absent; inspect reports missing marker
				},
			}
			key := durableDoctorTargetKey(dt)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, dt)
		}
	}
	return out, nil
}

func durableDoctorTargetKey(t indexDoctorTarget) string {
	// One durable diagnostic row per index set id (not per latest path string).
	id := t.Meta.IndexSetID
	if id == "" {
		id = t.Meta.SourcePath
	}
	return "durable|" + id
}

// durableDoctorTargetFromIdentityDir returns a durable doctor target only when a
// real cache/segments/<fullID>/ directory exists. Identity.json proves the set
// id; it does not prove a durable artifact was published. Requiring the segment
// set directory (not latest.json) preserves broken-marker diagnosis without
// synthesizing phantom durable backends for sqlite-only builds.
func durableDoctorTargetFromIdentityDir(opts indexreader.ResolveOptions, identityDir string) (indexDoctorTarget, bool) {
	if strings.TrimSpace(opts.SegmentCacheRoot) == "" {
		return indexDoctorTarget{}, false
	}
	identity, err := readDoctorIdentityMeta(identityDir)
	if err != nil {
		// Still allow dir without readable identity when segment match exists via dir name.
		identity = doctorIdentityMeta{}
	}
	fullID := identity.IndexSetID
	dirHex := strings.TrimPrefix(filepath.Base(identityDir), "idx_")
	if fullID == "" && dirHex != "" {
		// Prefer exact full id under segment cache; otherwise unique prefix match
		// against existing segment-set directories only.
		if len(dirHex) == 64 {
			candidate := "idx_" + dirHex
			if isDoctorSegmentSetDir(opts.SegmentCacheRoot, candidate) {
				fullID = candidate
			}
		}
		if fullID == "" {
			if matched, matchErr := matchDoctorSegmentCacheID(opts.SegmentCacheRoot, dirHex); matchErr == nil {
				fullID = matched
			}
		}
	}
	if fullID == "" || !isDoctorSegmentSetDir(opts.SegmentCacheRoot, fullID) {
		return indexDoctorTarget{}, false
	}
	latest := filepath.Join(opts.SegmentCacheRoot, fullID, "latest.json")
	return indexDoctorTarget{
		Format: indexreader.FormatDurableV2,
		Meta: indexreader.Meta{
			Format:      indexreader.FormatDurableV2,
			IndexSetID:  fullID,
			BaseURI:     identity.BaseURI,
			Provider:    identity.Provider,
			IdentityDir: identityDir,
			SourcePath:  latest,
		},
	}, true
}

func isDoctorSegmentSetDir(segmentRoot, setID string) bool {
	if segmentRoot == "" || setID == "" {
		return false
	}
	st, err := os.Stat(filepath.Join(segmentRoot, setID))
	return err == nil && st.IsDir()
}

type doctorIdentityMeta struct {
	IndexSetID string
	BaseURI    string
	Provider   string
}

func readDoctorIdentityMeta(dir string) (doctorIdentityMeta, error) {
	path := filepath.Join(dir, "identity.json")
	file, err := indexreader.ReadLocalIdentityFile(path, int64(maxHubMarkerBytes))
	if err != nil {
		return doctorIdentityMeta{}, err
	}
	return doctorIdentityMeta{
		IndexSetID: file.IndexSetID,
		BaseURI:    file.Payload.BaseURI,
		Provider:   file.Payload.Provider,
	}, nil
}

func matchDoctorSegmentCacheID(segmentRoot, wantHex string) (string, error) {
	entries, err := os.ReadDir(segmentRoot)
	if err != nil {
		return "", err
	}
	var matches []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirHex := strings.TrimPrefix(entry.Name(), "idx_")
		if strings.HasPrefix(dirHex, wantHex) || (len(wantHex) == 64 && strings.HasPrefix(wantHex, dirHex)) {
			// Segment-set directory is existence evidence (latest.json optional).
			matches = append(matches, entry.Name())
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no durable set matching %s", wantHex)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("ambiguous durable set matches: %s", strings.Join(matches, ", "))
	}
	return matches[0], nil
}

func findIdentityDirForSet(indexesRoot, fullSetID string) string {
	if indexesRoot == "" || fullSetID == "" {
		return ""
	}
	hexID := strings.TrimPrefix(fullSetID, "idx_")
	// Prefer short identity dir name (idx_<16hex>).
	if len(hexID) >= 16 {
		short := filepath.Join(indexesRoot, "idx_"+hexID[:16])
		if st, err := os.Stat(short); err == nil && st.IsDir() {
			return short
		}
	}
	entries, err := os.ReadDir(indexesRoot)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dirHex := strings.TrimPrefix(entry.Name(), "idx_")
		if strings.HasPrefix(hexID, dirHex) || strings.HasPrefix(dirHex, hexID) {
			return filepath.Join(indexesRoot, entry.Name())
		}
	}
	return ""
}

func resolveExplicitDoctorPath(path string) ([]indexDoctorTarget, error) {
	path = strings.TrimSpace(path)
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat index target: %w", err)
	}
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return nil, err
	}
	if root := strings.TrimSpace(indexDoctorRootDir); root != "" {
		opts.IndexesRoot = root
	}

	if info.IsDir() {
		// Segment-set directory under cache/segments/<idx_*>: durable-only selection
		// that retains the real identity directory when present.
		if isDoctorSegmentSetPath(opts.SegmentCacheRoot, path) {
			setID := filepath.Base(path)
			identityDir := findIdentityDirForSet(opts.IndexesRoot, setID)
			return []indexDoctorTarget{{
				Format: indexreader.FormatDurableV2,
				Meta: indexreader.Meta{
					Format:      indexreader.FormatDurableV2,
					IndexSetID:  setID,
					IdentityDir: identityDir,
					SourcePath:  filepath.Join(path, "latest.json"),
				},
			}}, nil
		}
		var out []indexDoctorTarget
		dbPath := filepath.Join(path, "index.db")
		if st, err := os.Stat(dbPath); err == nil && !st.IsDir() {
			out = append(out, indexDoctorTarget{
				Format: indexreader.FormatSQLiteV1,
				DBPath: dbPath,
				Meta: indexreader.Meta{
					Format:      indexreader.FormatSQLiteV1,
					IdentityDir: path,
					SourcePath:  dbPath,
				},
			})
		}
		if dt, ok := durableDoctorTargetFromIdentityDir(opts, path); ok {
			// Ensure durable target keeps the identity dir (not the segment path).
			if dt.Meta.IdentityDir == "" {
				dt.Meta.IdentityDir = path
			}
			out = append(out, dt)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no index.db or durable snapshot found under %s", path)
		}
		return out, nil
	}
	// Explicit file path → expect index.db
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("index db not found: %s", path)
	}
	return []indexDoctorTarget{{
		Format: indexreader.FormatSQLiteV1,
		DBPath: path,
		Meta: indexreader.Meta{
			Format:      indexreader.FormatSQLiteV1,
			IdentityDir: filepath.Dir(path),
			SourcePath:  path,
		},
	}}, nil
}

func isDoctorSegmentSetPath(segmentRoot, path string) bool {
	if segmentRoot == "" || path == "" {
		return false
	}
	cleanRoot := filepath.Clean(segmentRoot)
	cleanPath := filepath.Clean(path)
	if cleanPath == cleanRoot {
		return false
	}
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return false
	}
	// Exactly one path component under segment root named idx_*.
	if filepath.Base(rel) != rel {
		return false
	}
	return strings.HasPrefix(rel, "idx_")
}

func resolveNamedDoctorTarget(target string) ([]indexDoctorTarget, error) {
	if path, ok, err := resolveExistingIndexDoctorPath(target); err != nil {
		return nil, err
	} else if ok {
		// Named sqlite path — also attach durable sibling if present.
		return resolveExplicitDoctorPath(filepath.Dir(path))
	}
	if hasPathSeparator(target) {
		if st, err := os.Stat(target); err == nil && st.IsDir() {
			return resolveExplicitDoctorPath(target)
		}
		if strings.HasSuffix(target, "index.db") || strings.HasSuffix(target, string(os.PathSeparator)+"index.db") {
			return nil, fmt.Errorf("index db not found: %s", target)
		}
		return nil, fmt.Errorf("index not found: %s", target)
	}

	all, err := discoverAllDoctorTargets()
	if err != nil {
		return nil, err
	}
	want := strings.TrimPrefix(strings.TrimSpace(target), "idx_")
	if want == "" {
		return nil, fmt.Errorf("invalid index ID: %s", target)
	}
	// Prefix match only: candidate id/dir must start with want (never empty-hex match-all).
	matchesPrefix := func(hex string) bool {
		return hex != "" && strings.HasPrefix(hex, want)
	}
	var matches []indexDoctorTarget
	matchedDirs := map[string]struct{}{}
	for _, t := range all {
		hexID := strings.TrimPrefix(t.Meta.IndexSetID, "idx_")
		dirHex := ""
		dirName := ""
		if t.Meta.IdentityDir != "" {
			dirName = filepath.Base(t.Meta.IdentityDir)
			dirHex = strings.TrimPrefix(dirName, "idx_")
		}
		if t.DBPath != "" && dirName == "" {
			dirName = filepath.Base(filepath.Dir(t.DBPath))
			dirHex = strings.TrimPrefix(dirName, "idx_")
		}
		if matchesPrefix(hexID) || matchesPrefix(dirHex) {
			matches = append(matches, t)
			if dirName != "" {
				matchedDirs[dirName] = struct{}{}
			} else if t.Meta.IndexSetID != "" {
				// Full set id only (segment-cache durable without identity dir).
				matchedDirs[t.Meta.IndexSetID] = struct{}{}
			}
		}
	}

	// Also scan indexes root for empty/partial sibling dirs that share the prefix.
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return nil, err
	}
	if root := strings.TrimSpace(indexDoctorRootDir); root != "" {
		opts.IndexesRoot = root
	}
	if opts.IndexesRoot != "" {
		if entries, readErr := os.ReadDir(opts.IndexesRoot); readErr == nil {
			for _, entry := range entries {
				if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "idx_") {
					continue
				}
				dirHex := strings.TrimPrefix(entry.Name(), "idx_")
				if matchesPrefix(dirHex) {
					matchedDirs[entry.Name()] = struct{}{}
				}
			}
		}
	}
	if len(matchedDirs) > 1 {
		ids := make([]string, 0, len(matchedDirs))
		for name := range matchedDirs {
			ids = append(ids, name)
		}
		sort.Strings(ids)
		return nil, fmt.Errorf("ambiguous index ID %q matches %d indexes: %s", target, len(ids), strings.Join(ids, ", "))
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no index found matching ID: %s", target)
	}
	return matches, nil
}

func resolveExistingIndexDoctorPath(path string) (string, bool, error) {
	path = strings.TrimSpace(path)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", true, fmt.Errorf("stat index target: %w", err)
	}
	if info.IsDir() {
		dbPath := filepath.Join(path, "index.db")
		if _, err := os.Stat(dbPath); err != nil {
			return "", false, nil
		}
		return dbPath, true, nil
	}
	if _, err := os.Stat(path); err != nil {
		return "", true, fmt.Errorf("index db not found: %s", path)
	}
	return path, true, nil
}

func hasPathSeparator(value string) bool {
	return strings.Contains(value, string(os.PathSeparator)) || strings.Contains(value, "/")
}

func inspectDurableForDoctor(meta indexreader.Meta, opts indexDoctorOptions) (*indexDoctorEntry, error) {
	resolveOpts, err := indexReaderResolveOptions()
	if err != nil {
		return nil, err
	}
	dir := meta.IdentityDir
	if dir == "" && meta.SourcePath != "" {
		dir = filepath.Dir(meta.SourcePath)
	}
	if dir == "" {
		dir = meta.IndexSetID
	}
	entry := &indexDoctorEntry{
		Format:            formatLabel(indexreader.FormatDurableV2),
		Dir:               dir,
		DirName:           filepath.Base(dir),
		IndexSetID:        meta.IndexSetID,
		BaseURI:           meta.BaseURI,
		Provider:          meta.Provider,
		DurableLatestPath: meta.SourcePath,
		Notes:             nil,
	}

	// Identity.json
	identityPath := ""
	if meta.IdentityDir != "" {
		identityPath = filepath.Join(meta.IdentityDir, "identity.json")
	}
	entry.IdentityPath = identityPath
	fillDoctorIdentityFields(entry, identityPath, opts)

	// Job manifest provenance (optional) under identity dir.
	if meta.IdentityDir != "" {
		fillDoctorJobManifest(entry, filepath.Join(meta.IdentityDir, "manifest.json"), opts)
	}

	setMarkerOK := func(ok bool) {
		entry.DurableMarkerOK = &ok
	}
	if strings.TrimSpace(meta.SourcePath) == "" {
		setMarkerOK(false)
		entry.Notes = append(entry.Notes, "missing durable latest.json path")
		entry.IdentityOK = false
		return entry, nil
	}
	if _, err := os.Stat(meta.SourcePath); err != nil {
		setMarkerOK(false)
		entry.Notes = append(entry.Notes, fmt.Sprintf("durable latest.json: %v", err))
		entry.IdentityOK = false
		return entry, nil
	}

	// Durable trust chain: latest → complete → same-bytes manifest.
	snap, err := indexsubstrate.OpenLatestPublishedSnapshotBounded(meta.SourcePath, resolveOpts.MaxMarkerBytes, resolveOpts.MaxManifestBytes)
	if err != nil {
		setMarkerOK(false)
		entry.Notes = append(entry.Notes, fmt.Sprintf("durable marker chain: %v", err))
		entry.IdentityOK = false
		return entry, nil
	}
	setMarkerOK(true)
	entry.IndexSetID = snap.Manifest.IndexSetID
	entry.LatestRunID = snap.Manifest.RunID
	entry.LatestStatus = string(indexstore.RunStatusSuccess)
	entry.DurableManifestSHA = snap.Complete.ManifestSHA256
	entry.DurableSegmentCount = len(snap.Manifest.Segments)
	entry.CreatedAt = snap.Manifest.CreatedAt
	if ts, parseErr := time.Parse(time.RFC3339Nano, snap.Complete.CompletedAt); parseErr == nil {
		entry.LatestRunAt = &ts
	}
	if entry.IdentityIndexSetID != "" {
		entry.IdentityHashMatchesDB = entry.IdentityIndexSetID == entry.IndexSetID
		if !entry.IdentityHashMatchesDB {
			entry.Notes = append(entry.Notes, fmt.Sprintf("identity hash mismatch (identity %s, durable %s)", entry.IdentityIndexSetID, entry.IndexSetID))
		}
	}
	if entry.IdentityValidJSON && entry.IdentityPayload != nil {
		entry.IdentityBaseURIMatch = entry.IdentityPayload.BaseURI == entry.BaseURI || entry.BaseURI == ""
		entry.IdentityProviderMatch = entry.IdentityPayload.Provider == entry.Provider || entry.Provider == ""
		if entry.BaseURI == "" {
			entry.BaseURI = entry.IdentityPayload.BaseURI
		}
		if entry.Provider == "" {
			entry.Provider = entry.IdentityPayload.Provider
		}
		entry.StorageProvider = entry.IdentityPayload.StorageProvider
		entry.CloudProvider = entry.IdentityPayload.CloudProvider
		entry.Region = entry.IdentityPayload.Region
		entry.RegionKind = entry.IdentityPayload.RegionKind
		entry.EndpointHost = entry.IdentityPayload.EndpointHost
	}

	if opts.IncludeStats {
		active := int64(snap.Manifest.Counts.ActiveRows)
		deleted := int64(snap.Manifest.Counts.Tombstones)
		entry.ActiveObjectCount = &active
		entry.DeletedObjectCount = &deleted
	}

	// Verify every segment digest (doctor trust bar).
	if err := verifyDurableDoctorSegments(snap); err != nil {
		entry.Notes = append(entry.Notes, fmt.Sprintf("segment digest verify: %v", err))
	}

	markerOK := entry.DurableMarkerOK != nil && *entry.DurableMarkerOK
	entry.IdentityOK = entry.IndexSetID != "" && markerOK && len(entry.Notes) == 0
	return entry, nil
}

func verifyDurableDoctorSegments(snap indexsubstrate.PublishedSnapshot) error {
	// Walk with no-op visitor forces per-segment digest verify-before-emit.
	return indexsubstrate.WalkManifestRows(snap.SegmentDir, snap.Manifest, func(indexsubstrate.CurrentObjectRow) error {
		return nil
	})
}

func fillDoctorIdentityFields(entry *indexDoctorEntry, identityPath string, opts indexDoctorOptions) {
	if entry == nil {
		return
	}
	if strings.TrimSpace(identityPath) == "" {
		entry.IdentityPresent = false
		entry.Notes = append(entry.Notes, "missing identity.json")
		return
	}
	file, err := indexreader.ReadLocalIdentityFile(identityPath, int64(maxHubMarkerBytes))
	if err != nil {
		if os.IsNotExist(err) {
			entry.IdentityPresent = false
			entry.Notes = append(entry.Notes, "missing identity.json")
		} else if strings.Contains(err.Error(), "exceeds limit") {
			entry.IdentityPresent = true
			entry.IdentityValidJSON = false
			entry.Notes = append(entry.Notes, fmt.Sprintf("identity.json: %v", err))
		} else if strings.Contains(err.Error(), "empty") {
			entry.IdentityPresent = true
			entry.IdentityValidJSON = false
			entry.Notes = append(entry.Notes, "identity.json is empty")
		} else if strings.Contains(err.Error(), "parse") {
			entry.IdentityPresent = true
			entry.IdentityValidJSON = false
			entry.Notes = append(entry.Notes, fmt.Sprintf("invalid identity.json: %v", err))
		} else {
			entry.Notes = append(entry.Notes, fmt.Sprintf("read identity.json: %v", err))
		}
		return
	}
	entry.IdentityPresent = true
	sha := sha256.Sum256(file.Raw)
	shaHex := hex.EncodeToString(sha[:])
	entry.IdentityHash = shaHex
	// Presentation hash of raw content (historical list/doctor contract).
	entry.IdentityIndexSetID = "idx_" + shaHex
	entry.IdentityDirName = "idx_" + shaHex[:16]
	entry.IdentityDirMatches = entry.DirName == entry.IdentityDirName || strings.HasPrefix(strings.TrimPrefix(entry.DirName, "idx_"), shaHex[:16])
	if !entry.IdentityDirMatches && strings.HasPrefix(entry.DirName, "idx_") && len(entry.DirName) == len(entry.IdentityDirName) {
		entry.Notes = append(entry.Notes, fmt.Sprintf("dir name mismatch (expected %s)", entry.IdentityDirName))
	}
	entry.IdentityValidJSON = true
	if opts.IncludeIdentityPayload {
		payload := file.Payload
		entry.IdentityPayload = &payload
	}
	entry.IdentityBaseURIMatch = true
	entry.IdentityProviderMatch = true
}

func fillDoctorJobManifest(entry *indexDoctorEntry, manifestPath string, opts indexDoctorOptions) {
	if entry == nil {
		return
	}
	entry.ManifestPath = manifestPath
	// Job-manifest provenance is presentation-only; bound like identity metadata.
	manifestRaw, err := indexreader.ReadBoundedFile(manifestPath, int64(maxHubMarkerBytes))
	if err != nil {
		if !os.IsNotExist(err) {
			entry.Notes = append(entry.Notes, fmt.Sprintf("read manifest.json: %v", err))
		}
		return
	}
	entry.ManifestPresent = true
	trimmed := strings.TrimSpace(string(manifestRaw))
	if trimmed == "" {
		entry.ManifestValidJSON = false
		entry.Notes = append(entry.Notes, "manifest.json is empty")
		return
	}
	if !json.Valid([]byte(trimmed)) {
		entry.ManifestValidJSON = false
		entry.Notes = append(entry.Notes, "manifest.json is not valid JSON")
		return
	}
	entry.ManifestValidJSON = true
	if opts.IncludeManifest {
		entry.ManifestRaw = json.RawMessage(trimmed)
	}
}

func inspectIndexDBForDoctor(ctx context.Context, dbPath string, opts indexDoctorOptions) (result *indexDoctorEntry, err error) {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return nil, fmt.Errorf("empty db path")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dir := filepath.Dir(dbPath)
	entry := &indexDoctorEntry{
		Format:  formatLabel(indexreader.FormatSQLiteV1),
		DBPath:  dbPath,
		Dir:     dir,
		DirName: filepath.Base(dir),
		Notes:   nil,
	}

	// Read identity.json / job manifest with bounded single-open loaders.
	identityPath := filepath.Join(dir, "identity.json")
	entry.IdentityPath = identityPath
	fillDoctorIdentityFields(entry, identityPath, opts)
	fillDoctorJobManifest(entry, filepath.Join(dir, "manifest.json"), opts)

	// Open a strict snapshot. Marker-authoritative canonical databases hold the
	// same stable authority as writers and GC for the full inspection.
	snapshotOpts, err := sqliteSnapshotOptionsForPath(dbPath, "")
	if err != nil {
		return nil, err
	}
	snapshot, err := indexreader.OpenSQLiteSnapshot(ctx, snapshotOpts)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	db := snapshot.DB()

	sets, err := indexstore.ListIndexSets(ctx, db, "")
	if err != nil {
		return nil, fmt.Errorf("list index sets: %w", err)
	}
	if len(sets) == 0 {
		entry.Notes = append(entry.Notes, "db contains zero index sets")
		entry.IdentityOK = false
		return entry, nil
	}
	if len(sets) > 1 {
		entry.Notes = append(entry.Notes, fmt.Sprintf("db contains %d index sets (expected 1)", len(sets)))
		entry.IdentityOK = false
		return entry, nil
	}

	is := sets[0]
	entry.IndexSetID = is.IndexSetID
	entry.BaseURI = is.BaseURI
	entry.Provider = is.Provider
	entry.StorageProvider = is.StorageProvider
	entry.CloudProvider = is.CloudProvider
	entry.RegionKind = is.RegionKind
	entry.Region = is.Region
	entry.Endpoint = is.Endpoint
	entry.EndpointHost = is.EndpointHost
	entry.CreatedAt = is.CreatedAt

	// Latest run (cheap single-row query).
	loadLatestRun(ctx, db, entry)

	// Optional object counts (can be expensive on huge tables).
	if opts.IncludeStats {
		active, deleted, err := loadObjectCounts(ctx, db, entry.IndexSetID)
		if err != nil {
			entry.Notes = append(entry.Notes, fmt.Sprintf("object count unavailable: %v", err))
		} else {
			entry.ActiveObjectCount = &active
			entry.DeletedObjectCount = &deleted
		}
	}

	// Compare identity to DB if we have it.
	if entry.IdentityPresent && entry.IdentityHash != "" {
		entry.IdentityHashMatchesDB = (entry.IdentityIndexSetID == entry.IndexSetID)
		if !entry.IdentityHashMatchesDB {
			entry.Notes = append(entry.Notes, fmt.Sprintf("identity hash mismatch (identity %s, db %s)", entry.IdentityIndexSetID, entry.IndexSetID))
		}

		if entry.IdentityValidJSON {
			// Parse payload again if we didn't keep it.
			payload := entry.IdentityPayload
			if payload == nil {
				if idFile, err := indexreader.ReadLocalIdentityFile(entry.IdentityPath, int64(maxHubMarkerBytes)); err == nil {
					p := idFile.Payload
					payload = &p
				}
			}

			if payload != nil {
				entry.IdentityBaseURIMatch = (payload.BaseURI == entry.BaseURI)
				if !entry.IdentityBaseURIMatch {
					entry.Notes = append(entry.Notes, fmt.Sprintf("base_uri mismatch (identity %s, db %s)", payload.BaseURI, entry.BaseURI))
				}
				entry.IdentityProviderMatch = (payload.Provider == entry.Provider)
				if !entry.IdentityProviderMatch {
					entry.Notes = append(entry.Notes, fmt.Sprintf("provider mismatch (identity %s, db %s)", payload.Provider, entry.Provider))
				}
			}
		}
	}

	entry.IdentityOK = entry.IndexSetID != "" && len(entry.Notes) == 0
	return entry, nil
}

func loadLatestRun(ctx context.Context, db *sql.DB, entry *indexDoctorEntry) {
	if entry == nil || entry.IndexSetID == "" {
		return
	}
	var latestAt sql.NullTime
	var latestStatus sql.NullString
	err := db.QueryRowContext(ctx,
		`SELECT started_at, status FROM index_runs WHERE index_set_id = ? ORDER BY started_at DESC LIMIT 1`,
		entry.IndexSetID,
	).Scan(&latestAt, &latestStatus)
	if err != nil {
		return
	}
	if latestAt.Valid {
		t := latestAt.Time
		entry.LatestRunAt = &t
	}
	if latestStatus.Valid {
		entry.LatestStatus = latestStatus.String
	}
}

func loadObjectCounts(ctx context.Context, db *sql.DB, indexSetID string) (active int64, deleted int64, err error) {
	var activeCount sql.NullInt64
	var deletedCount sql.NullInt64
	err = db.QueryRowContext(ctx,
		`SELECT
			SUM(CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END) AS active,
			SUM(CASE WHEN deleted_at IS NOT NULL THEN 1 ELSE 0 END) AS deleted
		FROM objects_current
		WHERE index_set_id = ?`,
		indexSetID,
	).Scan(&activeCount, &deletedCount)
	if err != nil {
		return 0, 0, err
	}
	if activeCount.Valid {
		active = activeCount.Int64
	}
	if deletedCount.Valid {
		deleted = deletedCount.Int64
	}
	return active, deleted, nil
}

func printIndexDoctorTable(entries []indexDoctorEntry) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()

	_, _ = fmt.Fprintln(w, "DIR\tFORMAT\tINDEX_SET\tBASE_URI\tSTORAGE\tCLOUD\tREGION\tOBJECTS\tLATEST\tSTATUS\tIDENTITY\tNOTES")
	for _, e := range entries {
		latest := "-"
		if e.LatestRunAt != nil {
			latest = e.LatestRunAt.UTC().Format(time.RFC3339)
		}
		status := e.LatestStatus
		if status == "" {
			status = "-"
		}

		var identity string
		switch {
		case !e.IdentityPresent:
			identity = "missing"
		case e.IdentityPresent && !e.IdentityValidJSON:
			identity = "invalid"
		case e.IdentityHashMatchesDB && e.IdentityDirMatches && e.IdentityBaseURIMatch && e.IdentityProviderMatch:
			identity = "ok"
		case e.Format == formatLabel(indexreader.FormatDurableV2) && e.DurableMarkerOK != nil && *e.DurableMarkerOK && e.IdentityPresent && e.IdentityValidJSON && len(e.Notes) == 0:
			identity = "ok"
		default:
			identity = "mismatch"
		}

		objects := "-"
		if e.ActiveObjectCount != nil {
			objects = fmt.Sprintf("%d", *e.ActiveObjectCount)
		}

		notes := strings.Join(e.Notes, "; ")
		if notes == "" {
			notes = "-"
		}

		shortID := e.IndexSetID
		if strings.HasPrefix(shortID, "idx_") && len(shortID) > len("idx_")+8 {
			shortID = "idx_" + shortID[len("idx_"):len("idx_")+8]
		}

		storage := e.StorageProvider
		if storage == "" {
			storage = e.Provider
		}
		cloud := e.CloudProvider
		if cloud == "" {
			cloud = "-"
		}
		region := e.Region
		if region == "" {
			region = "-"
		}
		format := e.Format
		if format == "" {
			format = "-"
		}

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			e.DirName,
			format,
			shortID,
			e.BaseURI,
			storage,
			cloud,
			region,
			objects,
			latest,
			status,
			identity,
			notes,
		)
	}
	return nil
}
