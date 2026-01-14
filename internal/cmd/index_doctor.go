package cmd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var indexDoctorCmd = &cobra.Command{
	Use:     "doctor",
	Aliases: []string{"show"},
	Short:   "Show and validate local index stores",
	Long: `Inspect local index database files and their identity metadata.

This is a read-only introspection command (it does not repair or modify indexes).

It helps map the per-index directory name (idx_<hash-prefix>) to a human-readable identity
(base URI, provider identity, build parameters) and checks for common issues:

- missing or invalid identity.json
- identity.json hash not matching the DB's index_set_id
- DBs containing multiple index sets (unsupported)

Examples:
  # Show all local indexes (alias)
  gonimbus index show

  # Diagnose all local indexes
  gonimbus index doctor

  # Diagnose a specific index directory
  gonimbus index doctor --db ~/.local/share/gonimbus/indexes/idx_1234abcd5678ef90/

  # Include object counts (can be expensive on huge indexes)
  gonimbus index doctor --stats

  # Show detailed JSON for a single index
  gonimbus index doctor --db ~/.local/share/gonimbus/indexes/idx_1234abcd5678ef90/ --detail

  # Machine-readable output
  gonimbus index doctor --json`,
	RunE: runIndexDoctor,
}

var (
	indexDoctorRootDir string
	indexDoctorDB      string
)

func init() {
	indexCmd.AddCommand(indexDoctorCmd)
	indexDoctorCmd.Flags().StringVar(&indexDoctorRootDir, "root", "", "Override index root directory (defaults to app data dir indexes)")
	indexDoctorCmd.Flags().StringVar(&indexDoctorDB, "db", "", "Inspect a specific index db path or index directory (optional)")
	indexDoctorCmd.Flags().Bool("json", false, "Output as JSON")
	indexDoctorCmd.Flags().Bool("verbose", false, "Include identity payload details")
	indexDoctorCmd.Flags().Bool("stats", false, "Include object counts from the index DB (may be expensive on very large indexes)")
	indexDoctorCmd.Flags().Bool("detail", false, "Show detailed JSON report for a single index (includes identity and manifest when present)")
}

type indexDoctorEntry struct {
	DBPath  string `json:"db_path"`
	Dir     string `json:"dir"`
	DirName string `json:"dir_name"`

	// From DB
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

	// DB stats (optional)
	ActiveObjectCount  *int64 `json:"active_object_count,omitempty"`
	DeletedObjectCount *int64 `json:"deleted_object_count,omitempty"`

	// Latest run
	LatestRunAt  *time.Time `json:"latest_run_at,omitempty"`
	LatestStatus string     `json:"latest_status,omitempty"`

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

func runIndexDoctor(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	jsonOutput, _ := cmd.Flags().GetBool("json")
	verbose, _ := cmd.Flags().GetBool("verbose")
	includeStats, _ := cmd.Flags().GetBool("stats")
	detail, _ := cmd.Flags().GetBool("detail")

	dbPaths, err := resolveIndexDoctorTargets()
	if err != nil {
		return err
	}
	if len(dbPaths) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "No index databases found")
		return nil
	}

	opts := indexDoctorOptions{
		IncludeIdentityPayload: verbose || detail,
		IncludeStats:           includeStats || detail,
		IncludeManifest:        detail,
	}

	if detail {
		if len(dbPaths) != 1 {
			return fmt.Errorf("--detail requires exactly one target; use --db to select a specific index")
		}
		entry, err := inspectIndexDBForDoctor(ctx, dbPaths[0], opts)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(entry)
	}

	entries := make([]indexDoctorEntry, 0, len(dbPaths))
	for _, dbPath := range dbPaths {
		entry, err := inspectIndexDBForDoctor(ctx, dbPath, opts)
		if err != nil {
			entries = append(entries, indexDoctorEntry{
				DBPath:  dbPath,
				Dir:     filepath.Dir(dbPath),
				DirName: filepath.Base(filepath.Dir(dbPath)),
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

func resolveIndexDoctorTargets() ([]string, error) {
	// Explicit db/dir path wins.
	if strings.TrimSpace(indexDoctorDB) != "" {
		path := strings.TrimSpace(indexDoctorDB)
		// Allow specifying a directory containing index.db.
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			path = filepath.Join(path, "index.db")
		}
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("index db not found: %s", path)
		}
		return []string{path}, nil
	}

	// Override root dir.
	if strings.TrimSpace(indexDoctorRootDir) != "" {
		root := strings.TrimSpace(indexDoctorRootDir)
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("read index root: %w", err)
		}
		var paths []string
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			dbPath := filepath.Join(root, entry.Name(), "index.db")
			if _, err := os.Stat(dbPath); err != nil {
				continue
			}
			paths = append(paths, dbPath)
		}
		return paths, nil
	}

	// Default discovery.
	return listIndexDBPaths()
}

func inspectIndexDBForDoctor(ctx context.Context, dbPath string, opts indexDoctorOptions) (*indexDoctorEntry, error) {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return nil, fmt.Errorf("empty db path")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dir := filepath.Dir(dbPath)
	entry := &indexDoctorEntry{
		DBPath:  dbPath,
		Dir:     dir,
		DirName: filepath.Base(dir),
		Notes:   nil,
	}

	// Read identity.json if present.
	identityPath := filepath.Join(dir, "identity.json")
	entry.IdentityPath = identityPath
	identityRaw, err := os.ReadFile(identityPath)
	if err != nil {
		if os.IsNotExist(err) {
			entry.IdentityPresent = false
			entry.Notes = append(entry.Notes, "missing identity.json")
		} else {
			entry.Notes = append(entry.Notes, fmt.Sprintf("read identity.json: %v", err))
		}
	} else {
		entry.IdentityPresent = true
		trimmed := strings.TrimSpace(string(identityRaw))
		if trimmed == "" {
			entry.IdentityValidJSON = false
			entry.Notes = append(entry.Notes, "identity.json is empty")
		} else {
			sha := sha256.Sum256([]byte(trimmed))
			shaHex := hex.EncodeToString(sha[:])
			entry.IdentityHash = shaHex
			entry.IdentityIndexSetID = "idx_" + shaHex
			entry.IdentityDirName = "idx_" + shaHex[:16]
			entry.IdentityDirMatches = (entry.DirName == entry.IdentityDirName)
			if !entry.IdentityDirMatches {
				entry.Notes = append(entry.Notes, fmt.Sprintf("dir name mismatch (expected %s)", entry.IdentityDirName))
			}

			var payload indexstore.IndexSetIdentityPayload
			if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
				entry.IdentityValidJSON = false
				entry.Notes = append(entry.Notes, fmt.Sprintf("invalid identity.json: %v", err))
			} else {
				entry.IdentityValidJSON = true
				if opts.IncludeIdentityPayload {
					entry.IdentityPayload = &payload
				}
				entry.IdentityBaseURIMatch = true
				entry.IdentityProviderMatch = true
			}
		}
	}

	// Optional manifest.json for provenance.
	manifestPath := filepath.Join(dir, "manifest.json")
	entry.ManifestPath = manifestPath
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			entry.ManifestPresent = false
		} else {
			entry.Notes = append(entry.Notes, fmt.Sprintf("read manifest.json: %v", err))
		}
	} else {
		entry.ManifestPresent = true
		trimmed := strings.TrimSpace(string(manifestRaw))
		if trimmed == "" {
			entry.ManifestValidJSON = false
			entry.Notes = append(entry.Notes, "manifest.json is empty")
		} else if !json.Valid([]byte(trimmed)) {
			entry.ManifestValidJSON = false
			entry.Notes = append(entry.Notes, "manifest.json is not valid JSON")
		} else {
			entry.ManifestValidJSON = true
			if opts.IncludeManifest {
				entry.ManifestRaw = json.RawMessage(trimmed)
			}
		}
	}

	// Open DB and inspect index sets.
	db, err := openIndexDB(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

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
				var p indexstore.IndexSetIdentityPayload
				identityRaw, err := os.ReadFile(entry.IdentityPath)
				if err == nil {
					_ = json.Unmarshal([]byte(strings.TrimSpace(string(identityRaw))), &p)
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

	_, _ = fmt.Fprintln(w, "DIR\tINDEX_SET\tBASE_URI\tSTORAGE\tCLOUD\tREGION\tOBJECTS\tLATEST\tSTATUS\tIDENTITY\tNOTES")
	for _, e := range entries {
		latest := "-"
		if e.LatestRunAt != nil {
			latest = e.LatestRunAt.UTC().Format(time.RFC3339)
		}
		status := e.LatestStatus
		if status == "" {
			status = "-"
		}

		identity := "-"
		switch {
		case !e.IdentityPresent:
			identity = "missing"
		case e.IdentityPresent && !e.IdentityValidJSON:
			identity = "invalid"
		case e.IdentityHashMatchesDB && e.IdentityDirMatches && e.IdentityBaseURIMatch && e.IdentityProviderMatch:
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

		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			e.DirName,
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
