package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Manage local index stores",
	Long: `Manage local index stores used for index-backed search and tree operations.

Build local object indexes, query indexed objects, manage index sets and
background jobs, and export or hydrate index hubs.`,
}

var indexInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize the local index database",
	RunE:  runIndexInit,
}

var (
	indexDBPath string
)

func init() {
	rootCmd.AddCommand(indexCmd)
	indexCmd.AddCommand(indexInitCmd)

	indexInitCmd.Flags().StringVar(&indexDBPath, "db", "", "Index database path or libsql DSN (optional override)")
}

func runIndexInit(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	dbArg := strings.TrimSpace(indexDBPath)
	if dbArg == "" {
		indexDir, err := indexRootDir()
		if err != nil {
			return err
		}
		if err := mkdirAppDataDir(indexDir); err != nil {
			return fmt.Errorf("create index directory: %w", err)
		}
		_, _ = fmt.Fprintln(os.Stdout, "Index directory initialized")
		_, _ = fmt.Fprintf(os.Stdout, "dir=%s\n", indexDir)
		return nil
	}

	cfg := indexstore.Config{}
	if strings.HasPrefix(dbArg, "libsql://") || strings.HasPrefix(dbArg, "https://") {
		cfg.URL = dbArg
	} else {
		cfg.Path = dbArg
	}
	maintenance, err := acquireCanonicalIndexDBMaintenance(ctx, dbArg, "index-init")
	if err != nil {
		return err
	}
	if maintenance != nil {
		defer func() { _ = maintenance.Release() }()
		ctx = maintenance.Context()
	}

	db, err := indexstore.Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	migrateCtx := ctx
	if migrateCtx == nil {
		migrateCtx = context.Background()
	}
	if err := indexstore.Migrate(migrateCtx, db); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(os.Stdout, "Index database initialized")
	_, _ = fmt.Fprintf(os.Stdout, "db=%s\n", dbArg)
	_, _ = fmt.Fprintf(os.Stdout, "schema_version=%d\n", indexstore.SchemaVersion)
	return nil
}

// acquireCanonicalIndexDBMaintenance guards explicit mutable opens only when
// the path names a marker-authoritative database under the default indexes
// root. Remote and caller-owned external databases are outside local GC scope.
func acquireCanonicalIndexDBMaintenance(ctx context.Context, dbPath, holder string) (*indexSetMaintenanceGuard, error) {
	if strings.HasPrefix(dbPath, "libsql://") || strings.HasPrefix(dbPath, "https://") {
		return nil, nil
	}
	absDB, err := filepath.Abs(filepath.Clean(dbPath))
	if err != nil {
		return nil, err
	}
	root, err := indexRootDir()
	if err != nil {
		return nil, err
	}
	absRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, err
	}
	identityDir := filepath.Dir(absDB)
	if filepath.Base(absDB) != "index.db" {
		return nil, nil
	}
	parentInfo, parentErr := os.Stat(filepath.Dir(identityDir))
	rootInfo, rootErr := os.Stat(absRoot)
	if parentErr != nil || rootErr != nil || !os.SameFile(parentInfo, rootInfo) {
		return nil, nil
	}
	identity, identityErr := indexreader.ReadLocalIdentityFile(filepath.Join(identityDir, "identity.json"), int64(maxHubMarkerBytes))
	indexSetID := identity.IndexSetID
	if identityErr != nil || !validFullIndexSetID(indexSetID) {
		// During GC the identity marker moves with its root. Resolve an existing
		// stable authority file by the canonical idx_<prefix> directory so init
		// cannot recreate that root behind the held GC lock.
		indexSetID, err = resolveExistingCanonicalAuthorityID(filepath.Base(identityDir))
		if err != nil {
			return nil, err
		}
		if indexSetID == "" {
			// An unproven external/new directory is outside local GC scope.
			return nil, nil
		}
	}
	guard, err := acquireIndexSetMaintenance(ctx, indexSetID, holder)
	if err != nil {
		return nil, fmt.Errorf("acquire index-set maintenance authority: %w", err)
	}
	return guard, nil
}

func resolveExistingCanonicalAuthorityID(indexDirName string) (string, error) {
	want := strings.TrimPrefix(strings.TrimSpace(indexDirName), "idx_")
	if want == "" || !validHexPattern.MatchString(want) {
		return "", nil
	}
	segmentCacheRoot, err := appDataPath(appDataClassSegmentCache)
	if err != nil {
		return "", err
	}
	authorityRoot, err := indexcoord.AuthorityRoot(filepath.Join(segmentCacheRoot, "authority-probe"))
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(authorityRoot)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read stable index-set authority root: %w", err)
	}
	var matches []string
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".lock")
		if entry.Name() == id || !validFullIndexSetID(id) {
			continue
		}
		if strings.HasPrefix(strings.TrimPrefix(id, "idx_"), want) {
			matches = append(matches, id)
		}
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("canonical index directory matches multiple stable authority records")
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return "", nil
}
