package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
)

var indexCmd = &cobra.Command{
	Use:   "index",
	Short: "Manage local index stores",
	Long: `Manage local index stores used for index-backed search and tree operations.

This command group is the foundation for v0.1.3 indexing.

Note: index build/query/list/gc are implemented in follow-on checkpoints; this
command currently supports initializing the local index database.`,
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
		if err := os.MkdirAll(indexDir, 0755); err != nil {
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
