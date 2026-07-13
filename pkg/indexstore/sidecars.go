package indexstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CanonicalSQLiteQuarantinePrefix marks retained exact-epoch capture residue in
// the index directory. Ordinary open/recovery must discover these entries.
const CanonicalSQLiteQuarantinePrefix = ".gonimbus-sqlite-quarantine-"

// legacy alias used inside this package
const canonicalSQLiteQuarantinePrefix = CanonicalSQLiteQuarantinePrefix

// SQLiteTransactionSidecars returns transaction artifacts associated with an
// index database. Any returned entry means an immutable base-file snapshot is
// unsafe because committed or in-flight state may live outside index.db.
//
// This function is read-only inventory. Quarantine-prefix names are reported
// as blocking residue; it never deletes, truncates, or reclaims directory
// entries. Directory-entry removal requires a separately authorized recovery
// transaction that validates a durable receipt or exact binding — never
// prefix possession, emptiness, or whole-set authority alone.
func SQLiteTransactionSidecars(dbPath string) ([]string, error) {
	dbPath = strings.TrimSpace(dbPath)
	if dbPath == "" {
		return nil, fmt.Errorf("index database path is required")
	}
	dir := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var found []string
	for _, entry := range entries {
		name := entry.Name()
		if isCanonicalSQLiteQuarantineName(name) {
			found = append(found, name)
			continue
		}
		suffix := strings.TrimPrefix(name, base)
		if suffix == name {
			continue
		}
		if isSQLiteTransactionSidecarSuffix(suffix) {
			found = append(found, name)
		}
	}
	sort.Strings(found)
	return found, nil
}

// RejectSQLiteTransactionSidecars fails when any transaction artifact is
// present, including discoverable quarantine residue. Ordinary open and
// registration use this fail-closed gate.
func RejectSQLiteTransactionSidecars(dbPath string) error {
	found, err := SQLiteTransactionSidecars(dbPath)
	if err != nil {
		return fmt.Errorf("inspect SQLite transaction sidecars: %w", err)
	}
	if len(found) > 0 {
		return fmt.Errorf("SQLite transaction sidecars are present: %s", strings.Join(found, ", "))
	}
	return nil
}

// RejectLiveSQLiteTransactionSidecars fails only when live SQLite sidecar names
// remain (WAL, SHM, rollback/master/statement journals). Retained
// `.gonimbus-sqlite-quarantine-*` captures are not live hot state: content was
// destroyed via the bound descriptor and directory-entry removal is deferred to
// receipt-backed recovery. This helper never deletes residue. Canonical readers
// and mutable writers that must refuse unrecovered residue use
// RejectSQLiteTransactionSidecars (all quarantine-prefix names) instead.
func RejectLiveSQLiteTransactionSidecars(dbPath string) error {
	found, err := SQLiteTransactionSidecars(dbPath)
	if err != nil {
		return fmt.Errorf("inspect SQLite transaction sidecars: %w", err)
	}
	var live []string
	for _, name := range found {
		if isCanonicalSQLiteQuarantineName(name) {
			continue
		}
		live = append(live, name)
	}
	if len(live) > 0 {
		return fmt.Errorf("SQLite transaction sidecars are present: %s", strings.Join(live, ", "))
	}
	return nil
}

// QuarantineSQLiteTransactionResidue returns discoverable quarantine-prefix
// names beside dbPath without deleting them. Ordinary close may surface this
// list as blocking residue while leaving every entry intact.
func QuarantineSQLiteTransactionResidue(dbPath string) ([]string, error) {
	found, err := SQLiteTransactionSidecars(dbPath)
	if err != nil {
		return nil, err
	}
	var residue []string
	for _, name := range found {
		if isCanonicalSQLiteQuarantineName(name) {
			residue = append(residue, name)
		}
	}
	return residue, nil
}

func isSQLiteTransactionSidecarSuffix(suffix string) bool {
	return suffix == "-wal" || suffix == "-shm" || suffix == "-journal" ||
		strings.HasPrefix(suffix, "-mj ") || strings.HasPrefix(suffix, "-stmtjrnl")
}

func isCanonicalSQLiteQuarantineName(name string) bool {
	return strings.HasPrefix(name, canonicalSQLiteQuarantinePrefix)
}
