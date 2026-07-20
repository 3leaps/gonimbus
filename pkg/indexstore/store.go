package indexstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	// Path is a local filesystem path to the index database.
	// If set, it is converted into a libsql-compatible DSN (file:<path>).
	Path string

	// URL is a libsql/Turso URL, e.g. libsql://your-db.turso.io.
	URL string

	// AuthToken is appended to URL-based DSNs as authToken=... when not already present.
	AuthToken string

	// SynchronousFull requests PRAGMA synchronous=FULL on the local connection,
	// set and verified rather than inherited from the driver default. It is used
	// by durability-authoritative stores (the reflow checkpoint store) whose
	// terminal state is the resume authority; leave it false for rebuildable
	// stores (index builds) that tolerate the driver default under WAL.
	SynchronousFull bool
}

// OpenLocalReadOnly opens an existing local SQLite database without creating
// parent directories, changing journal mode, running migrations, or creating
// SQLite sidecars. The connection is bound to a no-follow file handle before
// SQLite sees it, so a pathname swap cannot substitute unverified metadata.
// Callers must reject transaction sidecars before and after this inspection.
func OpenLocalReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	return openLocalReadOnly(ctx, path, nil)
}

func openLocalReadOnly(ctx context.Context, path string, afterBind func() error) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("existing local index store path is required")
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("resolve index store path: %w", err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("inspect index store: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("index store must be an existing regular file")
	}
	bound, driverPath, err := openBoundSQLiteSnapshotFile(abs)
	if err != nil {
		return nil, fmt.Errorf("bind read-only index store: %w", err)
	}
	defer func() { _ = bound.Close() }()
	if afterBind != nil {
		if err := afterBind(); err != nil {
			return nil, fmt.Errorf("after binding read-only index store: %w", err)
		}
	}

	uriPath := filepath.ToSlash(driverPath)
	if filepath.VolumeName(driverPath) != "" && !strings.HasPrefix(uriPath, "/") {
		// SQLite requires a rooted URI path for Windows drive-letter names;
		// file:C:/... is parsed as an invalid URI authority.
		uriPath = "/" + uriPath
	}
	dsnURL := &url.URL{Scheme: "file", Path: uriPath}
	query := dsnURL.Query()
	query.Set("mode", "ro")
	query.Set("immutable", "1")
	dsnURL.RawQuery = query.Encode()

	db, err := sql.Open(snapshotSQLiteDriver, dsnURL.String())
	if err != nil {
		return nil, fmt.Errorf("open read-only index store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping read-only index store: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enforce read-only index store: %w", err)
	}
	return db, nil
}

// OpenLocalMutableCanonical opens an existing local SQLite file through its
// concrete canonical pathname. SQLite therefore uses that name for its lock,
// WAL, and shared-memory namespace. A per-connection VFS attests SQLite's exact
// main-file handle against bound and revalidates the callback and main path at
// SQLite access and I/O boundaries. This ordinary writer path requires
// transaction sidecars to have been absent at caller validation; VFS
// registration reasserts that absence and will not adopt an intervening WAL,
// SHM, rollback, master, or statement journal. The VFS exclusively reserves
// each new sidecar from the retained main path plus known suffixes (WAL and
// rollback journal from open-type flags; SHM before xShmMap) through a retained
// no-follow directory binding, then requires SQLite's exact handle to match
// that reservation. Exact-epoch cleanup captures the bound object before
// unlinking it. The VFS remains installed until the connection closes.
func OpenLocalMutableCanonical(ctx context.Context, path string, bound *os.File, beforeMutation func() error) (*sql.DB, error) {
	return openLocalMutableCanonical(ctx, path, bound, beforeMutation, mutableCanonicalOpenHooks{})
}

type mutableCanonicalOpenHooks struct {
	beforeDriverOpen                     func() error
	afterVFSRegistrationBeforeDriverOpen func() error
	afterDriverOpen                      func() error
	afterConnectionAttestation           func() error
	afterAuthorityCheckBeforeSQLite      func() error
	beforeSidecarReservation             func(path string) error
	// beforeExactEpochRemoval runs after directory revalidation and before the
	// atomic capture/rename of a reserved transaction artifact. Tests use it to
	// prove substituted epochs are refused and preserved.
	beforeExactEpochRemoval func(path string) error
	// afterExactEpochCapture runs after the live name has been rename-captured
	// into the quarantine entry and before open/attestation. Tests use it to
	// recreate the canonical name before mismatch restore.
	afterExactEpochCapture func(path, quarantine string) error
	// afterExactEpochAttest runs after the captured object has been opened and
	// attested as the reserved epoch, and before exact-object destruction.
	// Tests use it to replace the quarantine name after attestation.
	afterExactEpochAttest func(path, quarantine string) error
}

// resolveCanonicalSQLitePath returns a concrete absolute pathname for an
// existing index database. Intermediate directory symlinks are resolved so the
// DSN and VFS authority path match the names modernc SQLite constructs for
// WAL/journal sidecars (notably macOS /var vs /private/var).
func resolveCanonicalSQLitePath(path string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(strings.TrimSpace(path)))
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

func openLocalMutableCanonical(ctx context.Context, path string, bound *os.File, beforeMutation func() error, hooks mutableCanonicalOpenHooks) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("existing canonical local index store path is required")
	}
	if beforeMutation == nil {
		return nil, errors.New("canonical local index store revalidation callback is required")
	}
	if bound == nil {
		return nil, errors.New("canonical local index store retained binding is required")
	}
	// Resolve intermediate directory symlinks (for example macOS /var ->
	// /private/var) so the DSN, VFS authority path, and SQLite's sidecar names
	// share one concrete pathname identity.
	abs, err := resolveCanonicalSQLitePath(path)
	if err != nil {
		return nil, fmt.Errorf("resolve canonical index store path: %w", err)
	}
	uriPath := filepath.ToSlash(abs)
	if filepath.VolumeName(abs) != "" && !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	dsnURL := &url.URL{Scheme: "file", Path: uriPath}
	query := dsnURL.Query()
	query.Set("mode", "rw")
	dsnURL.RawQuery = query.Encode()
	dsn := dsnURL.String()

	connector := newMutableCanonicalConnector(dsn, abs, bound, beforeMutation, hooks)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, errors.Join(fmt.Errorf("open and attest canonical index store: %w", err), connector.failure())
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, errors.Join(fmt.Errorf("ping attested canonical index store: %w", err), connector.failure())
	}
	if err := configureLocalSQLiteConn(ctx, conn, dsn); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, errors.Join(err, connector.failure())
	}
	if err := conn.Close(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("release attested canonical index store connection: %w", err)
	}
	return db, nil
}

// ValidateCurrentSchemaReadOnly verifies that db has exactly the schema this
// binary understands. It never upgrades older schemas; callers must retain
// those artifacts until an explicit migration operation is authorized.
func ValidateCurrentSchemaReadOnly(ctx context.Context, db *sql.DB) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return errors.New("db is nil")
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT schema_version FROM schema_meta WHERE id=1`).Scan(&version); err != nil {
		return fmt.Errorf("read schema_version without migration: %w", err)
	}
	if version != SchemaVersion {
		return fmt.Errorf("index schema version %d is not current version %d", version, SchemaVersion)
	}
	return nil
}

func buildDSN(cfg Config) (string, error) {
	if u := strings.TrimSpace(cfg.URL); u != "" {
		return addAuthToken(u, cfg.AuthToken)
	}

	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return "", errors.New("index store path or url is required")
	}
	if path == ":memory:" {
		return path, nil
	}

	if strings.HasPrefix(path, "file:") || strings.HasPrefix(path, "libsql:") {
		if strings.HasPrefix(path, "file:") {
			localPath, err := extractFilePath(path)
			if err != nil {
				return "", err
			}
			if err := ensureStoreDir(localPath); err != nil {
				return "", err
			}
		}
		return path, nil
	}

	if err := ensureStoreDir(path); err != nil {
		return "", err
	}

	return "file:" + filepath.Clean(path), nil
}

func addAuthToken(dsn string, token string) (string, error) {
	if strings.TrimSpace(token) == "" {
		return dsn, nil
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid store url: %w", err)
	}

	query := parsed.Query()
	if query.Get("authToken") == "" {
		query.Set("authToken", token)
		parsed.RawQuery = query.Encode()
	}

	return parsed.String(), nil
}

func extractFilePath(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("invalid store path: %w", err)
	}

	if parsed.Path != "" {
		return strings.TrimPrefix(parsed.Path, "//"), nil
	}

	return strings.TrimPrefix(parsed.Opaque, "//"), nil
}

// sqliteSynchronousFull is the numeric PRAGMA synchronous value for FULL.
const sqliteSynchronousFull = 2

func configureLocalSQLite(ctx context.Context, db *sql.DB, dsn string, synchronousFull bool) error {
	if db == nil {
		return errors.New("store connection is nil")
	}
	if dsn == ":memory:" || !strings.HasPrefix(dsn, "file:") {
		// A non-local target (in-memory or a remote/URL DSN) cannot carry the
		// local WAL+FULL guarantee. A rebuildable store tolerates that and is
		// left as-is; a durability-authoritative store must fail closed rather
		// than silently claim a guarantee it did not receive. The DSN is not
		// echoed — URL DSNs may carry an auth token.
		if synchronousFull {
			return errors.New("synchronous=FULL durability requires a local file: SQLite target")
		}
		return nil
	}

	// Keep a single connection and use WAL to reduce lock contention.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout=5000").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}

	if !synchronousFull {
		return nil
	}

	// Durability-authoritative store: set synchronous=FULL and verify the
	// resolved durability configuration rather than trusting the driver
	// default. synchronous is connection-local; the single held connection
	// (SetMaxOpenConns(1)) carries it, and reopen re-runs this verification.
	if _, err := db.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return fmt.Errorf("set synchronous=FULL: %w", err)
	}
	var syncLevel int
	if err := db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&syncLevel); err != nil {
		return fmt.Errorf("read synchronous: %w", err)
	}
	if syncLevel != sqliteSynchronousFull {
		return fmt.Errorf("synchronous not FULL after set: got %d, want %d", syncLevel, sqliteSynchronousFull)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("journal_mode not WAL after set: got %q", journalMode)
	}
	if busyTimeout != 5000 {
		return fmt.Errorf("busy_timeout not 5000 after set: got %d", busyTimeout)
	}
	return nil
}

func configureLocalSQLiteConn(ctx context.Context, conn *sql.Conn, dsn string) error {
	if conn == nil {
		return errors.New("store connection is nil")
	}
	if dsn == ":memory:" || !strings.HasPrefix(dsn, "file:") {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var busyTimeout int
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout=5000").Scan(&busyTimeout); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}
	var journalMode string
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}
	return nil
}

func ensureStoreDir(path string) error {
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return nil
	}

	dir := filepath.Dir(filepath.Clean(path))
	if dir == "." || dir == string(filepath.Separator) {
		return nil
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create store directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil { // #nosec G302 -- directories require owner execute permission
		return fmt.Errorf("chmod store directory: %w", err)
	}
	return nil
}
