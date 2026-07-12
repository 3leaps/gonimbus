package indexstore

import (
	"database/sql"

	sqlite "modernc.org/sqlite"
)

// snapshotSQLiteDriver is deliberately independent of driverLibsql. Strict
// local snapshot inspection must keep the same immutable SQLite semantics even
// when the general-purpose store is built with remote libsql support.
const snapshotSQLiteDriver = "gonimbus_snapshot_sqlite"

func init() {
	sql.Register(snapshotSQLiteDriver, &sqlite.Driver{})
}
