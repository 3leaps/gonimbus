// Package indexreader provides a format-aware local index read seam.
//
// It dispatches on authoritative markers (sqlite-v1 | durable-v2) and exposes
// a narrow reader used by index query (and, later, stats/doctor/list). Durable
// reads stream verified segment rows; they do not hydrate into ephemeral
// SQLite.
//
// This package is Experimental. The durable-v2 path is an internal-render
// surface, not a publication or de-identification path.
package indexreader
