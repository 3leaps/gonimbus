// Package file implements the provider interface for local filesystem paths.
//
// Library consumers can use Config and New directly when they need a local
// provider for tests, examples, or transfer workflows. Config is an explicit
// per-instance value; the package does not read gonimbus CLI configuration or
// GONIMBUS_* environment variables as part of provider construction.
//
// # IfAbsent publication and reserved staging names
//
// Conditional IfAbsent puts stage the complete body under the destination
// directory with CreateTemp pattern "gonimbus-ifabsent-*.tmp", then publish
// with os.Link (atomic no-replace). The final object key is never opened for
// streaming under O_EXCL, so a hard interrupt mid-write cannot leave a visible
// partial final. Plain os.Rename is not used for IfAbsent (replace semantics).
//
// List omits only basenames that match the reserved staging shape
// gonimbus-ifabsent-<nonce>.tmp (non-empty nonce without '.' or path
// separators). Ordinary object keys that merely contain the token
// "gonimbus-ifabsent" (for example gonimbus-ifabsent-report.csv) remain
// listable. Callers must not rely on that reserved basename shape as a
// durable user key.
//
// See docs/library-consumers.md for the embedded-use contract shared by the
// supported library packages.
//
// API stability: Stable. Breaking changes to exported symbols or documented
// behavior follow the Library API protocol in docs/api-stability.md.
package file
