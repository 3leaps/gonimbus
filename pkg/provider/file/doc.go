// Package file implements the provider interface for local filesystem paths.
//
// Library consumers can use Config and New directly when they need a local
// provider for tests, examples, or transfer workflows. Config is an explicit
// per-instance value; the package does not read gonimbus CLI configuration or
// GONIMBUS_* environment variables as part of provider construction.
//
// See docs/library-consumers.md for the embedded-use contract shared by the
// supported library packages.
package file
