// Package indexbuild contains the Experimental embeddable workflow engine for
// durable index builds.
//
// The engine owns the v2 build pipeline over the internal journal/segment
// substrate: crawl with an injected provider, seal observation journals, compact
// sealed journals, publish immutable segments/manifests, write the completion
// marker, and advance the local latest pointer. Command packages remain adapters
// that parse operator input and construct typed Config values.
//
// Providers are injected as pkg/provider handles. This package does not import
// concrete provider packages, command packages, cobra/viper, or SQLite-backed
// index-store packages. The package is Experimental; see docs/api-stability.md.
package indexbuild
