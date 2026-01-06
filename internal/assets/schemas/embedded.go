// Package schemasassets provides embedded JSON schemas for standalone binary behavior.
//
// Schemas are embedded at compile time to ensure the CLI and library work
// correctly regardless of the working directory or installation location.
package schemasassets

import _ "embed"

// JobManifestSchema is the embedded job-manifest JSON schema.
//
// This allows manifest validation to work in installed binaries and library
// consumers without requiring the schema files to be present on disk.
//
//go:embed job-manifest.schema.json
var JobManifestSchema []byte

// TransferManifestSchema is the embedded transfer-manifest JSON schema.
//
// This allows transfer manifest validation to work in installed binaries and library
// consumers without requiring the schema files to be present on disk.
//
//go:embed transfer-manifest.schema.json
var TransferManifestSchema []byte
