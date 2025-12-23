---
title: "Accessing Crucible Documentation via gofulmen"
description: "How Workhorse template developers can read embedded Crucible docs, schemas, and config directly from gofulmen"
last_updated: "2025-12-17"
---

# Accessing Crucible Documentation via gofulmen

When you import `github.com/fulmenhq/gofulmen`, you automatically gain access to the Crucible single-source-of-truth (SSOT) assets that ship with gofulmen: documentation, schemas, and configuration catalog files. This guide shows how to read that embedded content so you can surface Fulmen ecosystem guidance inside your services without cloning the Crucible repository.

## Prerequisites

- gofulmen `v0.1.21` or newer in your `go.mod`
- Crucible `v0.2.21` assets (bundled transitively with the gofulmen release)
- Go 1.25+ (matches the current toolchain)

## Quick Start

```go
import "github.com/fulmenhq/gofulmen/crucible"

registry := crucible.GetRegistry()
docsFS := registry.DocsFS()
schemasFS := registry.SchemasFS()
configFS := registry.ConfigFS()
```

`DocsFS`, `SchemasFS`, and `ConfigFS` each expose an `embed.FS`. Use the standard library (`io/fs`) or helper functions in the `crucible` package to explore and read files.

## Reading Documentation Files

Paths are always relative to the Crucible root (no leading slash and no `docs/` prefix).

```go
package docs

import (
	"io/fs"

	"github.com/fulmenhq/gofulmen/crucible"
)

func ReadTechnicalManifesto() ([]byte, error) {
	registry := crucible.GetRegistry()
	content, err := fs.ReadFile(registry.DocsFS(), "fulmen-technical-manifesto.md")
	return content, err
}
```

### Listing Available Docs

```go
docs, err := crucible.ListDocs("")        // list everything
section, err := crucible.ListDocs("sop")  // list within docs/sop/
```

Filter the slice to locate the path you need (for example, `architecture/fulmen-ecosystem-guide.md`).

## Reading Schemas and Config

```go
schema, err := crucible.GetSchema("observability/logging/v1.0.0/log-event.schema.json")

library := crucible.ConfigRegistry.Library()
httpStatuses, err := library.Foundry().HTTPStatuses()
mimeTypes, err := library.Foundry().MIMETypes()
```

These helpers wrap the embedded FS so you can hydrate validators, reference data, or configuration defaults at runtime.

## Verifying Crucible Availability

```go
registry := crucible.GetRegistry()
if _, err := fs.ReadFile(registry.DocsFS(), "standards/coding/go.md"); err != nil {
	panic("crucible docs not embedded: " + err.Error())
}
```

For debugging, `crucible.ListDocs("")` can confirm which files shipped with your current gofulmen release.

## Common Paths

- `fulmen-technical-manifesto.md` – ecosystem manifesto referenced in project docs
- `architecture/fulmen-ecosystem-guide.md` – layer cake overview
- `standards/coding/go.md` – Fulmen Go coding standards
- `sop/repository-structure.md` – repository structure SOP

## Usage in This Template

- Prefer `crucible.GetDoc(<path>)` within CLI commands (`envinfo`, `doctor`) when you need to surface Fulmen guidance.
- Keep paths centralized (for example, constants in `internal/crucible/paths.go`) so upgrades to Crucible releases are easy to track.
- When adding developer docs, link back to relevant Crucible paths so team members know the canonical source.

## Further Reading

- gofulmen `crucible` package README: `github.com/fulmenhq/gofulmen/crucible`
- Fulmen Ecosystem Guide: accessible via `crucible.GetDoc("architecture/fulmen-ecosystem-guide.md")`
- Fulmen Technical Manifesto: accessible via `crucible.GetDoc("fulmen-technical-manifesto.md")`
