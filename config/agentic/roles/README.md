# Role Catalog

Baseline role prompts for AI agent sessions.

Forked from [fulmenhq/crucible](https://github.com/fulmenhq/crucible/tree/main/config/agentic/roles)
with gonimbus-specific adaptations. These files are an intentional fork, not an
auto-synced mirror: they carry local content and their own version line, and
upstream changes are adopted deliberately rather than pulled.

The `version` field is the only provenance handle the role schema provides
(`additionalProperties: false`, and the version pattern admits no build
metadata), so a gonimbus version is not comparable to an upstream version of
the same slug.

**Schema**: [`role-prompt.schema.json`](https://schemas.3leaps.dev/agentic/v0/role-prompt.schema.json)

## Available Roles

### Development & Engineering

| Role                                | Slug      | Category | Use When                       |
| ----------------------------------- | --------- | -------- | ------------------------------ |
| [Development Lead](devlead.yaml)    | `devlead` | agentic  | Building features, fixing bugs |
| [Development Reviewer](devrev.yaml) | `devrev`  | review   | Reviewing code changes         |
| [Quality Assurance](qa.yaml)        | `qa`      | review   | Test design, coverage analysis |
| [Security Review](secrev.yaml)      | `secrev`  | review   | Security-sensitive review      |
| [Data Engineering](dataeng.yaml)    | `dataeng` | agentic  | Pipeline operations, manifests |

### Release & Automation

| Role                               | Slug     | Category   | Use When                            |
| ---------------------------------- | -------- | ---------- | ----------------------------------- |
| [Release Engineering](releng.yaml) | `releng` | automation | Version bumps, changelogs, releases |

### Documentation & Content

| Role                                   | Slug       | Category | Use When                    |
| -------------------------------------- | ---------- | -------- | --------------------------- |
| [Information Architect](infoarch.yaml) | `infoarch` | agentic  | Documentation, schemas      |
| [Product Marketing](prodmktg.yaml)     | `prodmktg` | agentic  | README, messaging, personas |

### Governance & Architecture

| Role                               | Slug           | Category   | Use When                              |
| ---------------------------------- | -------------- | ---------- | ------------------------------------- |
| [Delivery Lead](deliverylead.yaml) | `deliverylead` | governance | Sprint planning, release coordination |
| [Cloud Architect](cloudarch.yaml)  | `cloudarch`    | governance | High-scale cloud architecture, ADRs   |

## Gonimbus-Specific Roles

These roles are defined inline in `AGENTS.md` (not as YAML files) because they are specific to the gonimbus domain:

| Role       | Focus                                                            |
| ---------- | ---------------------------------------------------------------- |
| `provider` | Cloud storage providers (S3-compatible, GCS, Azure), auth chains |
| `crawler`  | Crawl engine, matching, outputs, pipeline                        |

## Usage

Reference roles by slug in `AGENTS.md`. Default to `devlead` for most implementation work.

The `dataeng` role in gonimbus is extended with pipeline-operations context (manifest authoring, probe configs, production runs) beyond the baseline database/pipeline focus.

## References

- [fulmenhq/crucible roles](https://github.com/fulmenhq/crucible/tree/main/config/agentic/roles) - Fork origin for the baseline roles
- [3leaps/crucible roles](https://github.com/3leaps/crucible/tree/main/config/agentic/roles) - 3leaps collection roles (org-spanning roles live here)
- [crucible agent-identity standard](https://crucible.3leaps.dev/repository/agent-identity) - Operating modes and attribution
