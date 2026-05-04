# Gonimbus – AI Agent Guide

**Project**: gonimbus
**Purpose**: Cloud object storage crawl/inspect engine (library + CLI + server)
**Maintainers**: See `MAINTAINERS.md`

## Operating Model

| Aspect   | Setting                                  |
| -------- | ---------------------------------------- |
| Mode     | Supervised (human reviews before commit) |
| Role     | devlead (default)                        |
| Identity | Per session (no persistent memory)       |

See [3leaps-crucible agent-identity standard](https://crucible.3leaps.dev/repository/agent-identity) for operating modes and attribution patterns.

## Read First

1. **Confirm your role.** Roles are defined in [`config/agentic/roles/`](config/agentic/roles/). Default to `devlead` if unspecified.
2. **Check `AGENTS.local.md`** if it exists (gitignored) for machine-specific instructions, credential guidance, and tactical session overrides. This file is the final authority on local environment configuration.
3. **Read `MAINTAINERS.md`** for human maintainer contacts.
4. **Read files before editing them.**

## Quick Reference

| Task           | Command          | Notes                 |
| -------------- | ---------------- | --------------------- |
| Quality checks | `make check-all` | Run before committing |
| Tests          | `make test`      | Must pass             |
| Format         | `make fmt`       | Auto-format code      |
| Build          | `make build`     | Build binary          |
| Run            | `make run`       | Start dev server      |

## Role-Based Development

Agents operate in role contexts. Each role has defined scope, mindset, and escalation paths.

Full role definitions live in [`config/agentic/roles/`](config/agentic/roles/) as YAML files following the [crucible role-prompt schema](https://schemas.3leaps.dev/agentic/v0/role-prompt.schema.json). When assigned a role, read its YAML file and constrain actions to that scope.

### Catalog Roles

| Role                                                     | Focus                                               |
| -------------------------------------------------------- | --------------------------------------------------- |
| [`devlead`](config/agentic/roles/devlead.yaml)           | Core implementation, CLI, integration               |
| [`devrev`](config/agentic/roles/devrev.yaml)             | Code review, four-eyes audit                        |
| [`dataeng`](config/agentic/roles/dataeng.yaml)           | Pipeline operations, manifests, integration testing |
| [`qa`](config/agentic/roles/qa.yaml)                     | Testing, validation, coverage                       |
| [`secrev`](config/agentic/roles/secrev.yaml)             | Security analysis, vulnerabilities                  |
| [`releng`](config/agentic/roles/releng.yaml)             | Releases, versioning, CI/CD validation              |
| [`deliverylead`](config/agentic/roles/deliverylead.yaml) | Sprint planning, delivery coordination              |
| [`cloudarch`](config/agentic/roles/cloudarch.yaml)       | High-scale cloud architecture, ADRs                 |
| [`infoarch`](config/agentic/roles/infoarch.yaml)         | Documentation, schemas                              |
| [`prodmktg`](config/agentic/roles/prodmktg.yaml)         | Use cases, personas, value messaging                |

### Gonimbus-Specific Roles (Inline)

These roles are specific to the gonimbus domain and defined here rather than in YAML files:

#### provider – Provider & Auth

- **Scope**: Provider implementations (S3-compatible, GCS, Azure Blob Storage), auth chains, SDK integration
- **Responsibilities**: Provider interface, credential handling, endpoint configuration
- **Escalates to**: devlead for API design decisions

#### crawler – Crawl Engine

- **Scope**: Crawl engine, pattern matching, prefix derivation, outputs
- **Responsibilities**: Pipeline implementation, JSONL writer, backpressure, rate limiting
- **Escalates to**: devlead for architecture decisions

**Gonimbus-specific context**: This tool solves a narrow but incredibly valuable problem for users with GIGANTIC buckets (10M+ objects). The prodmktg role articulates use cases like "retail transaction data acquisition" where date is buried below site, indexes enable instant queries, and streaming enables content-aware reorganization.

## Session Protocol

### Startup

1. Read `AGENTS.local.md` if it exists (gitignored; machine-specific instructions, credential guidance, local overrides)
2. Identify your role from context or request assignment; read the role YAML from `config/agentic/roles/`
3. Scan relevant code before making changes

### Before Committing

1. Run quality gates: `make check-all`
2. Verify tests pass
3. Stage all modified files
4. Use proper attribution format

### Escalation

Escalate to maintainers (see `MAINTAINERS.md`) for:

- Releases and version tags
- Breaking changes
- Security concerns
- Architectural decisions

## Commit Attribution

**MANDATORY** — all AI-assisted commits must use the exact format below. No exceptions.

### Why `noreply@3leaps.net`

AI model providers ship default Co-Authored-By emails like `noreply@anthropic.com`. A GitHub user has associated such an email with their account, causing them to appear as a contributor on any repository that uses that default attribution. This is email squatting — it creates false contributor provenance.

We use `noreply@3leaps.net` (a domain we control) to eliminate this attack vector entirely. **Never use `noreply@anthropic.com`, `noreply@openai.com`, or any other model provider email in Co-Authored-By lines.**

### Format

```
<type>(<scope>): <subject>

<body>

Changes:
- Specific change 1
- Specific change 2

Generated by <Model> via <Interface> under supervision of @3leapsdave

Co-Authored-By: <Model> <noreply@3leaps.net>
Role: <role>
Committer-of-Record: Dave Thompson <dave.thompson@3leaps.net> [@3leapsdave]
```

### Rules

1. **`Co-Authored-By` email MUST be `noreply@3leaps.net`** — never a model provider domain
2. **`<Model>` is the specific model name** (e.g., `Claude Opus 4.6`, `Claude Sonnet 4.6`) — not just `Claude`
3. **`<Interface>` is the tool used** (e.g., `Claude Code`, `API`)
4. **`Committer-of-Record` identifies the human** who supervised and approved the commit
5. **`Role` matches the active role slug** from the role catalog

### Example

```
feat(provider): add S3 provider with default auth chain

Implements S3 provider using AWS SDK v2 default configuration.

Changes:
- Add pkg/provider/s3/provider.go with List/Head operations
- Support custom endpoints for S3-compatible stores
- Wire default credential chain (env, profiles, assume-role)

Generated by Claude Opus 4.6 via Claude Code under supervision of @3leapsdave

Co-Authored-By: Claude Opus 4.6 <noreply@3leaps.net>
Role: provider
Committer-of-Record: Dave Thompson <dave.thompson@3leaps.net> [@3leapsdave]
```

## DO / DO NOT

### DO

- Run `make check-all` before commits
- Read files before editing them
- Keep changes minimal and focused
- Test manually when changing CLI behavior
- Document decisions in code comments or docs/

### DO NOT

- Push without maintainer approval
- Skip quality gates
- Commit secrets or credentials
- Reference client data, paths, or identifiers in repo content
- Create unnecessary files
- Touch code outside your task scope

## References

- `AGENTS.local.md` - Machine-specific instructions (gitignored; read if present)
- `MAINTAINERS.md` - Human maintainers
- `README.md` - Project overview
- `docs/provenance.md` - Ecosystem heritage
- `docs/architecture.md` - Component design
- `config/agentic/roles/` - Role catalog (YAML definitions with schemas, checklists, examples)

### Planning artifacts (no longer in this repo)

- **Feature briefs / stream board**: `~/dev/3leaps/3leaps-productbook-internal/content/projmgmt/gonimbus/` (private; GON-NNN board + briefs)
- **Client-touching dogfood narrative**: see Lead Maintainer for path (never referenced from this repo)
- The previous `.plans/` convention has been retired

### Upstream

- [crucible/config/agentic/roles/](https://github.com/3leaps/crucible/tree/main/config/agentic/roles) - Baseline role definitions
- [crucible agent-identity standard](https://crucible.3leaps.dev/repository/agent-identity) - Operating modes and attribution
