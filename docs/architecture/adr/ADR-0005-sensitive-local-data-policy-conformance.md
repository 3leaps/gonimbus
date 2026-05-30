# ADR-0005: Sensitive Local Data Policy Conformance

**Status:** Accepted
**Date:** 2026-05-29
**Decision Makers:** @3leapsdave

## Context

Gonimbus is an open-source data movement tool. Development and release work can
involve local validation inputs that are useful to maintainers but are not safe
for public repository history.

This is a cross-repository governance concern shared with other 3 Leaps
maintained open-source projects. The canonical policy lives outside this
repository so updates can apply consistently across projects:

- [3 Leaps Sensitive Local Data Policy](https://github.com/3leaps/oss-policies/blob/main/SENSITIVE-LOCAL-DATA.md)

## Decision

Gonimbus complies with the 3 Leaps Sensitive Local Data Policy.

Sensitive local data MUST live outside the repository working tree, including
feature worktrees. `.gitignore` is not a security boundary.

Tracked Gonimbus files may describe abstract placeholders and process
requirements, but they must not contain concrete sensitive values. Tooling that
later accepts file-backed sensitive inputs must reject paths that resolve inside
the repository tree and must not echo the sensitive content in its output.

## Consequences

- Gonimbus does not copy the full policy text; this ADR declares conformance
  and links to the canonical policy.
- `AGENTS.md`, selected role prompts, and `RELEASE_CHECKLIST.md` carry specific
  reminders so agents and release owners do not rely on a generic policy link.
- Future confidentiality tooling inherits this ADR: sensitive local inputs live
  out of tree and are configured through local-only process, never
  tracked-or-ignored in-repo files.
