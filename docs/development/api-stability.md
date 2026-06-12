# API Stability Gate

GON-039 adds a soft API acknowledgement gate for the Stable embedded library
surface documented in `docs/api-stability.md`.

## Tooling Choice

The gate uses a small repository-local Go checker at
`internal/tools/apistability`. It uses only the Go standard library:

- `go/parser` and `go/ast` extract exported symbols from Stable packages.
- `git archive <tag>` provides the baseline tree for the latest release tag.
- `CHANGELOG.md` is parsed for an `Unreleased` `Library API`
  acknowledgement, or for the section matching the current `VERSION` during a
  release-prep version cut.

This avoids adding a new external tool dependency while still enforcing the
protocol in local checks and CI. The check is intentionally soft: it does not
forbid a Stable API break, but it requires a deliberate changelog entry before
the break can pass CI.

## Local Command

```bash
make api-stability
```

To compare against a specific release tag instead of the latest `v*` tag:

```bash
GONIMBUS_API_BASE_TAG=v0.2.2 make api-stability
```

## Changelog Acknowledgement

When a Stable exported API changes, add this section under `## [Unreleased]`:

```markdown
### Library API

- Breaking: describe the Stable package change and the migration path.
```

The checker treats a non-empty `### Library API` subsection under `Unreleased`
as an acknowledgement. During a release-prep version cut, it also accepts the
section matching the current `VERSION` after the release entry has been cut from
`Unreleased`. General prose elsewhere in the changelog does not waive the gate.

## CI Behavior

The CI workflow runs `make api-stability` after formatting and before lint/test.
Pull requests that change Stable exported symbols without a `Library API`
changelog entry fail with the changed symbols listed in the job log. The same
change passes once the entry is present. Experimental package changes are
outside this diff gate.
