# Release Checklist

Standard checklist for gonimbus releases to ensure consistency and quality.

## Pre-Release Phase

### Version Planning

- [ ] Feature briefs in the private release planning board marked done
- [ ] All planned features implemented and tested
- [ ] Breaking changes documented
- [ ] Migration guide written (if applicable)
- [ ] Version number decided (semantic versioning: MAJOR.MINOR.PATCH)

### Code Quality

- [ ] All tests passing: `make test`
- [ ] Code formatted: `make fmt`
- [ ] Lint checks clean: `make lint`
- [ ] Library API stability gate passing: `make api-stability`
- [ ] Public-surface hygiene and content checks passing: `make surface-scan`
- [ ] Release provenance and tooling suites passing: `make release-provenance-check` and `make release-tooling-test`
- [ ] Sensitive local data reviewed against `docs/architecture/adr/ADR-0005-sensitive-local-data-policy-conformance.md` and the [3 Leaps Sensitive Local Data Policy](https://github.com/3leaps/oss-policies/blob/main/SENSITIVE-LOCAL-DATA.md); none is stored inside the repo tree relying only on `.gitignore`.
- [ ] Application builds: `make build`
- [ ] Manual smoke tests completed:
  - [ ] `./bin/gonimbus version`
  - [ ] `./bin/gonimbus version --json && ./bin/gonimbus version --json --extended`
  - [ ] `./bin/gonimbus serve` (starts without errors)
  - [ ] `./bin/gonimbus health`
  - [ ] `./bin/gonimbus doctor`
  - [ ] Graceful shutdown (Ctrl+C)

### Documentation

- [ ] `README.md` reviewed and updated
- [ ] Feature documentation added to `docs/` (if applicable)
- [ ] CLI help text accurate
- [ ] Release notes authored consistently across all public release surfaces:
  - [ ] `CHANGELOG.md` has a section for the release version, with entries in
        the correct version section
  - [ ] `RELEASE_NOTES.md` has the current release summary for the GitHub
        release body and recent-release landing page
  - [ ] `docs/releases/v<version>.md` exists and carries the complete release
        narrative derived from the current release notes
  - [ ] `docs/releases/v<version>.md` follows `docs/releases/TEMPLATE.md`,
        including Release provenance and an Install section for this tag. It
        is the GitHub release body; `make release-preflight` refuses notes
        without install instructions for the tag.
  - [ ] The three surfaces agree on headline features, dates, upgrade guidance,
        and user-visible compatibility notes

### Dependencies

- [ ] `go.mod` dependencies reviewed
- [ ] Local replace directives removed (switch to GitHub releases)
- [ ] Dependency versions finalized
- [ ] `go mod tidy` executed
- [ ] No security vulnerabilities in dependencies

## Release Preparation

### Version Updates

- [ ] Update VERSION file
- [ ] Sync all embedded version copies from VERSION:
  - [ ] `make sync-app-version` (updates `.fulmen/app.yaml` **and**
        `internal/buildinfo/VERSION` — the latter is the embed checked by
        `internal/buildinfo` tests; `sync-embedded-identity` alone does not
        cover it)
  - [ ] `make sync-embedded-identity` (regenerates the embedded app identity
        under `internal/assets/appidentity/`)
- [ ] Version consistency guard: `make verify-app-version` (also runs as part
      of `make prepush`)
- [ ] Version sanity check: `GONIMBUS_RELEASE_TAG=v<version> make release-guard-signing-tag`
- [ ] Search for hardcoded version references

### Git Hygiene

- [ ] All changes committed
- [ ] Commit messages follow attribution standard
- [ ] No uncommitted changes: `git status` clean
- [ ] All commits have proper trailers
- [ ] Pre-push and final quality gates run: `make check-all`, `make prepush`, and `make pr-final`

### Final Validation

- [ ] Fresh clone test: Clone repo fresh, run `make build && make test`
- [ ] Integration tests pass
- [ ] Performance benchmarks acceptable (if applicable)

## Release Execution

### Tagging (maintainer machine)

The tag is signed by `3 Leaps Infosec Team <infosec@3leaps.net>` with the
pinned signing subkey (`config/release/tagger-identity.txt`,
`config/release/signing-subkey.txt`, `docs/security/release-signing-keys.asc`,
`docs/security/gonimbus-minisign.pub`, and `keys/expected-fingerprints.{txt,ndjson}`).
Set in the shell, never in the repo: `GONIMBUS_RELEASE_TAG`,
`GONIMBUS_DECERNOR_BIN` (absolute path to `decernor >= 0.1.8`),
`GONIMBUS_TAG_MESSAGE_DIR` (outside the repository), `GONIMBUS_TAGGER_NAME`,
`GONIMBUS_TAGGER_EMAIL`, `GONIMBUS_GPG_SIGNING_FINGERPRINT` (primary),
`GONIMBUS_PGP_KEY_ID` (`<subkey>!` matching `config/release/signing-subkey.txt`),
`GONIMBUS_GPG_HOMEDIR` (isolated `0700` directory outside the repository, never
`~/.gnupg`), `GONIMBUS_RELEASE_ANCHOR_FILE` (absolute path outside the
repository, never a symlink), and `LIMENSAFE_CATALOG_PATH` (required by
`make release-preflight` for `make surface-scan-release`).

The tag comes only from `GONIMBUS_RELEASE_TAG`. Every `release-*` target refuses
to start while the generic `RELEASE_TAG` is set, even to an empty value, so
`unset RELEASE_TAG` first. Each target that downloads, drafts, signs, uploads or
publishes also requires the tag to match `VERSION`; after a version bump on
`main`, run the remaining steps from a checkout of the tagged commit.

- [ ] Remote GitHub tag-protection ruleset and immutable releases preflight: `make release-github-preflight`
- [ ] Pre-tag gate on the merged main commit (runs committed anchor verification, `make surface-scan-release`, workflow SHA-256 pin check against `config/release/release-workflow.sha256`, `make release-github-preflight`, content-check, dates check, outside-`$HOME` clean-clone test, and `make pr-final`): `make release-preflight`
- [ ] Prepare and review the external tag message: `make release-prepare-tag-message`
- [ ] Create and verify the local signed tag: `make release-tag`
- [ ] Publish and verify the tag (ruleset, remote object, GitHub `valid`): `make release-push-tag`
- [ ] Verify post-tag ceremony selector environment (`GONIMBUS_RELEASE_TAG`, primary fingerprint, and `<subkey>!` matching `config/release/signing-subkey.txt`): `make release-post-tag-selector`
- [ ] Verify the published remote tag and record the external ceremony anchor (`GONIMBUS_RELEASE_ANCHOR_FILE`): `make release-record-anchor`

### Release Artifacts & Signing

The tag workflow is read-only. It verifies the signed tag, builds, and uploads
`release-artifacts-v<version>` as a workflow artifact. It does not create the
GitHub Release. A green tag workflow is not release authorization.

- [ ] Download the CI artifacts for the verified tag and cross-check the recorded ceremony anchor (`GONIMBUS_RELEASE_ANCHOR_FILE`).
      Name the exact Release workflow run and its successful, latest attempt
      (`gh run list --workflow release.yml`); staging binds to that run's single
      `release-artifacts-v<version>` artifact by id and digest and refuses if a
      re-run appears during the download:

  ```bash
  export GONIMBUS_RELEASE_TAG=v<version>
  export GONIMBUS_RELEASE_ANCHOR_FILE=<abs-path-outside-repo>
  export RELEASE_RUN_ID=<run id> RELEASE_RUN_ATTEMPT=<attempt>
  make release-clean
  make release-download
  make release-verify-checksums
  ```

- [ ] Create the draft release from the verified local artifacts (body is
      `docs/releases/v<version>.md` from the tagged commit): `make release-create-draft`
- [ ] Sign the manifests (minisign and PGP both required; also set
      `GONIMBUS_MINISIGN_KEY`): `make release-sign`
- [ ] Stage and check public keys (`gonimbus-minisign.pub`, `gonimbus-release-signing-key.asc`) and Decernor fingerprint anchors (`expected-fingerprints.txt`, `expected-fingerprints.ndjson`) from the verified tagged commit: `make release-export-keys release-verify-keys`
- [ ] Verify signatures (both minisign and exact signing-subkey OpenPGP signatures on `SHA256SUMS` and `SHA512SUMS`): `make release-verify-signatures`
- [ ] Stage notes: `make release-notes`
- [ ] Upload provenance assets (`SHA256SUMS`, `SHA512SUMS`, `.minisig`, `.asc`, both public keys, both fingerprint anchor files, and `release-notes-v<version>.md`) to the draft: `make release-upload`
- [ ] Preserve the package-manager URL and SHA256 block printed by `make release-upload`
- [ ] Promote the draft after byte-verifying it against the local signed set, then re-download every published asset and assert GitHub immutable release state (`.draft == false` and `.immutable == true`): `make release-publish`
- [ ] Clean-room download and full verification of the published immutable GitHub release into `dist/published` (verifies `.draft == false`, `.immutable == true`, exact 16-asset set and digests, tagged-commit blob parity for public keys/anchors/notes, Decernor fingerprint verify, and minisign + OpenPGP signatures): `make release-stage-published`

### Distribution

**Raw-Asset Byte-for-Byte Packaging Invariant:**

1. **Raw-executable release contract:** Official `gonimbus` GitHub Releases publish unwrapped, standalone platform executables (`gonimbus-darwin-arm64`, `gonimbus-linux-amd64`, `gonimbus-linux-arm64`, `gonimbus-windows-amd64.exe`, `gonimbus-windows-arm64.exe`) listed directly in signed `SHA256SUMS` and `SHA512SUMS`.
2. **Byte-for-byte package-manager equivalence:** `3leaps/homebrew-tap` (`Formula/gonimbus.rb`, and any versioned `Formula/gonimbus@0.<minor>.rb`) and `3leaps/scoop-bucket` (`bucket/gonimbus.json`, and any versioned `bucket/gonimbus-0.<minor>.json`) **must install those exact release assets byte-for-byte** — no source rebuilds, no Homebrew bottles (`bottle do` / Mach-O header rewriting / re-signing), no wrapper scripts (`write_env_script`), and no archive repackaging.
3. **Consumer-visible change gate:** Replacing raw executable assets with `.tar.gz` / `.zip` archives (or introducing bottles/wrappers that alter installed binary bytes) is a **consumer-visible packaging change** that requires prior brief review and explicit announcement in release notes (`CHANGELOG.md`, `RELEASE_NOTES.md`, `docs/releases/v<version>.md`).

- [ ] Verify `go install github.com/3leaps/gonimbus/cmd/gonimbus@v<version>` works
- [ ] Update `3leaps/homebrew-tap` `Formula/gonimbus.rb` from the uploaded release asset URLs and SHA256 values:
  - [ ] macOS ARM64 asset present (raw `gonimbus-darwin-arm64` URL, no archive/bottle/wrapper)
  - [ ] Linux AMD64 asset present (raw `gonimbus-linux-amd64` URL, no archive/bottle/wrapper)
  - [ ] Linux ARM64 asset present (raw `gonimbus-linux-arm64` URL, no archive/bottle/wrapper)
  - [ ] Intel macOS omitted unless the upstream release publishes `gonimbus-darwin-amd64`
  - [ ] Formula license matches gonimbus (`Apache-2.0`)
- [ ] Verify Homebrew install resolves and runs: `brew install 3leaps/tap/gonimbus && gonimbus version`
- [ ] Update `3leaps/scoop-bucket` `bucket/gonimbus.json` from the uploaded release asset URLs and SHA256 values:
  - [ ] Windows AMD64 asset present (raw `gonimbus-windows-amd64.exe` URL, 2-element `bin` rename pair, no `extract_dir`)
  - [ ] Windows ARM64 asset present (raw `gonimbus-windows-arm64.exe` URL, 2-element `bin` rename pair, no `extract_dir`)
  - [ ] `checkver` and `autoupdate` follow bucket convention
- [ ] Verify Scoop install resolves and runs: `scoop install 3leaps/gonimbus && gonimbus version`
- [ ] Verify Go module proxy (`proxy.golang.org`, `sum.golang.org`, `pkg.go.dev`), `3leaps/homebrew-tap`, and `3leaps/scoop-bucket` against the staged published release (`dist/published`) and tagged commit: `make release-verify-registries`
- [ ] If cutting a new minor line (starting with `v0.5.0`), preserve the terminal patch release of the prior minor line as a pinned minor-series package (`Formula/gonimbus@0.4.rb` with `keg_only :versioned_formula` in `3leaps/homebrew-tap` and `bucket/gonimbus-0.4.json` in `3leaps/scoop-bucket`)
- [ ] Test CLI commands work correctly

## Post-Release

### Communication

- [ ] Announce release in the relevant maintainer release channel
- [ ] Notify gofulmen team if integration patterns changed

### Housekeeping

- [ ] Update the private release planning board for shipped briefs
- [ ] Plan next version features in the private planning system

### Monitoring

- [ ] Monitor GitHub issues for release-related bugs

## Version-Specific Checklists

### For Major Releases (x.0.0)

- [ ] Breaking changes documented with upgrade guide
- [ ] Deprecation warnings added to old APIs
- [ ] Migration scripts provided (if complex changes)

### For Minor Releases (0.x.0)

- [ ] New features documented with examples
- [ ] Integration tests cover new functionality
- [ ] **Minor-line retention rule (starting with `v0.5.0`):** Preserve the final patch release of the prior minor line as a pinned minor-series package (`Formula/gonimbus@0.<prior-minor>.rb` with `keg_only :versioned_formula` in `3leaps/homebrew-tap`, and `bucket/gonimbus-0.<prior-minor>.json` in `3leaps/scoop-bucket`) so consumers remaining on the prior minor line across a schema transition can still install its terminal build via package managers

### For Patch Releases (0.0.x)

- [ ] Bug fixes documented with issue references
- [ ] Regression tests added for fixed bugs
- [ ] Security patches highlighted (if applicable)
- [ ] No new features or breaking changes

## Emergency Hotfix Process

### Hotfix Identification

- [ ] Critical bug or security issue identified
- [ ] Severity assessed (production-impacting?)
- [ ] Hotfix branch created: `hotfix/v<version>`

### Rapid Development

- [ ] Minimal fix implemented
- [ ] Tests added for regression prevention
- [ ] Code review expedited (but not skipped)
- [ ] Quality gates still enforced (no shortcuts)

### Hotfix Release

- [ ] Version bumped (patch level)
- [ ] Tag pushed immediately after merge
- [ ] Users notified of critical update

### Post-Hotfix

- [ ] Root cause analysis documented
- [ ] Process improvements identified

## Notes

- This checklist may evolve with project maturity
- Some items may not apply to all releases (use judgment)
- Prioritize quality over speed - never skip tests or code review
- When in doubt, consult @3leapsdave before proceeding
