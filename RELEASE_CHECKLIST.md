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
- [ ] Sensitive local data reviewed against `docs/architecture/adr/ADR-0005-sensitive-local-data-policy-conformance.md` and the [3 Leaps Sensitive Local Data Policy](https://github.com/3leaps/oss-policies/blob/main/SENSITIVE-LOCAL-DATA.md); none is stored inside the repo tree relying only on `.gitignore`.
- [ ] Application builds: `make build`
- [ ] Manual smoke tests completed:
  - [ ] `./bin/gonimbus version`
  - [ ] `./bin/gonimbus serve` (starts without errors)
  - [ ] `./bin/gonimbus health`
  - [ ] `./bin/gonimbus doctor`
  - [ ] Graceful shutdown (Ctrl+C)

### Documentation

- [ ] `README.md` reviewed and updated
- [ ] Feature documentation added to `docs/` (if applicable)
- [ ] CLI help text accurate

### Dependencies

- [ ] `go.mod` dependencies reviewed
- [ ] Local replace directives removed (switch to GitHub releases)
- [ ] Dependency versions finalized
- [ ] `go mod tidy` executed
- [ ] No security vulnerabilities in dependencies

## Release Preparation

### Version Updates

- [ ] Update VERSION file
- [ ] Update `.fulmen/app.yaml` version
- [ ] Sync embedded identity: `make sync-embedded-identity`
- [ ] Version sanity check: `GONIMBUS_RELEASE_TAG=v<version> make release-guard-signing-tag`
- [ ] Search for hardcoded version references

### Git Hygiene

- [ ] All changes committed
- [ ] Commit messages follow attribution standard
- [ ] No uncommitted changes: `git status` clean
- [ ] All commits have proper trailers
- [ ] Pre-push checks run: `make prepush`

### Final Validation

- [ ] Fresh clone test: Clone repo fresh, run `make build && make test`
- [ ] Integration tests pass
- [ ] Performance benchmarks acceptable (if applicable)

## Release Execution

### Release Artifacts & Signing

Follow the release distribution handoff:

1. **devlead** tags and pushes the release after clearance, then verifies CI.
2. **maintainer** runs the signing and upload ceremony on the MFA-gated host.
3. **devlead** publishes Homebrew and Scoop updates after uploaded assets are verified.

The signing ceremony follows the Fulmen "manifest-only" provenance pattern:

- Generate SHA256 + SHA512 manifests
- Sign manifests with minisign (primary) and optionally PGP
- Ship trust anchors (public keys) with the release

- [ ] Download CI-built artifacts and generate manifests:

  ```bash
  export GONIMBUS_RELEASE_TAG=v<version>

  make release-clean
  make release-download
  make release-checksums
  make release-verify-checksums
  ```

- [ ] Sign manifests (minisign required; PGP optional):

  ```bash
  export GONIMBUS_RELEASE_TAG=v<version>
  export GONIMBUS_MINISIGN_KEY=/path/to/gonimbus.key
  export GONIMBUS_MINISIGN_PUB=/path/to/gonimbus.pub
  export GONIMBUS_PGP_KEY_ID="security@fulmenhq.dev"   # optional
  export GONIMBUS_GPG_HOMEDIR=/path/to/gnupg-fulmenhq # required if PGP_KEY_ID set

  make release-sign
  ```

- [ ] Export public keys: `make release-export-keys`
- [ ] Verify exported keys are public-only: `make release-verify-keys`
- [ ] Verify signatures: `make release-verify-signatures`
- [ ] Copy release notes: `make release-notes RELEASE_TAG=v<version>`
- [ ] Upload provenance assets: `make release-upload`
- [ ] Preserve the package-manager URL and SHA256 block printed by `make release-upload`

### Tagging

- [ ] Confirm tag/version match: `GONIMBUS_RELEASE_TAG=v<version> make release-guard-signing-tag`
- [ ] Create annotated git tag: `git tag -a v<version> -m "Release v<version>"`
- [ ] Tag message includes brief release summary

### Publishing

- [ ] Push commits: `git push origin main`
- [ ] Push tag: `git push origin v<version>`
- [ ] Verify GitHub release appears
- [ ] Create GitHub Release notes

### Distribution

- [ ] Verify `go install github.com/3leaps/gonimbus/cmd/gonimbus@v<version>` works
- [ ] Update `3leaps/homebrew-tap` `Formula/gonimbus.rb` from the uploaded release asset URLs and SHA256 values:
  - [ ] macOS ARM64 asset present
  - [ ] Linux AMD64 asset present
  - [ ] Linux ARM64 asset present
  - [ ] Intel macOS omitted unless the upstream release publishes `gonimbus-darwin-amd64`
  - [ ] Formula license matches gonimbus (`Apache-2.0`)
- [ ] Verify Homebrew install resolves and runs: `brew install 3leaps/tap/gonimbus && gonimbus version`
- [ ] Update `3leaps/scoop-bucket` `bucket/gonimbus.json` from the uploaded release asset URLs and SHA256 values:
  - [ ] Windows AMD64 asset present
  - [ ] Windows ARM64 asset present
  - [ ] `checkver` and `autoupdate` follow bucket convention
- [ ] Verify Scoop install resolves and runs: `scoop install gonimbus && gonimbus version`
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
