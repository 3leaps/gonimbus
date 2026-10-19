# vX.Y.Z Release Notes

This file is the GitHub release body. `make release-create-draft` uploads it
verbatim from the tagged commit, so write it for someone installing the
release.

## Summary

Short operator-focused summary for the release.

## Highlights

- ...

## Library API

Include this section when the release changes the Stable embedded library API
listed in `docs/api-stability.md`. For a Stable break, describe the break, the
migration path, and the advance-notice status. Omit this section when there are
no Stable API changes.

## Release provenance

- Release tags are GPG-signed and verify against
  [`docs/security/release-signing-keys.asc`](../security/release-signing-keys.asc)
  (fingerprints in [`keys/expected-fingerprints.txt`](../../keys/expected-fingerprints.txt)).
  Verify with `git tag -v vX.Y.Z` after importing that key.
- Checksum manifests are signed with minisign and PGP; the published assets are
  byte-identical to the signed set.

## Install

```bash
brew install 3leaps/tap/gonimbus                      # macOS Apple Silicon, Linux
go install github.com/3leaps/gonimbus/cmd/gonimbus@vX.Y.Z
```

```powershell
scoop bucket add 3leaps https://github.com/3leaps/scoop-bucket
scoop install gonimbus
```

## Verification

- ...

See [CHANGELOG.md](../../CHANGELOG.md) for the full list.
