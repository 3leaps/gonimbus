# vX.Y.Z Release Notes

This file is the GitHub release body. `make release-create-draft` uploads it
verbatim from the tagged commit, so write it for someone installing the
release. Relative links do not resolve on a release page: link repository
files with tag-scoped URLs, `https://github.com/3leaps/gonimbus/blob/vX.Y.Z/<path>`.

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

- Before importing a downloaded public key, establish the expected GPG signing
  key and minisign public-key fingerprint through an independently trusted
  maintainer channel. Follow the complete
  [release-verification guide](https://github.com/3leaps/gonimbus/blob/vX.Y.Z/docs/security/README.md).
- Release tags are GPG-signed. After validating the public material against
  the committed anchors, verify with `git tag -v vX.Y.Z`.
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

See [CHANGELOG.md](https://github.com/3leaps/gonimbus/blob/vX.Y.Z/CHANGELOG.md) for the full list.
