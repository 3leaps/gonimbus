# Release verification

Official releases use a GPG-signed annotated Git tag and minisign-signed
SHA256SUMS and SHA512SUMS manifests. These are separate signatures.

Establish the expected GPG primary and exact signing subkey, and the minisign
public-blob fingerprint, through an independently trusted maintainer channel
before accepting downloaded public keys. A key distributed beside a signature
is not independent authentication.

The public pins are [`release-signing-keys.asc`](release-signing-keys.asc) and
[`gonimbus-minisign.pub`](gonimbus-minisign.pub). The tagger identity is
[`config/release/tagger-identity.txt`](../../config/release/tagger-identity.txt).
Tag verification reads these from the tagged commit as inert blobs, not from
the current checkout.

The text and NDJSON anchors in [`keys/`](../../keys/) use Decernor's
`openpgp-fingerprint-v1` primary and `minisign-public-blob-sha256-v1` schemes.
The minisign fingerprint covers the decoded public blob, not its comment or
whole file. Verify exported public material against both anchors with Decernor
0.1.8 or newer:

```bash
decernor fingerprint verify \
  --anchors keys/expected-fingerprints.txt \
  --anchors-ndjson keys/expected-fingerprints.ndjson \
  --gpg docs/security/release-signing-keys.asc \
  --minisign docs/security/gonimbus-minisign.pub
```

Then use the approved minisign public key to verify both manifests and check
every listed digest. Require the complete expected asset set; do not use
`--ignore-missing` for a full release verification.

Tag verification (`scripts/verify-pinned-tag.sh`) uses an isolated keyring and
requires the exact signing subkey and the committed tagger identity. GitHub
Verified with reason `valid` is an additional check. Tags created before the
pin was committed are not verified by it and are not re-signed.

The tag workflow has read-only permissions. It verifies the tag, builds the
release artifacts, and uploads them as a workflow artifact. It does not sign,
create a GitHub Release, or publish packages. A green tag workflow is a
consistency signal, not release authorization. Maintainers verify the remote
tag, object and commit from trusted checkout code before signing or
publication. Tag protection and the maintainer ceremony remain part of the
publication boundary.
