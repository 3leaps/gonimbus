#!/usr/bin/env bash
# Public keys stage from git cat-file of the tagged commit, byte-exact.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
repo="$scratch/repo"
git init -q -b main "$repo"
git -C "$repo" config user.name Fixture
git -C "$repo" config user.email fixture@example.invalid
git -C "$repo" config commit.gpgsign false
mkdir -p "$repo/docs/security" "$repo/scripts" "$scratch/dest"
printf 'untrusted comment: fixture minisign\nRWTfixturekeyblobAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n' \
    > "$repo/docs/security/gonimbus-minisign.pub"
cat > "$repo/docs/security/release-signing-keys.asc" << 'ASC'
-----BEGIN PGP PUBLIC KEY BLOCK-----

fixture
-----END PGP PUBLIC KEY BLOCK-----
ASC
cp "$root/scripts/release-stage-tagged-pubkeys.sh" "$repo/scripts/"
git -C "$repo" add docs/security scripts
git -C "$repo" commit -qm fixture
git -C "$repo" tag -am 'v1.2.3' v1.2.3
(cd "$repo" && ./scripts/release-stage-tagged-pubkeys.sh v1.2.3 "$scratch/dest") > /dev/null
cmp -s "$repo/docs/security/gonimbus-minisign.pub" "$scratch/dest/gonimbus-minisign.pub"
cmp -s "$repo/docs/security/release-signing-keys.asc" "$scratch/dest/gonimbus-release-signing-key.asc"
# A later working-tree edit must not change what staging produces: the command
# reads the tagged blob, not the dirty tree.
printf 'untrusted comment: dirty working tree\nRWTfixturekeyblobAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n' \
    > "$repo/docs/security/gonimbus-minisign.pub"
(cd "$repo" && ./scripts/release-stage-tagged-pubkeys.sh v1.2.3 "$scratch/dest") > /dev/null
git -C "$repo" show v1.2.3:docs/security/gonimbus-minisign.pub > "$scratch/tagged.pub"
cmp -s "$scratch/tagged.pub" "$scratch/dest/gonimbus-minisign.pub" || {
    echo 'error: staging followed the dirty working tree' >&2
    exit 1
}
# Replacement refs on the pin blob or on the tagged commit must not alter the
# staged public key bytes.
pub_blob="$(git -C "$repo" --no-replace-objects rev-parse v1.2.3:docs/security/gonimbus-minisign.pub)"
replacement_blob="$(printf 'untrusted comment: replaced\nRWTreplacedkeyblobAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n' | git -C "$repo" hash-object -w --stdin)"
git -C "$repo" replace "$pub_blob" "$replacement_blob"
(cd "$repo" && ./scripts/release-stage-tagged-pubkeys.sh v1.2.3 "$scratch/dest") > /dev/null
cmp -s "$scratch/tagged.pub" "$scratch/dest/gonimbus-minisign.pub" || {
    echo 'error: staging followed a git replace ref' >&2
    exit 1
}
git -C "$repo" replace -d "$pub_blob" > /dev/null
# Pre-existing symlink in dest must be refused rather than written through.
ln -sf "$scratch/symlink-target" "$scratch/dest/gonimbus-minisign.pub"
if (cd "$repo" && ./scripts/release-stage-tagged-pubkeys.sh v1.2.3 "$scratch/dest") > /dev/null 2>&1; then
    echo 'error: staging wrote through a destination symlink' >&2
    exit 1
fi
rm -f "$scratch/dest/gonimbus-minisign.pub"
echo '[ok] public keys stage byte-exact from the tagged commit'
