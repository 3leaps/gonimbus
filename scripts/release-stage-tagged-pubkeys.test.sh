#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Public keys and fingerprint anchors stage from git cat-file of the tagged
# commit, byte-exact, verified by Decernor, and isolated from GIT_* redirects
# and replacement refs.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
bin="${GONIMBUS_DECERNOR_BIN:-$(command -v decernor || true)}"
[[ -n "$bin" && -x "$bin" && "$bin" == /* && ! -L "$bin" ]] || {
    echo 'error: GONIMBUS_DECERNOR_BIN required' >&2
    exit 1
}
export GONIMBUS_DECERNOR_BIN="$bin"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

repo="$scratch/repo"
mkdir -p "$repo/docs/security" "$repo/keys" "$repo/scripts" "$scratch/dest"
cp "$root/docs/security/gonimbus-minisign.pub" "$repo/docs/security/gonimbus-minisign.pub"
cp "$root/docs/security/release-signing-keys.asc" "$repo/docs/security/release-signing-keys.asc"
cp "$root/keys/expected-fingerprints.txt" "$repo/keys/expected-fingerprints.txt"
cp "$root/keys/expected-fingerprints.ndjson" "$repo/keys/expected-fingerprints.ndjson"
cp "$root/scripts/release-stage-tagged-pubkeys.sh" "$root/scripts/release-decernor.sh" "$repo/scripts/"
chmod +x "$repo/scripts/release-stage-tagged-pubkeys.sh"

git init -q -b main "$repo"
git -C "$repo" config user.name Fixture
git -C "$repo" config user.email fixture@example.invalid
git -C "$repo" config commit.gpgsign false
git -C "$repo" add docs/security keys scripts
git -C "$repo" commit -qm 'initial'
git -C "$repo" tag -am 'Release v1.2.3' v1.2.3
git -C "$repo" tag -am 'Leading zero tag' v01.2.3
git -C "$repo" tag lightweight-v1.2.3

# Mutate the working tree and HEAD after the tag: staging must still read the tag.
printf 'untrusted comment: wrong working tree key\nRWRXUk9ORzE=\n' > "$repo/docs/security/gonimbus-minisign.pub"
printf -- '-----BEGIN PGP PUBLIC KEY BLOCK-----\nwrong\n-----END PGP PUBLIC KEY BLOCK-----\n' > "$repo/docs/security/release-signing-keys.asc"
printf 'gpg 0000000000000000000000000000000000000000\nminisign 0000000000000000000000000000000000000000000000000000000000000000\n' > "$repo/keys/expected-fingerprints.txt"
git -C "$repo" commit -qam 'later commit with different keys'

stage() { (cd "$scratch" && "$repo/scripts/release-stage-tagged-pubkeys.sh" "$@") > /dev/null 2>&1; }
refused() {
    local why="$1"
    shift
    if stage "$@"; then
        echo "error: accepted $why" >&2
        exit 1
    fi
}

# Even when invoked outside $repo and with hostile GIT_DIR / GIT_WORK_TREE /
# GIT_REPLACE_REF_BASE / GONIMBUS_MINISIGN_PUB in the environment, the script
# binds to $repo and stages the exact tagged commit blobs.
GIT_DIR="$scratch/nonexistent-git-dir" \
    GIT_WORK_TREE="$scratch/nonexistent-worktree" \
    GIT_OBJECT_DIRECTORY="$scratch/nonexistent-objects" \
    GIT_REPLACE_REF_BASE="refs/replace/" \
    GONIMBUS_MINISIGN_PUB="$repo/docs/security/gonimbus-minisign.pub" \
    stage v1.2.3 "$scratch/dest"

cmp -s "$root/docs/security/gonimbus-minisign.pub" "$scratch/dest/gonimbus-minisign.pub" || {
    echo 'error: staged minisign pubkey did not come from the tagged commit' >&2
    exit 1
}
cmp -s "$root/docs/security/release-signing-keys.asc" "$scratch/dest/gonimbus-release-signing-key.asc" || {
    echo 'error: staged PGP pubkey did not come from the tagged commit' >&2
    exit 1
}
cmp -s "$root/keys/expected-fingerprints.txt" "$scratch/dest/expected-fingerprints.txt" || {
    echo 'error: staged expected-fingerprints.txt did not come from the tagged commit' >&2
    exit 1
}
cmp -s "$root/keys/expected-fingerprints.ndjson" "$scratch/dest/expected-fingerprints.ndjson" || {
    echo 'error: staged expected-fingerprints.ndjson did not come from the tagged commit' >&2
    exit 1
}

# Replacement refs on the pin blob or on the tagged commit must not alter the
# staged public key bytes.
pub_blob="$(git -C "$repo" --no-replace-objects rev-parse v1.2.3:docs/security/gonimbus-minisign.pub)"
replacement_blob="$(printf 'untrusted comment: replaced\nRWTreplacedkeyblobAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n' | git -C "$repo" hash-object -w --stdin)"
git -C "$repo" replace "$pub_blob" "$replacement_blob"
stage v1.2.3 "$scratch/dest"
cmp -s "$root/docs/security/gonimbus-minisign.pub" "$scratch/dest/gonimbus-minisign.pub" || {
    echo 'error: staging followed a git replace ref' >&2
    exit 1
}
git -C "$repo" replace -d "$pub_blob" > /dev/null

# Pre-existing symlink in dest must be refused rather than written through.
ln -sf "$scratch/symlink-target" "$scratch/dest/gonimbus-minisign.pub"
refused 'destination symlink' v1.2.3 "$scratch/dest"
rm -f "$scratch/dest/gonimbus-minisign.pub"

# Missing, symlink, relative, or spoofed (exit 0 without ceremony version)
# GONIMBUS_DECERNOR_BIN must fail before any files are staged into dest.
mkdir -p "$scratch/dest-baddec"
ln -sf "$bin" "$scratch/symlink-decernor"
printf '#!/usr/bin/env bash\nexit 0\n' > "$scratch/spoof-decernor"
chmod +x "$scratch/spoof-decernor"
for bad_dec in "$scratch/missing-decernor" "$scratch/symlink-decernor" "relative/decernor" "$scratch/spoof-decernor"; do
    if (cd "$scratch" && GONIMBUS_DECERNOR_BIN="$bad_dec" "$repo/scripts/release-stage-tagged-pubkeys.sh" v1.2.3 "$scratch/dest-baddec") > /dev/null 2>&1; then
        echo "error: accepted untrusted GONIMBUS_DECERNOR_BIN=$bad_dec" >&2
        exit 1
    fi
    if [[ -n "$(find "$scratch/dest-baddec" -mindepth 1 -maxdepth 1 -print)" ]]; then
        echo "error: staged files into dest before GONIMBUS_DECERNOR_BIN validation ($bad_dec)" >&2
        exit 1
    fi
done

mkdir -p "$scratch/dest2"
refused 'leading-zero tag v01.2.3' v01.2.3 "$scratch/dest2"
refused 'non-semver tag' lightweight-v1.2.3 "$scratch/dest2"
refused 'missing tag' v9.9.9 "$scratch/dest2"

git -C "$repo" tag v1.2.4
refused 'lightweight vX.Y.Z tag' v1.2.4 "$scratch/dest2"

# A tag whose expected-fingerprints.txt disagrees with the public keys is refused by decernor.
git -C "$repo" tag -a v1.2.5 -m 'Bad anchors'
refused 'mismatched fingerprint anchors' v1.2.5 "$scratch/dest2"

# A tag whose minisign file contains secret material is refused.
printf 'untrusted comment: minisign secret key\nRWRTRUNSRVQ=\n' > "$repo/docs/security/gonimbus-minisign.pub"
git -C "$repo" commit -qam 'bad minisign'
git -C "$repo" tag -a v1.2.6 -m 'Bad minisign'
refused 'secret in minisign pub' v1.2.6 "$scratch/dest2"

echo '[ok] tagged public-key and fingerprint-anchor staging controls passed'
