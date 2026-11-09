#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Stage public key assets and fingerprint anchors from the tagged commit only
# (PDR-0008 §2.1). Never copies from the operator keyring, working tree, or an
# env-named file.
#
# Usage: release-stage-tagged-pubkeys.sh vX.Y.Z [dest_dir]
set -euo pipefail
# Read git objects as stored: no caller repository or object-store redirects
# (GIT_*), and no replacement refs.
for _git_var in $(compgen -e); do
    case "$_git_var" in
        GIT_COMMITTER_NAME | GIT_COMMITTER_EMAIL | GIT_AUTHOR_NAME | GIT_AUTHOR_EMAIL) ;;
        GIT_*) unset "$_git_var" ;;
    esac
done
unset _git_var
export GIT_NO_REPLACE_OBJECTS=1

die() {
    echo "error: $*" >&2
    exit 1
}

tag="${1:-}"
dest="${2:-dist/release}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    die 'usage: release-stage-tagged-pubkeys.sh vX.Y.Z [dest_dir]'
[[ -d "$dest" && ! -L "$dest" ]] || die "destination must be a directory: $dest"

root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/release-decernor.sh
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony || die 'trusted Decernor identity/version check failed'

[[ "$(git --no-replace-objects -C "$root" cat-file -t "refs/tags/$tag" 2> /dev/null || true)" == tag ]] ||
    die "annotated tag $tag not found locally"
commit="$(git --no-replace-objects -C "$root" rev-parse --verify --quiet "refs/tags/${tag}^{commit}")" ||
    die "cannot peel tagged commit $tag"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

extract_blob() {
    local path="$1" out="$2" mode
    mode="$(git --no-replace-objects -C "$root" ls-tree "$commit" -- "$path" | awk '{print $1}')"
    [[ "$mode" == 100644 ]] || die "committed $path required in $tag"
    git --no-replace-objects -C "$root" cat-file blob "$commit:$path" > "$out"
    [[ -s "$out" ]] || die "committed $path is empty in $tag"
}

extract_blob docs/security/gonimbus-minisign.pub "$work/gonimbus-minisign.pub"
extract_blob docs/security/release-signing-keys.asc "$work/gonimbus-release-signing-key.asc"
extract_blob keys/expected-fingerprints.txt "$work/expected-fingerprints.txt"
extract_blob keys/expected-fingerprints.ndjson "$work/expected-fingerprints.ndjson"

grep -q '^untrusted comment:' "$work/gonimbus-minisign.pub" ||
    die 'tagged gonimbus-minisign.pub is not a minisign public key'
if grep -qi 'secret' "$work/gonimbus-minisign.pub"; then
    die 'tagged gonimbus-minisign.pub contains secret material'
fi
grep -q '^-----BEGIN PGP PUBLIC KEY BLOCK-----$' "$work/gonimbus-release-signing-key.asc" ||
    die 'tagged release-signing-keys.asc is not an armored PGP public key'
if grep -q 'PRIVATE KEY BLOCK' "$work/gonimbus-release-signing-key.asc"; then
    die 'tagged release-signing-keys.asc contains private key material'
fi

"$RELEASE_DECERNOR_BIN" fingerprint verify \
    --anchors "$work/expected-fingerprints.txt" \
    --anchors-ndjson "$work/expected-fingerprints.ndjson" \
    --gpg "$work/gonimbus-release-signing-key.asc" \
    --minisign "$work/gonimbus-minisign.pub" > /dev/null ||
    die 'tagged identity files failed decernor fingerprint verify'

stage_verified_blob() {
    local name="$1" src_file="$work/$1" out="$dest/$1"
    [[ ! -L "$out" ]] || die "destination file must not be a symlink: $out"
    rm -f "$out"
    cp "$src_file" "$out"
    cmp -s "$src_file" "$out" || {
        rm -f "$out"
        die "staged $out is not byte-identical to $tag:$name"
    }
}

for asset in \
    gonimbus-minisign.pub \
    gonimbus-release-signing-key.asc \
    expected-fingerprints.txt \
    expected-fingerprints.ndjson; do
    [[ ! -L "$dest/$asset" ]] || die "destination file must not be a symlink: $dest/$asset"
done

for asset in \
    gonimbus-minisign.pub \
    gonimbus-release-signing-key.asc \
    expected-fingerprints.txt \
    expected-fingerprints.ndjson; do
    stage_verified_blob "$asset"
done

echo "[ok] staged public keys and fingerprint anchors from $tag ($commit) into $dest"
