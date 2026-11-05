#!/usr/bin/env bash
# Stage public key assets from the tagged commit only (PDR-0008 §2.1).
# Never copies from the operator keyring or an env-named file.
set -euo pipefail
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
commit="$(git -C "$root" rev-parse --verify --quiet "${tag}^{commit}")" ||
    die "cannot peel tagged commit $tag"
stage_blob() {
    local path="$1" out="$2" mode expected
    mode="$(git -C "$root" ls-tree "$commit" -- "$path" | awk '{print $1}')"
    [[ "$mode" == 100644 || "$mode" == 100755 ]] || die "committed $path required in $tag"
    expected="$(mktemp)"
    git -C "$root" cat-file blob "$commit:$path" > "$expected"
    [[ -s "$expected" ]] || {
        rm -f "$expected"
        die "committed $path is empty in $tag"
    }
    # Write then byte-compare so a later edit of the dest file cannot pass
    # as "the tagged blob".
    cp "$expected" "$out"
    cmp -s "$expected" "$out" || {
        rm -f "$expected" "$out"
        die "staged $out is not byte-identical to $tag:$path"
    }
    rm -f "$expected"
}
stage_blob docs/security/gonimbus-minisign.pub "$dest/gonimbus-minisign.pub"
stage_blob docs/security/release-signing-keys.asc "$dest/gonimbus-release-signing-key.asc"
echo "[ok] staged public keys from $tag ($commit) into $dest"
