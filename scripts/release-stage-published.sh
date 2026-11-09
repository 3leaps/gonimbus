#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Clean-room download and full verification of the published GitHub release.
set -euo pipefail
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
caller_pwd="$(pwd -P)"
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
# shellcheck source=scripts/release-gh.sh
source "$root/scripts/release-gh.sh"
# shellcheck source=scripts/release-decernor.sh
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony || die 'trusted Decernor identity/version check failed'

if [[ "${1:-}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    export GONIMBUS_RELEASE_TAG="$1"
    dest="${2:-dist/published}"
else
    dest="${1:-dist/published}"
fi
tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || die 'canonical GONIMBUS_RELEASE_TAG required'
repo="${GONIMBUS_RELEASE_REPO:-3leaps/gonimbus}"
binary="${GONIMBUS_BINARY_NAME:-gonimbus}"

while [[ "$dest" == */ && "$dest" != "/" ]]; do
    dest="${dest%/}"
done
[[ -n "$dest" && "$dest" != "." && "$dest" != ".." && "$dest" != "/" ]] || die 'unsafe published staging destination'
[[ "$dest" == /* ]] || dest="$caller_pwd/$dest"
while [[ "$dest" == */ && "$dest" != "/" ]]; do
    dest="${dest%/}"
done
[[ "$dest" != "/" && "$dest" != "$root" && "$dest" != "$caller_pwd" && ! -L "$dest" && ! -L "$dest.anchor" ]] ||
    die 'unsafe published staging destination'
rm -rf "$dest" "$dest.anchor"

scratch="$(mktemp -d)"
staged="$scratch/staged"
mkdir -p "$staged"
staged_ok=0
cleanup() {
    rm -rf "$scratch"
    if [[ "$staged_ok" -ne 1 ]]; then
        rm -rf "$dest" "$dest.anchor"
    fi
}
trap cleanup EXIT

GONIMBUS_RELEASE_TAG="$tag" GONIMBUS_ANCHOR_OUT="$scratch/anchor" "$root/scripts/release-verify-published-tag.sh" > /dev/null ||
    die "published tag $tag failed verification"
commit="$(awk -F= '$1=="commit" {print $2}' "$scratch/anchor")"
object="$(awk -F= '$1=="object" {print $2}' "$scratch/anchor")"
[[ "$commit" =~ ^[0-9a-f]{40}$ && "$object" =~ ^[0-9a-f]{40}$ ]] || die 'verification returned no valid anchor'

packages=(
    "${binary}-darwin-arm64"
    "${binary}-linux-amd64"
    "${binary}-linux-arm64"
    "${binary}-windows-amd64.exe"
    "${binary}-windows-arm64.exe"
)
provenance=(
    SHA256SUMS SHA256SUMS.asc SHA256SUMS.minisig
    SHA512SUMS SHA512SUMS.asc SHA512SUMS.minisig
    "${binary}-minisign.pub" "${binary}-release-signing-key.asc"
    expected-fingerprints.txt expected-fingerprints.ndjson
    "release-notes-${tag}.md"
)
expected=("${packages[@]}" "${provenance[@]}")

release_gh api "repos/$repo/releases/tags/$tag" > "$scratch/release.json" ||
    die "could not read published release metadata for $tag"
jq -e --arg tag "$tag" '.tag_name == $tag and .draft == false and .immutable == true' "$scratch/release.json" > /dev/null ||
    die 'release is not a published immutable release for this tag'

printf '%s\n' "${expected[@]}" | LC_ALL=C sort > "$scratch/expected.names"
jq -r '.assets[].name' "$scratch/release.json" | LC_ALL=C sort > "$scratch/remote.names"
cmp -s "$scratch/expected.names" "$scratch/remote.names" || die 'published release asset names do not match expected set'

mkdir -p "$scratch/download"
release_gh release download "$tag" --repo "$repo" --dir "$scratch/download" ||
    die 'could not download published release assets'

while IFS= read -r name; do
    file="$scratch/download/$name"
    [[ -f "$file" && ! -L "$file" && -s "$file" ]] || die "missing downloaded asset $name"
    size="$(wc -c < "$file" | tr -d ' ')"
    digest="sha256:$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$file")"
    jq -e --arg name "$name" --argjson size "$size" --arg digest "$digest" \
        '.assets[] | select(.name == $name and .size == $size and .digest == $digest)' "$scratch/release.json" > /dev/null ||
        die "downloaded asset $name does not match published release metadata"
    mv "$file" "$staged/$name"
done < "$scratch/expected.names"

[[ -z "$(find "$scratch/download" -mindepth 1 -maxdepth 1 -print)" ]] || die 'unexpected downloaded files'

# Verify checksums cover all 5 packages and match
for bits in 256 512; do
    diff <(awk '{sub(/^\*/, "", $2); print $2}' "$staged/SHA${bits}SUMS" | LC_ALL=C sort) \
        <(printf '%s\n' "${packages[@]}" | LC_ALL=C sort) > /dev/null ||
        die "SHA${bits}SUMS does not list exactly the expected packages"
done
"$root/scripts/verify-checksums.sh" "$staged" > /dev/null

# Verify staged public files match the tagged commit blobs
for pair in \
    "keys/expected-fingerprints.txt:expected-fingerprints.txt" \
    "keys/expected-fingerprints.ndjson:expected-fingerprints.ndjson" \
    "docs/security/${binary}-minisign.pub:${binary}-minisign.pub" \
    "docs/security/release-signing-keys.asc:${binary}-release-signing-key.asc" \
    "docs/releases/${tag}.md:release-notes-${tag}.md"; do
    repo_path="${pair%%:*}"
    staged_name="${pair##*:}"
    git --no-replace-objects -C "$root" cat-file -e "$commit:$repo_path" 2> /dev/null || die "missing $repo_path at tagged commit $commit"
    git --no-replace-objects -C "$root" cat-file blob "$commit:$repo_path" > "$scratch/expected.blob"
    cmp -s "$scratch/expected.blob" "$staged/$staged_name" ||
        die "staged $staged_name differs from $repo_path at tagged commit $commit"
done

"$RELEASE_DECERNOR_BIN" fingerprint verify \
    --anchors "$staged/expected-fingerprints.txt" \
    --anchors-ndjson "$staged/expected-fingerprints.ndjson" \
    --gpg "$staged/${binary}-release-signing-key.asc" \
    --minisign "$staged/${binary}-minisign.pub" > /dev/null ||
    die 'published identity files failed decernor fingerprint verify'

"$root/scripts/release-verify-signatures.sh" --tag "$tag" "$staged" > /dev/null

mkdir -p "$(dirname "$dest")"
mv "$staged" "$dest"
cp "$scratch/anchor" "$dest.anchor"
staged_ok=1
echo "[ok] published release $tag staged and verified in $dest"
