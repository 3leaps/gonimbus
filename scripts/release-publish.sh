#!/usr/bin/env bash
# Promote a draft release to published, only after the draft on GitHub is shown
# to be exactly the locally verified set. Runs on the maintainer machine.
#
# Usage: release-publish.sh vX.Y.Z [source_dir]
#
# Before promotion it requires, in order:
#   1. the published tag still matches the anchor the packages were staged
#      against (release-verify-published-tag);
#   2. the release is a draft whose asset names are exactly the expected set;
#   3. both manifests verify against the local packages;
#   4. both manifests verify under minisign with the key whose blob hash is the
#      committed pin in keys/expected-fingerprints.txt, and under PGP with the
#      committed public key at the pinned primary;
#   5. every draft asset, downloaded fresh, is byte-identical to the local file.
set -euo pipefail
die() {
    echo "error: $*" >&2
    exit 1
}
tag="${1:-}"
src="${2:-dist/release}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || die 'usage: release-publish.sh vX.Y.Z [source_dir]'
root="$(cd "$(dirname "$0")/.." && pwd -P)"
repo="${GONIMBUS_RELEASE_REPO:-3leaps/gonimbus}"
for tool in gh jq minisign gpg cmp git; do
    command -v "$tool" > /dev/null 2>&1 || die "$tool not found in PATH"
done
[[ -d "$src" && ! -L "$src" ]] || die "source dir not found: $src"
binary="${GONIMBUS_BINARY_NAME:-gonimbus}"
packages=(
    "${binary}-linux-amd64"
    "${binary}-linux-arm64"
    "${binary}-darwin-arm64"
    "${binary}-windows-amd64.exe"
    "${binary}-windows-arm64.exe"
)
provenance=(
    SHA256SUMS SHA256SUMS.asc SHA256SUMS.minisig
    SHA512SUMS SHA512SUMS.asc SHA512SUMS.minisig
    "${binary}-minisign.pub" "${binary}-release-signing-key.asc"
    "release-notes-${tag}.md"
)
expected=("${packages[@]}" "${provenance[@]}")
for want in "${expected[@]}"; do
    [[ -f "$src/$want" && ! -L "$src/$want" && -s "$src/$want" ]] || die "missing local release file: $want"
done

# 1. The tag still matches the staged anchor.
anchor="$src.anchor"
[[ -f "$anchor" && ! -L "$anchor" ]] || die "missing $anchor; stage packages with make release-download"
[[ "$(awk -F= '$1=="tag" {print $2}' "$anchor")" == "$tag" ]] || die 'staged anchor is for a different tag'
anchor_object="$(awk -F= '$1=="object" {print $2}' "$anchor")"
anchor_commit="$(awk -F= '$1=="commit" {print $2}' "$anchor")"
GONIMBUS_RELEASE_TAG="$tag" GONIMBUS_EXPECTED_TAG_OBJECT="$anchor_object" GONIMBUS_EXPECTED_COMMIT="$anchor_commit" \
    "$root/scripts/release-verify-published-tag.sh" > /dev/null ||
    die 'published tag no longer matches the verified anchor'

# The notes asset must be the per-version notes at the anchored tag commit, the
# same bytes the draft body was created from.
tagged_notes="$(git -C "$root" rev-parse --verify --quiet "$anchor_commit:docs/releases/$tag.md")" ||
    die "docs/releases/$tag.md is absent at the tagged commit"
[[ "$(git -C "$root" hash-object -- "$src/release-notes-$tag.md")" == "$tagged_notes" ]] ||
    die "release-notes-$tag.md differs from docs/releases/$tag.md at the tagged commit"

# 2. A draft with exactly the expected asset names.
release="$(gh release view "$tag" --repo "$repo" --json isDraft,assets)" || die "no release for $tag"
[[ "$(jq -r '.isDraft' <<< "$release")" == true ]] || die "$tag is not a draft; refusing to re-promote"
diff <(jq -r '.assets[].name' <<< "$release" | sort) <(printf '%s\n' "${expected[@]}" | sort) > /dev/null ||
    die 'draft assets differ from the expected set'

# 3. Each manifest lists exactly the packages, and they match.
for bits in 256 512; do
    diff <(awk '{sub(/^\*/, "", $2); print $2}' "$src/SHA${bits}SUMS" | sort) \
        <(printf '%s\n' "${packages[@]}" | sort) > /dev/null ||
        die "SHA${bits}SUMS does not list exactly the expected packages"
    if command -v "sha${bits}sum" > /dev/null 2>&1; then
        (cd "$src" && "sha${bits}sum" -c "SHA${bits}SUMS" > /dev/null)
    else
        (cd "$src" && shasum -a "$bits" -c "SHA${bits}SUMS" > /dev/null)
    fi || die 'local packages do not match the manifests'
done

# 4. Signatures verify against the pins committed in the anchored tag commit,
# read as inert blobs; the working tree is never consulted, so a later edit or
# key rotation on main cannot change who is trusted for this release.
gpg_home="$(mktemp -d)"
fresh="$(mktemp -d)"
pinned="$(mktemp -d)"
trap 'gpgconf --homedir "$gpg_home" --kill all > /dev/null 2>&1 || true; rm -rf "$gpg_home" "$fresh" "$pinned"' EXIT
chmod 700 "$gpg_home"
tagged_blob() {
    local path="$1" out="$2" mode
    mode="$(git -C "$root" ls-tree "$anchor_commit" -- "$path" | awk '{print $1}')"
    [[ "$mode" == 100644 || "$mode" == 100755 ]] || die "committed $path required in the tagged commit"
    git -C "$root" cat-file blob "$anchor_commit:$path" > "$out"
    [[ -s "$out" ]] || die "committed $path is empty in the tagged commit"
}
tagged_blob keys/expected-fingerprints.txt "$pinned/anchors"
tagged_blob docs/security/release-signing-keys.asc "$pinned/pgp.asc"
tagged_blob "docs/security/${binary}-minisign.pub" "$pinned/minisign.pub"
pin_gpg="$(awk '$1=="gpg" {print $2}' "$pinned/anchors")"
pin_minisign="$(awk '$1=="minisign" {print $2}' "$pinned/anchors")"
[[ "$pin_gpg" =~ ^[0-9A-F]{40}$ && "$pin_minisign" =~ ^[0-9a-f]{64}$ ]] || die 'committed pins are malformed'
committed_minisign="$(sed -n 2p "$pinned/minisign.pub" | base64 -d | shasum -a 256 | awk '{print $1}')"
[[ "$committed_minisign" == "$pin_minisign" ]] || die 'committed minisign key does not match the committed anchor'
cmp -s "$pinned/minisign.pub" "$src/${binary}-minisign.pub" ||
    die 'staged minisign key is not the key committed in the tagged commit'
gpg --homedir "$gpg_home" --batch --quiet --import "$pinned/pgp.asc" 2> /dev/null || die 'committed PGP key failed to import'
listing="$(gpg --homedir "$gpg_home" --batch --with-colons --with-subkey-fingerprint --list-keys)"
[[ "$(awk -F: '$1=="pub" {n++} END {print n+0}' <<< "$listing")" == 1 ]] || die 'committed PGP key must hold exactly one primary'
[[ "$(awk -F: '$1=="fpr" {print $10; exit}' <<< "$listing")" == "$pin_gpg" ]] || die 'committed PGP key is not the anchored primary'
pin_subkey="$(awk -F: '$1=="sub" {s=1; cap=$12; next} s && $1=="fpr" {if (cap ~ /s/) print $10; s=0}' <<< "$listing")"
[[ "$pin_subkey" =~ ^[0-9A-F]{40}$ ]] || die 'committed PGP key must hold exactly one signing subkey'
for manifest in SHA256SUMS SHA512SUMS; do
    minisign -Vq -p "$pinned/minisign.pub" -m "$src/$manifest" -x "$src/$manifest.minisig" ||
        die "minisign verification failed for $manifest"
    gpg --homedir "$gpg_home" --batch --status-fd 1 --verify "$src/$manifest.asc" "$src/$manifest" 2> /dev/null |
        awk -v primary="$pin_gpg" -v subkey="$pin_subkey" '
          $2=="VALIDSIG" {valid++; signer=$3; signer_primary=$NF}
          $2=="GOODSIG" {good++}
          $2 ~ /^(EXPKEYSIG|EXPSIG|REVKEYSIG|KEYREVOKED|BADSIG|ERRSIG)$/ {bad++}
          END { exit !(valid==1 && good==1 && !bad && signer==subkey && signer_primary==primary) }' ||
        die "PGP signature on $manifest is not a good signature by the pinned signing subkey"
done

# 5. The draft on GitHub is byte-identical to the verified local set.
gh release download "$tag" --repo "$repo" --dir "$fresh" > /dev/null || die 'could not download the draft assets'
for want in "${expected[@]}"; do
    cmp -s "$fresh/$want" "$src/$want" || die "draft asset differs from the verified local file: $want"
done

# Residual: GitHub has no atomic compare-and-publish, so a writer with release
# access could still swap an asset in the seconds between the comparison above
# and this call. Re-download and verify after publishing if that matters.
gh release edit "$tag" --repo "$repo" --draft=false > /dev/null
echo "[ok] $tag published from ${#expected[@]} byte-verified asset(s)"
