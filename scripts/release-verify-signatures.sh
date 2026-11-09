#!/usr/bin/env bash
# Verify all four manifest signatures in a staged release directory. Fails
# closed: both manifests, both minisign signatures, both PGP signatures and both
# exported public keys are required, and any verification failure stops.
#
# Usage: release-verify-signatures.sh [--tag vX.Y.Z] [dir]   (default dist/release)
#
# PGP verification uses an isolated keyring holding only the exported public
# key in the directory, and requires a good signature from the pinned signing
# subkey (keys/expected-fingerprints.txt primary + the key's signing subkey).
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
if [[ -n "${GONIMBUS_EXPECTED_FINGERPRINTS_TXT:-}" || -n "${GONIMBUS_EXPECTED_FINGERPRINTS_NDJSON:-}" ]]; then
    die 'environment override of fingerprint anchors is not permitted (PDR-0008 §2.1)'
fi
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/release-decernor.sh
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony || die 'trusted Decernor identity/version check failed'

arg_tag=""
positional=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            [[ $# -ge 2 ]] || die '--tag requires vX.Y.Z'
            arg_tag="$2"
            shift 2
            ;;
        --tag=*)
            arg_tag="${1#--tag=}"
            shift
            ;;
        -*)
            die "unknown option: $1"
            ;;
        *)
            positional+=("$1")
            shift
            ;;
    esac
done
[[ ${#positional[@]} -le 1 ]] || die 'usage: release-verify-signatures.sh [--tag vX.Y.Z] [dir]'
dir="${positional[0]:-dist/release}"
[[ "$dir" == /* ]] || dir="$PWD/$dir"
[[ -d "$dir" && ! -L "$dir" ]] || die "release dir not found: $dir"
binary="${GONIMBUS_BINARY_NAME:-gonimbus}"
for tool in git minisign gpg gpgconf; do
    command -v "$tool" > /dev/null 2>&1 || die "$tool not found in PATH"
done
minisign_pub="$dir/${binary}-minisign.pub"
pgp_pub="$dir/${binary}-release-signing-key.asc"
for want in SHA256SUMS SHA512SUMS SHA256SUMS.minisig SHA512SUMS.minisig \
    SHA256SUMS.asc SHA512SUMS.asc "${minisign_pub##*/}" "${pgp_pub##*/}"; do
    [[ -f "$dir/$want" && ! -L "$dir/$want" && -s "$dir/$want" ]] || die "missing release file: $want"
done

scratch_keys="$(mktemp -d)"
keyring=""
trap 'rm -rf "$scratch_keys"' EXIT

tag="${arg_tag:-${GONIMBUS_RELEASE_TAG:-}}"
if [[ -n "$tag" ]]; then
    [[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
        die "canonical release tag required (got: $tag)"
fi

if [[ -n "$arg_tag" ]] || { [[ -n "$tag" ]] && git --no-replace-objects -C "$root" rev-parse --verify --quiet "refs/tags/$tag^{commit}" > /dev/null 2>&1; }; then
    anchor_commit="$(git --no-replace-objects -C "$root" rev-parse --verify "refs/tags/$tag^{commit}" 2> /dev/null)" ||
        die "tag $tag not found in repository"
    for ext in txt ndjson; do
        mode="$(git --no-replace-objects -C "$root" ls-tree "$anchor_commit" -- "keys/expected-fingerprints.$ext" | awk '{print $1}')"
        [[ "$mode" == "100644" ]] || die "missing regular blob keys/expected-fingerprints.$ext at tagged commit $anchor_commit"
        git --no-replace-objects -C "$root" cat-file blob "$anchor_commit:keys/expected-fingerprints.$ext" > "$scratch_keys/expected-fingerprints.$ext" ||
            die "failed to read keys/expected-fingerprints.$ext at tagged commit $anchor_commit"
        [[ -s "$scratch_keys/expected-fingerprints.$ext" ]] ||
            die "empty keys/expected-fingerprints.$ext at tagged commit $anchor_commit"
        if [[ -e "$dir/expected-fingerprints.$ext" || -L "$dir/expected-fingerprints.$ext" ]]; then
            [[ -f "$dir/expected-fingerprints.$ext" && ! -L "$dir/expected-fingerprints.$ext" ]] ||
                die "staged expected-fingerprints.$ext is not a regular file"
            cmp -s "$scratch_keys/expected-fingerprints.$ext" "$dir/expected-fingerprints.$ext" ||
                die "staged expected-fingerprints.$ext differs from keys/expected-fingerprints.$ext at tagged commit $anchor_commit"
        fi
    done
    anchors_txt="$scratch_keys/expected-fingerprints.txt"
    anchors_ndjson="$scratch_keys/expected-fingerprints.ndjson"
else
    anchors_txt="$root/keys/expected-fingerprints.txt"
    anchors_ndjson="$root/keys/expected-fingerprints.ndjson"
    [[ -f "$anchors_txt" && ! -L "$anchors_txt" && -s "$anchors_txt" ]] ||
        die "missing fingerprint anchor: $anchors_txt"
    [[ -f "$anchors_ndjson" && ! -L "$anchors_ndjson" && -s "$anchors_ndjson" ]] ||
        die "missing fingerprint anchor: $anchors_ndjson"
    if git --no-replace-objects -C "$root" rev-parse --verify --quiet "HEAD^{commit}" > /dev/null 2>&1; then
        head_commit="$(git --no-replace-objects -C "$root" rev-parse --verify "HEAD^{commit}")"
        for ext in txt ndjson; do
            mode="$(git --no-replace-objects -C "$root" ls-tree "$head_commit" -- "keys/expected-fingerprints.$ext" | awk '{print $1}')"
            [[ "$mode" == "100644" ]] || die "missing committed keys/expected-fingerprints.$ext at HEAD"
            git --no-replace-objects -C "$root" cat-file blob "$head_commit:keys/expected-fingerprints.$ext" > "$scratch_keys/expected-fingerprints.$ext"
            cmp -s "$scratch_keys/expected-fingerprints.$ext" "$root/keys/expected-fingerprints.$ext" ||
                die "working tree keys/expected-fingerprints.$ext differs from HEAD"
        done
    fi
fi

# The staged minisign key must be exactly a comment line plus a key line, and
# both staged public keys must verify against the fingerprint anchors with
# Decernor (PDR-0008 §2.2, §2.3).
pin_minisign="$(awk '$1=="minisign" {print $2}' "$anchors_txt")"
[[ "$pin_minisign" =~ ^[0-9a-f]{64}$ ]] || die 'minisign anchor missing from keys/expected-fingerprints.txt'
minisign_comment="$(sed -n 1p "$minisign_pub")"
minisign_key="$(sed -n 2p "$minisign_pub")"
[[ "$minisign_comment" == "untrusted comment: "* && "$minisign_key" =~ ^[A-Za-z0-9+/]+=*$ ]] ||
    die 'staged minisign key is malformed'
cmp -s "$minisign_pub" <(printf '%s\n%s\n' "$minisign_comment" "$minisign_key") ||
    die 'staged minisign key must be exactly a comment line and a key line'
"$RELEASE_DECERNOR_BIN" fingerprint verify \
    --anchors "$anchors_txt" \
    --anchors-ndjson "$anchors_ndjson" \
    --gpg "$pgp_pub" --minisign "$minisign_pub" > /dev/null ||
    die 'staged public keys do not match the fingerprint anchors (does not match the minisign anchor or primary anchor)'
minisign_rebuilt="$scratch_keys/minisign.pub"
printf '%s\n%s\n' "$minisign_comment" "$minisign_key" > "$minisign_rebuilt"
for manifest in SHA256SUMS SHA512SUMS; do
    minisign -V -q -p "$minisign_rebuilt" -m "$dir/$manifest" -x "$dir/$manifest.minisig" ||
        die "minisign verification failed for $manifest"
done
echo "[ok] minisign signatures verified by the anchored key (SHA256SUMS, SHA512SUMS)"

primary="$(awk '$1=="gpg" {print $2}' "$anchors_txt")"
[[ "$primary" =~ ^[0-9A-F]{40}$ ]] || die 'primary anchor missing from keys/expected-fingerprints.txt'
keyring="$(mktemp -d)"
trap 'gpgconf --homedir "$keyring" --kill all > /dev/null 2>&1 || true; rm -rf "$keyring" "$scratch_keys"' EXIT
chmod 700 "$keyring"
GNUPGHOME="$keyring" gpg --batch --quiet --import "$pgp_pub" || die 'exported PGP public key failed to import'
listing="$(GNUPGHOME="$keyring" gpg --batch --with-colons --with-subkey-fingerprint --list-keys)"
[[ "$(awk -F: '$1=="pub" {n++} END {print n+0}' <<< "$listing")" == 1 ]] || die 'exported PGP key must hold exactly one primary'
[[ "$(awk -F: '$1=="fpr" {print $10; exit}' <<< "$listing")" == "$primary" ]] || die 'exported PGP key is not the anchored primary'
subkey="$(awk -F: '$1=="sub" {s=1; cap=$12; next} s && $1=="fpr" {if (cap ~ /s/) print $10; s=0}' <<< "$listing")"
[[ "$subkey" =~ ^[0-9A-F]{40}$ ]] || die 'exported PGP key must hold exactly one signing subkey'
for manifest in SHA256SUMS SHA512SUMS; do
    status="$keyring/status-$manifest"
    GNUPGHOME="$keyring" gpg --batch --status-fd 3 --verify "$dir/$manifest.asc" "$dir/$manifest" \
        3> "$status" > /dev/null 2>&1 || die "PGP verification failed for $manifest"
    awk -v primary="$primary" -v subkey="$subkey" '
      $1=="[GNUPG:]" && $2=="VALIDSIG" {valid++; signer=$3; signer_primary=$NF}
      $1=="[GNUPG:]" && $2=="GOODSIG" {good++}
      $1=="[GNUPG:]" && $2 ~ /^(EXPKEYSIG|EXPSIG|REVKEYSIG|KEYREVOKED|BADSIG|ERRSIG)$/ {bad++}
      END { if (bad || good!=1 || valid!=1 || signer!=subkey || signer_primary!=primary) exit 1 }
    ' "$status" || die "PGP signature on $manifest is not a good signature by the pinned subkey"
done
echo "[ok] PGP signatures verified by the pinned signing subkey (SHA256SUMS, SHA512SUMS)"
