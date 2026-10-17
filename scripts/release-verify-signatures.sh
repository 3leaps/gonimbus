#!/usr/bin/env bash
# Verify all four manifest signatures in a staged release directory. Fails
# closed: both manifests, both minisign signatures, both PGP signatures and both
# exported public keys are required, and any verification failure stops.
#
# Usage: release-verify-signatures.sh [dir]   (default dist/release)
#
# PGP verification uses an isolated keyring holding only the exported public
# key in the directory, and requires a good signature from the pinned signing
# subkey (keys/expected-fingerprints.txt primary + the key's signing subkey).
set -euo pipefail
die() {
    echo "error: $*" >&2
    exit 1
}
root="$(cd "$(dirname "$0")/.." && pwd -P)"
dir="${1:-dist/release}"
[[ "$dir" == /* ]] || dir="$PWD/$dir"
[[ -d "$dir" && ! -L "$dir" ]] || die "release dir not found: $dir"
binary="${GONIMBUS_BINARY_NAME:-gonimbus}"
for tool in minisign gpg gpgconf; do
    command -v "$tool" > /dev/null 2>&1 || die "$tool not found in PATH"
done
minisign_pub="$dir/${binary}-minisign.pub"
pgp_pub="$dir/${binary}-release-signing-key.asc"
for want in SHA256SUMS SHA512SUMS SHA256SUMS.minisig SHA512SUMS.minisig \
    SHA256SUMS.asc SHA512SUMS.asc "${minisign_pub##*/}" "${pgp_pub##*/}"; do
    [[ -f "$dir/$want" && ! -L "$dir/$want" && -s "$dir/$want" ]] || die "missing release file: $want"
done

# The staged minisign key must be exactly a comment line plus a key line, and
# the key line must hash to the minisign anchor. Verification then uses a key
# file rebuilt from that line, never the staged file as given.
pin_minisign="$(awk '$1=="minisign" {print $2}' "$root/keys/expected-fingerprints.txt")"
[[ "$pin_minisign" =~ ^[0-9a-f]{64}$ ]] || die 'minisign anchor missing from keys/expected-fingerprints.txt'
minisign_comment="$(sed -n 1p "$minisign_pub")"
minisign_key="$(sed -n 2p "$minisign_pub")"
[[ "$minisign_comment" == "untrusted comment: "* && "$minisign_key" =~ ^[A-Za-z0-9+/]+=*$ ]] ||
    die 'staged minisign key is malformed'
cmp -s "$minisign_pub" <(printf '%s\n%s\n' "$minisign_comment" "$minisign_key") ||
    die 'staged minisign key must be exactly a comment line and a key line'
staged_minisign="$(printf '%s' "$minisign_key" | base64 -d | shasum -a 256 | awk '{print $1}')"
[[ "$staged_minisign" == "$pin_minisign" ]] || die 'staged minisign key does not match the minisign anchor'
scratch_keys="$(mktemp -d)"
keyring=""
trap 'rm -rf "$scratch_keys"' EXIT
minisign_rebuilt="$scratch_keys/minisign.pub"
printf '%s\n%s\n' "$minisign_comment" "$minisign_key" > "$minisign_rebuilt"
for manifest in SHA256SUMS SHA512SUMS; do
    minisign -V -q -p "$minisign_rebuilt" -m "$dir/$manifest" -x "$dir/$manifest.minisig" ||
        die "minisign verification failed for $manifest"
done
echo "[ok] minisign signatures verified by the anchored key (SHA256SUMS, SHA512SUMS)"

primary="$(awk '$1=="gpg" {print $2}' "$root/keys/expected-fingerprints.txt")"
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
