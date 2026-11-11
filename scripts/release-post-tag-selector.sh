#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Post-tag ceremony selector guard:
# - Always validates GONIMBUS_GPG_HOMEDIR containment (external mode-0700 non-symlink
#   directory, not default ~/.gnupg, and outside the repository root, all git worktrees,
#   --git-dir, and --git-common-dir).
# - When a staged release directory [dir] is passed, binds selectors to the verified
#   staged public material in [dir] and verifies the key in GONIMBUS_GPG_HOMEDIR.
# - Otherwise, verifies GONIMBUS_RELEASE_TAG against VERSION,
#   GONIMBUS_GPG_SIGNING_FINGERPRINT against keys/expected-fingerprints.txt, and
#   GONIMBUS_PGP_KEY_ID against config/release/signing-subkey.txt (<subkey>!).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/release-tag-common.sh
source "$root/scripts/release-tag-common.sh"

die() {
    echo "error: $*" >&2
    exit 1
}

tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    die 'canonical GONIMBUS_RELEASE_TAG required'

approved="${GONIMBUS_GPG_SIGNING_FINGERPRINT:-}"
selector="${GONIMBUS_PGP_KEY_ID:-}"
[[ "$approved" =~ ^[0-9A-F]{40}$ && "$selector" =~ ^[0-9A-F]{40}!$ ]] ||
    die 'approved primary and exact subkey (<subkey>!) required'

tag_bind_operator_gnupg_home || exit 1
home="$GONIMBUS_GPG_HOMEDIR"

if [[ $# -ge 1 && -n "${1:-}" ]]; then
    directory="$1"
    [[ -d "$directory" && ! -L "$directory" ]] || die "staged directory required: $directory"
    resolve_release_decernor ceremony || die 'trusted Decernor identity/version check failed'
    "$RELEASE_DECERNOR_BIN" fingerprint verify \
        --anchors "$directory/expected-fingerprints.txt" \
        --anchors-ndjson "$directory/expected-fingerprints.ndjson" \
        --gpg "$directory/gonimbus-release-signing-key.asc" \
        --minisign "$directory/gonimbus-minisign.pub" > /dev/null ||
        die 'staged public identity files failed decernor fingerprint verify'
    [[ "$(awk '$1=="gpg" {print $2}' "$directory/expected-fingerprints.txt")" == "$approved" ]] ||
        die 'released anchor differs from approved primary'
    keyring="$(mktemp -d)"
    trap 'gpgconf --homedir "$keyring" --kill all >/dev/null 2>&1 || true; rm -rf "$keyring"' EXIT
    chmod 700 "$keyring"
    pin_listing="$(gpg --homedir "$keyring" --batch --with-colons --with-subkey-fingerprint --show-keys "$directory/gonimbus-release-signing-key.asc" 2> /dev/null)"
    permitted="$(awk -F: '$1=="sub" {s=1;cap=$12;next} s && $1=="fpr" {if(cap ~ /[sS]/) print $10; s=0}' <<< "$pin_listing")"
    [[ "$permitted!" == "$selector" ]] ||
        die 'selector differs from released signing subkey'
    listing="$(gpg --homedir "$home" --batch --with-colons --fingerprint --with-subkey-fingerprint --list-keys "${selector%!}" 2> /dev/null)"
    primary="$(awk -F: '$1=="pub" {p=1;next} p && $1=="fpr" {print $10;exit}' <<< "$listing")"
    subkey="$(awk -F: -v wanted="${selector%!}" '$1=="sub" {s=1;cap=$12;valid=$2;next} s && $1=="fpr" {if($10==wanted && cap ~ /[sS]/ && valid !~ /[redi]/) print $10; s=0}' <<< "$listing")"
    [[ "$primary" == "$approved" && "$subkey!" == "$selector" ]] ||
        die 'configured home lacks the approved released signing subkey'
    echo "[ok] post-tag selector uses the approved released pin for $tag"
    exit 0
fi

cd "$root"
version="$(tr -d '[:space:]' < "$root/VERSION")"
[[ "$tag" == "v$version" ]] ||
    die "GONIMBUS_RELEASE_TAG ($tag) does not match VERSION (v$version)"

expected_primary="$(awk '$1=="gpg" {print $2}' "$root/keys/expected-fingerprints.txt")"
[[ "$expected_primary" =~ ^[0-9A-F]{40}$ ]] ||
    die 'missing gpg primary fingerprint in keys/expected-fingerprints.txt'
[[ "$approved" == "$expected_primary" ]] ||
    die 'GONIMBUS_GPG_SIGNING_FINGERPRINT does not match the committed primary fingerprint'

expected_subkey="$(tr -d '[:space:]' < "$root/config/release/signing-subkey.txt")"
[[ "$expected_subkey" =~ ^[0-9A-F]{40}$ ]] ||
    die 'config/release/signing-subkey.txt is malformed'
[[ "$selector" == "${expected_subkey}!" ]] ||
    die "GONIMBUS_PGP_KEY_ID must be ${expected_subkey}! (exact signing subkey selector)"

echo "[ok] post-tag selector verified for $tag"
