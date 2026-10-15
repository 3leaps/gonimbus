#!/usr/bin/env bash
# Maintainer-only: derive the two public anchors from the committed pin and the
# approved minisign public export. Never overwrites existing anchors.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
stop() {
    echo "STOP: $1" >&2
    exit 1
}
: "${GONIMBUS_MINISIGN_PUB:?public minisign export required}"
pin=docs/security/release-signing-keys.asc
anchors=keys/expected-fingerprints.txt
[[ -f "$pin" && -s "$pin" && ! -L "$pin" &&
    -f "$GONIMBUS_MINISIGN_PUB" && -s "$GONIMBUS_MINISIGN_PUB" && ! -L "$GONIMBUS_MINISIGN_PUB" ]] ||
    stop 'both public exports required'
[[ ! -e "$anchors" && ! -L "$anchors" ]] ||
    stop 'fingerprint anchors already exist; remove them in a reviewed change before regenerating'
[[ ! -L keys && (! -e keys || -d keys) ]] || stop 'keys must be a regular directory, not a symlink'

scratch="$(mktemp -d)"
cleanup() {
    local home
    for home in "$scratch"/*.gnupg; do
        [[ -d "$home" ]] && gpgconf --homedir "$home" --kill all > /dev/null 2>&1 || true
    done
    rm -rf "$scratch"
}
trap cleanup EXIT
chmod 700 "$scratch"

# GPG: primary fingerprint from an isolated keyring that holds only the pin.
# minisign: SHA-256 of the decoded public key blob (release-minisign-blob.py).
derive() {
    local out="$1" home="$1.gnupg" shown listing primary minisign
    mkdir -m 700 "$home"
    shown="$(gpg --homedir "$home" --batch --with-colons --show-keys "$pin" 2> /dev/null)" ||
        stop 'pin is not a readable OpenPGP export'
    if [[ "$(awk -F: '$1=="sec"||$1=="ssb" {n++} END {print n+0}' <<< "$shown")" != 0 ]] ||
        grep -q 'PRIVATE KEY BLOCK' "$pin"; then
        stop 'private material in gpg public export'
    fi
    GNUPGHOME="$home" gpg --batch --quiet --import "$pin" || stop 'pin import failed'
    listing="$(GNUPGHOME="$home" gpg --batch --with-colons --fingerprint --list-keys)"
    [[ "$(awk -F: '$1=="pub" {n++} END {print n+0}' <<< "$listing")" == 1 ]] ||
        stop 'pin must contain exactly one public primary'
    primary="$(awk -F: '$1=="fpr" {print $10;exit}' <<< "$listing")"
    [[ "$primary" =~ ^[0-9A-F]{40}$ ]] || stop 'primary fingerprint malformed'
    minisign="$("$root/scripts/release-minisign-blob.py" "$GONIMBUS_MINISIGN_PUB")" ||
        stop 'minisign public export rejected'
    [[ "$minisign" =~ ^[0-9a-f]{64}$ ]] || stop 'minisign fingerprint malformed'
    printf 'gpg %s\nminisign %s\n' "$primary" "$minisign" > "$out"
}
derive "$scratch/first"
derive "$scratch/second"
cmp -s "$scratch/first" "$scratch/second" || stop 'anchor derivation is not reproducible'

mkdir -p keys
# Noclobber: a concurrently created file is never replaced.
(
    set -C
    cat "$scratch/first" > "$anchors"
) || stop 'cannot create fingerprint anchors'
"$root/scripts/validate-release-anchors.sh" > /dev/null || {
    rm -f "$anchors"
    stop 'generated anchors failed validation'
}
cat "$anchors"
echo '[ok] generated public fingerprint anchors for review'
