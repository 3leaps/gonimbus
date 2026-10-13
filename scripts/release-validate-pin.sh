#!/usr/bin/env bash
# Maintainer-only: inspect the approved public exports without changing the pin.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
stop() {
    echo "STOP: $1" >&2
    exit 1
}

: "${GONIMBUS_PGP_KEY_ID:?load the approved signing-subkey selector}"
: "${GONIMBUS_GPG_SIGNING_FINGERPRINT:?load the approved primary fingerprint}"
: "${GONIMBUS_MINISIGN_PUB:?load the approved minisign public export}"
[[ "$GONIMBUS_PGP_KEY_ID" =~ ^[0-9A-F]{40}!$ && "$GONIMBUS_GPG_SIGNING_FINGERPRINT" =~ ^[0-9A-F]{40}$ ]] ||
    stop 'invalid configured primary or signing-subkey fingerprint'

pin=docs/security/release-signing-keys.asc
[[ -f "$pin" && -s "$pin" && ! -L "$pin" ]] || stop 'public GPG pin must be a nonempty regular file, not a symlink'
[[ -f "$GONIMBUS_MINISIGN_PUB" && -s "$GONIMBUS_MINISIGN_PUB" && ! -L "$GONIMBUS_MINISIGN_PUB" ]] ||
    stop 'minisign public export must be a nonempty regular file, not a symlink'

# Every gpg call below uses a fresh, empty home: no operator keyring is opened.
verify_tmp="$(mktemp -d)"
trap 'gpgconf --homedir "$verify_tmp" --kill all > /dev/null 2>&1 || true; rm -rf "$verify_tmp"' EXIT
chmod 700 "$verify_tmp"
# Secret-key markers, armored or as parsed by gpg.
secret_markers=(-e 'PRIVATE KEY BLOCK' -e 'minisign secret key' -e 'minisign encrypted secret key')
has_secret_gpg() {
    local shown
    shown="$(gpg --homedir "$verify_tmp" --batch --with-colons --show-keys "$1" 2> /dev/null)" || return 1
    [[ "$(awk -F: '$1=="sec"||$1=="ssb" {n++} END {print n+0}' <<< "$shown")" != 0 ]]
}
if grep -q "${secret_markers[@]}" "$pin" || has_secret_gpg "$pin"; then
    stop 'private material in gpg public export'
fi
if grep -qi -e 'secret key' -e 'PRIVATE KEY' "$GONIMBUS_MINISIGN_PUB" ||
    ! "$root/scripts/release-minisign-blob.py" "$GONIMBUS_MINISIGN_PUB" > /dev/null 2>&1; then
    stop 'private material in minisign export, or not exactly one minisign public key'
fi
grep -q '^untrusted comment:' "$GONIMBUS_MINISIGN_PUB" || stop 'minisign public export lacks its header'
while IFS= read -r -d '' file; do
    if grep -q "${secret_markers[@]}" "$file" || has_secret_gpg "$file"; then
        stop 'public pin scan failed'
    fi
done < <(find docs/security -type f -print0)

shown="$(gpg --homedir "$verify_tmp" --batch --with-colons --show-keys "$pin" 2> /dev/null)" || stop 'public GPG pin is not a readable OpenPGP export'
# Exactly one primary and one subkey, in that order, with the subkey signing-capable.
layout="$(awk -F: '
	$1=="pub" || $1=="sub" {kind=$1; cap=$12; want=1; next}
	want && $1=="fpr" {print kind, $10, cap; want=0}
' <<< "$shown")"
[[ "$(awk '{print $1}' <<< "$layout" | paste -sd, -)" == pub,sub ]] ||
    stop 'expected exactly one public primary and signing subkey'
read -r _ pin_primary _ < <(sed -n 1p <<< "$layout")
read -r _ pin_subkey pin_cap < <(sed -n 2p <<< "$layout")
[[ "$pin_primary" == "$GONIMBUS_GPG_SIGNING_FINGERPRINT" ]] || stop 'public pin differs from approved primary'
[[ "$pin_subkey" == "${GONIMBUS_PGP_KEY_ID%!}" && "$pin_cap" == *[sS]* ]] ||
    stop 'public pin differs from approved signing subkey'

gpg --homedir "$verify_tmp" --batch --show-keys \
    --fingerprint --with-subkey-fingerprint "$pin" || stop 'isolated public GPG display failed'
echo '[ok] Public signing pin validated; inspect displayed expiry and revocation before proceeding'
