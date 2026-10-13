#!/usr/bin/env bash
# Test the public-pin precursor targets with synthetic keys and exports only.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
fixture="$(mktemp -d)"
cleanup() {
    local home
    for home in "$fixture/home" "$fixture/other-home/.gnupg" "$fixture/repo/in-repo-home"; do
        [[ -d "$home" ]] && gpgconf --homedir "$home" --kill all > /dev/null 2>&1 || true
    done
    rm -rf "$fixture"
}
trap cleanup EXIT
mkdir -p "$fixture/repo/scripts" "$fixture/repo/docs" "$fixture/home" "$fixture/other-home/.gnupg"
chmod 700 "$fixture/home" "$fixture/other-home" "$fixture/other-home/.gnupg"
cp "$root/Makefile" "$root/VERSION" "$fixture/repo/"
cp "$root/scripts/"{release-export-pin.sh,release-validate-pin.sh,release-insert-anchors.sh,validate-release-anchors.sh,release-minisign-blob.py} "$fixture/repo/scripts/"
python3 - "$fixture/public.pub" "$fixture/private.pub" "$fixture/other.pub" << 'PY'
import base64
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text('untrusted comment: synthetic public key\n' + base64.b64encode(b'Ed' + b'\x01' * 40).decode() + '\n')
pathlib.Path(sys.argv[2]).write_text('untrusted comment: minisign encrypted secret key\n' + base64.b64encode(b'\x02' * 128).decode() + '\n')
pathlib.Path(sys.argv[3]).write_text('untrusted comment: synthetic public key\n' + base64.b64encode(b'Ed' + b'\x03' * 40).decode() + '\n')
PY
# Independent oracle for the documented derivation: SHA-256 of the decoded blob.
expected_minisign="$(python3 -c 'import hashlib; print(hashlib.sha256(b"Ed" + b"\x01" * 40).hexdigest())')"
export GONIMBUS_MINISIGN_PUB="$fixture/public.pub"
export GONIMBUS_GPG_HOMEDIR="$fixture/home"
gpg --homedir "$fixture/home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-gen-key 'Synthetic signing <signing@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
primary="$(gpg --homedir "$fixture/home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1 == "fpr" { print $10; exit }')"
gpg --homedir "$fixture/home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
selector="$(gpg --homedir "$fixture/home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1 == "sub" { subkey=1; next } subkey && $1 == "fpr" { print $10 "!"; exit }')"
export GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" GONIMBUS_PGP_KEY_ID="$selector"
unset GONIMBUS_RELEASE_TAG GONIMBUS_TAG_MESSAGE_DIR || true

run() { make --no-print-directory -s -C "$fixture/repo" "$@"; }
pin_times() {
    if [[ "$(uname -s)" == Darwin ]]; then
        stat -f '%m:%c' "$1"
    else
        stat -c '%Y:%Z' "$1"
    fi
}
fail() {
    local reason="$1"
    shift
    if "$@" > "$fixture/output" 2>&1; then
        echo "error: expected rejection: $reason" >&2
        exit 1
    fi
    grep -q "$reason" "$fixture/output" || {
        echo "error: missing named rejection: $reason" >&2
        cat "$fixture/output" >&2
        exit 1
    }
}
pin="$fixture/repo/docs/security/release-signing-keys.asc"
anchors="$fixture/repo/keys/expected-fingerprints.txt"
fail 'public GPG pin' run release-validate-pin
fail 'public GPG pin' run release-insert-anchors
fail 'both public exports required' "$fixture/repo/scripts/release-insert-anchors.sh"
fail 'committed fingerprint anchors missing' "$fixture/repo/scripts/validate-release-anchors.sh"
fail 'minisign public export' env GONIMBUS_MINISIGN_PUB="$fixture/missing.pub" make --no-print-directory -s -C "$fixture/repo" release-export-pin
[[ ! -e "$pin" ]]
# A GPG home inside the repository is refused before any export, so private
# GPG state can never be created under the working tree.
mkdir -m 700 "$fixture/repo/in-repo-home"
cp -R "$fixture/home/." "$fixture/repo/in-repo-home/"
fail 'GPG home must be outside the repository' env GONIMBUS_GPG_HOMEDIR="$fixture/repo/in-repo-home" make --no-print-directory -s -C "$fixture/repo" release-export-pin
[[ ! -e "$pin" ]]
rm -rf "$fixture/repo/in-repo-home"
bash -c 'make --no-print-directory -s -C "$1" release-export-pin' bash "$fixture/repo" > "$fixture/output"
[[ -s "$pin" ]]
# Distinct old mtime catches a same-second touch despite stat's second resolution.
touch -t 202001010000 "$pin"
cp "$pin" "$fixture/pin-copy"
before="$(pin_times "$pin")"
env -u GONIMBUS_GPG_HOMEDIR make --no-print-directory -s -C "$fixture/repo" release-validate-pin > "$fixture/output"
bash -c 'make --no-print-directory -s -C "$1" release-validate-pin' bash "$fixture/repo" > "$fixture/output"
if command -v zsh > /dev/null 2>&1; then
    zsh -c 'make --no-print-directory -s -C "$1" release-validate-pin' zsh "$fixture/repo" > "$fixture/output"
    # shellcheck disable=SC2016 # The nested zsh expands $1.
    fail 'public pin already exists' zsh -c 'make --no-print-directory -s -C "$1" release-export-pin' zsh "$fixture/repo"
elif [[ "$(uname -s)" == Darwin ]]; then
    echo 'error: zsh required for macOS release precursor test' >&2
    exit 1
else
    echo '[skip] zsh unavailable; bash-to-make coverage passed'
fi
cmp "$pin" "$fixture/pin-copy"
after="$(pin_times "$pin")"
[[ "$before" == "$after" ]] || {
    echo 'error: existing public pin timestamp changed' >&2
    exit 1
}
fail 'public pin already exists' run release-export-pin
cmp "$pin" "$fixture/pin-copy"
fail 'private material in minisign' env GONIMBUS_MINISIGN_PUB="$fixture/private.pub" make --no-print-directory -s -C "$fixture/repo" release-validate-pin
fail 'minisign public export rejected' env GONIMBUS_MINISIGN_PUB="$fixture/private.pub" "$fixture/repo/scripts/release-insert-anchors.sh"
[[ ! -e "$anchors" ]]
other_digit=A
[[ "${primary: -1}" == A && "${selector: -2:1}" == A ]] && other_digit=B
wrong_primary="${primary%?}${other_digit}"
[[ "$wrong_primary" != "$primary" ]] || wrong_primary="${primary%?}B"
wrong_subkey="${selector%??}${other_digit}!"
[[ "$wrong_subkey" != "$selector" ]] || wrong_subkey="${selector%??}B!"
fail 'approved primary' env GONIMBUS_GPG_SIGNING_FINGERPRINT="$wrong_primary" make --no-print-directory -s -C "$fixture/repo" release-validate-pin
fail 'signing subkey' env GONIMBUS_PGP_KEY_ID="$wrong_subkey" make --no-print-directory -s -C "$fixture/repo" release-validate-pin
fail 'invalid configured' env GONIMBUS_PGP_KEY_ID="${selector%!}" make --no-print-directory -s -C "$fixture/repo" release-validate-pin

# Anchors: generated once from the pin and the minisign blob, never overwritten.
run release-insert-anchors > "$fixture/output"
[[ "$(cat "$anchors")" == "gpg $primary"$'\n'"minisign $expected_minisign" ]] || {
    echo 'error: generated anchors differ from the documented derivation' >&2
    exit 1
}
"$fixture/repo/scripts/validate-release-anchors.sh" > /dev/null
cp "$anchors" "$fixture/anchors-copy"
fail 'fingerprint anchors already exist' env GONIMBUS_MINISIGN_PUB="$fixture/other.pub" make --no-print-directory -s -C "$fixture/repo" release-insert-anchors
cmp "$anchors" "$fixture/anchors-copy"
printf 'gpg %s\nminisign %s\n' "$wrong_primary" "$expected_minisign" > "$anchors"
fail 'primary pin differs' "$fixture/repo/scripts/validate-release-anchors.sh"
printf 'gpg %s\nminisign %s\n' "$(tr 'A-F' 'a-f' <<< "$primary")" "$expected_minisign" > "$anchors"
fail 'exactly a gpg line' "$fixture/repo/scripts/validate-release-anchors.sh"
printf 'gpg %s\nminisign %s\nextra\n' "$primary" "$expected_minisign" > "$anchors"
fail 'exactly a gpg line' "$fixture/repo/scripts/validate-release-anchors.sh"
printf 'minisign %s\ngpg %s\n' "$expected_minisign" "$primary" > "$anchors"
fail 'exactly a gpg line' "$fixture/repo/scripts/validate-release-anchors.sh"
cp "$fixture/anchors-copy" "$anchors"
"$fixture/repo/scripts/validate-release-anchors.sh" > /dev/null
rm "$anchors"

gpg --homedir "$fixture/home" --batch --armor --export-secret-keys "$selector" > "$pin"
fail 'private material in gpg' run release-validate-pin
fail 'private material in gpg' "$fixture/repo/scripts/release-insert-anchors.sh"
[[ ! -e "$anchors" ]]
cp "$fixture/pin-copy" "$pin"
gpg --homedir "$fixture/home" --batch --armor --export-secret-keys "$selector" > "$fixture/repo/docs/security/unexpected.asc"
fail 'public pin scan failed' run release-validate-pin
rm "$fixture/repo/docs/security/unexpected.asc"
gpg --homedir "$fixture/home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-gen-key 'Synthetic extra <extra@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
extra="$(gpg --homedir "$fixture/home" --batch --with-colons --fingerprint --list-keys 'Synthetic extra' | awk -F: '$1 == "fpr" { print $10; exit }')"
gpg --homedir "$fixture/home" --batch --armor --export "$extra" >> "$pin"
fail 'exactly one public primary' run release-validate-pin
fail 'exactly one public primary' "$fixture/repo/scripts/release-insert-anchors.sh"
cp "$fixture/pin-copy" "$pin"
rm "$pin"
fail 'default GPG home' env HOME="$fixture/other-home" GONIMBUS_GPG_HOMEDIR="$fixture/other-home/.gnupg" make --no-print-directory -s -C "$fixture/repo" release-export-pin
ln -s "$fixture/pin-copy" "$pin"
fail 'public pin already exists' run release-export-pin
fail 'public GPG pin' run release-validate-pin
echo '[ok] Synthetic pin export, anchor derivation, no-overwrite and precursor guards passed'
