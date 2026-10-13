#!/usr/bin/env bash
# Artifact signing and key export accept the exact signing-subkey selector (<fpr>!).
# Throwaway key in a temporary GNUPGHOME; nothing is written inside the repository.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"
scratch="$(mktemp -d)"
cleanup() {
    local home
    for home in "$scratch"/*/; do
        gpgconf --homedir "$home" --kill all > /dev/null 2>&1 || true
    done
    rm -rf "$scratch"
}
trap cleanup EXIT
home="$scratch/gpg"
mkdir -m 700 "$home"
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Synthetic signing <signing@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
primary="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
# Two signing subkeys: the selector must pick the named one, not the newest.
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --homedir "$home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
dist="$scratch/dist"
mkdir -p "$dist"
printf 'artifact\n' > "$dist/gonimbus-fixture.tar.gz"
(cd "$dist" && sha_sum 256 gonimbus-fixture.tar.gz > SHA256SUMS && sha_sum 512 gonimbus-fixture.tar.gz > SHA512SUMS)

minisign -G -W -p "$scratch/minisign.pub" -s "$scratch/minisign.key" > /dev/null

run() {
    env -u GONIMBUS_MINISIGN_PUB CI="${FIXTURE_CI:-}" \
        GONIMBUS_MINISIGN_KEY="${FIXTURE_MINISIGN_KEY-$scratch/minisign.key}" \
        GONIMBUS_PGP_KEY_ID="${FIXTURE_PGP_KEY_ID-$subkey!}" GONIMBUS_GPG_HOMEDIR="$home" "$@"
}
refused() {
    if run "$root/scripts/sign-release-manifests.sh" v1.2.3 "$dist" > /dev/null 2>&1; then
        echo "error: signing must be refused $1" >&2
        exit 1
    fi
}
FIXTURE_CI=true refused 'in CI'
FIXTURE_MINISIGN_KEY='' refused 'without a minisign key'
FIXTURE_PGP_KEY_ID='' refused 'without a PGP key'
[[ ! -e "$dist/SHA256SUMS.minisig" && ! -e "$dist/SHA256SUMS.asc" ]] || {
    echo 'error: a refused run must not leave a signature behind' >&2
    exit 1
}
run "$root/scripts/sign-release-manifests.sh" v1.2.3 "$dist" > /dev/null
for manifest in SHA256SUMS SHA512SUMS; do
    minisign -V -q -p "$scratch/minisign.pub" -m "$dist/$manifest" || {
        echo "error: $manifest minisign signature does not verify" >&2
        exit 1
    }
done
for manifest in SHA256SUMS SHA512SUMS; do
    signer="$(gpg --homedir "$home" --batch --status-fd 1 --verify "$dist/$manifest.asc" "$dist/$manifest" 2> /dev/null |
        awk '$2=="VALIDSIG" {print $3, $NF}')"
    [[ "$signer" == "$subkey $primary" ]] || {
        echo "error: $manifest was not signed by the selected subkey of the pinned primary" >&2
        exit 1
    }
done
run "$root/scripts/export-release-keys.sh" "$dist" > /dev/null
exported="$dist/gonimbus-release-signing-key.asc"
mkdir -m 700 "$scratch/show"
shown="$(gpg --homedir "$scratch/show" --batch --with-colons --show-keys "$exported" 2> /dev/null)"
[[ "$(awk -F: '$1=="sec"||$1=="ssb" {n++} END {print n+0}' <<< "$shown")" == 0 ]]
[[ "$(awk -F: '$1=="fpr" {print $10;exit}' <<< "$shown")" == "$primary" ]]
grep -q "^fpr:::::::::$subkey:" <<< "$shown"
"$root/scripts/verify-public-key.sh" "$exported" > /dev/null
echo '[ok] artifact signing requires both keys and accepts the exact subkey selector'
