#!/usr/bin/env bash
# release-verify-signatures fails closed: every one of the four signatures and
# both public keys must be present and verify, by the anchored primary's only
# signing subkey. Throwaway keys in temporary homes; nothing is written inside
# the repository. Also runs the Make target with relative and absolute paths.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"
scratch="$(cd "$(mktemp -d)" && pwd -P)"
cleanup() {
    local home
    for home in "$scratch"/gpg-*/; do
        gpgconf --homedir "$home" --kill all > /dev/null 2>&1 || true
    done
    rm -rf "$scratch"
}
trap cleanup EXIT
unset RELEASE_TAG

# A key with one signing subkey, exported public-only. The primary can also
# sign, like the real release key, so a primary-made signature is testable.
new_key() {
    local home="$scratch/gpg-$1"
    mkdir -m 700 "$home"
    gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
        --quick-generate-key "Synthetic $1 <$1@example.invalid>" ed25519 cert,sign 1d > /dev/null 2>&1
    local fpr
    fpr="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
    gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$fpr" ed25519 sign 1d > /dev/null 2>&1
    printf '%s\n' "$fpr"
}
subkey_of() {
    gpg --homedir "$scratch/gpg-$1" --batch --with-colons --with-subkey-fingerprint --list-keys |
        awk -F: '$1=="sub" {s=1; next} s && $1=="fpr" {print $10; exit}'
}
primary="$(new_key good)"
other="$(new_key other)"
good_sub="$(subkey_of good)"
other_sub="$(subkey_of other)"

# The repository copy supplies the script and the primary anchor only.
repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/keys"
cp "$root/scripts/release-verify-signatures.sh" "$repo/scripts/"
minisign -G -W -p "$scratch/minisign.pub" -s "$scratch/minisign.key" > /dev/null
minisign -G -W -p "$scratch/other-minisign.pub" -s "$scratch/other-minisign.key" > /dev/null
minisign_pin="$(sed -n 2p "$scratch/minisign.pub" | base64 -d | shasum -a 256 | awk '{print $1}')"
printf 'gpg %s\nminisign %s\n' "$primary" "$minisign_pin" > "$repo/keys/expected-fingerprints.txt"

stage() {
    local dir="$1" pgp_home="${2:-$scratch/gpg-good}" msk="${3:-$scratch/minisign.key}"
    local signer="${4:-}" mpub="${5:-$scratch/minisign.pub}"
    if [[ -z "$signer" ]]; then
        if [[ "$pgp_home" == "$scratch/gpg-other" ]]; then signer="$other_sub!"; else signer="$good_sub!"; fi
    fi
    rm -rf "$dir"
    mkdir -p "$dir"
    printf 'binary\n' > "$dir/gonimbus-linux-amd64"
    (cd "$dir" && sha_sum 256 gonimbus-linux-amd64 > SHA256SUMS && sha_sum 512 gonimbus-linux-amd64 > SHA512SUMS)
    for m in SHA256SUMS SHA512SUMS; do
        minisign -S -s "$msk" -m "$dir/$m" -x "$dir/$m.minisig" > /dev/null < /dev/null
        gpg --homedir "$pgp_home" --batch --yes --pinentry-mode loopback --passphrase '' \
            --local-user "$signer" --armor --detach-sign --output "$dir/$m.asc" "$dir/$m" > /dev/null 2>&1
    done
    cp "$mpub" "$dir/gonimbus-minisign.pub"
    gpg --homedir "$scratch/gpg-good" --batch --armor --export "$primary" > "$dir/gonimbus-release-signing-key.asc"
}
verify() { (cd "$repo" && ./scripts/release-verify-signatures.sh "$@"); }
refuse() {
    local reason="$1" dir="$2" expect="${3:-error:}"
    if verify "$dir" > "$scratch/out" 2>&1; then
        echo "error: accepted: $reason" >&2
        exit 1
    fi
    grep -qF -- "$expect" "$scratch/out" || {
        echo "error: no named refusal for: $reason (want: $expect)" >&2
        cat "$scratch/out" >&2
        exit 1
    }
}

d="$scratch/dist"
stage "$d"
verify "$d" > /dev/null

# Each of the four signatures, missing and tampered, refuses on its own.
for sig in SHA256SUMS.minisig SHA512SUMS.minisig SHA256SUMS.asc SHA512SUMS.asc; do
    stage "$d"
    rm "$d/$sig"
    refuse "missing $sig" "$d"
    stage "$d"
    head -c 40 "$d/$sig" > "$d/$sig.cut" && mv "$d/$sig.cut" "$d/$sig"
    refuse "truncated $sig" "$d"
done
# A manifest changed after signing refuses (both formats cover it).
for m in SHA256SUMS SHA512SUMS; do
    stage "$d"
    printf 'extra\n' >> "$d/$m"
    refuse "manifest $m edited after signing" "$d"
done
# Either public key missing refuses.
for key in gonimbus-minisign.pub gonimbus-release-signing-key.asc; do
    stage "$d"
    rm "$d/$key"
    refuse "missing $key" "$d"
done
# PGP signatures from a different key refuse, even with that key's export.
stage "$d" "$scratch/gpg-other"
refuse 'PGP signed by an unanchored key' "$d"
stage "$d" "$scratch/gpg-other"
gpg --homedir "$scratch/gpg-other" --batch --armor --export "$other" > "$d/gonimbus-release-signing-key.asc"
refuse 'PGP key export swapped to the signing key' "$d"
# minisign signatures by a different key refuse.
stage "$d" "$scratch/gpg-good" "$scratch/other-minisign.key"
refuse 'minisign signed by a different key' "$d"
# A set re-signed by another minisign key with that key staged beside it
# refuses: the staged key must hash to the anchor.
stage "$d" "$scratch/gpg-good" "$scratch/other-minisign.key" '' "$scratch/other-minisign.pub"
refuse 'minisign key swapped to the signing key' "$d" 'does not match the minisign anchor'
# A staged minisign key with trailing junk, a missing final newline, or an
# extra line refuses even though its key line is the anchored one.
for shape in junk nonl extra; do
    stage "$d"
    case "$shape" in
    junk) printf 'junk' >> "$d/gonimbus-minisign.pub" ;;
    nonl) printf '%s\n%s' "$(sed -n 1p "$scratch/minisign.pub")" "$(sed -n 2p "$scratch/minisign.pub")" > "$d/gonimbus-minisign.pub" ;;
    extra) printf 'extra\n' >> "$d/gonimbus-minisign.pub" ;;
    esac
    refuse "minisign key file shape: $shape" "$d" 'exactly a comment line and a key line'
done
# A first line that is not an untrusted comment refuses.
stage "$d"
printf 'not a comment\n%s\n' "$(sed -n 2p "$scratch/minisign.pub")" > "$d/gonimbus-minisign.pub"
refuse 'minisign key without a comment line' "$d" 'staged minisign key is malformed'
# A PGP signature made by the anchored primary itself, not its signing subkey,
# refuses.
stage "$d" "$scratch/gpg-good" "$scratch/minisign.key" "$primary!"
refuse 'PGP signed by the primary, not the pinned subkey' "$d" 'is not a good signature by the pinned subkey'

# The Make target fails closed and resolves paths the same way, relative or absolute.
mk="$scratch/mk"
mkdir -p "$mk/scripts" "$mk/keys"
cp "$repo/scripts/release-verify-signatures.sh" "$mk/scripts/"
cp "$repo/keys/expected-fingerprints.txt" "$mk/keys/"
sed -n '/^release-verify-signatures:/,/^$/p' "$root/Makefile" > "$mk/Makefile"
printf 'DIST_RELEASE ?= dist/release\n' >> "$mk/Makefile"
stage "$mk/dist/release"
make --no-print-directory -C "$mk" release-verify-signatures > /dev/null
make --no-print-directory -C "$mk" release-verify-signatures DIST_RELEASE="$mk/dist/release" > /dev/null
rm "$mk/dist/release/SHA512SUMS.asc"
for target_dir in dist/release "$mk/dist/release"; do
    if make --no-print-directory -C "$mk" release-verify-signatures DIST_RELEASE="$target_dir" > /dev/null 2>&1; then
        echo "error: make target accepted a missing PGP signature (DIST_RELEASE=$target_dir)" >&2
        exit 1
    fi
done
stage "$mk/dist/release"
head -c 40 "$mk/dist/release/SHA256SUMS.minisig" > "$scratch/cut" && mv "$scratch/cut" "$mk/dist/release/SHA256SUMS.minisig"
if make --no-print-directory -C "$mk" release-verify-signatures > /dev/null 2>&1; then
    echo 'error: make target accepted a damaged minisign signature' >&2
    exit 1
fi

echo '[ok] signature verification requires all four signatures and both keys, by the pinned subkey'
