#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Manifest, signature, and anchor writers refuse to write through an existing
# entry or planted symlink, leaving link targets untouched.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(cd "$(mktemp -d)" && pwd -P)"
trap 'rm -rf "$scratch"' EXIT

assets="$scratch/assets"
mkdir -p "$assets"
printf 'artifact\n' > "$assets/gonimbus-fixture.tar.gz"
printf 'victim\n' > "$scratch/victim"

refused() {
    local reason="$1"
    shift
    if "$@" > "$scratch/out" 2>&1; then
        echo "error: accepted: $*" >&2
        exit 1
    fi
    grep -q -- "$reason" "$scratch/out" || {
        echo "error: refused for another reason than '$reason'" >&2
        cat "$scratch/out" >&2
        exit 1
    }
    [[ "$(cat "$scratch/victim")" == victim ]] || {
        echo 'error: the link target was written' >&2
        exit 1
    }
}

# 1. A manifest planted as a link or pre-existing file is refused, and its target is untouched.
for manifest in SHA256SUMS SHA512SUMS; do
    ln -s "$scratch/victim" "$assets/$manifest"
    refused "$manifest already exists" "$root/scripts/generate-checksums.sh" "$assets" gonimbus
    rm "$assets/$manifest"
done

"$root/scripts/generate-checksums.sh" "$assets" gonimbus > /dev/null
[[ -f "$assets/SHA256SUMS" && ! -L "$assets/SHA256SUMS" ]]
[[ -f "$assets/SHA512SUMS" && ! -L "$assets/SHA512SUMS" ]]

# Calling generate-checksums again on an already-populated directory is refused.
refused 'SHA256SUMS already exists' "$root/scripts/generate-checksums.sh" "$assets" gonimbus

# 2. A signature planted as a link or pre-existing file is refused before anything is signed.
printf 'not a key\n' > "$scratch/minisign.key"
export GONIMBUS_MINISIGN_KEY="$scratch/minisign.key"
export GONIMBUS_PGP_KEY_ID='0123456789ABCDEF0123456789ABCDEF01234567!'
export GONIMBUS_GPG_HOMEDIR="$scratch/gpg"
for signature in SHA256SUMS.minisig SHA512SUMS.minisig SHA256SUMS.asc SHA512SUMS.asc; do
    ln -s "$scratch/victim" "$assets/$signature"
    refused "$signature already exists" "$root/scripts/sign-release-manifests.sh" v1.2.3 "$assets"
    rm "$assets/$signature"
done

# 3. Anchor writer refuses to clobber an existing file or planted symlink (set -C).
anchor_link="$scratch/anchor-link"
ln -s "$scratch/victim" "$anchor_link"
if GONIMBUS_RELEASE_TAG=v1.2.3 \
    GONIMBUS_RELEASE_ANCHOR_FILE="$anchor_link" \
    GONIMBUS_EXPECTED_TAG_OBJECT=1111111111111111111111111111111111111111 \
    GONIMBUS_EXPECTED_COMMIT=2222222222222222222222222222222222222222 \
    "$root/scripts/release-record-anchor.sh" > /dev/null 2>&1; then
    echo 'error: release-record-anchor overwrote a planted symlink' >&2
    exit 1
fi
[[ "$(cat "$scratch/victim")" == victim ]]

echo '[ok] manifest, signature, and anchor writers refuse to write through existing entries'
