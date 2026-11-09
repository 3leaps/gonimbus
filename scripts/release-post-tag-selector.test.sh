#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
version="$(tr -d '[:space:]' < "$root/VERSION")"
tag="v$version"
primary="$(awk '$1=="gpg" {print $2}' "$root/keys/expected-fingerprints.txt")"
subkey="$(tr -d '[:space:]' < "$root/config/release/signing-subkey.txt")"

GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    "$root/scripts/release-post-tag-selector.sh" > /dev/null

# Missing bang (!) on subkey selector refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="$subkey" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted subkey selector without trailing !" >&2
    exit 1
fi

# Primary key passed as GONIMBUS_PGP_KEY_ID refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${primary}!" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted primary key instead of signing subkey" >&2
    exit 1
fi

# Wrong primary fingerprint refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$(printf '%040d' 0)" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted wrong primary fingerprint" >&2
    exit 1
fi

# Version mismatch refuses
if GONIMBUS_RELEASE_TAG="v99.99.99" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted mismatched tag version" >&2
    exit 1
fi

echo "[ok] post-tag selector controls passed"
