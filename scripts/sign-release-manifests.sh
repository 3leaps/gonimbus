#!/usr/bin/env bash

set -euo pipefail

# Dual-format release signing: minisign and PGP, both required.
# Signs checksum manifests only (SHA256SUMS, SHA512SUMS).
#
# Usage: sign-release-manifests.sh <tag> [dir]
#
# Env:
#   GONIMBUS_MINISIGN_KEY - path to minisign secret key (required)
#   GONIMBUS_PGP_KEY_ID   - exact signing-subkey selector <40-hex>! (required)
#   GONIMBUS_GPG_HOMEDIR  - isolated gpg homedir for signing (required)
#   CI                   - if "true", signing is refused (safety guard)

TAG=${1:?'usage: sign-release-manifests.sh <tag> [dir]'}
DIR=${2:-dist/release}

if [ "${CI:-}" = "true" ]; then
    echo "error: signing is disabled in CI" >&2
    exit 1
fi

if [ ! -d "$DIR" ]; then
    echo "error: directory $DIR not found" >&2
    exit 1
fi

MINISIGN_KEY="${GONIMBUS_MINISIGN_KEY:-}"
PGP_KEY_ID="${GONIMBUS_PGP_KEY_ID:-}"
GPG_HOME="${GONIMBUS_GPG_HOMEDIR:-}"

if [ -z "${MINISIGN_KEY}" ] || [ -z "${PGP_KEY_ID}" ]; then
    echo "error: both GONIMBUS_MINISIGN_KEY and GONIMBUS_PGP_KEY_ID are required (publish refuses a release missing either signature)" >&2
    exit 1
fi

if [ -n "${MINISIGN_KEY}" ]; then
    if [ ! -f "${MINISIGN_KEY}" ]; then
        echo "error: GONIMBUS_MINISIGN_KEY=${MINISIGN_KEY} not found" >&2
        exit 1
    fi
    if ! command -v minisign > /dev/null 2>&1; then
        echo "error: minisign not found in PATH" >&2
        echo "  install: brew install minisign (macOS) or see https://jedisct1.github.io/minisign/" >&2
        exit 1
    fi
    echo "minisign signing enabled (key: ${MINISIGN_KEY})"
fi

if [ -n "${PGP_KEY_ID}" ]; then
    if ! command -v gpg > /dev/null 2>&1; then
        echo "error: GONIMBUS_PGP_KEY_ID set but gpg not found in PATH" >&2
        exit 1
    fi
    if [ -z "${GPG_HOME}" ]; then
        echo "error: GONIMBUS_GPG_HOMEDIR must be set for PGP signing" >&2
        exit 1
    fi
    if ! gpg --homedir "${GPG_HOME}" --list-secret-keys "${PGP_KEY_ID}" > /dev/null 2>&1; then
        echo "error: secret key ${PGP_KEY_ID} not found in GONIMBUS_GPG_HOMEDIR=${GPG_HOME}" >&2
        exit 1
    fi
    echo "PGP signing enabled (key: ${PGP_KEY_ID}, homedir: ${GPG_HOME})"
fi

echo ""

if [ ! -f "${DIR}/SHA256SUMS" ]; then
    echo "error: ${DIR}/SHA256SUMS not found (run 'make release-checksums' or 'make release-build' first)" >&2
    exit 1
fi

timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

sign_minisign() {
    local manifest="$1"
    local base="${DIR}/${manifest}"

    if [ ! -f "${base}" ]; then
        return 0
    fi

    echo "🔏 [minisign] Signing ${manifest}"
    rm -f "${base}.minisig"
    minisign -S -s "${MINISIGN_KEY}" -t "gonimbus ${TAG} ${timestamp}" -m "${base}"
}

sign_pgp() {
    local manifest="$1"
    local base="${DIR}/${manifest}"

    if [ ! -f "${base}" ]; then
        return 0
    fi

    echo "🔏 [PGP] Signing ${manifest}"
    rm -f "${base}.asc"
    gpg --batch --yes --armor --homedir "${GPG_HOME}" --local-user "${PGP_KEY_ID}" --detach-sign -o "${base}.asc" "${base}"
}

sign_minisign "SHA256SUMS"
sign_minisign "SHA512SUMS"
sign_pgp "SHA256SUMS"
sign_pgp "SHA512SUMS"

echo ""
echo "✅ Signing complete for ${TAG}"
echo "   minisign: SHA256SUMS.minisig$([ -f "${DIR}/SHA512SUMS" ] && echo ", SHA512SUMS.minisig")"
echo "   PGP: SHA256SUMS.asc$([ -f "${DIR}/SHA512SUMS" ] && echo ", SHA512SUMS.asc")"
