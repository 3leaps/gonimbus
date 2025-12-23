#!/usr/bin/env bash

set -euo pipefail

# Dual-format release signing: minisign (primary) + optional PGP.
# Signs checksum manifests only (SHA256SUMS, SHA512SUMS).
#
# Usage: sign-release-manifests.sh <tag> [dir]
#
# Env:
#   SIGNING_ENV_PREFIX - prefix for "<APP>_" env var lookups (ex: GRONINGEN)
#   SIGNING_APP_NAME   - human-readable name for signing metadata (ex: groningen)
#   MINISIGN_KEY       - path to minisign secret key (required for minisign signing)
#   MINISIGN_PUB       - optional path to minisign public key (not required for signing)
#   PGP_KEY_ID         - gpg key/email/fingerprint for PGP signing (optional)
#   GPG_HOME           - isolated gpg homedir for signing (required if PGP_KEY_ID is set)
#   CI                 - if "true", signing is refused (safety guard)

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

SIGNING_ENV_PREFIX=${SIGNING_ENV_PREFIX:-}
SIGNING_APP_NAME=${SIGNING_APP_NAME:-workhorse}

get_var() {
    local name="$1"
    local val="${!name:-}"
    if [ -n "$val" ]; then
        echo "$val"
        return 0
    fi

    if [ -n "${SIGNING_ENV_PREFIX}" ]; then
        local prefixed_name="${SIGNING_ENV_PREFIX}_${name}"
        echo "${!prefixed_name:-}"
        return 0
    fi

    echo ""
}

MINISIGN_KEY="$(get_var MINISIGN_KEY)"
MINISIGN_PUB="$(get_var MINISIGN_PUB)"
PGP_KEY_ID="$(get_var PGP_KEY_ID)"
GPG_HOME="$(get_var GPG_HOME)"

# NOTE: MINISIGN_PUB is intentionally unused for signing; it is used by export-release-keys.sh.

has_minisign=false
has_pgp=false

if [ -n "${MINISIGN_KEY}" ]; then
    if [ ! -f "${MINISIGN_KEY}" ]; then
        echo "error: MINISIGN_KEY=${MINISIGN_KEY} not found" >&2
        exit 1
    fi
    if ! command -v minisign > /dev/null 2>&1; then
        echo "error: minisign not found in PATH" >&2
        echo "  install: brew install minisign (macOS) or see https://jedisct1.github.io/minisign/" >&2
        exit 1
    fi
    has_minisign=true
    echo "minisign signing enabled (key: ${MINISIGN_KEY})"
fi

if [ -n "${PGP_KEY_ID}" ]; then
    if ! command -v gpg > /dev/null 2>&1; then
        echo "error: PGP_KEY_ID set but gpg not found in PATH" >&2
        exit 1
    fi
    if [ -z "${GPG_HOME}" ]; then
        echo "error: GPG_HOME (or ${SIGNING_ENV_PREFIX}_GPG_HOME) must be set for PGP signing" >&2
        exit 1
    fi
    if ! gpg --homedir "${GPG_HOME}" --list-secret-keys "${PGP_KEY_ID}" > /dev/null 2>&1; then
        echo "error: secret key ${PGP_KEY_ID} not found in GPG_HOME=${GPG_HOME}" >&2
        exit 1
    fi
    has_pgp=true
    echo "PGP signing enabled (key: ${PGP_KEY_ID}, homedir: ${GPG_HOME})"
fi

echo ""

if [ "${has_minisign}" = false ] && [ "${has_pgp}" = false ]; then
    echo "error: no signing method available" >&2
    echo "  set MINISIGN_KEY (or ${SIGNING_ENV_PREFIX}_MINISIGN_KEY) for minisign signing" >&2
    echo "  optionally set PGP_KEY_ID (or ${SIGNING_ENV_PREFIX}_PGP_KEY_ID) for PGP signing" >&2
    exit 1
fi

if [ ! -f "${DIR}/SHA256SUMS" ]; then
    echo "error: ${DIR}/SHA256SUMS not found (run 'make checksums' or 'make release-build' first)" >&2
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
    minisign -S -s "${MINISIGN_KEY}" -t "${SIGNING_APP_NAME} ${TAG} ${timestamp}" -m "${base}"
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

if [ "${has_minisign}" = true ]; then
    sign_minisign "SHA256SUMS"
    sign_minisign "SHA512SUMS"
fi

if [ "${has_pgp}" = true ]; then
    sign_pgp "SHA256SUMS"
    sign_pgp "SHA512SUMS"
fi

echo ""
echo "✅ Signing complete for ${TAG}"
if [ "${has_minisign}" = true ]; then
    echo "   minisign: SHA256SUMS.minisig$([ -f "${DIR}/SHA512SUMS" ] && echo ", SHA512SUMS.minisig")"
fi
if [ "${has_pgp}" = true ]; then
    echo "   PGP: SHA256SUMS.asc$([ -f "${DIR}/SHA512SUMS" ] && echo ", SHA512SUMS.asc")"
fi
