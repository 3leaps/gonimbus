#!/usr/bin/env bash

set -euo pipefail

# Export public signing keys into the release artifact directory.
# Usage: export-release-keys.sh [dir]
#
# Env:
#   SIGNING_ENV_PREFIX - prefix for "<APP>_" env var lookups (ex: GRONINGEN)
#   SIGNING_APP_NAME   - used for output file naming (ex: groningen)
#   MINISIGN_KEY       - path to minisign secret key (used to locate .pub)
#   MINISIGN_PUB       - optional explicit path to minisign public key
#   PGP_KEY_ID         - gpg key/email/fingerprint to export (optional)
#   GPG_HOME           - isolated gpg homedir containing the signing key (required if PGP_KEY_ID is set)

DIR=${1:-dist/release}
mkdir -p "$DIR"

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

exported_any=false

if [ -n "${MINISIGN_KEY}" ] || [ -n "${MINISIGN_PUB}" ]; then
    pub_path="${MINISIGN_PUB}"
    if [ -z "${pub_path}" ]; then
        pub_path="${MINISIGN_KEY%.key}.pub"
    fi

    if [ ! -f "${pub_path}" ]; then
        echo "error: minisign public key not found (expected at ${pub_path}); set MINISIGN_PUB to override" >&2
        exit 1
    fi

    out="${DIR}/${SIGNING_APP_NAME}-minisign.pub"
    cp "${pub_path}" "${out}"
    echo "✅ Exported minisign public key to ${out}"
    exported_any=true
else
    echo "ℹ️  Skipping minisign public key export (set MINISIGN_KEY or MINISIGN_PUB to enable)"
fi

if [ -n "${PGP_KEY_ID}" ]; then
    if ! command -v gpg > /dev/null 2>&1; then
        echo "error: gpg not found in PATH (required to export PGP key)" >&2
        exit 1
    fi
    if [ -z "${GPG_HOME}" ]; then
        echo "error: GPG_HOME (or ${SIGNING_ENV_PREFIX}_GPG_HOME) must be set for PGP export" >&2
        exit 1
    fi
    if ! gpg --homedir "${GPG_HOME}" --list-keys "${PGP_KEY_ID}" > /dev/null 2>&1; then
        echo "error: public key ${PGP_KEY_ID} not found in GPG_HOME=${GPG_HOME}" >&2
        exit 1
    fi
    out="${DIR}/fulmenhq-release-signing-key.asc"
    gpg --homedir "${GPG_HOME}" --armor --output "${out}" --export "${PGP_KEY_ID}"
    echo "✅ Exported PGP public key to ${out} (homedir: ${GPG_HOME})"
    exported_any=true
else
    echo "ℹ️  Skipping PGP public key export (set PGP_KEY_ID to enable)"
fi

if [ "${exported_any}" = false ]; then
    echo "warning: no keys exported (set MINISIGN_KEY/MINISIGN_PUB and/or PGP_KEY_ID)" >&2
fi
