#!/usr/bin/env bash

set -euo pipefail

DIR=${1:-dist/release}
BINARY_NAME=${2:-}

if [ -z "${BINARY_NAME}" ]; then
    echo "usage: $0 [dir] <binary_name>" >&2
    exit 1
fi

if [ ! -d "${DIR}" ]; then
    echo "error: directory ${DIR} not found" >&2
    exit 1
fi

cd "${DIR}"

rm -f SHA256SUMS.txt SHA256SUMS.txt.*

if ls "${BINARY_NAME}-"* > /dev/null 2>&1; then
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum "${BINARY_NAME}-"* > SHA256SUMS
    else
        shasum -a 256 "${BINARY_NAME}-"* > SHA256SUMS
    fi

    if command -v sha512sum > /dev/null 2>&1; then
        sha512sum "${BINARY_NAME}-"* > SHA512SUMS
    else
        shasum -a 512 "${BINARY_NAME}-"* > SHA512SUMS
    fi
else
    echo "error: no artifacts found matching ${BINARY_NAME}-* in ${DIR}" >&2
    exit 1
fi

echo "✅ Wrote ${DIR}/SHA256SUMS and ${DIR}/SHA512SUMS"
