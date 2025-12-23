#!/usr/bin/env bash

set -euo pipefail

DIR=${1:-dist/release}

if [ ! -d "${DIR}" ]; then
    echo "error: directory ${DIR} not found" >&2
    exit 1
fi

cd "${DIR}"

if [ ! -f SHA256SUMS ]; then
    echo "error: missing ${DIR}/SHA256SUMS" >&2
    exit 1
fi

if command -v sha256sum > /dev/null 2>&1; then
    sha256sum -c SHA256SUMS
else
    shasum -a 256 -c SHA256SUMS
fi

if [ ! -f SHA512SUMS ]; then
    echo "error: missing ${DIR}/SHA512SUMS" >&2
    exit 1
fi

if command -v sha512sum > /dev/null 2>&1; then
    sha512sum -c SHA512SUMS
else
    shasum -a 512 -c SHA512SUMS
fi

echo "✅ Checksums verified"
