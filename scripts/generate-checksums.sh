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

shopt -s nullglob
candidates=("${BINARY_NAME}-"*)
binaries=()
for f in "${candidates[@]}"; do
    case "$f" in
        *.pub | *.asc | *.minisig | *.sig) ;;
        *) binaries+=("$f") ;;
    esac
done

if [ "${#binaries[@]}" -gt 0 ]; then
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum "${binaries[@]}" > SHA256SUMS
    else
        shasum -a 256 "${binaries[@]}" > SHA256SUMS
    fi

    if command -v sha512sum > /dev/null 2>&1; then
        sha512sum "${binaries[@]}" > SHA512SUMS
    else
        shasum -a 512 "${binaries[@]}" > SHA512SUMS
    fi
else
    echo "error: no artifacts found matching ${BINARY_NAME}-* in ${DIR}" >&2
    exit 1
fi

echo "✅ Wrote ${DIR}/SHA256SUMS and ${DIR}/SHA512SUMS"
