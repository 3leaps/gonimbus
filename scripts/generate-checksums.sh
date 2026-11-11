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

# Never write through a link or overwrite an existing manifest.
for manifest in SHA256SUMS SHA512SUMS; do
    if [ -e "${manifest}" ] || [ -L "${manifest}" ]; then
        echo "error: ${manifest} already exists" >&2
        exit 1
    fi
done

rm -f SHA256SUMS.txt SHA256SUMS.txt.*

shopt -s nullglob
candidates=("${BINARY_NAME}-"*)
binaries=()
for f in ${candidates[@]+"${candidates[@]}"}; do
    case "$f" in
        *.pub | *.asc | *.minisig | *.sig) ;;
        *) binaries+=("$f") ;;
    esac
done

if [ "${#binaries[@]}" -gt 0 ]; then
    set -C
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum ${binaries[@]+"${binaries[@]}"} > SHA256SUMS
    else
        shasum -a 256 ${binaries[@]+"${binaries[@]}"} > SHA256SUMS
    fi

    if command -v sha512sum > /dev/null 2>&1; then
        sha512sum ${binaries[@]+"${binaries[@]}"} > SHA512SUMS
    else
        shasum -a 512 ${binaries[@]+"${binaries[@]}"} > SHA512SUMS
    fi
else
    echo "error: no artifacts found matching ${BINARY_NAME}-* in ${DIR}" >&2
    exit 1
fi

echo "✅ Wrote ${DIR}/SHA256SUMS and ${DIR}/SHA512SUMS"
