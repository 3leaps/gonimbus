#!/usr/bin/env bash

set -euo pipefail

FILE="${1:-}"

if [[ -z "${FILE}" ]]; then
    echo "usage: $0 path/to/minisign.pub" >&2
    exit 1
fi

if [[ ! -f "${FILE}" ]]; then
    echo "❌ File not found: ${FILE}" >&2
    exit 1
fi

if rg -n "minisign secret key" "${FILE}" > /dev/null 2>&1; then
    echo "❌ Refusing: minisign secret key material detected in ${FILE}" >&2
    exit 1
fi

echo "✅ Looks like a public-only minisign key: ${FILE}"
