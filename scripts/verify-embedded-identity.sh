#!/usr/bin/env bash

set -euo pipefail

SRC=${1:-.fulmen/app.yaml}
DST=${2:-internal/assets/appidentity/app.yaml}

if [ ! -f "${SRC}" ]; then
    echo "❌ Missing source identity file: ${SRC}" >&2
    exit 1
fi

if [ ! -f "${DST}" ]; then
    echo "❌ Missing embedded identity mirror: ${DST}" >&2
    echo "Run: make sync-embedded-identity" >&2
    exit 1
fi

if ! cmp -s "${SRC}" "${DST}"; then
    echo "❌ Embedded identity mirror is out of sync" >&2
    echo "  ${SRC} != ${DST}" >&2
    echo "Run: make sync-embedded-identity" >&2
    exit 1
fi

echo "✅ Embedded identity mirror is in sync"
