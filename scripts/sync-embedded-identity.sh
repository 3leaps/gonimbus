#!/usr/bin/env bash

set -euo pipefail

SRC=${1:-.fulmen/app.yaml}
DST=${2:-internal/assets/appidentity/app.yaml}

if [ ! -f "${SRC}" ]; then
    echo "❌ Missing source identity file: ${SRC}" >&2
    exit 1
fi

mkdir -p "$(dirname "${DST}")"

cp "${SRC}" "${DST}"

echo "✅ Synced ${SRC} → ${DST}"
