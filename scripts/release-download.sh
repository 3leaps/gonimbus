#!/usr/bin/env bash

set -euo pipefail

TAG="${1:-}"
DEST_DIR="${2:-dist/release}"

if [[ -z "${TAG}" ]]; then
    echo "usage: $0 vX.Y.Z [dest_dir]" >&2
    exit 1
fi

if ! command -v gh > /dev/null 2>&1; then
    echo "❌ gh (GitHub CLI) not found in PATH" >&2
    echo "Install: https://cli.github.com/" >&2
    exit 1
fi

mkdir -p "${DEST_DIR}"

echo "→ Downloading release assets for ${TAG} into ${DEST_DIR}"
# --clobber allows re-running safely
gh release download "${TAG}" --dir "${DEST_DIR}" --clobber

echo "✅ Download complete"
