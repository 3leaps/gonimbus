#!/usr/bin/env bash

set -euo pipefail

TAG="${1:-}"
SOURCE_DIR="${2:-dist/release}"

if [[ -z "${TAG}" ]]; then
    echo "usage: $0 vX.Y.Z [source_dir]" >&2
    exit 1
fi

if ! command -v gh > /dev/null 2>&1; then
    echo "❌ gh (GitHub CLI) not found in PATH" >&2
    echo "Install: https://cli.github.com/" >&2
    exit 1
fi

if [[ ! -d "${SOURCE_DIR}" ]]; then
    echo "❌ Source dir not found: ${SOURCE_DIR}" >&2
    exit 1
fi

# Safety rail: the release pipeline should stage artifacts in dist/release.
# Abort if obvious dev/tooling artifacts are present.
if [[ -f "${SOURCE_DIR}/goneat" ]]; then
    echo "❌ Refusing: found ${SOURCE_DIR}/goneat (run 'make release-clean' and restage)" >&2
    exit 1
fi

shopt -s nullglob

assets=()
assets+=("${SOURCE_DIR}"/*-darwin-*)
assets+=("${SOURCE_DIR}"/*-linux-*)
assets+=("${SOURCE_DIR}"/*-windows-*.exe)

# Manifests
assets+=("${SOURCE_DIR}/SHA256SUMS" "${SOURCE_DIR}/SHA512SUMS")
assets+=("${SOURCE_DIR}/SHA256SUMS."* "${SOURCE_DIR}/SHA512SUMS."*)

# Public keys (optional depending on signing method)
assets+=("${SOURCE_DIR}"/*.pub)
assets+=("${SOURCE_DIR}"/*release-signing-key.asc)

# Release notes (optional)
assets+=("${SOURCE_DIR}"/release-notes-*.md)

# Keep only existing files (no associative arrays; bash 3.x compatible)
final_assets=()
for f in "${assets[@]}"; do
    if [[ -f "$f" ]]; then
        final_assets+=("$f")
    fi
done

if [[ ${#final_assets[@]} -eq 0 ]]; then
    echo "❌ No assets found to upload from ${SOURCE_DIR}" >&2
    exit 1
fi

echo "→ Uploading ${#final_assets[@]} asset(s) to ${TAG} (clobber)"
gh release upload "${TAG}" "${final_assets[@]}" --clobber

echo "✅ Upload complete"
