#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
# shellcheck source=scripts/release-gh.sh
source "$(dirname "${BASH_SOURCE[0]}")/release-gh.sh"

TAG="${1:-}"
DIR="${2:-dist/release}"

if [[ -z "${TAG}" ]]; then
    echo "usage: $0 vX.Y.Z [dir]" >&2
    exit 1
fi

if ! command -v gh > /dev/null 2>&1; then
    echo "❌ gh (GitHub CLI) not found in PATH" >&2
    echo "Install: https://cli.github.com/" >&2
    exit 1
fi

if [[ ! -d "${DIR}" ]]; then
    echo "❌ Directory not found: ${DIR}" >&2
    exit 1
fi

shopt -s nullglob

binaries=("${DIR}/gonimbus-"*)
filtered_binaries=()
for b in "${binaries[@]}"; do
    case "$b" in
        *.minisig | *.asc | *.sig) ;;
        *)
            if [[ "$(basename "$b")" == "gonimbus-minisign.pub" ]]; then
                continue
            fi
            filtered_binaries+=("$b")
            ;;
    esac
done

provenance=(
    "${DIR}/SHA256SUMS"
    "${DIR}/SHA256SUMS.minisig"
    "${DIR}/SHA256SUMS.asc"
    "${DIR}/SHA512SUMS"
    "${DIR}/SHA512SUMS.minisig"
    "${DIR}/SHA512SUMS.asc"
    "${DIR}/gonimbus-minisign.pub"
    "${DIR}/gonimbus-release-signing-key.asc"
    "${DIR}/expected-fingerprints.txt"
    "${DIR}/expected-fingerprints.ndjson"
    "${DIR}/release-notes-${TAG}.md"
)

final_assets=()
for f in "${filtered_binaries[@]}" "${provenance[@]}"; do
    if [[ -f "$f" ]]; then
        final_assets+=("$f")
    fi
done

if [[ ${#final_assets[@]} -eq 0 ]]; then
    echo "❌ No release assets found in ${DIR}" >&2
    exit 1
fi

is_draft="$(release_gh release view "${TAG}" --json isDraft --jq .isDraft)"
if [[ "${is_draft}" != "true" ]]; then
    echo "❌ Refusing to upload: release ${TAG} is not a draft (isDraft=${is_draft})" >&2
    exit 1
fi

echo "→ Uploading ${#final_assets[@]} assets to ${TAG} (clobber enabled)"
release_gh release upload "${TAG}" "${final_assets[@]}" --clobber

echo "✅ Upload complete"
