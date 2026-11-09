#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
# shellcheck source=scripts/release-gh.sh
source "$(dirname "${BASH_SOURCE[0]}")/release-gh.sh"

TAG="${1:-}"
DIR="${2:-dist/release}"
REPO="${GONIMBUS_RELEASE_REPO:-3leaps/gonimbus}"

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

scratch_dir="$(mktemp -d)"
trap 'rm -rf "${scratch_dir}"' EXIT

verify_manifest_matches_draft() {
    local tag="$1"
    local manifest_name="$2"
    local local_path="$3"

    local tmpdir="${scratch_dir}/${manifest_name}"
    rm -rf "${tmpdir}"
    mkdir -p "${tmpdir}"

    if ! release_gh release download "${tag}" --repo "${REPO}" --pattern "${manifest_name}" --dir "${tmpdir}" --clobber > /dev/null 2>&1 ||
        [[ ! -f "${tmpdir}/${manifest_name}" ]]; then
        echo "❌ Failed to download '${manifest_name}' from release ${tag}" >&2
        exit 1
    fi

    if ! cmp -s "${tmpdir}/${manifest_name}" "${local_path}"; then
        echo "❌ Refusing to upload: local ${manifest_name} does not match draft release asset" >&2
        echo "   Re-run: make release-download (or regenerate manifests carefully)" >&2
        exit 1
    fi

    echo "✅ Verified ${manifest_name} matches draft release asset"
}

assets=(
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

if [[ ! -f "${DIR}/SHA256SUMS" ]]; then
    echo "❌ Missing ${DIR}/SHA256SUMS (run 'make release-download' first)" >&2
    exit 1
fi

for required in \
    "${DIR}/SHA256SUMS.minisig" \
    "${DIR}/SHA256SUMS.asc" \
    "${DIR}/SHA512SUMS" \
    "${DIR}/SHA512SUMS.minisig" \
    "${DIR}/SHA512SUMS.asc" \
    "${DIR}/gonimbus-minisign.pub" \
    "${DIR}/gonimbus-release-signing-key.asc" \
    "${DIR}/expected-fingerprints.txt" \
    "${DIR}/expected-fingerprints.ndjson" \
    "${DIR}/release-notes-${TAG}.md"; do
    if [[ ! -f "${required}" ]]; then
        echo "❌ Missing required provenance asset: ${required}" >&2
        echo "   Ensure you ran: make release-sign && make release-export-keys && make release-notes" >&2
        exit 1
    fi
done

verify_manifest_matches_draft "${TAG}" "SHA256SUMS" "${DIR}/SHA256SUMS"
verify_manifest_matches_draft "${TAG}" "SHA512SUMS" "${DIR}/SHA512SUMS"

is_draft="$(release_gh release view "${TAG}" --repo "${REPO}" --json isDraft --jq .isDraft)"
if [[ "${is_draft}" != "true" ]]; then
    echo "❌ Refusing to upload: release ${TAG} is not a draft (isDraft=${is_draft})" >&2
    exit 1
fi

echo "→ Uploading ${#assets[@]} provenance assets to ${TAG} (clobber enabled)"
release_gh release upload "${TAG}" --repo "${REPO}" "${assets[@]}" --clobber

echo "✅ Upload complete"
