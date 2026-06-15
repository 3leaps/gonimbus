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

print_distribution_details() {
    local tag="$1"
    local source_dir="$2"
    local sums_file="${source_dir}/SHA256SUMS"

    if [[ ! -f "${sums_file}" ]]; then
        echo "ℹ️  Distribution details unavailable: ${sums_file} not found"
        return 0
    fi

    if ! asset_urls="$(gh release view "${tag}" --json assets --jq '.assets[] | [.name, .url] | @tsv')" || [[ -z "${asset_urls}" ]]; then
        echo "ℹ️  Distribution details unavailable: could not read uploaded release assets"
        return 0
    fi

    asset_url() {
        local name="$1"
        awk -F '\t' -v name="${name}" '$1 == name {print $2; found=1; exit} END {exit found ? 0 : 1}' <<< "${asset_urls}"
    }

    asset_sha256() {
        local name="$1"
        awk -v name="${name}" '$2 == name || $2 == "*" name {print $1; found=1; exit} END {exit found ? 0 : 1}' "${sums_file}"
    }

    print_asset() {
        local name="$1"
        local url
        local sha256

        if ! url="$(asset_url "${name}")"; then
            echo "# ${name}: uploaded asset URL not found"
            return 0
        fi
        if ! sha256="$(asset_sha256 "${name}")"; then
            echo "# ${name}: SHA256 not found in SHA256SUMS"
            return 0
        fi

        echo "${name}"
        echo "  url: ${url}"
        echo "  sha256: ${sha256}"
    }

    cat << EOF

Package-manager asset details:

Homebrew (3leaps/homebrew-tap Formula/gonimbus.rb):
EOF
    print_asset "gonimbus-darwin-arm64"
    print_asset "gonimbus-linux-amd64"
    print_asset "gonimbus-linux-arm64"

    cat << EOF

Scoop (3leaps/scoop-bucket bucket/gonimbus.json):
EOF
    print_asset "gonimbus-windows-amd64.exe"
    print_asset "gonimbus-windows-arm64.exe"
}

# Upload only provenance outputs (never binaries) to avoid clobbering CI-built assets.
# Expected inputs:
# - SHA256SUMS, SHA512SUMS
# - SHA256SUMS.minisig/.asc, SHA512SUMS.minisig/.asc
# - *.pub and *release-signing-key.asc
# - release-notes-*.md
shopt -s nullglob

assets=()
assets+=("${SOURCE_DIR}/SHA256SUMS" "${SOURCE_DIR}/SHA512SUMS")
assets+=("${SOURCE_DIR}/SHA256SUMS."* "${SOURCE_DIR}/SHA512SUMS."*)
assets+=("${SOURCE_DIR}"/*.pub)
assets+=("${SOURCE_DIR}"/*release-signing-key.asc)
assets+=("${SOURCE_DIR}"/release-notes-*.md)

final_assets=()
for f in "${assets[@]}"; do
    if [[ -f "$f" ]]; then
        final_assets+=("$f")
    fi
done

if [[ ${#final_assets[@]} -eq 0 ]]; then
    echo "❌ No provenance assets found to upload from ${SOURCE_DIR}" >&2
    exit 1
fi

echo "→ Uploading ${#final_assets[@]} provenance asset(s) to ${TAG} (clobber)"
gh release upload "${TAG}" "${final_assets[@]}" --clobber

echo "✅ Upload complete"
print_distribution_details "${TAG}" "${SOURCE_DIR}" || true
