# shellcheck shell=bash
# Portable digest helpers for release-tooling tests: hosted runners and macOS
# ship shasum; minimal Linux images ship only coreutils sha256sum/sha512sum.
sha_sum() {
    local bits="$1"
    shift
    if command -v "sha${bits}sum" > /dev/null 2>&1; then
        "sha${bits}sum" "$@"
    else
        shasum -a "$bits" "$@"
    fi
}
