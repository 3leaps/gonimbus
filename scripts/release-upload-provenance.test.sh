#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Controls for scripts/release-upload-provenance.sh with stubbed gh.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

mkdir -p "$scratch/bin" "$scratch/remote" "$scratch/local"
tag="v1.2.3"

stage_assets() {
    rm -rf "$scratch/remote" "$scratch/local"
    mkdir -p "$scratch/remote" "$scratch/local"
    printf '0000000000000000000000000000000000000000000000000000000000000001  gonimbus-linux-amd64\n' > "$scratch/remote/SHA256SUMS"
    printf '0000000000000000000000000000000000000000000000000000000000000002  gonimbus-linux-amd64\n' > "$scratch/remote/SHA512SUMS"
    cp "$scratch/remote/SHA256SUMS" "$scratch/local/SHA256SUMS"
    cp "$scratch/remote/SHA512SUMS" "$scratch/local/SHA512SUMS"
    for f in \
        SHA256SUMS.minisig \
        SHA256SUMS.asc \
        SHA512SUMS.minisig \
        SHA512SUMS.asc \
        gonimbus-minisign.pub \
        gonimbus-release-signing-key.asc \
        expected-fingerprints.txt \
        expected-fingerprints.ndjson \
        "release-notes-${tag}.md"; do
        printf 'fixture-%s\n' "$f" > "$scratch/local/$f"
    done
}

cat > "$scratch/bin/gh" << 'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "${GH_HOST:-}" == "github.com" ]] || { echo "unpinned GH_HOST" >&2; exit 2; }
case "$1 $2" in
"release download")
    pattern=""
    dir=""
    saw_repo=0
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --pattern) pattern="$2"; shift 2 ;;
            --dir) dir="$2"; shift 2 ;;
            --repo) [[ "$2" == "${GONIMBUS_RELEASE_REPO:-3leaps/gonimbus}" ]] && saw_repo=1; shift 2 ;;
            *) shift ;;
        esac
    done
    [[ "$saw_repo" -eq 1 && -n "$pattern" && -n "$dir" ]] || exit 1
    [[ -f "$FIXTURE_REMOTE/$pattern" ]] || exit 1
    cp "$FIXTURE_REMOTE/$pattern" "$dir/$pattern"
    ;;
"release view")
    echo "${FIXTURE_IS_DRAFT:-true}"
    ;;
"release upload")
    printf '%s\n' "$*" > "$FIXTURE_UPLOAD_CALL"
    ;;
*)
    exit 1
    ;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH"
export FIXTURE_REMOTE="$scratch/remote"
export FIXTURE_UPLOAD_CALL="$scratch/upload.call"

# Positive pass on draft release with matching SHA256SUMS & SHA512SUMS and all 11 files
stage_assets
"$root/scripts/release-upload-provenance.sh" "$tag" "$scratch/local" > /dev/null
grep -q "release-notes-${tag}.md" "$FIXTURE_UPLOAD_CALL"

# Refuses when SHA256SUMS differs from draft asset
stage_assets
printf 'tampered\n' >> "$scratch/local/SHA256SUMS"
if "$root/scripts/release-upload-provenance.sh" "$tag" "$scratch/local" > /dev/null 2>&1; then
    echo 'error: accepted tampered SHA256SUMS' >&2
    exit 1
fi

# Refuses when SHA512SUMS differs from draft asset
stage_assets
printf 'tampered\n' >> "$scratch/local/SHA512SUMS"
if "$root/scripts/release-upload-provenance.sh" "$tag" "$scratch/local" > /dev/null 2>&1; then
    echo 'error: accepted tampered SHA512SUMS' >&2
    exit 1
fi

# Refuses when SHA512SUMS or release-notes-${tag}.md is missing
for req in SHA512SUMS "release-notes-${tag}.md" expected-fingerprints.ndjson; do
    stage_assets
    rm "$scratch/local/$req"
    if "$root/scripts/release-upload-provenance.sh" "$tag" "$scratch/local" > /dev/null 2>&1; then
        echo "error: accepted missing $req" >&2
        exit 1
    fi
done

# Refuses when release is not a draft
stage_assets
if FIXTURE_IS_DRAFT=false "$root/scripts/release-upload-provenance.sh" "$tag" "$scratch/local" > /dev/null 2>&1; then
    echo 'error: accepted non-draft release' >&2
    exit 1
fi

echo '[ok] release-upload-provenance controls passed'
