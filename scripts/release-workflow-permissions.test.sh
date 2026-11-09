#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
export PYTHONDONTWRITEBYTECODE=1

if [[ $# -gt 0 ]]; then
    python3 -B -E "$root/scripts/release-workflow-permissions.py" check "$1"
    exit 0
fi

python3 -B -E "$root/scripts/release-workflow-permissions.py" check
python3 -B -E "$root/scripts/release-workflow-permissions.py" mutations
python3 -B -E "$root/scripts/release-workflow-pin.py" check

# Verify release-workflow-pin.py verify-commit ignores hostile GIT_* env vars
# and git replace refs.
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
repo="$scratch/repo"
mkdir -p "$repo/.github/workflows" "$repo/config/release" "$repo/scripts"
cp "$root"/.github/workflows/*.yml "$repo/.github/workflows/"
cp "$root/config/release/release-workflow.sha256" "$repo/config/release/release-workflow.sha256"
cp "$root/scripts/release-workflow-pin.py" "$repo/scripts/"
git init -q -b main "$repo"
git -C "$repo" config user.name Fixture
git -C "$repo" config user.email fixture@example.invalid
git -C "$repo" config commit.gpgsign false
git -C "$repo" add .github/workflows config/release/release-workflow.sha256
git -C "$repo" commit -qm 'good workflows'
good_commit="$(git -C "$repo" rev-parse HEAD)"

printf '# tampered\n' >> "$repo/.github/workflows/release.yml"
git -C "$repo" commit -qam 'tampered workflow'
bad_commit="$(git -C "$repo" rev-parse HEAD)"
bad_blob="$(git -C "$repo" --no-replace-objects rev-parse "$bad_commit:.github/workflows/release.yml")"
good_blob="$(git -C "$repo" --no-replace-objects rev-parse "$good_commit:.github/workflows/release.yml")"
git -C "$repo" replace "$bad_blob" "$good_blob"

GIT_DIR="$scratch/nonexistent" GIT_WORK_TREE="$scratch/nonexistent" \
    python3 -B -E "$repo/scripts/release-workflow-pin.py" verify-commit "$good_commit" > /dev/null
if GIT_DIR="$scratch/nonexistent" GIT_REPLACE_REF_BASE="refs/replace/" \
    python3 -B -E "$repo/scripts/release-workflow-pin.py" verify-commit "$bad_commit" 2> /dev/null; then
    echo 'error: release-workflow-pin.py verify-commit followed a git replace ref' >&2
    exit 1
fi

echo '[ok] release workflow permissions, mutation controls, and SHA-256 pin verified'
