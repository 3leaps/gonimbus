#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Post-publish Go module proxy, Homebrew tap, and Scoop bucket verification against
# the verified published release assets and tagged commit.
set -euo pipefail
for _git_var in $(compgen -e); do
    case "$_git_var" in
        GIT_COMMITTER_NAME | GIT_COMMITTER_EMAIL | GIT_AUTHOR_NAME | GIT_AUTHOR_EMAIL) ;;
        GIT_*) unset "$_git_var" ;;
    esac
done
unset _git_var
export GIT_NO_REPLACE_OBJECTS=1
die() {
    echo "error: $*" >&2
    exit 1
}
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"

directory="dist/published"
registry="all"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --registry)
            [[ $# -ge 2 ]] || die '--registry requires a value (all|go|brew|scoop)'
            registry="$2"
            shift 2
            ;;
        --registry=*)
            registry="${1#--registry=}"
            shift
            ;;
        --directory)
            [[ $# -ge 2 ]] || die '--directory requires a path'
            directory="$2"
            shift 2
            ;;
        --directory=*)
            directory="${1#--directory=}"
            shift
            ;;
        all | go | brew | scoop)
            registry="$1"
            shift
            ;;
        -*)
            die "unknown option: $1"
            ;;
        *)
            directory="$1"
            shift
            ;;
    esac
done

case "$registry" in
    all | go | brew | scoop) ;;
    *) die "registry selector must be all, go, brew, or scoop (got: $registry)" ;;
esac

tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    die 'canonical GONIMBUS_RELEASE_TAG required'

anchor="$directory.anchor"
[[ -f "$anchor" && ! -L "$anchor" ]] ||
    die "missing $anchor; run make release-stage-published first"
[[ "$(awk -F= '$1=="tag" {print $2}' "$anchor")" == "$tag" ]] ||
    die "staged anchor tag does not match $tag"
commit="$(awk -F= '$1=="commit" {print $2}' "$anchor")"
object="$(awk -F= '$1=="object" {print $2}' "$anchor")"
[[ "$commit" =~ ^[0-9a-f]{40}$ && "$object" =~ ^[0-9a-f]{40}$ ]] ||
    die 'staged anchor is malformed'

if [[ -n "${GONIMBUS_SKIP_STAGE_CHECKS:-}" ]]; then
    die 'GONIMBUS_SKIP_STAGE_CHECKS is not permitted'
fi
binary="${GONIMBUS_BINARY_NAME:-gonimbus}"
GONIMBUS_RELEASE_TAG="$tag" GONIMBUS_EXPECTED_TAG_OBJECT="$object" GONIMBUS_EXPECTED_COMMIT="$commit" \
    "$root/scripts/release-verify-published-tag.sh" > /dev/null ||
    die 'published tag no longer matches the verified anchor'
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
for pair in \
    "keys/expected-fingerprints.txt:expected-fingerprints.txt" \
    "keys/expected-fingerprints.ndjson:expected-fingerprints.ndjson" \
    "docs/security/${binary}-minisign.pub:${binary}-minisign.pub" \
    "docs/security/release-signing-keys.asc:${binary}-release-signing-key.asc" \
    "docs/releases/${tag}.md:release-notes-${tag}.md"; do
    repo_path="${pair%%:*}"
    staged_name="${pair##*:}"
    git --no-replace-objects -C "$root" cat-file blob "$commit:$repo_path" > "$scratch/expected.blob" ||
        die "missing $repo_path at tagged commit $commit"
    cmp -s "$scratch/expected.blob" "$directory/$staged_name" ||
        die "staged $staged_name differs from $repo_path at tagged commit $commit"
done
"$root/scripts/verify-checksums.sh" "$directory" > /dev/null
"$root/scripts/release-verify-signatures.sh" --tag "$tag" "$directory" > /dev/null

python3 -I -B "$root/scripts/release-verify-registries.py" \
    --directory "$directory" \
    --tag "$tag" \
    --commit "$commit" \
    --registry "$registry"
