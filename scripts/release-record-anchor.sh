#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Verify the published release tag and write the external ceremony anchor file.
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

tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    die 'canonical GONIMBUS_RELEASE_TAG required'

anchor_file="${GONIMBUS_RELEASE_ANCHOR_FILE:-}"
[[ -n "$anchor_file" && "$anchor_file" == /* && ! -e "$anchor_file" && ! -L "$anchor_file" ]] ||
    die 'absent absolute GONIMBUS_RELEASE_ANCHOR_FILE outside the repository required'

[[ "${GONIMBUS_EXPECTED_TAG_OBJECT:-}" =~ ^[0-9a-f]{40}$ && "${GONIMBUS_EXPECTED_COMMIT:-}" =~ ^[0-9a-f]{40}$ ]] ||
    die 'original approved tag object (GONIMBUS_EXPECTED_TAG_OBJECT) and commit (GONIMBUS_EXPECTED_COMMIT) required'

mkdir -p "$(dirname "$anchor_file")"
destination="$(cd "$(dirname "$anchor_file")" && pwd -P)/$(basename "$anchor_file")"
for probe in "$anchor_file" "$destination"; do
    cur="$probe"
    while true; do
        if [[ -e "$cur" && "$cur" -ef "$root" ]]; then
            die 'GONIMBUS_RELEASE_ANCHOR_FILE must be outside the repository working tree'
        fi
        [[ "$cur" != / ]] || break
        cur="$(dirname "$cur")"
    done
done
case "$destination" in "$root" | "$root"/*)
    die 'GONIMBUS_RELEASE_ANCHOR_FILE must be outside the repository working tree'
    ;;
esac

python3 -I - "$root" "$anchor_file" << 'PY'
from pathlib import Path
import sys

repo = Path(sys.argv[1]).resolve()
target = Path(sys.argv[2])
cur = Path(target.anchor)
for part in target.parts[1:-1]:
    cur = cur / part
    if cur.exists() and cur.is_symlink():
        raise SystemExit("error: GONIMBUS_RELEASE_ANCHOR_FILE parent traverses a symlink")
if target.exists() or target.is_symlink():
    raise SystemExit("error: GONIMBUS_RELEASE_ANCHOR_FILE must not already exist or be a symlink")
resolved_parent = target.parent.resolve()
resolved = resolved_parent / target.name
if resolved == repo or resolved.is_relative_to(repo):
    raise SystemExit("error: GONIMBUS_RELEASE_ANCHOR_FILE must be outside the repository working tree")
PY

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_EXPECTED_TAG_OBJECT="$GONIMBUS_EXPECTED_TAG_OBJECT" \
    GONIMBUS_EXPECTED_COMMIT="$GONIMBUS_EXPECTED_COMMIT" \
    GONIMBUS_ANCHOR_OUT="$scratch/anchor" \
    "$root/scripts/release-verify-published-tag.sh"
object="$(awk -F= '$1=="object" {print $2}' "$scratch/anchor")"
commit="$(awk -F= '$1=="commit" {print $2}' "$scratch/anchor")"
[[ "$object" =~ ^[0-9a-f]{40}$ && "$commit" =~ ^[0-9a-f]{40}$ ]] ||
    die 'published tag verification did not return a valid anchor'

set -C
cat "$scratch/anchor" > "$anchor_file"
chmod 600 "$anchor_file"
echo "[ok] recorded ceremony anchor for $tag at $anchor_file (object $object, commit $commit)"
