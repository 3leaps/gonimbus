#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# content-check refuses each shape in a committed file, accepts synthetic fixture
# homes, and reads committed blobs at HEAD only (symlink targets included; the
# working tree never hides a committed leak). Fixtures are built from fragments at run
# time, so this file never holds a whole token or path.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rc=$?; rm -rf "$scratch"; exit "$rc"' EXIT
repo="$scratch/repo"
git init -q "$repo"
git -C "$repo" config core.autocrlf false
git -C "$repo" config user.name fixture
git -C "$repo" config user.email fixture@example.invalid
git -C "$repo" config commit.gpgsign false
check() { "${PYTHON:-python3}" -I -B "$root/scripts/content-check.py" "$repo" >"$scratch/out" 2>&1; }
plant() {
	printf '%s\n' "$1" >"$repo/f.txt"
	git -C "$repo" add -A
	git -C "$repo" commit -qm fixture --allow-empty
}
refused() {
	local reason="$1" text="$2"
	plant "$text"
	if check; then echo "error: accepted: $reason" >&2; exit 1; fi
	grep -q -- "$reason" "$scratch/out" || { echo "error: refused for another reason than '$reason'" >&2; cat "$scratch/out" >&2; exit 1; }
}
accepted() {
	plant "$1"
	check || { echo "error: refused: $1" >&2; cat "$scratch/out" >&2; exit 1; }
}
u='Us''ers' h='ho''me' name='jd''oe'
refused 'home directory' "see /$u/$name/projects/x"
refused 'home directory' "log /$h/$name/build"
refused 'home directory' "see /$u/Jane""Doe/projects/x"
refused 'home directory' "log /$h/j/work"
refused 'home directory' "end of line /$u/$name"
refused 'home directory' "quoted \"/$h/$name\""
refused 'home directory' "near miss /$h/someone2/x"
refused 'Windows user profile' "C:\\$u\\$name\\code"
refused 'Windows user profile' "d:/$u/$name/code"
refused 'Windows user profile' "C:\\$u\\Jane""Doe\\code"
refused 'private key block' "-----BEGIN OPEN""SSH PRIVATE KEY-----"
refused 'private key block' "-----BEGIN PRIVATE"" KEY-----"
refused 'GitHub token' "token gh""p_$(printf 'A%.0s' $(seq 1 24))"
refused 'GitHub token' "token github""_pat_$(printf 'b%.0s' $(seq 1 30))"
refused 'AWS access key' "key AK""IA$(printf 'Q%.0s' $(seq 1 16))"
refused 'remove-before-commit marker' "x = 'DO_NOT""_COMMIT'"
# Synthetic fixture homes, CI runners and URLs are not local paths.
accepted "fixture /$h/someone/x and C:/$u/someone/y"
accepted "ci /$h/runner/work and C:\\$u\\runneradmin\\x"
accepted "url https://example.com/$u/$name/"
accepted "token ghp_short"
accepted 'plain text'
# Only committed blobs at HEAD are read: an untracked file is not, and a
# local edit cannot hide a committed leak.
plant 'clean'
printf 'see /%s/%s/x\n' "$u" "$name" >"$repo/untracked.txt"
check || { echo 'error: read an untracked file' >&2; cat "$scratch/out" >&2; exit 1; }
rm -f "$repo/untracked.txt"
plant "token gh""p_$(printf 'C%.0s' $(seq 1 24))"
printf 'scrubbed locally\n' >"$repo/f.txt"
if check; then echo 'error: an unstaged scrub hid a committed token' >&2; exit 1; fi
grep -q 'GitHub token' "$scratch/out"
git -C "$repo" checkout -q -- f.txt
plant 'clean'
# A committed symlink's target is a committed blob (staged as a 120000 git entry
# so Windows hosts without symlink privilege exercise the exact same tree object).
link_oid="$(printf '%s' "/$u/$name/projects/x" | git -C "$repo" hash-object -w --stdin)"
git -C "$repo" update-index --add --cacheinfo "120000,$link_oid,link"
git -C "$repo" commit -qm 'add link'
if check; then echo 'error: accepted a symlink to a home path' >&2; exit 1; fi
grep -q 'link:1: home directory' "$scratch/out"
git -C "$repo" update-index --force-remove link
git -C "$repo" commit -qm 'drop link'
# Another ref can be checked.
check_ref() { "${PYTHON:-python3}" -I -B "$root/scripts/content-check.py" "$repo" "$1" >"$scratch/out" 2>&1; }
if check_ref HEAD~1; then echo 'error: the earlier ref with a symlink passed' >&2; exit 1; fi
# GIT_* environment variables and git replace refs cannot redirect or hide a
# committed leak in committed_tree.py.
plant "token gh""p_$(printf 'D%.0s' $(seq 1 24))"
leak_blob="$(git -C "$repo" --no-replace-objects rev-parse HEAD:f.txt)"
clean_blob="$(printf 'clean replacement\n' | git -C "$repo" hash-object -w --stdin)"
git -C "$repo" replace "$leak_blob" "$clean_blob"
if GIT_DIR="$scratch/nonexistent" GIT_WORK_TREE="$scratch/nonexistent" GIT_REPLACE_REF_BASE="refs/replace/" check; then
	echo 'error: GIT_DIR or git replace hid a committed token' >&2
	exit 1
fi
git -C "$repo" replace -d "$leak_blob" >/dev/null
plant 'clean'
GIT_DIR="$scratch/nonexistent" GIT_WORK_TREE="$scratch/nonexistent" check || {
	echo 'error: hostile GIT_DIR broke content-check on a clean repo' >&2
	cat "$scratch/out" >&2
	exit 1
}
# The real tree is clean.
"${PYTHON:-python3}" -I -B "$root/scripts/content-check.py" "$root" >/dev/null
echo '[ok] content-check: home paths, Windows profiles, private keys, GitHub and AWS tokens, commit markers; synthetic homes and untracked files pass'
