#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Release steps reach github.com only: an inherited GitHub host, repository or
# enterprise token is refused, and every call is pinned to github.com.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir "$scratch/bin"
cat >"$scratch/bin/gh" <<'SH'
#!/usr/bin/env bash
printf 'host=%s args=%s\n' "${GH_HOST:-}" "$*" >>"$FIXTURE_CALLS"
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_CALLS="$scratch/calls"
call() { bash -c 'source "$1/scripts/release-gh.sh"; shift; release_gh "$@"' _ "$root" "$@"; }
call api repos/3leaps/gonimbus/git/tags/abc --jq .tag
call release view v1.2.3 --repo 3leaps/gonimbus
grep -qx 'host=github.com args=api repos/3leaps/gonimbus/git/tags/abc --jq .tag --hostname github.com' "$scratch/calls"
grep -qx 'host=github.com args=release view v1.2.3 --repo 3leaps/gonimbus' "$scratch/calls"
for name in GH_HOST GH_REPO GH_ENTERPRISE_TOKEN GITHUB_ENTERPRISE_TOKEN; do
	: >"$scratch/calls"
	if env "$name=other.example.invalid" bash -c 'source "$1/scripts/release-gh.sh"; release_gh api repos/x' _ "$root" \
		>"$scratch/out" 2>&1; then
		echo "error: release_gh accepted inherited $name" >&2
		exit 1
	fi
	grep -q "$name is set" "$scratch/out"
	[[ ! -s "$scratch/calls" ]] || {
		echo "error: gh ran despite inherited $name" >&2
		exit 1
	}
done
# No release script calls gh directly.
if grep -nE '(^|[^_[:alnum:]-])gh (api|release|run|repo|auth|workflow) ' "$root"/scripts/*.sh | grep -v '\.test\.sh:' | grep -v 'release-gh.sh:'; then
	echo 'error: a release script calls gh without release_gh' >&2
	exit 1
fi
echo '[ok] GitHub host pinning controls passed'
