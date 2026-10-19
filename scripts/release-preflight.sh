#!/usr/bin/env bash
# Pre-tag gate: every check that would otherwise fail only after the signed tag
# is pushed. Read-only; run from the exact main commit that will be tagged.
#
# Usage: release-preflight.sh   (GONIMBUS_RELEASE_TAG, else v<VERSION>)
set -euo pipefail
die() {
    echo "[!!] $*" >&2
    exit 1
}
ok() { echo "[ok] $*"; }
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
version="$(tr -d ' \t\r\n' < VERSION)"
tag="${GONIMBUS_RELEASE_TAG:-v$version}"
[[ "$tag" == "v$version" ]] || die "release tag $tag does not match VERSION $version"

[[ -z "$(git status --porcelain)" ]] || die 'working tree is not clean'
ok 'working tree is clean'
git fetch --quiet --no-tags origin '+refs/heads/main:refs/remotes/origin/main' ||
    die 'could not fetch origin/main'
[[ "$(git rev-parse HEAD)" == "$(git rev-parse refs/remotes/origin/main)" ]] ||
    die 'HEAD is not origin/main; tag only the merged main commit'
ok "HEAD is origin/main ($(git rev-parse --short HEAD))"


grep -qE "^## \[$version\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" CHANGELOG.md || die "CHANGELOG.md has no dated [$version] heading"
grep -qE "^## \[$version\] - " RELEASE_NOTES.md || die "RELEASE_NOTES.md has no [$version] heading"
[[ -s "docs/releases/$tag.md" ]] || die "per-version notes docs/releases/$tag.md are missing"
ok "changelog, release notes and docs/releases/$tag.md cover $version"
# The per-version notes become the release body, so they must tell a reader how
# to install this exact version (see docs/releases/TEMPLATE.md).
install_section="$(awk '/^## /{in_install = ($0 == "## Install")} in_install' "docs/releases/$tag.md")"
[[ -n "$install_section" ]] || die "docs/releases/$tag.md has no '## Install' section"
for want in 'brew install 3leaps/tap/gonimbus' 'scoop install gonimbus' \
    "go install github.com/3leaps/gonimbus/cmd/gonimbus@$tag"; do
    grep -qF -- "$want" <<< "$install_section" ||
        die "docs/releases/$tag.md Install section is missing: $want"
done
ok "docs/releases/$tag.md has install instructions for $tag"

./scripts/workflow-pins.test.sh > /dev/null || die 'workflow pins check failed (run scripts/workflow-pins.test.sh)'
ok 'workflow actions are pinned and off Node 20'
goneat assess --categories dates --fail-on high > /dev/null 2>&1 ||
    die 'dates check failed (run goneat assess --categories dates)'
ok 'dates are consistent with repository history'

# Tests must not depend on where the checkout lives (packagers build outside $HOME).
if [[ -d /private/tmp ]]; then
    outside="$(mktemp -d /private/tmp/gonimbus-preflight.XXXXXX)"
else
    outside="$(mktemp -d /tmp/gonimbus-preflight.XXXXXX)"
fi
trap 'rm -rf "$outside"' EXIT
outside_real="$(cd "$outside" && pwd -P)"
home_real="$(cd "$HOME" && pwd -P)"
[[ "$outside_real/" != "$home_real/"* ]] || die "temporary clone $outside_real is under the home directory"
git clone --quiet --no-hardlinks "$root" "$outside/src"
if ! (cd "$outside/src" && go test ./... > "$outside/test.log" 2>&1); then
    tail -20 "$outside/test.log" >&2
    die 'tests fail from a clone outside the home directory'
fi
ok 'tests pass from a clone outside the home directory'

make --no-print-directory pr-final > /dev/null || die 'make pr-final failed'
ok 'pr-final gate passed'
echo "[ok] preflight passed for $tag at $(git rev-parse --short HEAD); ready to tag"
