#!/usr/bin/env bash
# Download the release packages that the tag-triggered workflow built for the
# verified tag, from the workflow run on that exact tagged commit. The workflow
# holds no write authority; the draft release is created locally afterwards.
# Usage: release-fetch-ci-artifacts.sh vX.Y.Z [dest_dir]
set -euo pipefail
die() {
    echo "error: $*" >&2
    exit 1
}
tag="${1:-}"
dest="${2:-dist/release}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || die 'usage: release-fetch-ci-artifacts.sh vX.Y.Z [dest_dir]'
command -v gh > /dev/null 2>&1 || die 'gh (GitHub CLI) not found in PATH'
root="$(cd "$(dirname "$0")/.." && pwd -P)"
mkdir -p "$dest"
[[ -z "$(find "$dest" -mindepth 1 -maxdepth 1 2> /dev/null)" ]] || die "$dest is not empty; run make release-clean first"
# Verify the published tag now and record the verified object and commit. Every
# later step, including draft creation, is bound to this anchor.
anchor="$(mktemp)"
trap 'rm -f "$anchor"' EXIT
GONIMBUS_RELEASE_TAG="$tag" GONIMBUS_ANCHOR_OUT="$anchor" "$root/scripts/release-verify-published-tag.sh" > /dev/null ||
    die "published tag $tag failed verification"
commit="$(awk -F= '$1=="commit" {print $2}' "$anchor")"
object="$(awk -F= '$1=="object" {print $2}' "$anchor")"
[[ "$commit" =~ ^[0-9a-f]{40}$ && "$object" =~ ^[0-9a-f]{40}$ ]] || die 'verification did not return an anchor'
# The maintainer names the exact run and attempt (re-runs share a run id). The
# named attempt must be a successful push-triggered release.yml run for this tag
# on the tagged commit.
run_id="${RELEASE_RUN_ID:-}"
run_attempt="${RELEASE_RUN_ATTEMPT:-}"
[[ "$run_id" =~ ^[1-9][0-9]*$ ]] || die 'RELEASE_RUN_ID required (the Release workflow run for this tag)'
[[ "$run_attempt" =~ ^[1-9][0-9]*$ ]] || die 'RELEASE_RUN_ATTEMPT required (the successful attempt of that run)'
run="$(gh api "repos/3leaps/gonimbus/actions/runs/$run_id/attempts/$run_attempt" \
    --jq '{path, event, head_branch, head_sha, status, conclusion, run_attempt}')" ||
    die "could not read run $run_id attempt $run_attempt"
jq -e --arg tag "$tag" --arg commit "$commit" --argjson attempt "$run_attempt" '
    .path == ".github/workflows/release.yml" and .event == "push" and .head_branch == $tag and
    .head_sha == $commit and .status == "completed" and .conclusion == "success" and
    .run_attempt == $attempt' <<< "$run" > /dev/null ||
    die "run $run_id attempt $run_attempt is not a successful release run for $tag at $commit"
latest="$(gh api "repos/3leaps/gonimbus/actions/runs/$run_id" --jq '.run_attempt')" ||
    die "could not read run $run_id"
[[ "$latest" == "$run_attempt" ]] ||
    die "run $run_id has a later attempt ($latest); artifacts belong to the latest attempt, name it"
gh run download "$run_id" --repo 3leaps/gonimbus --name "release-artifacts-$tag" --dir "$dest" ||
    die "could not download release-artifacts-$tag from run $run_id"
# The anchor lives outside the staged directory so it is never uploaded.
printf 'tag=%s\nobject=%s\ncommit=%s\nrun=%s\nattempt=%s\n' "$tag" "$object" "$commit" "$run_id" "$run_attempt" > "$dest.anchor"
echo "[ok] downloaded release packages for $tag (object $object, commit $commit) from run $run_id attempt $run_attempt into $dest"
