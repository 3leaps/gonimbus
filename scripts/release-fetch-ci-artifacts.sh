#!/usr/bin/env bash
# Download the release artifacts that the tag-triggered workflow built for the
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
# `gh run download` cannot select an attempt, so bind to one artifact instead:
# record its id and digest while the named attempt is the latest, download that
# exact id, check the bytes against the digest, then confirm nothing moved.
artifact_name="release-artifacts-$tag"
snapshot() {
    local latest listing
    latest="$(gh api "repos/3leaps/gonimbus/actions/runs/$run_id" --jq '.run_attempt')" ||
        die "could not read run $run_id"
    [[ "$latest" == "$run_attempt" ]] ||
        die "run $run_id has a later attempt ($latest); artifacts belong to the latest attempt, name it"
    listing="$(gh api "repos/3leaps/gonimbus/actions/runs/$run_id/artifacts")" ||
        die "could not list artifacts for run $run_id"
    listing="$(jq --arg n "$artifact_name" '[.artifacts[] | select(.name == $n and .expired == false)]' <<< "$listing")"
    jq -e --arg c "$commit" --argjson r "$run_id" \
        'length == 1 and (.[0].workflow_run.id == $r) and (.[0].workflow_run.head_sha == $c)' <<< "$listing" > /dev/null ||
        die "run $run_id does not hold exactly one unexpired $artifact_name for $commit"
    jq -r '.[0] | "\(.id) \(.digest // "")"' <<< "$listing"
}
read -r artifact_id artifact_digest <<< "$(snapshot)"
[[ "$artifact_id" =~ ^[1-9][0-9]*$ ]] || die 'artifact id is malformed'
[[ "$artifact_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || die "artifact $artifact_id has no sha256 digest"
work="$(mktemp -d)"
trap 'rm -f "$anchor"; rm -rf "$work"' EXIT
gh api "repos/3leaps/gonimbus/actions/artifacts/$artifact_id/zip" > "$work/artifact.zip" ||
    die "could not download artifact $artifact_id"
actual="sha256:$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$work/artifact.zip")"
[[ "$actual" == "$artifact_digest" ]] || die "artifact $artifact_id bytes do not match its recorded digest"
read -r after_id after_digest <<< "$(snapshot)"
[[ "$after_id" == "$artifact_id" && "$after_digest" == "$artifact_digest" ]] ||
    die "run $run_id changed during download (artifact $artifact_id -> $after_id); restage"
python3 -I - "$work/artifact.zip" "$dest" << 'PY'
import pathlib, sys, zipfile
dest = pathlib.Path(sys.argv[2])
with zipfile.ZipFile(sys.argv[1]) as z:
    for info in z.infolist():
        name = info.filename
        if info.is_dir() or '/' in name or '\\' in name or name.startswith('.') or name in ('', '..'):
            raise SystemExit(f'error: unexpected entry in artifact: {name!r}')
        (dest / name).write_bytes(z.read(info))
PY
# The anchor lives outside the staged directory so it is never uploaded.
printf 'tag=%s\nobject=%s\ncommit=%s\nrun=%s\nattempt=%s\nartifact=%s\ndigest=%s\n' \
    "$tag" "$object" "$commit" "$run_id" "$run_attempt" "$artifact_id" "$artifact_digest" > "$dest.anchor"
echo "[ok] downloaded release artifacts for $tag (object $object, commit $commit) from run $run_id attempt $run_attempt, artifact $artifact_id into $dest"
