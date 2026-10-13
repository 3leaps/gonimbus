#!/usr/bin/env bash
# Stub-gh controls for release-fetch-ci-artifacts.sh and release-create-draft.sh.
# No live GitHub call is made; the stub records every invocation.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
repo="$scratch/repo"
git init -q -b main "$repo"
git -C "$repo" config user.name 'Fixture'
git -C "$repo" config user.email fixture@example.invalid
printf 'x\n' > "$repo/file"
git -C "$repo" add file
mkdir -p "$repo/docs/releases"
printf '# fixture v1.2.3\n\nCurated notes.\n' > "$repo/docs/releases/v1.2.3.md"
git -C "$repo" add docs/releases/v1.2.3.md
git -C "$repo" commit -qm fixture
git -C "$repo" tag -am 'Fixture release' v1.2.3
commit="$(git -C "$repo" rev-parse HEAD)"
mkdir -p "$repo/scripts"
cp "$root/scripts/release-fetch-ci-artifacts.sh" "$root/scripts/release-create-draft.sh" "$repo/scripts/"
object="$(git -C "$repo" rev-parse refs/tags/v1.2.3)"
# Stand-in for the trusted published-tag gate (its own suite uses real
# signatures). It reports the fixture anchor, records any expected anchor it is
# asked to enforce, and fails when told the remote tag was replaced.
cat > "$repo/scripts/release-verify-published-tag.sh" << SH
#!/usr/bin/env bash
printf 'reverify %s expect=%s/%s\\n' "\$GONIMBUS_RELEASE_TAG" "\${GONIMBUS_EXPECTED_TAG_OBJECT:-}" "\${GONIMBUS_EXPECTED_COMMIT:-}" >> "\$FIXTURE_LOG"
[[ "\$GONIMBUS_RELEASE_TAG" == v1.2.3 ]] || { echo 'error: tag absent' >&2; exit 1; }
[[ "\${FIXTURE_TAG_REPLACED:-0}" == 0 ]] || { echo 'error: tag replaced' >&2; exit 1; }
if [[ -n "\${GONIMBUS_ANCHOR_OUT:-}" ]]; then
    printf 'tag=v1.2.3\\nobject=%s\\ncommit=%s\\n' "$object" "$commit" > "\$GONIMBUS_ANCHOR_OUT"
fi
SH
chmod +x "$repo/scripts/release-verify-published-tag.sh"

# Stub gh. FIXTURE_ATTEMPT is the run-attempt JSON the API returns and
# FIXTURE_LATEST is the latest attempt number of that run;
# FIXTURE_RELEASE_EXISTS controls `gh release view`. Every call is logged.
mkdir -p "$scratch/bin"
cat > "$scratch/bin/gh" << 'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FIXTURE_LOG"
case "$1 $2" in
"api "*)
    path="$2"
    jq_filter=""
    while [[ $# -gt 0 ]]; do
        [[ "$1" == --jq ]] && jq_filter="$2"
        shift
    done
    case "$path" in
    */attempts/*) body="${FIXTURE_ATTEMPT:-}" ;;
    *) body="{\"run_attempt\":${FIXTURE_LATEST:-1}}" ;;
    esac
    [[ -n "$body" ]] || exit 1
    jq -c "$jq_filter" <<< "$body"
    ;;
"run download")
    dir=""
    name=""
    while [[ $# -gt 0 ]]; do
        [[ "$1" == --dir ]] && dir="$2"
        [[ "$1" == --name ]] && name="$2"
        shift
    done
    [[ "$name" == "release-artifacts-v1.2.3" ]] || exit 1
    printf 'pkg\n' > "$dir/gonimbus-linux-amd64"
    ;;
"release view") [[ "${FIXTURE_RELEASE_EXISTS:-0}" == 1 ]] ;;
"release create") exit 0 ;;
*) exit 1 ;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_LOG="$scratch/gh.log"

fail() {
    local reason="$1"
    shift
    if (cd "$repo" && "$@") > "$scratch/out" 2>&1; then
        echo "error: expected rejection: $reason" >&2
        exit 1
    fi
    grep -q "$reason" "$scratch/out" || {
        echo "error: missing named rejection: $reason" >&2
        cat "$scratch/out" >&2
        exit 1
    }
}
attempt_json() { # conclusion event head_sha attempt [path] [branch]
    printf '{"path":"%s","event":"%s","head_branch":"%s","head_sha":"%s","status":"completed","conclusion":"%s","run_attempt":%s}' \
        "${5:-.github/workflows/release.yml}" "$2" "${6:-v1.2.3}" "$3" "$1" "$4"
}
good="$(attempt_json success push "$commit" 2)"
export RELEASE_RUN_ID=11 RELEASE_RUN_ATTEMPT=2 FIXTURE_LATEST=2

# Fetch: refuses a non-canonical tag, a missing local tag, a missing run id or
# attempt, and any named attempt that is not a successful push-triggered
# release run for this tag on the tagged commit or is not the latest attempt.
fail 'usage' ./scripts/release-fetch-ci-artifacts.sh 1.2.3 "$scratch/d0"
fail 'published tag v9.9.9 failed verification' ./scripts/release-fetch-ci-artifacts.sh v9.9.9 "$scratch/d0"
RELEASE_RUN_ID='' FIXTURE_ATTEMPT="$good" fail 'RELEASE_RUN_ID required' \
    ./scripts/release-fetch-ci-artifacts.sh v1.2.3 "$scratch/d1"
RELEASE_RUN_ATTEMPT='' FIXTURE_ATTEMPT="$good" fail 'RELEASE_RUN_ATTEMPT required' \
    ./scripts/release-fetch-ci-artifacts.sh v1.2.3 "$scratch/d1"
for bad in \
    "$(attempt_json failure push "$commit" 2)" \
    "$(attempt_json success workflow_dispatch "$commit" 2)" \
    "$(attempt_json success push "$(printf '%040d' 0)" 2)" \
    "$(attempt_json success push "$commit" 1)" \
    "$(attempt_json success push "$commit" 2 .github/workflows/ci.yml)" \
    "$(attempt_json success push "$commit" 2 .github/workflows/release.yml v1.2.4)" \
    "$(attempt_json success push "$commit" 2 | sed 's/"completed"/"in_progress"/')"; do
    FIXTURE_ATTEMPT="$bad" fail 'is not a successful release run' \
        ./scripts/release-fetch-ci-artifacts.sh v1.2.3 "$scratch/d1"
done
FIXTURE_ATTEMPT="$good" FIXTURE_LATEST=3 fail 'has a later attempt' \
    ./scripts/release-fetch-ci-artifacts.sh v1.2.3 "$scratch/d1"
# Refuses a non-empty destination.
mkdir -p "$scratch/full"
printf 'stale\n' > "$scratch/full/stale"
FIXTURE_ATTEMPT="$good" fail 'is not empty' ./scripts/release-fetch-ci-artifacts.sh v1.2.3 "$scratch/full"
# Downloads only the named artifact from the named, verified attempt.
: > "$FIXTURE_LOG"
(cd "$repo" && FIXTURE_ATTEMPT="$good" ./scripts/release-fetch-ci-artifacts.sh v1.2.3 "$scratch/d2") > /dev/null
grep -q '^run download 11 --repo 3leaps/gonimbus --name release-artifacts-v1.2.3 ' "$FIXTURE_LOG"
[[ "$(cat "$scratch/d2.anchor")" == "$(printf 'tag=v1.2.3\nobject=%s\ncommit=%s\nrun=11\nattempt=2' "$object" "$commit")" ]]
[[ -f "$scratch/d2/gonimbus-linux-amd64" ]]

# Draft. Packages are staged against the anchor that fetch recorded.
stage() {
    local dir="$1" name
    rm -rf "$dir"
    mkdir -p "$dir"
    for name in gonimbus-linux-amd64 gonimbus-linux-arm64 \
        gonimbus-windows-amd64.exe gonimbus-windows-arm64.exe gonimbus-darwin-arm64; do
        printf '%s\n' "$name" > "$dir/$name"
    done
    (cd "$dir" && sha_sum 256 gonimbus-* > SHA256SUMS &&
        sha_sum 512 gonimbus-* > SHA512SUMS)
    printf 'tag=v1.2.3\nobject=%s\ncommit=%s\nrun=11\nattempt=2\n' "$object" "$commit" > "$dir.anchor"
}
no_create() {
    if grep -q '^release create' "$FIXTURE_LOG"; then
        echo "error: draft created despite: $1" >&2
        exit 1
    fi
}
d="$scratch/draft"
draft_fail() {
    local reason="$1"
    : > "$FIXTURE_LOG"
    fail "$reason" ./scripts/release-create-draft.sh v1.2.3 "$d"
    no_create "$reason"
}

stage "$d"
FIXTURE_RELEASE_EXISTS=1 draft_fail 'already exists'
# Unlisted binary for a platform outside the release matrix, alongside a valid set.
stage "$d"
printf 'old\n' > "$d/gonimbus-darwin-amd64"
draft_fail 'unexpected file'
# Extra, unexpected file of any kind (tooling residue).
stage "$d"
printf 'tool\n' > "$d/goneat"
draft_fail 'unexpected file'
# Hidden extra files, including names starting with "..", refuse.
for hidden in .stale '..stale' '...x'; do
    stage "$d"
    printf 'x\n' > "$d/$hidden"
    draft_fail 'unexpected file'
done
# A subdirectory is not a regular file and refuses.
stage "$d"
mkdir "$d/extra-dir"
draft_fail 'unexpected file'

# Missing expected archive.
stage "$d"
rm "$d/gonimbus-darwin-arm64"
draft_fail 'missing release file'
# Manifest that omits an archive, and one listing an extra name.
stage "$d"
grep -v darwin "$d/SHA256SUMS" > "$d/x" && mv "$d/x" "$d/SHA256SUMS"
draft_fail 'does not list exactly'
stage "$d"
printf '%0128d  gonimbus-darwin-amd64\n' 0 >> "$d/SHA512SUMS"
draft_fail 'malformed or duplicate\|does not list exactly'
# Archive whose bytes no longer match the manifest.
stage "$d"
printf 'tampered\n' > "$d/gonimbus-linux-amd64"
draft_fail 'digest mismatch'
# Tag replaced between the earlier checks and the API call.
stage "$d"
FIXTURE_TAG_REPLACED=1 draft_fail 'no longer matches the verified anchor'
# Direct invocation with an empty directory fails closed.
rm -rf "$d"
mkdir -p "$d"
draft_fail 'missing release file'

# Missing, mismatched-tag or malformed anchor refuses.
stage "$d"
rm "$d.anchor"
draft_fail 'missing'
stage "$d"
sed -i.bak 's/^tag=v1.2.3$/tag=v1.2.4/' "$d.anchor"
draft_fail 'different tag'
stage "$d"
printf 'tag=v1.2.3\nobject=zz\ncommit=zz\n' > "$d.anchor"
draft_fail 'malformed'

# The draft body must come from the committed per-version notes.
stage "$d"
mv "$repo/docs/releases/v1.2.3.md" "$scratch/notes.hold"
draft_fail 'missing release notes'
mv "$scratch/notes.hold" "$repo/docs/releases/v1.2.3.md"
stage "$d"
printf 'edited after tagging\n' >> "$repo/docs/releases/v1.2.3.md"
draft_fail 'differs from the tagged commit'
git -C "$repo" checkout -q -- docs/releases/v1.2.3.md

# Valid creation: exactly the five archives plus both manifests, re-verified
# immediately before the create call.
stage "$d"
: > "$FIXTURE_LOG"
(cd "$repo" && ./scripts/release-create-draft.sh v1.2.3 "$d") > /dev/null
create="$(grep '^release create' "$FIXTURE_LOG")"
[[ "$create" == *' --verify-tag --draft '* ]]
[[ "$create" == *"--notes-file $repo/docs/releases/v1.2.3.md"* || "$create" == *'--notes-file '*'/docs/releases/v1.2.3.md'* ]]
[[ "$create" != *'--generate-notes'* ]]
[[ "$(grep -o 'gonimbus-\(linux\|darwin\|windows\)-\(amd64\|arm64\)\(\.exe\)\?' <<< "$create" | sort -u | wc -l | tr -d ' ')" == 5 ]]
[[ "$create" == *'SHA256SUMS'* && "$create" == *'SHA512SUMS'* ]]
[[ "$(grep -n '' "$FIXTURE_LOG" | grep -E 'reverify|release create' | cut -d: -f2 | head -1)" == reverify* ]]
# The pre-API re-verification is bound to exactly the staged anchor.
grep -q "^reverify v1.2.3 expect=$object/$commit\$" "$FIXTURE_LOG"
echo '[ok] CI-artifact fetch and local draft controls passed'
