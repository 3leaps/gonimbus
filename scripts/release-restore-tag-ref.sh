#!/usr/bin/env bash
# Restore the annotated release tag after actions/checkout peels it locally.
#
# The tag object is resolved through the GitHub API with the job token
# (GH_TOKEN), not by anonymous `git ls-remote`, so the lookup works on private
# repositories and forks without persisting checkout credentials. The object is
# then fetched into the runner-local ref and bound to HEAD.
set -euo pipefail

die() {
    echo "error: $*" >&2
    exit 1
}

repo="3leaps/gonimbus"
tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || die 'canonical release tag required'
[[ -n "${GH_TOKEN:-}" ]] || die 'GH_TOKEN (job token) required for the tag lookup'
case "$(git config --get remote.origin.url)" in
    "https://github.com/$repo" | "https://github.com/$repo.git") ;;
    *) die 'unexpected release origin' ;;
esac

ref_json="$(gh api "repos/$repo/git/ref/tags/$tag")" || die 'remote release tag is absent'
remote_type="$(jq -r '.object.type // empty' <<< "$ref_json")"
remote_object="$(jq -r '.object.sha // empty' <<< "$ref_json")"
[[ "$remote_type" == tag ]] || die 'remote release tag must be annotated'
[[ "$remote_object" =~ ^[0-9a-f]{40}$ ]] || die 'remote tag object is invalid'
if [[ -n "${GONIMBUS_EXPECTED_TAG_OBJECT+x}" ]]; then
    [[ "$GONIMBUS_EXPECTED_TAG_OBJECT" =~ ^[0-9a-f]{40}$ &&
        "$remote_object" == "$GONIMBUS_EXPECTED_TAG_OBJECT" ]] ||
        die 'remote tag object differs from validated object'
fi

# A full-history checkout usually already holds the tag object. Otherwise fetch
# it with the job token for this one command only; checkout credentials stay
# unpersisted. Either way only the runner-local ref changes, and the checks
# below bind it to the API answer.
if [[ "$(git cat-file -t "$remote_object" 2> /dev/null || true)" == tag ]]; then
    git update-ref "refs/tags/$tag" "$remote_object"
else
    # The header travels in the environment, not argv, and its encoded form is
    # masked because the runner only masks the raw token.
    auth="$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 | tr -d '\n')"
    [[ -n "${GITHUB_ACTIONS:-}" ]] && echo "::add-mask::$auth"
    GIT_CONFIG_COUNT=1 \
        GIT_CONFIG_KEY_0='http.https://github.com/.extraheader' \
        GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $auth" \
        git fetch --quiet --no-tags origin "+refs/tags/$tag:refs/tags/$tag"
fi
[[ "$(git rev-parse "refs/tags/$tag")" == "$remote_object" &&
    "$(git cat-file -t "refs/tags/$tag")" == tag &&
    "$(git rev-parse "refs/tags/$tag^{}")" == "$(git rev-parse HEAD)" ]] ||
    die 'runner-local tag is not the remote annotated tag at HEAD'
echo "[ok] runner-local annotated tag restored for $tag"
