#!/usr/bin/env bash
# Test double for `gh api` that answers the GitHub git-data endpoints the
# release scripts read, from a local bare repository (FIXTURE_REMOTE).
#
#   repos/<owner>/<repo>/git/ref/tags/<tag>   -> {"object":{"type","sha"}}
#   repos/<owner>/<repo>/git/ref/heads/<b>    -> {"object":{"type":"commit","sha"}}
#   repos/<owner>/<repo>/git/tags/<object>    -> {"tag","object","verification"}
#
# An absent ref prints the GitHub 404 text and exits 1. FIXTURE_GH_VERIFIED
# (default true) sets verification.verified. `--jq EXPR` is applied with jq.
# Calls other than `gh api` exit 2 so a test notices an unexpected command.
set -euo pipefail
[[ "${1:-}" == api ]] || {
    echo "fake gh: unsupported command: $*" >&2
    exit 2
}
shift
path=""
jq_expr=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jq)
            jq_expr="$2"
            shift 2
            ;;
        -*) shift ;;
        *)
            path="$1"
            shift
            ;;
    esac
done
remote="${FIXTURE_REMOTE:?FIXTURE_REMOTE required}"
not_found() {
    echo 'gh: Not Found (HTTP 404)' >&2
    exit 1
}
emit() {
    if [[ -n "$jq_expr" ]]; then
        jq -r "$jq_expr" <<< "$1"
    else
        printf '%s\n' "$1"
    fi
}
case "$path" in
    repos/*/git/ref/tags/*)
        tag="${path##*/git/ref/tags/}"
        sha="$(git -C "$remote" rev-parse --verify --quiet "refs/tags/$tag")" || not_found
        emit "{\"ref\":\"refs/tags/$tag\",\"object\":{\"type\":\"$(git -C "$remote" cat-file -t "$sha")\",\"sha\":\"$sha\"}}"
        ;;
    repos/*/git/ref/heads/*)
        branch="${path##*/git/ref/heads/}"
        sha="$(git -C "$remote" rev-parse --verify --quiet "refs/heads/$branch")" || not_found
        emit "{\"ref\":\"refs/heads/$branch\",\"object\":{\"type\":\"commit\",\"sha\":\"$sha\"}}"
        ;;
    repos/*/git/tags/*)
        object="${path##*/git/tags/}"
        [[ "$(git -C "$remote" cat-file -t "$object" 2> /dev/null || true)" == tag ]] || not_found
        name="$(git -C "$remote" cat-file tag "$object" | sed -n 's/^tag //p' | head -1)"
        commit="$(git -C "$remote" rev-parse "$object^{}")"
        emit "{\"tag\":\"$name\",\"object\":{\"type\":\"commit\",\"sha\":\"$commit\"},\"verification\":{\"verified\":${FIXTURE_GH_VERIFIED:-true},\"reason\":\"valid\"}}"
        ;;
    *)
        echo "fake gh: unsupported api path: $path" >&2
        exit 2
        ;;
esac
