#!/usr/bin/env bash
# Validate the sole release tag input and, in strict mode, bind it to origin/main.

set -euo pipefail

main() {
    local root version expected tag require_tag
    root="$(git rev-parse --show-toplevel)"
    cd "$root"

    if [[ -n "${RELEASE_TAG+x}" ]]; then
        echo "error: generic RELEASE_TAG alias is forbidden; use GONIMBUS_RELEASE_TAG" >&2
        exit 1
    fi

    version="$(cat VERSION)"
    if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
        echo "error: VERSION must contain one stable semantic version" >&2
        exit 1
    fi
    expected="v${version}"
    tag="${GONIMBUS_RELEASE_TAG:-$expected}"
    require_tag="${GONIMBUS_REQUIRE_TAG:-0}"

    if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
        echo "error: release tag must be canonical vX.Y.Z" >&2
        exit 1
    fi
    if [[ "$tag" != "$expected" ]]; then
        echo "error: release tag does not match VERSION" >&2
        exit 1
    fi

    if [[ "$require_tag" == "1" ]]; then
        if [[ -z "${GONIMBUS_RELEASE_TAG:-}" ]]; then
            echo "error: strict release guard requires GONIMBUS_RELEASE_TAG" >&2
            exit 1
        fi
        if git symbolic-ref -q HEAD > /dev/null; then
            echo "error: strict release work must use a detached checkout" >&2
            exit 1
        fi
        if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
            echo "error: strict release work requires a clean checkout" >&2
            exit 1
        fi
        local origin_url
        origin_url="$(git config --get remote.origin.url || true)"
        case "$origin_url" in
            https://github.com/3leaps/gonimbus | \
                https://github.com/3leaps/gonimbus.git) ;;
            git@*:3leaps/gonimbus | git@*:3leaps/gonimbus.git)
                local ssh_host resolved_host
                ssh_host="${origin_url#git@}"
                ssh_host="${ssh_host%%:*}"
                resolved_host="$(ssh -G "$ssh_host" 2> /dev/null |
                    awk '$1 == "hostname" { print $2; exit }')"
                [[ "$resolved_host" == "github.com" ]] || {
                    echo "error: origin SSH host must resolve to github.com" >&2
                    exit 1
                }
                ;;
            *)
                echo "error: origin must identify github.com/3leaps/gonimbus" >&2
                exit 1
                ;;
        esac
        # Remote state comes from the GitHub API with the job token (GH_TOKEN),
        # not anonymous git transport, so private repositories work too.
        if [[ -z "${GH_TOKEN:-}" ]]; then
            echo "error: strict release guard requires GH_TOKEN for remote lookups" >&2
            exit 1
        fi
        if [[ "$(git cat-file -t "refs/tags/$tag" 2> /dev/null || true)" != "tag" ]]; then
            echo "error: strict release tag must be annotated" >&2
            exit 1
        fi
        local remote_object local_object
        remote_object="$(gh api "repos/3leaps/gonimbus/git/ref/tags/${tag}" --jq 'select(.object.type == "tag") | .object.sha' 2> /dev/null || true)"
        local_object="$(git rev-parse "refs/tags/${tag}")"
        if [[ -z "$remote_object" || "$remote_object" != "$local_object" ]]; then
            echo "error: remote and local annotated tag objects must match" >&2
            exit 1
        fi
        local tag_commit head_commit main_commit
        tag_commit="$(git rev-parse "refs/tags/${tag}^{}")"
        head_commit="$(git rev-parse HEAD)"
        main_commit="$(gh api "repos/3leaps/gonimbus/git/ref/heads/main" --jq '.object.sha' 2> /dev/null || true)"
        if [[ "$tag_commit" != "$head_commit" ]]; then
            echo "error: release tag and HEAD must be identical" >&2
            exit 1
        fi
        if [[ "$tag_commit" != "$main_commit" ]]; then
            echo "error: release tag and origin/main must be identical" >&2
            exit 1
        fi
    fi

    echo "[ok] release guard passed for $tag"
}

main "$@"
