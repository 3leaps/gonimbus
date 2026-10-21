#!/usr/bin/env bash
# The release Makefile takes the tag only from GONIMBUS_RELEASE_TAG. A generic
# RELEASE_TAG from the environment or command line, a stale or unset
# GONIMBUS_RELEASE_TAG must each stop every tag-consuming release target before
# any fetch, draft, sign, upload or publish script runs. Runs the repository
# Makefile against a throwaway checkout whose ceremony scripts are stubs that
# record any call; the real tag guard is used.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/gonimbus-release-make-tag.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/scripts" "$fixture/.fulmen" "$fixture/docs/releases"
cp "$root/Makefile" "$fixture/Makefile"
cp "$root/scripts/release-guard-tag-version.sh" "$fixture/scripts/"
printf '1.2.3\n' > "$fixture/VERSION"
printf 'app:\n  version: 1.2.3\n' > "$fixture/.fulmen/app.yaml"
printf 'notes\n' > "$fixture/docs/releases/v1.2.3.md"
side_effects="$fixture/side-effects.log"
for stub in release-verify-published-tag release-fetch-ci-artifacts \
    release-create-draft sign-release-manifests release-upload-provenance \
    release-upload release-publish verify-checksums release-verify-signatures \
    verify-minisign-public-key verify-public-key; do
    printf '#!/usr/bin/env bash\necho "%s $*" >> "%s"\n' "$stub" "$side_effects" \
        > "$fixture/scripts/$stub.sh"
    chmod +x "$fixture/scripts/$stub.sh"
done
git -C "$fixture" init -q -b main
git -C "$fixture" config user.name "release make test"
git -C "$fixture" config user.email "noreply@example.invalid"
git -C "$fixture" add -A
git -C "$fixture" commit -qm fixture

targets=(release-guard-tag-name release-guard-tag-version release-guard-signing-tag
    release-verify-published-tag release-download release-create-draft
    release-sign release-notes release-upload release-upload-provenance
    release-upload-all release-publish)

run_make() {
    (cd "$fixture" && env -u RELEASE_TAG -u GONIMBUS_RELEASE_TAG -u MAKEFLAGS \
        "$@")
}

# expect_refused <label> <pattern> <command...>: non-zero exit, stderr names
# the prefixed variable, and no ceremony stub ran.
expect_refused() {
    local label="$1" pattern="$2" out
    shift 2
    : > "$side_effects"
    if out="$(run_make "$@" 2>&1)"; then
        echo "expected failure ($label): $*" >&2
        exit 1
    fi
    grep -Eq "$pattern" <<< "$out" || {
        echo "missing '$pattern' ($label): $*" >&2
        echo "$out" >&2
        exit 1
    }
    if [[ -s "$side_effects" ]]; then
        echo "side effect before refusal ($label): $*" >&2
        cat "$side_effects" >&2
        exit 1
    fi
}

for target in "${targets[@]}"; do
    for dry in "" "-n"; do
        expect_refused "env alias" 'RELEASE_TAG is forbidden.*GONIMBUS_RELEASE_TAG' \
            RELEASE_TAG=v1.2.3 GONIMBUS_RELEASE_TAG=v1.2.3 make -s $dry "$target"
        expect_refused "empty env alias" 'RELEASE_TAG is forbidden.*GONIMBUS_RELEASE_TAG' \
            RELEASE_TAG= GONIMBUS_RELEASE_TAG=v1.2.3 make -s $dry "$target"
        expect_refused "command-line alias" 'RELEASE_TAG is forbidden.*GONIMBUS_RELEASE_TAG' \
            GONIMBUS_RELEASE_TAG=v1.2.3 make -s $dry "$target" RELEASE_TAG=v1.2.3
    done
    expect_refused "unset tag" 'GONIMBUS_RELEASE_TAG not set' make -s "$target"
done

# A stale tag that is canonical but not VERSION stops every target except the
# format-only name guard and the read-only published-tag verifier, which may
# check any published tag. Every target that fetches, drafts, signs, uploads or
# publishes runs the VERSION guard first.
for target in "${targets[@]}"; do
    case "$target" in
        release-guard-tag-name | release-verify-published-tag) continue ;;
    esac
    expect_refused "stale tag" 'does not match VERSION' \
        GONIMBUS_RELEASE_TAG=v1.2.2 make -s "$target"
done

# Under make -j, read-only local checks may run beside the VERSION guard, but the
# published-tag check (it rewrites the local tag ref) and the ceremony scripts
# must not start.
for target in release-download release-create-draft release-sign \
    release-upload-provenance release-upload-all release-publish; do
    : > "$side_effects"
    if run_make GONIMBUS_RELEASE_TAG=v1.2.2 make -s -j8 "$target" > /dev/null 2>&1; then
        echo "expected failure (stale tag, -j8): $target" >&2
        exit 1
    fi
    if grep -Ev '^(verify-checksums|release-verify-signatures|verify-minisign-public-key|verify-public-key) ' \
        "$side_effects" | grep -q .; then
        echo "tag fetch or ceremony script ran beside a failed guard (-j8): $target" >&2
        cat "$side_effects" >&2
        exit 1
    fi
done

# The format guard reports success; the matching tag reaches the stubs.
out="$(run_make GONIMBUS_RELEASE_TAG=v1.2.3 make -s release-guard-tag-name)"
grep -q '^\[ok\] release tag name v1.2.3 is canonical$' <<< "$out" || {
    echo "release-guard-tag-name printed no [ok] line" >&2
    exit 1
}
: > "$side_effects"
run_make GONIMBUS_RELEASE_TAG=v1.2.3 make -s release-sign > /dev/null
grep -qx 'sign-release-manifests v1.2.3 dist/release' "$side_effects" || {
    echo "release-sign did not pass GONIMBUS_RELEASE_TAG to its script" >&2
    cat "$side_effects" >&2
    exit 1
}

# Non-release goals ignore a stray generic name from another repository's shell.
run_make RELEASE_TAG=v9.9.9 make -s -n version > /dev/null

echo "[ok] release make tag-source tests passed"
