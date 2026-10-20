#!/usr/bin/env bash
# Drive release-preflight.sh through every gate to its terminal `make pr-final`
# call, with that final gate passing and failing. go, goneat and make are test
# doubles; the repository, origin and clone-outside-$HOME steps are real.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
unset RELEASE_TAG GONIMBUS_RELEASE_TAG

repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/docs/releases" "$scratch/bin"
cp "$root/scripts/release-preflight.sh" "$root/scripts/workflow-pins.test.sh" "$root/scripts/workflow-yaml.sh" "$repo/scripts/"
mkdir -p "$repo/.github/workflows"
cp "$root/.github/workflows/"*.yml "$repo/.github/workflows/"
printf '1.2.3\n' > "$repo/VERSION"
printf '# Changelog\n\n## [1.2.3] - 2026-10-13\n' > "$repo/CHANGELOG.md"
printf '# Release notes\n\n## [1.2.3] - 2026-10-13\n' > "$repo/RELEASE_NOTES.md"
good_notes() {
    cat << 'MD'
# v1.2.3

Notes.

## Install

```bash
brew install 3leaps/tap/gonimbus
go install github.com/3leaps/gonimbus/cmd/gonimbus@v1.2.3
```

```powershell
scoop install gonimbus
```

## Verification
MD
}
good_notes > "$repo/docs/releases/v1.2.3.md"
git init -q -b main "$repo"
git -C "$repo" -c user.name=fixture -c user.email=fixture@example.invalid add -A
git -C "$repo" -c user.name=fixture -c user.email=fixture@example.invalid commit -qm fixture
git init -q --bare -b main "$scratch/origin.git"
git -C "$repo" remote add origin "$scratch/origin.git"
git -C "$repo" push -q origin main

# Test doubles: record calls; make honors FIXTURE_PR_FINAL.
cat > "$scratch/bin/go" << 'SH'
#!/usr/bin/env bash
printf 'go %s\n' "$*" >> "$FIXTURE_LOG"
SH
cat > "$scratch/bin/goneat" << 'SH'
#!/usr/bin/env bash
printf 'goneat %s\n' "$*" >> "$FIXTURE_LOG"
SH
cat > "$scratch/bin/make" << 'SH'
#!/usr/bin/env bash
printf 'make %s\n' "$*" >> "$FIXTURE_LOG"
[[ "$*" == *pr-final* ]] || exit 3
[[ "${FIXTURE_PR_FINAL:-pass}" == pass ]]
SH
chmod +x "$scratch/bin/"*
export PATH="$scratch/bin:$PATH" FIXTURE_LOG="$scratch/calls.log"

run() {
    : > "$FIXTURE_LOG"
    (cd "$repo" && "$@" ./scripts/release-preflight.sh) > "$scratch/out" 2>&1
}

# Final gate passes: preflight completes and pr-final was the terminal call.
run env FIXTURE_PR_FINAL=pass || {
    cat "$scratch/out" >&2
    echo 'error: preflight failed with a passing final gate' >&2
    exit 1
}
grep -q '^\[ok\] pr-final gate passed' "$scratch/out"
grep -q '^\[ok\] preflight passed for v1.2.3' "$scratch/out"
[[ "$(tail -1 "$FIXTURE_LOG")" == 'make --no-print-directory pr-final' ]]
grep -q '^go test ./...$' "$FIXTURE_LOG"

# Final gate fails: preflight fails, names the gate, and never claims success.
if run env FIXTURE_PR_FINAL=fail; then
    echo 'error: preflight passed with a failing final gate' >&2
    exit 1
fi
grep -q 'make pr-final failed' "$scratch/out"
if grep -q 'preflight passed' "$scratch/out"; then
    echo 'error: preflight claimed success after a failed final gate' >&2
    exit 1
fi

# A dirty tree stops before any later gate runs.
printf 'x\n' > "$repo/untracked"
if run env FIXTURE_PR_FINAL=pass; then
    echo 'error: preflight passed with a dirty tree' >&2
    exit 1
fi
grep -q 'working tree is not clean' "$scratch/out"
[[ ! -s "$FIXTURE_LOG" ]]
rm "$repo/untracked"

# The per-version notes are the release body: they must carry install
# instructions for this exact tag, inside the Install section.
notes_case() { # reason notes-content
    local reason="$1"
    printf '%s' "$2" > "$repo/docs/releases/v1.2.3.md"
    git -C "$repo" -c user.name=fixture -c user.email=fixture@example.invalid commit -qam "notes: $reason"
    git -C "$repo" push -q origin main
    if run env FIXTURE_PR_FINAL=pass; then
        echo "error: preflight passed with notes: $reason" >&2
        exit 1
    fi
    grep -qF -- "$reason" "$scratch/out" || {
        echo "error: missing named refusal: $reason" >&2
        cat "$scratch/out" >&2
        exit 1
    }
    [[ "$(grep -c '^make' "$FIXTURE_LOG")" == 0 ]] || {
        echo 'error: later gates ran after a notes refusal' >&2
        exit 1
    }
}
good="$(good_notes)"
notes_case "has no '## Install' section" "${good//## Install/## Installation}"
notes_case "missing: go install github.com/3leaps/gonimbus/cmd/gonimbus@v1.2.3" "${good//@v1.2.3/@v1.2.2}"
# The exact tag is a whole token: a longer version or a prerelease suffix
# that begins with the tag does not satisfy it.
notes_case "missing: go install github.com/3leaps/gonimbus/cmd/gonimbus@v1.2.3" "${good//@v1.2.3/@v1.2.30}"
notes_case "missing: go install github.com/3leaps/gonimbus/cmd/gonimbus@v1.2.3" "${good//@v1.2.3/@v1.2.3-rc.1}"
notes_case "missing: scoop install gonimbus" "${good//scoop install gonimbus/scoop install gonimbus-extra}"
# A prefixed command and a tag whose dots are other characters do not count.
notes_case 'missing: brew install 3leaps/tap/gonimbus' "${good//brew install/xbrew install}"
notes_case "missing: go install github.com/3leaps/gonimbus/cmd/gonimbus@v1.2.3" "${good//@v1.2.3/@v1x2x3}"
notes_case 'missing: brew install 3leaps/tap/gonimbus' "${good//brew install/brew reinstall}"
notes_case 'missing: scoop install gonimbus' "${good//scoop install gonimbus/scoop install other}"
# Install commands outside the Install section do not count.
notes_case "has no '## Install' section" "$(printf '# v1.2.3\n\nbrew install 3leaps/tap/gonimbus\nscoop install gonimbus\ngo install github.com/3leaps/gonimbus/cmd/gonimbus@v1.2.3\n')"
# Relative links do not resolve on the release page; each form refuses.
for link in '../security/README.md' './x.md' '/docs/x.md' 'CHANGELOG.md' 'docs/x'; do
    notes_case 'has a relative link (use a tag-scoped URL)' "$good
See [guide]($link).
"
done
# Absolute links and in-page anchors are accepted.
printf '%s\nSee [guide](https://github.com/3leaps/gonimbus/blob/v1.2.3/docs/security/README.md) and [below](#install).\n' "$good" > "$repo/docs/releases/v1.2.3.md"
git -C "$repo" -c user.name=fixture -c user.email=fixture@example.invalid commit -qam 'notes: absolute links'
git -C "$repo" push -q origin main
run env FIXTURE_PR_FINAL=pass || {
    cat "$scratch/out" >&2
    echo 'error: preflight refused absolute links' >&2
    exit 1
}
grep -q '^\[ok\] docs/releases/v1.2.3.md links are absolute' "$scratch/out"
# Accepted: the exact tag followed by a comment, or by trailing whitespace.
for variant in '@v1.2.3  # pinned' '@v1.2.3 '; do
    printf '%s' "${good//@v1.2.3/$variant}" > "$repo/docs/releases/v1.2.3.md"
    git -C "$repo" -c user.name=fixture -c user.email=fixture@example.invalid commit -qam "notes: accept $variant"
    git -C "$repo" push -q origin main
    run env FIXTURE_PR_FINAL=pass || {
        cat "$scratch/out" >&2
        echo "error: preflight refused valid install line: $variant" >&2
        exit 1
    }
done
# Restore valid notes: preflight passes again.
good_notes > "$repo/docs/releases/v1.2.3.md"
git -C "$repo" -c user.name=fixture -c user.email=fixture@example.invalid commit -qam 'notes: restore'
git -C "$repo" push -q origin main
run env FIXTURE_PR_FINAL=pass || {
    cat "$scratch/out" >&2
    echo 'error: preflight failed with valid notes' >&2
    exit 1
}
grep -q '^\[ok\] docs/releases/v1.2.3.md has install instructions for v1.2.3' "$scratch/out"

echo '[ok] preflight reaches its final gate and fails closed when it fails'
