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
printf '# v1.2.3\n\nNotes.\n' > "$repo/docs/releases/v1.2.3.md"
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

echo '[ok] preflight reaches its final gate and fails closed when it fails'
