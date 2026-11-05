#!/usr/bin/env bash
# ci-install-decernor.sh must print only the binary path on stdout and fail
# closed when the archive digest does not match config/release/decernor-ci-sha256.txt.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"
fail() {
    echo "FAIL: $*" >&2
    exit 1
}

scratch="$(mktemp -d "${TMPDIR:-/tmp}/decernor-ci-test.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/repo/scripts" "$scratch/repo/config/release" "$scratch/bin" "$scratch/pkg" "$scratch/dest" "$scratch/bad-dest"
cp "$root/scripts/ci-install-decernor.sh" "$scratch/repo/scripts/"
cat > "$scratch/pkg/decernor" << 'SH'
#!/usr/bin/env bash
echo "decernor 0.1.8"
SH
chmod 0755 "$scratch/pkg/decernor"
tar -czf "$scratch/decernor_0.1.8_linux_amd64.tar.gz" -C "$scratch/pkg" decernor
good_digest="$(sha_sum 256 "$scratch/decernor_0.1.8_linux_amd64.tar.gz" | awk '{print $1}')"
printf '%s  decernor_0.1.8_linux_amd64.tar.gz\n' "$good_digest" > "$scratch/repo/config/release/decernor-ci-sha256.txt"

cat > "$scratch/bin/curl" << 'SH'
#!/usr/bin/env bash
out=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --output)
            shift
            out="$1"
            ;;
    esac
    shift
done
if [[ "${FIXTURE_TAMPER_ARCHIVE:-0}" == 1 ]]; then
    printf 'tampered\n' > "$out"
else
    cp "$FIXTURE_ARCHIVE" "$out"
fi
SH
chmod +x "$scratch/bin/curl"
export PATH="$scratch/bin:$PATH" FIXTURE_ARCHIVE="$scratch/decernor_0.1.8_linux_amd64.tar.gz"

err="$scratch/stderr"
set +e
out="$("$scratch/repo/scripts/ci-install-decernor.sh" "$scratch/dest" 2> "$err")"
rc=$?
set -e
[ "$rc" -eq 0 ] || fail "ci-install-decernor.sh exited $rc: $(cat "$err")"
n="$(printf '%s\n' "$out" | wc -l | tr -d ' ')"
[ "$n" -eq 1 ] || fail "stdout must be exactly one line (got $n): $(printf '%s' "$out" | od -An -tx1)"
[[ "$out" == /*/decernor ]] || fail "stdout must be an absolute path ending in /decernor: $out"
[ -x "$out" ] && [ ! -L "$out" ] || fail "installed binary is not a regular executable: $out"
grep -q ': OK' <<< "$out" && fail "sha256sum --check wrote to stdout (GITHUB_ENV would reject this)"

# Tampered archive must fail closed before extracting the binary.
if FIXTURE_TAMPER_ARCHIVE=1 "$scratch/repo/scripts/ci-install-decernor.sh" "$scratch/bad-dest" > /dev/null 2>&1; then
    fail "tampered archive was accepted"
fi
[ ! -e "$scratch/bad-dest/decernor" ] || fail "tampered archive extracted a binary"
echo "[ok] Decernor CI install stdout is a single path and digest check fails closed"
