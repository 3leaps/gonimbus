#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Remote byte agreement and partial/completed draft-upload retry controls
# across release-upload-provenance.sh and release-publish.sh, without live API writes.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"
scratch="$(mktemp -d)"
cleanup() {
    gpgconf --homedir "$scratch/gpg" --kill all > /dev/null 2>&1 || true
    rm -rf "$scratch"
}
trap cleanup EXIT

repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/keys" "$repo/docs/security" "$repo/docs/releases" "$scratch/bin" "$scratch/gpg" "$scratch/local" "$scratch/remote"
chmod 700 "$scratch/gpg"
cp "$root/scripts/"{release-publish.sh,release-upload-provenance.sh,release-decernor.sh,release-gh.sh} "$repo/scripts/"

cat > "$scratch/bin/decernor" << 'EOF'
#!/usr/bin/env bash
case "${1:-} ${2:-}" in
  'version ' | 'version') echo 'decernor 0.1.8' ;;
  'version -e')
    printf 'Version:         0.1.8\nCommit:          fake\nBuild Date:      fake\nGo Version:      fake\nGofulmen:        fake\nCrucible:        fake\n'
    ;;
  'fingerprint verify') exit 0 ;;
  *) exit 0 ;;
esac
EOF
chmod +x "$scratch/bin/decernor"
export GONIMBUS_DECERNOR_BIN="$scratch/bin/decernor"

cat > "$repo/scripts/release-verify-published-tag.sh" << 'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "${MOVED:-0}" == 0 && "$GONIMBUS_EXPECTED_COMMIT" == "$(git rev-parse refs/tags/v1.2.3^{})" ]]
SH
chmod +x "$repo/scripts/release-verify-published-tag.sh"

home="$scratch/gpg"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Synthetic signing <signing@example.invalid>' ed25519 cert,sign 1d > /dev/null 2>&1
primary="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --homedir "$home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --armor --export "$primary" > "$repo/docs/security/release-signing-keys.asc"
minisign -G -W -p "$scratch/minisign.pub" -s "$scratch/minisign.key" > /dev/null
cp "$scratch/minisign.pub" "$repo/docs/security/gonimbus-minisign.pub"
pin_minisign="$(sed -n 2p "$scratch/minisign.pub" | base64 -d | sha_sum 256 | awk '{print $1}')"
printf 'gpg %s\nminisign %s\n' "$primary" "$pin_minisign" > "$repo/keys/expected-fingerprints.txt"
printf '{"kind":"gpg"}\n{"kind":"minisign"}\n' > "$repo/keys/expected-fingerprints.ndjson"
printf '# notes\n' > "$repo/docs/releases/v1.2.3.md"

git -C "$repo" init -q -b main
git -C "$repo" config commit.gpgsign false
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid add -A
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm fixture
git -C "$repo" tag v1.2.3
commit="$(git -C "$repo" rev-parse HEAD)"

for f in linux-amd64 linux-arm64 windows-amd64.exe windows-arm64.exe darwin-arm64; do
    printf 'pkg %s\n' "$f" > "$scratch/local/gonimbus-$f"
done
(cd "$scratch/local" && sha_sum 256 gonimbus-linux-* gonimbus-darwin-* gonimbus-windows-* > SHA256SUMS && sha_sum 512 gonimbus-linux-* gonimbus-darwin-* gonimbus-windows-* > SHA512SUMS)
for m in SHA256SUMS SHA512SUMS; do
    minisign -S -s "$scratch/minisign.key" -m "$scratch/local/$m" -x "$scratch/local/$m.minisig" > /dev/null
    gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
        --local-user "$subkey!" --armor --detach-sign --output "$scratch/local/$m.asc" "$scratch/local/$m" 2> /dev/null
done
cp "$scratch/minisign.pub" "$scratch/local/gonimbus-minisign.pub"
cp "$repo/docs/security/release-signing-keys.asc" "$scratch/local/gonimbus-release-signing-key.asc"
cp "$repo/keys/expected-fingerprints.txt" "$scratch/local/expected-fingerprints.txt"
cp "$repo/keys/expected-fingerprints.ndjson" "$scratch/local/expected-fingerprints.ndjson"
cp "$repo/docs/releases/v1.2.3.md" "$scratch/local/release-notes-v1.2.3.md"
printf 'tag=v1.2.3\nobject=%040d\ncommit=%s\n' 1 "$commit" > "$scratch/local.anchor"
cp "$scratch/local"/* "$scratch/remote/"

cat > "$scratch/bin/gh" << 'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FIXTURE_CALLS"
case "$1 $2" in
"release view")
    if [[ "$*" == *"--jq .isDraft"* ]]; then
        if [[ "${PUBLIC:-0}" == "1" ]]; then echo false; else echo true; fi
    else
        python3 - "$FIXTURE_REMOTE" << 'PY'
import json, os, pathlib, sys
print(json.dumps({
    "isDraft": os.environ.get("PUBLIC", "0") != "1",
    "assets": [{"name": p.name} for p in sorted(pathlib.Path(sys.argv[1]).iterdir())],
}))
PY
    fi
    ;;
"release download")
    dir=""
    pattern=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dir) dir="$2"; shift 2 ;;
            --pattern) pattern="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    if [[ -n "$pattern" ]]; then
        cp "$FIXTURE_REMOTE/$pattern" "$dir/$pattern"
    else
        cp "$FIXTURE_REMOTE"/* "$dir/"
    fi
    ;;
"release upload")
    for file in "$@"; do
        [[ ! -f "$file" ]] || cp "$file" "$FIXTURE_REMOTE/"
    done
    ;;
"release edit")
    echo edited
    ;;
"api repos/"*)
    jq -n --argjson d false --argjson i "${FIXTURE_IMMUTABLE:-true}" '{draft: $d, immutable: $i}'
    ;;
*)
    exit 1
    ;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_REMOTE="$scratch/remote" FIXTURE_CALLS="$scratch/calls"
cd "$repo"

fail() {
    : > "$scratch/calls"
    if "$@" > "$scratch/output" 2>&1; then
        echo 'error: expected promotion/recovery rejection' >&2
        exit 1
    fi
    if grep -q -- '--draft=false' "$scratch/calls"; then
        echo 'error: held bytes were promoted' >&2
        exit 1
    fi
}

./scripts/release-publish.sh v1.2.3 "$scratch/local" > /dev/null

for name in gonimbus-linux-amd64 SHA256SUMS SHA256SUMS.minisig; do
    printf poison > "$scratch/remote/$name"
    fail ./scripts/release-publish.sh v1.2.3 "$scratch/local"
    cp "$scratch/local/$name" "$scratch/remote/$name"
done

# Partial or poisoned provenance subsets on a draft can be safely restored by release-upload-provenance.sh.
rm "$scratch/remote/SHA256SUMS.minisig" "$scratch/remote/SHA512SUMS.minisig"
./scripts/release-upload-provenance.sh v1.2.3 "$scratch/local" > /dev/null
rm "$scratch/remote/SHA512SUMS.minisig"
printf poison > "$scratch/remote/SHA256SUMS.minisig"
./scripts/release-upload-provenance.sh v1.2.3 "$scratch/local" > /dev/null
./scripts/release-upload-provenance.sh v1.2.3 "$scratch/local" > /dev/null
./scripts/release-publish.sh v1.2.3 "$scratch/local" > /dev/null

printf extra > "$scratch/remote/extra"
fail ./scripts/release-publish.sh v1.2.3 "$scratch/local"
rm "$scratch/remote/extra"

fail env PUBLIC=1 ./scripts/release-upload-provenance.sh v1.2.3 "$scratch/local"
fail env PUBLIC=1 ./scripts/release-publish.sh v1.2.3 "$scratch/local"
fail env MOVED=1 ./scripts/release-publish.sh v1.2.3 "$scratch/local"
if FIXTURE_IMMUTABLE=false ./scripts/release-publish.sh v1.2.3 "$scratch/local" > "$scratch/output" 2>&1; then
    echo 'error: expected post-promote immutability failure' >&2
    exit 1
fi
grep -q 'is not immutable on GitHub' "$scratch/output" || {
    cat "$scratch/output" >&2
    exit 1
}

echo '[ok] remote byte agreement and safe partial/completed draft-upload retries'
