#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Tests for scripts/release-stage-published.sh with throwaway keys and stub gh.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"
bin="${GONIMBUS_DECERNOR_BIN:-$(command -v decernor || true)}"
[[ -n "$bin" && -x "$bin" && "$bin" == /* && ! -L "$bin" ]] || {
    echo 'error: GONIMBUS_DECERNOR_BIN required' >&2
    exit 1
}
export GONIMBUS_DECERNOR_BIN="$bin"
scratch="$(mktemp -d)"
cleanup() {
    gpgconf --homedir "$scratch/gpg" --kill all > /dev/null 2>&1 || true
    rm -rf "$scratch"
}
trap cleanup EXIT

repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/keys" "$repo/docs/security" "$repo/docs/releases" "$scratch/bin" "$scratch/gpg"
chmod 700 "$scratch/gpg"
cp "$root/scripts/release-stage-published.sh" "$root/scripts/release-gh.sh" \
    "$root/scripts/release-decernor.sh" "$root/scripts/verify-checksums.sh" \
    "$root/scripts/release-verify-signatures.sh" "$repo/scripts/"
chmod +x "$repo/scripts/release-stage-published.sh"

home="$scratch/gpg"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Synthetic signing <signing@example.invalid>' ed25519 cert,sign 1d > /dev/null 2>&1
primary="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --homedir "$home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --armor --export "$primary" > "$repo/docs/security/release-signing-keys.asc"
minisign -G -W -p "$scratch/minisign.pub" -s "$scratch/minisign.key" > /dev/null
cp "$scratch/minisign.pub" "$repo/docs/security/gonimbus-minisign.pub"
"$bin" fingerprint "$repo/docs/security/release-signing-keys.asc" --class public --kind gpg \
    --format ndjson --path-mode none --gpg-role primary > "$scratch/gpg.ndjson"
"$bin" fingerprint "$repo/docs/security/gonimbus-minisign.pub" --class public --kind minisign \
    --format ndjson --path-mode none > "$scratch/minisign.ndjson"
python3 - "$scratch" "$repo/keys" << 'PY'
import json, pathlib, sys
base, keys = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
g = [json.loads(line) for line in (base / 'gpg.ndjson').read_text().splitlines() if line]
m = [json.loads(line) for line in (base / 'minisign.ndjson').read_text().splitlines() if line and json.loads(line).get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
(keys / 'expected-fingerprints.ndjson').write_text(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in (g[0], m[0])))
(keys / 'expected-fingerprints.txt').write_text('gpg ' + g[0]['fingerprint'] + '\nminisign ' + m[0]['fingerprint'] + '\n')
PY
printf '# notes v1.2.3\n' > "$repo/docs/releases/v1.2.3.md"

git -C "$repo" init -q -b main
git -C "$repo" config commit.gpgsign false
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid add .
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm fixture
commit="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" tag v1.2.3 "$commit"
object="$(printf '%040d' 7)"

cat > "$repo/scripts/release-verify-published-tag.sh" << SH
#!/usr/bin/env bash
[[ "\${FIXTURE_TAG_FAIL:-0}" == 0 ]] || exit 1
if [[ -n "\${GONIMBUS_ANCHOR_OUT:-}" ]]; then
    printf 'tag=v1.2.3\nobject=%s\ncommit=%s\n' "$object" "$commit" > "\$GONIMBUS_ANCHOR_OUT"
fi
SH
chmod +x "$repo/scripts/release-verify-published-tag.sh"

remote="$scratch/remote"
stage_remote() {
    rm -rf "$remote"
    mkdir -p "$remote"
    for f in darwin-arm64 linux-amd64 linux-arm64 windows-amd64.exe windows-arm64.exe; do
        printf 'pkg %s\n' "$f" > "$remote/gonimbus-$f"
    done
    (cd "$remote" && sha_sum 256 gonimbus-* > SHA256SUMS && sha_sum 512 gonimbus-* > SHA512SUMS)
    for m in SHA256SUMS SHA512SUMS; do
        minisign -S -s "$scratch/minisign.key" -m "$remote/$m" -x "$remote/$m.minisig" > /dev/null
        gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
            --local-user "$subkey!" --armor --detach-sign --output "$remote/$m.asc" "$remote/$m" 2> /dev/null
    done
    cp "$repo/docs/security/gonimbus-minisign.pub" "$remote/gonimbus-minisign.pub"
    cp "$repo/docs/security/release-signing-keys.asc" "$remote/gonimbus-release-signing-key.asc"
    cp "$repo/keys/expected-fingerprints.txt" "$remote/expected-fingerprints.txt"
    cp "$repo/keys/expected-fingerprints.ndjson" "$remote/expected-fingerprints.ndjson"
    cp "$repo/docs/releases/v1.2.3.md" "$remote/release-notes-v1.2.3.md"
}

cat > "$scratch/bin/gh" << 'SH'
#!/usr/bin/env bash
case "$1 $2" in
"api repos/"*)
    python3 - "$FIXTURE_REMOTE" "${FIXTURE_DRAFT:-false}" "${FIXTURE_IMMUTABLE:-true}" "${FIXTURE_BAD_DIGEST:-0}" << 'PY'
import hashlib, json, pathlib, sys
remote = pathlib.Path(sys.argv[1])
draft = sys.argv[2] == "true"
immutable = sys.argv[3] == "true"
bad_digest = sys.argv[4] == "1"
assets = []
for p in sorted(remote.iterdir()):
    if p.is_file():
        digest = "sha256:" + ("0" * 64 if bad_digest and p.name == "SHA256SUMS" else hashlib.sha256(p.read_bytes()).hexdigest())
        assets.append({"name": p.name, "size": p.stat().st_size, "digest": digest})
print(json.dumps({"tag_name": "v1.2.3", "draft": draft, "immutable": immutable, "assets": assets}))
PY
    ;;
"release download")
    dir=""
    while [[ $# -gt 0 ]]; do
        [[ "$1" == --dir ]] && dir="$2"
        shift
    done
    cp "$FIXTURE_REMOTE"/* "$dir/"
    ;;
*) exit 1 ;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_REMOTE="$remote"

stage_remote
(cd "$repo" && GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/pub") > /dev/null
[[ -f "$scratch/pub/SHA256SUMS" && -f "$scratch/pub/expected-fingerprints.txt" && -f "$scratch/pub/expected-fingerprints.ndjson" ]]
[[ -f "$scratch/pub.anchor" ]]

# Fail closed on draft release
if (cd "$repo" && FIXTURE_DRAFT=true GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/pub-draft") > /dev/null 2>&1; then
    echo 'error: accepted draft release' >&2
    exit 1
fi
[[ ! -e "$scratch/pub-draft" && ! -e "$scratch/pub-draft.anchor" ]]

# Fail closed on non-immutable release
if (cd "$repo" && FIXTURE_IMMUTABLE=false GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/pub-mut") > /dev/null 2>&1; then
    echo 'error: accepted mutable release' >&2
    exit 1
fi
[[ ! -e "$scratch/pub-mut" && ! -e "$scratch/pub-mut.anchor" ]]

# Fail closed on digest mismatch
if (cd "$repo" && FIXTURE_BAD_DIGEST=1 GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/pub-baddig") > /dev/null 2>&1; then
    echo 'error: accepted digest mismatch' >&2
    exit 1
fi
[[ ! -e "$scratch/pub-baddig" && ! -e "$scratch/pub-baddig.anchor" ]]

# Fail closed on non-byte-identical gonimbus-release-signing-key.asc (after download)
# and ensure neither $dest nor $dest.anchor remains on disk
stage_remote
printf '\n' >> "$remote/gonimbus-release-signing-key.asc"
if (cd "$repo" && GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/pub-badpgp") > /dev/null 2>&1; then
    echo 'error: accepted non-byte-identical gonimbus-release-signing-key.asc' >&2
    exit 1
fi
[[ ! -e "$scratch/pub-badpgp" && ! -e "$scratch/pub-badpgp.anchor" ]]

# Fail closed on missing expected-fingerprints.ndjson
stage_remote
rm "$remote/expected-fingerprints.ndjson"
if (cd "$repo" && GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/pub-missing") > /dev/null 2>&1; then
    echo 'error: accepted missing expected-fingerprints.ndjson' >&2
    exit 1
fi
[[ ! -e "$scratch/pub-missing" && ! -e "$scratch/pub-missing.anchor" ]]

# Fail closed on symlink destination with trailing slash
stage_remote
mkdir -p "$scratch/real-target"
printf 'keep\n' > "$scratch/real-target/sentinel"
ln -s "$scratch/real-target" "$scratch/sym-dest"
if (cd "$repo" && GONIMBUS_RELEASE_TAG=v1.2.3 ./scripts/release-stage-published.sh "$scratch/sym-dest/") > /dev/null 2>&1; then
    echo 'error: accepted symlink destination with trailing slash' >&2
    exit 1
fi
[[ -f "$scratch/real-target/sentinel" && ! -e "$scratch/sym-dest.anchor" ]]

echo '[ok] release-stage-published controls passed'
