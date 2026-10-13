#!/usr/bin/env bash
# Stub-gh controls for release-publish.sh with throwaway minisign and PGP keys.
# No live GitHub call is made; promotion is recorded only when every gate holds.
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
for tool in minisign gpg jq git; do
    command -v "$tool" > /dev/null 2>&1 || {
        echo "error: $tool is required for the publish controls" >&2
        exit 1
    }
done
repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/keys" "$repo/docs/security" "$scratch/bin" "$scratch/gpg"
chmod 700 "$scratch/gpg"
cp "$root/scripts/release-publish.sh" "$repo/scripts/"
cat > "$repo/scripts/release-verify-published-tag.sh" << 'SH'
#!/usr/bin/env bash
[[ "${FIXTURE_TAG_REPLACED:-0}" == 0 ]] || { echo 'error: tag replaced' >&2; exit 1; }
SH
chmod +x "$repo/scripts/release-verify-published-tag.sh"

# Throwaway signing keys and the committed pins they correspond to.
home="$scratch/gpg"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Synthetic signing <signing@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
primary="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --homedir "$home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --armor --export "$primary" > "$repo/docs/security/release-signing-keys.asc"
minisign -G -W -p "$scratch/minisign.pub" -s "$scratch/minisign.key" > /dev/null
cp "$scratch/minisign.pub" "$repo/docs/security/gonimbus-minisign.pub"
pin_minisign="$(sed -n 2p "$scratch/minisign.pub" | base64 -d | sha_sum 256 | awk '{print $1}')"
printf 'gpg %s\nminisign %s\n' "$primary" "$pin_minisign" > "$repo/keys/expected-fingerprints.txt"

mkdir -p "$repo/docs/releases"
printf '# notes\n' > "$repo/docs/releases/v1.2.3.md"
git -C "$repo" init -q -b main
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid add docs/releases/v1.2.3.md \
    keys/expected-fingerprints.txt docs/security/release-signing-keys.asc docs/security/gonimbus-minisign.pub
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm fixture
tagged_commit="$(git -C "$repo" rev-parse HEAD)"

d="$scratch/dist"
stage() {
    rm -rf "$d" "$scratch/draft"
    mkdir -p "$d" "$scratch/draft"
    for f in linux-amd64 linux-arm64 windows-amd64.exe windows-arm64.exe darwin-arm64; do
        printf 'pkg %s\n' "$f" > "$d/gonimbus-$f"
    done
    (cd "$d" && sha_sum 256 gonimbus-linux-* gonimbus-darwin-* gonimbus-windows-* > SHA256SUMS && sha_sum 512 gonimbus-linux-* gonimbus-darwin-* gonimbus-windows-* > SHA512SUMS)
    for m in SHA256SUMS SHA512SUMS; do
        minisign -S -s "$scratch/minisign.key" -m "$d/$m" -x "$d/$m.minisig" > /dev/null
        gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
            --local-user "$subkey!" --armor --detach-sign --output "$d/$m.asc" "$d/$m" 2> /dev/null
    done
    cp "$scratch/minisign.pub" "$d/gonimbus-minisign.pub"
    git -C "$repo" cat-file blob "$tagged_commit:docs/security/release-signing-keys.asc" > "$d/gonimbus-release-signing-key.asc"
    cp "$repo/docs/releases/v1.2.3.md" "$d/release-notes-v1.2.3.md"
    printf 'tag=v1.2.3\nobject=%040d\ncommit=%s\n' 1 "$tagged_commit" > "$d.anchor"
    cp "$d"/* "$scratch/draft/"
}

# Stub gh: the draft's assets are whatever sits in $scratch/draft.
cat > "$scratch/bin/gh" << 'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FIXTURE_LOG"
case "$1 $2" in
"release view")
    names="$(cd "$FIXTURE_DRAFT" && ls -1 | jq -R . | jq -s 'map({name: .})')"
    jq -n --argjson a "$names" --argjson d "${FIXTURE_IS_DRAFT:-true}" '{isDraft: $d, assets: $a}'
    ;;
"release download")
    dir=""
    while [[ $# -gt 0 ]]; do
        [[ "$1" == --dir ]] && dir="$2"
        shift
    done
    cp "$FIXTURE_DRAFT"/* "$dir/"
    ;;
"release edit") ;;
*) exit 1 ;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_LOG="$scratch/gh.log" FIXTURE_DRAFT="$scratch/draft"

refuse() {
    local reason="$1"
    : > "$FIXTURE_LOG"
    if (cd "$repo" && ./scripts/release-publish.sh v1.2.3 "$d") > "$scratch/out" 2>&1; then
        echo "error: expected rejection: $reason" >&2
        exit 1
    fi
    grep -q "$reason" "$scratch/out" || {
        echo "error: missing named rejection: $reason" >&2
        cat "$scratch/out" >&2
        exit 1
    }
    ! grep -q '^release edit' "$FIXTURE_LOG" || {
        echo "error: promoted despite: $reason" >&2
        exit 1
    }
}

stage
FIXTURE_TAG_REPLACED=1 refuse 'no longer matches the verified anchor'
stage
printf 'sneaky post-tag edit\n' >> "$d/release-notes-v1.2.3.md"
cp "$d/release-notes-v1.2.3.md" "$scratch/draft/"
refuse 'differs from docs/releases/v1.2.3.md at the tagged commit'
stage
FIXTURE_IS_DRAFT=false refuse 'is not a draft'
stage
printf 'extra\n' > "$scratch/draft/extra.bin"
refuse 'draft assets differ from the expected set'
stage
printf 'tampered\n' >> "$d/gonimbus-linux-amd64"
refuse 'local packages do not match the manifests'
# A manifest that omits a package still passes a plain -c check; refuse it.
stage
grep -v linux-arm64 "$d/SHA256SUMS" > "$d/SHA256SUMS.short"
mv "$d/SHA256SUMS.short" "$d/SHA256SUMS"
cp "$d/SHA256SUMS" "$scratch/draft/"
refuse 'SHA256SUMS does not list exactly the expected packages'
stage
minisign -G -W -f -p "$scratch/other.pub" -s "$scratch/other.key" > /dev/null
cp "$scratch/other.pub" "$d/gonimbus-minisign.pub"
refuse 'staged minisign key is not the key committed in the tagged commit'
stage
# Flip one signature byte (minisign ignores appended lines), then mirror the
# tampered file into the draft so only the signature gate can catch it.
python3 - "$d/SHA256SUMS.minisig" << 'PY'
import base64
import sys
lines = open(sys.argv[1]).read().split('\n')
sig = bytearray(base64.b64decode(lines[1]))
sig[-1] ^= 1
lines[1] = base64.b64encode(bytes(sig)).decode()
open(sys.argv[1], 'w').write('\n'.join(lines))
PY
cp "$d/SHA256SUMS.minisig" "$scratch/draft/"
refuse 'minisign verification failed'
stage
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Other signer <other@example.invalid>' ed25519 sign 1d > /dev/null 2>&1
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' --local-user other@example.invalid \
    --armor --detach-sign --yes --output "$d/SHA512SUMS.asc" "$d/SHA512SUMS" 2> /dev/null
cp "$d/SHA512SUMS.asc" "$scratch/draft/"
refuse 'not a good signature by the pinned signing subkey'
stage
printf 'swapped on the server\n' > "$scratch/draft/gonimbus-darwin-arm64"
refuse 'draft asset differs from the verified local file'

# Trust comes from the pins in the anchored tag commit, never the working tree.
# A second key set B stands in for an attacker's or a later rotation's keys.
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Rotated signing <rotated@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
rot_primary="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys rotated@example.invalid | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$rot_primary" ed25519 sign 1d > /dev/null 2>&1
rot_subkey="$(gpg --homedir "$home" --batch --with-colons --with-subkey-fingerprint --list-keys rotated@example.invalid | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
minisign -G -W -f -p "$scratch/rot.pub" -s "$scratch/rot.key" > /dev/null
rot_pin="$(sed -n 2p "$scratch/rot.pub" | base64 -d | sha_sum 256 | awk '{print $1}')"
pins=(keys/expected-fingerprints.txt docs/security/release-signing-keys.asc docs/security/gonimbus-minisign.pub)
save_pins() { for p in "${pins[@]}"; do cp "$repo/$p" "$scratch/saved-${p//\//_}"; done; }
restore_pins() { for p in "${pins[@]}"; do cp "$scratch/saved-${p//\//_}" "$repo/$p"; done; }
write_rotated_pins() {
    gpg --homedir "$home" --batch --armor --export "$rot_primary" > "$repo/docs/security/release-signing-keys.asc"
    cp "$scratch/rot.pub" "$repo/docs/security/gonimbus-minisign.pub"
    printf 'gpg %s\nminisign %s\n' "$rot_primary" "$rot_pin" > "$repo/keys/expected-fingerprints.txt"
}
# Stage a release signed by B, with B's public keys staged beside it.
stage_signed_by_rotated() {
    stage
    for m in SHA256SUMS SHA512SUMS; do
        minisign -S -s "$scratch/rot.key" -m "$d/$m" -x "$d/$m.minisig" > /dev/null
        gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
            --local-user "$rot_subkey!" --armor --detach-sign --yes --output "$d/$m.asc" "$d/$m" 2> /dev/null
    done
    cp "$scratch/rot.pub" "$d/gonimbus-minisign.pub"
    gpg --homedir "$home" --batch --armor --export "$rot_primary" > "$d/gonimbus-release-signing-key.asc"
    cp "$d"/* "$scratch/draft/"
}
save_pins
# Working-tree pins replaced with B (uncommitted): a B-signed set is refused.
write_rotated_pins
stage_signed_by_rotated
refuse 'staged minisign key is not the key committed in the tagged commit'
restore_pins
# Working-tree pins deleted: the anchored commit still supplies them, and a
# correctly signed set publishes.
for p in "${pins[@]}"; do rm "$repo/$p"; done
stage
: > "$FIXTURE_LOG"
(cd "$repo" && ./scripts/release-publish.sh v1.2.3 "$d") > /dev/null
[[ "$(grep -c '^release edit v1.2.3 .*--draft=false' "$FIXTURE_LOG")" == 1 ]]
restore_pins
# A later key rotation committed on main, anchor still on the tagged commit: the
# release must stay bound to the tagged pins. B-signed is refused ...
write_rotated_pins
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid add "${pins[@]}"
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm 'rotate keys'
stage_signed_by_rotated
refuse 'staged minisign key is not the key committed in the tagged commit'
# ... and the original signer is still accepted against the tagged pins.
stage
: > "$FIXTURE_LOG"
(cd "$repo" && ./scripts/release-publish.sh v1.2.3 "$d") > /dev/null
[[ "$(grep -c '^release edit v1.2.3 .*--draft=false' "$FIXTURE_LOG")" == 1 ]]
# A tagged commit without the pins refuses outright.
notes_blob="$(git -C "$repo" rev-parse "$tagged_commit:docs/releases/v1.2.3.md")"
releases_tree="$(printf '100644 blob %s\tv1.2.3.md\n' "$notes_blob" | git -C "$repo" mktree)"
docs_tree="$(printf '040000 tree %s\treleases\n' "$releases_tree" | git -C "$repo" mktree)"
unpinned_tree="$(printf '040000 tree %s\tdocs\n' "$docs_tree" | git -C "$repo" mktree)"
unpinned_commit="$(git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid commit-tree "$unpinned_tree" -m 'notes, no pins')"
stage
printf 'tag=v1.2.3\nobject=%040d\ncommit=%s\n' 1 "$unpinned_commit" > "$d.anchor"
refuse 'committed keys/expected-fingerprints.txt required in the tagged commit'

# Every gate holds: promotion happens exactly once, after the byte comparison.
stage
: > "$FIXTURE_LOG"
(cd "$repo" && ./scripts/release-publish.sh v1.2.3 "$d") > /dev/null
[[ "$(grep -c '^release edit v1.2.3 .*--draft=false' "$FIXTURE_LOG")" == 1 ]]
[[ "$(grep -n '' "$FIXTURE_LOG" | grep -E 'release (download|edit)' | head -1)" == *'release download'* ]]
echo '[ok] publish promotes only a byte-verified, pin-signed draft'
