#!/usr/bin/env bash
# Synthetic, short-lived signing fixture; all generated material stays outside git.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
# Stop every agent started under the scratch homes, then remove them.
cleanup() {
    local home
    for home in "$scratch/gpg" "$scratch/expired-ring"; do
        [[ -d "$home" ]] && gpgconf --homedir "$home" --kill all > /dev/null 2>&1 || true
    done
    rm -rf "$scratch"
}
trap cleanup EXIT
export GNUPGHOME="$scratch/gpg"
mkdir -m 700 "$GNUPGHOME"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
    '3 Leaps Infosec Team <infosec@3leaps.net>' ed25519 sign 0 > /dev/null 2>&1
primary="$(gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$primary" | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
fixture="$scratch/repo"
git init -q -b main "$fixture"
mkdir -p "$fixture/docs/security" "$fixture/keys" "$fixture/scripts" "$fixture/config/release"
cp "$root/scripts/verify-pinned-tag.sh" "$root/scripts/validate-release-anchors.sh" "$fixture/scripts/"
cp "$root/config/release/tagger-identity.txt" "$fixture/config/release/"
gpg --batch --armor --export "$primary" > "$fixture/docs/security/release-signing-keys.asc"
printf 'gpg %s\nminisign %064d\n' "$primary" 0 > "$fixture/keys/expected-fingerprints.txt"
printf 'fixture\n' > "$fixture/file"
printf 'Fixture release\n' > "$scratch/message.txt"
git -C "$fixture" config user.name '3 Leaps Infosec Team'
git -C "$fixture" config user.email infosec@3leaps.net
# The trust material is committed: verification reads it from the tagged commit.
git -C "$fixture" add file docs/security keys config scripts
git -C "$fixture" commit -qm fixture
good="$(git -C "$fixture" rev-parse HEAD)"
export GONIMBUS_RELEASE_TAG=v1.2.3
export GONIMBUS_TAGGER_NAME='3 Leaps Infosec Team'
export GONIMBUS_TAGGER_EMAIL=infosec@3leaps.net
(
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git tag -s -a --cleanup=verbatim -u "$subkey!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
)
git -C "$fixture" cat-file tag v1.2.3 | grep -q -- '^-----BEGIN PGP SIGNATURE-----$' || {
    echo 'error: fixture tag not signed' >&2
    exit 1
}
expect_fail() { if "$@" > /dev/null 2>&1; then
    echo 'expected signature control failure' >&2
    exit 1
fi; }
verify() { (
    cd "$fixture"
    ./scripts/verify-pinned-tag.sh
); }
retag() { (
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git tag -fs -a --cleanup=verbatim -u "$1" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
) > /dev/null; }
# commit_and_expect_fail commits the current working tree as the tagged
# commit, re-signs the tag there, expects verification to fail, then restores
# the approved commit and tag.
commit_and_expect_fail() {
    git -C "$fixture" add -A
    git -C "$fixture" commit -qm mutated --allow-empty
    retag "$subkey!"
    expect_fail verify
    git -C "$fixture" reset -q --hard "$good"
    retag "$subkey!"
}
verify > /dev/null
GONIMBUS_PGP_KEY_ID="$subkey!" verify > /dev/null
cp "$fixture/keys/expected-fingerprints.txt" "$scratch/anchors.good"
cp "$fixture/docs/security/release-signing-keys.asc" "$scratch/approved.asc"
# Committed anchors that disagree with the committed pin fail.
printf 'gpg %s\nminisign %064d\n' "$(printf '%040d' 0)" 0 > "$fixture/keys/expected-fingerprints.txt"
commit_and_expect_fail
printf 'gpg %s\nminisign %s\n' "$primary" "$(printf 'A%063d' 0)" > "$fixture/keys/expected-fingerprints.txt"
commit_and_expect_fail
git -C "$fixture" rm -q keys/expected-fingerprints.txt
commit_and_expect_fail
# The working tree is never the trust root: a tampered or deleted working-tree
# pin and anchors cannot change the result while the commit is unchanged.
printf 'gpg %s\nminisign %064d\n' "$(printf '%040d' 0)" 0 > "$fixture/keys/expected-fingerprints.txt"
rm "$fixture/docs/security/release-signing-keys.asc"
verify > /dev/null
git -C "$fixture" checkout -q -- keys docs
# Pin and anchors present only as untracked files fail: they were never
# committed in the tagged commit.
git -C "$fixture" rm -q --cached docs/security/release-signing-keys.asc keys/expected-fingerprints.txt
git -C "$fixture" commit -qm untrack
retag "$subkey!"
[[ -f "$fixture/docs/security/release-signing-keys.asc" && -f "$fixture/keys/expected-fingerprints.txt" ]]
expect_fail verify
git -C "$fixture" reset -q --hard "$good"
retag "$subkey!"
verify > /dev/null
git -C "$fixture" update-ref refs/remotes/origin/main "$(git -C "$fixture" rev-parse HEAD)"
# shellcheck source=scripts/release-tag-common.sh
source "$root/scripts/release-tag-common.sh"
(
    cd "$fixture"
    tag_verify_object refs/tags/v1.2.3 "$scratch/message.txt"
)
(
    cd "$fixture"
    printf 'Tampered release\n' > "$scratch/tampered.txt"
    expect_fail tag_verify_object refs/tags/v1.2.3 "$scratch/tampered.txt"
)
printf '# Heading\nFixture release\n' > "$scratch/message.txt"
(
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git tag -fs -a --cleanup=verbatim -u "$subkey!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
    tag_verify_object refs/tags/v1.2.3 "$scratch/message.txt"
)
verify > /dev/null
printf 'Fixture release\n' > "$scratch/message.txt"
(
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git tag -fs -a --cleanup=verbatim -u "$subkey!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
)
git -C "$fixture" rm -q docs/security/release-signing-keys.asc
commit_and_expect_fail
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
    'Fixture Extra <extra@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
extra="$(gpg --batch --with-colons --fingerprint --list-keys 'Fixture Extra' | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --armor --export "$extra" >> "$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
gpg --batch --armor --export "$extra" > "$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
# The pin defines exactly one permitted signing subkey. A non-signing subkey
# does not count; a second signing subkey in the committed pin is rejected
# even though CI sets no operator selector.
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" cv25519 encr 1d > /dev/null 2>&1
gpg --batch --armor --export "$primary" > "$fixture/docs/security/release-signing-keys.asc"
git -C "$fixture" add -A
git -C "$fixture" commit -qm 'pin with encryption subkey'
retag "$subkey!"
verify > /dev/null
git -C "$fixture" reset -q --hard "$good"
retag "$subkey!"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
second="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$primary" |
    awk -F: -v first="$subkey" '$1=="sub" {s=1; cap=$12; next} s && $1=="fpr" {if (cap ~ /s/ && $10 != first) print $10; s=0}' | tail -1)"
[[ "$second" =~ ^[0-9A-F]{40}$ ]]
gpg --batch --armor --export "$primary" > "$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
# A tag signed by a signing subkey absent from the committed pin is rejected.
retag "$second!"
expect_fail verify
retag "$subkey!"
verify > /dev/null
(
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git tag -fs -a --cleanup=verbatim -u "$primary!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
)
expect_fail verify
(
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git tag -fs -a --cleanup=verbatim -u "$subkey!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
)
verify > /dev/null
real_gpg="$(command -v gpg)"
mkdir -p "$scratch/bin"
cat > "$scratch/bin/gpg" << 'SH'
#!/usr/bin/env bash
if [[ -n "${FIXTURE_FUTURE_TIME:-}" ]]; then
	exec "$FIXTURE_REAL_GPG" --faked-system-time "$FIXTURE_FUTURE_TIME" "$@"
fi
exec "$FIXTURE_REAL_GPG" "$@"
SH
chmod +x "$scratch/bin/gpg"
export FIXTURE_REAL_GPG="$real_gpg" PATH="$scratch/bin:$PATH"
export FIXTURE_FUTURE_TIME="$(($(date +%s) + 172800))"
mkdir -m 700 "$scratch/expired-ring"
GNUPGHOME="$scratch/expired-ring" gpg --batch --quiet --import "$fixture/docs/security/release-signing-keys.asc"
(
    cd "$fixture"
    GNUPGHOME="$scratch/expired-ring" git verify-tag --raw v1.2.3 > "$scratch/expired-status" 2>&1
) || true
grep -q '^\[GNUPG:\] VALIDSIG ' "$scratch/expired-status"
grep -q '^\[GNUPG:\] EXPKEYSIG ' "$scratch/expired-status"
expect_fail verify
unset FIXTURE_FUTURE_TIME
verify > /dev/null
git -C "$fixture" tag -d "$GONIMBUS_RELEASE_TAG" > /dev/null
git -C "$fixture" tag "$GONIMBUS_RELEASE_TAG"
expect_fail verify
echo '[ok] isolated pinned-tag signature controls passed'
