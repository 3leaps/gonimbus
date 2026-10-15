#!/usr/bin/env bash
# Synthetic controls for the published-tag gate; all keys and remotes are temporary.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
cleanup() {
    local home
    for home in "$scratch/gpg" "$scratch/attacker"; do
        [[ -d "$home" ]] && gpgconf --homedir "$home" --kill all > /dev/null 2>&1 || true
    done
    rm -rf "$scratch"
}
trap cleanup EXIT
export GNUPGHOME="$scratch/gpg"
mkdir -m 700 "$GNUPGHOME"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
    '3 Leaps Infosec Team <infosec@3leaps.net>' ed25519 cert 0 > /dev/null 2>&1
primary="$(gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$primary" | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"

git init -q --bare -b main "$scratch/remote.git"
src="$scratch/source"
git init -q -b main "$src"
git -C "$src" config user.name '3 Leaps Infosec Team'
git -C "$src" config user.email infosec@3leaps.net
mkdir -p "$src/scripts" "$src/docs/security" "$src/keys" "$src/config/release"
cp "$root/scripts/"{verify-pinned-tag.sh,validate-release-anchors.sh,release-verify-published-tag.sh} "$src/scripts/"
cp "$root/config/release/tagger-identity.txt" "$src/config/release/"
gpg --batch --armor --export "$primary" > "$src/docs/security/release-signing-keys.asc"
printf 'gpg %s\nminisign %064d\n' "$primary" 0 > "$src/keys/expected-fingerprints.txt"
git -C "$src" add -A
git -C "$src" commit -qm fixture
commit="$(git -C "$src" rev-parse HEAD)"
sign_tag() {
    printf '%s\n' "$1" > "$scratch/msg"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git -C "$src" tag -fs -a --cleanup=verbatim -u "$subkey!" -F "$scratch/msg" v1.2.3 > /dev/null
}
sign_tag 'Fixture release'
git -C "$src" remote add origin "$scratch/remote.git"
git -C "$src" push -q origin main refs/tags/v1.2.3

# The operator checkout is on a later main: the gate must not depend on it.
op="$scratch/operator"
git clone -q "$scratch/remote.git" "$op"
git -C "$op" config user.name 'Fixture Operator'
git -C "$op" config user.email operator@example.invalid
printf 'later\n' > "$op/later"
git -C "$op" add later
git -C "$op" commit -qm 'later main'
git -C "$op" remote set-url origin https://github.com/3leaps/gonimbus.git
git -C "$op" config url."$scratch/remote.git".insteadOf https://github.com/3leaps/gonimbus.git

# Stub gh: answer the git-data API from the fixture remote.
mkdir -p "$scratch/bin"
ln -s "$root/scripts/testdata/fake-gh-api.sh" "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_REMOTE="$scratch/remote.git" GONIMBUS_RELEASE_TAG=v1.2.3
export GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary"

# The operator's trusted checkout carries the current verifier and helpers.
cp "$root/scripts/"{verify-pinned-tag.sh,validate-release-anchors.sh,release-verify-published-tag.sh} "$op/scripts/"
gate() { (cd "$op" && ./scripts/release-verify-published-tag.sh); }
expect_fail() {
    if gate > "$scratch/out" 2>&1; then
        echo "error: expected rejection: $1" >&2
        exit 1
    fi
    grep -q "$1" "$scratch/out" || {
        echo "error: missing named rejection: $1" >&2
        cat "$scratch/out" >&2
        exit 1
    }
}
gate > /dev/null
# The temporary verification worktree is always removed.
[[ "$(git -C "$op" worktree list --porcelain | grep -c '^worktree ')" == 1 ]] || {
    echo 'error: verification worktree left behind' >&2
    exit 1
}

FIXTURE_GH_VERIFIED=false expect_fail 'GitHub does not report'

# Same commit, different (unsigned) annotated object: must fail.
GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
    git -C "$src" tag -fa -m 'Replacement' v1.2.3 "$commit"
git -C "$src" push -q --force origin refs/tags/v1.2.3
expect_fail 'does not verify against the pin'

# Lightweight replacement: must fail. The ref API reports a commit, not a tag
# object, so the lookup refuses before any local ref changes.
git -C "$scratch/remote.git" update-ref refs/tags/v1.2.3 "$commit"
expect_fail 'annotated tag required'

# Restore a valid signed tag: passes again.
sign_tag 'Fixture release'
git -C "$src" push -q --force origin refs/tags/v1.2.3
gate > /dev/null

# The approved primary must come from the maintainer environment.
(
    unset GONIMBUS_GPG_SIGNING_FINGERPRINT
    expect_fail 'approved primary'
)
GONIMBUS_GPG_SIGNING_FINGERPRINT="$(printf '%040d' 0)" expect_fail 'not the approved primary'

# An attacker commit carrying its own key and a matching fingerprint file is
# internally consistent but not approved, so it is rejected.
export GNUPGHOME="$scratch/attacker"
mkdir -m 700 "$GNUPGHOME"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
    '3 Leaps Infosec Team <infosec@3leaps.net>' ed25519 cert 0 > /dev/null 2>&1
attacker="$(gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$attacker" ed25519 sign 1d > /dev/null 2>&1
attacker_sub="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$attacker" | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --batch --armor --export "$attacker" > "$src/docs/security/release-signing-keys.asc"
printf 'gpg %s\nminisign %064d\n' "$attacker" 0 > "$src/keys/expected-fingerprints.txt"
git -C "$src" add -A
git -C "$src" commit -qm 'attacker pin'
printf 'Fixture release\n' > "$scratch/msg"
GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
    git -C "$src" tag -fs -a --cleanup=verbatim -u "$attacker_sub!" -F "$scratch/msg" v1.2.3 > /dev/null
git -C "$src" push -q origin main
git -C "$src" push -q --force origin refs/tags/v1.2.3
expect_fail 'not the approved primary'
gpgconf --homedir "$GNUPGHOME" --kill all > /dev/null 2>&1 || true
export GNUPGHOME="$scratch/gpg"
git -C "$src" reset -q --hard "$commit"
sign_tag 'Fixture release'
git -C "$src" push -q --force origin main refs/tags/v1.2.3
gate > /dev/null

# An unverified target carrying a marker-writing fake verifier and helper must
# never be executed: the gate runs only the operator's trusted scripts.
marker="$scratch/target-code-executed"
for script in verify-pinned-tag.sh validate-release-anchors.sh release-verify-published-tag.sh; do
    printf '#!/usr/bin/env bash\ntouch %q\nexit 0\n' "$marker" > "$src/scripts/$script"
    chmod +x "$src/scripts/$script"
done
git -C "$src" add -A
git -C "$src" commit -qm 'unverified target with fake verifier'
GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
    git -C "$src" tag -fa -m 'Unsigned replacement' v1.2.3
git -C "$src" push -q origin main
git -C "$src" push -q --force origin refs/tags/v1.2.3
expect_fail 'does not verify against the pin'
[[ ! -e "$marker" ]] || {
    echo 'error: code from the unverified tag target was executed' >&2
    exit 1
}
# Even a correctly signed target cannot run its own scripts in the gate.
sign_tag 'Fixture release'
git -C "$src" push -q --force origin refs/tags/v1.2.3
gate > /dev/null
[[ ! -e "$marker" ]] || {
    echo 'error: code from the tag target was executed' >&2
    exit 1
}

# Anchor binding with REAL signed replacements from the approved key. Packages
# are staged against the anchor below; a later replacement must not pass.
anchor_file="$scratch/anchor"
(cd "$op" && GONIMBUS_ANCHOR_OUT="$anchor_file" ./scripts/release-verify-published-tag.sh) > /dev/null
anchor_object="$(awk -F= '$1=="object" {print $2}' "$anchor_file")"
anchor_commit="$(awk -F= '$1=="commit" {print $2}' "$anchor_file")"
[[ "$anchor_commit" == "$(git -C "$src" rev-parse HEAD)" ]]
anchored() {
    (cd "$op" && GONIMBUS_EXPECTED_TAG_OBJECT="$anchor_object" GONIMBUS_EXPECTED_COMMIT="$anchor_commit" \
        ./scripts/release-verify-published-tag.sh)
}
anchored > /dev/null
# Same version, approved key, DIFFERENT target commit: fails before any ref change.
printf 'other build\n' >> "$src/file"
git -C "$src" add file
git -C "$src" commit -qm 'different target, same version'
sign_tag 'Fixture release'
git -C "$src" push -q origin main
git -C "$src" push -q --force origin refs/tags/v1.2.3
before="$(git -C "$op" rev-parse refs/tags/v1.2.3)"
if anchored > "$scratch/out" 2>&1; then
    echo 'error: approved-key replacement on another commit passed the anchor' >&2
    exit 1
fi
grep -q 'differs from the verified anchor' "$scratch/out"
[[ "$(git -C "$op" rev-parse refs/tags/v1.2.3)" == "$before" ]] || {
    echo 'error: local tag ref changed before the anchor check' >&2
    exit 1
}
gate > /dev/null
# Same commit, approved key, DIFFERENT tag object (new message): fails.
git -C "$src" reset -q --hard "$anchor_commit"
sign_tag 'Fixture release, re-signed'
git -C "$src" push -q --force origin main refs/tags/v1.2.3
[[ "$(git -C "$scratch/remote.git" rev-parse 'refs/tags/v1.2.3^{}')" == "$anchor_commit" ]]
[[ "$(git -C "$scratch/remote.git" rev-parse refs/tags/v1.2.3)" != "$anchor_object" ]]
if anchored > "$scratch/out" 2>&1; then
    echo 'error: same-commit replacement object passed the anchor' >&2
    exit 1
fi
grep -q 'differs from the verified anchor' "$scratch/out"
# A partial anchor is refused.
(cd "$op" && GONIMBUS_EXPECTED_TAG_OBJECT="$anchor_object" ./scripts/release-verify-published-tag.sh) > "$scratch/out" 2>&1 &&
    {
        echo 'error: partial anchor accepted' >&2
        exit 1
    }
grep -q 'must both be 40-hex' "$scratch/out"
# Restore the original signed object: the anchored check passes again.
git -C "$scratch/remote.git" update-ref refs/tags/v1.2.3 "$anchor_object"
anchored > /dev/null

# Absent remote tag: must fail.
git -C "$scratch/remote.git" update-ref -d refs/tags/v1.2.3
expect_fail 'remote release tag is absent'
echo '[ok] published-tag gate controls passed'
