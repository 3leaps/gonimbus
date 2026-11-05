#!/usr/bin/env bash
# Synthetic, short-lived signing fixture; all generated material stays outside git.
# Trust material is committed: verification reads it from the tagged commit.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
bin="${GONIMBUS_DECERNOR_BIN:-$(command -v decernor || true)}"
[[ -n "$bin" && -x "$bin" && "$bin" == /* && ! -L "$bin" ]] || {
    echo 'error: GONIMBUS_DECERNOR_BIN required (CI installs Decernor from config/release/decernor-ci-sha256.txt)' >&2
    exit 1
}
export GONIMBUS_DECERNOR_BIN="$bin"
# Decernor fingerprint verify must precede the listing check. Reversing them
# in the script must fail this test even when both would succeed on a fixture.
if ! python3 - "$root/scripts/verify-pinned-tag.sh" << 'PY'; then
from pathlib import Path
import sys
text = Path(sys.argv[1]).read_text()

def decernor_first(s):
    dec = s.find("fingerprint verify")
    listing = s.find("validate-release-anchors.sh")
    return dec >= 0 and listing >= 0 and dec < listing

if not decernor_first(text):
    raise SystemExit("fingerprint verify must precede validate-release-anchors.sh")
i, j = text.find("fingerprint verify"), text.find("validate-release-anchors.sh")
a, b = "fingerprint verify", "validate-release-anchors.sh"
rev = text[:i] + b + text[i + len(a) : j] + a + text[j + len(b) :]
if decernor_first(rev):
    raise SystemExit("order checker does not fail when Decernor/listing are reversed")
PY
    echo 'error: verify-pinned-tag.sh must call Decernor before the listing check' >&2
    exit 1
fi
# Exercise the actual status predicate under mawk when installed, otherwise
# the system awk. This catches interval-regex regressions without a GPG mock.
python3 - "$root/scripts/verify-pinned-tag.sh" << 'PYAWK'
from pathlib import Path
import re, shutil, subprocess, sys
text = Path(sys.argv[1]).read_text()
program = re.search(r"awk -v primary=.*? '\n(.*?)\n' \"\$keyring/status\"", text, re.S).group(1)
primary, subkey = "A" * 40, "B" * 40
status = f"[GNUPG:] GOODSIG fixture\n[GNUPG:] VALIDSIG {subkey} {primary}\n"
engines = dict.fromkeys(filter(None, (shutil.which("awk"), shutil.which("mawk"))))
cases = [("", True), (subkey + "!", True), (subkey, False),
         (subkey[:-1] + "!", False), (subkey + "B!", False),
         (subkey.lower() + "!", False), ("G" * 40 + "!", False),
         (primary + "!", False), ("C" * 40 + "!", False)]
for engine in engines:
    def accepts(selector, data=status):
        return subprocess.run([engine, "-v", "primary=" + primary, "-v",
            "permitted=" + subkey, "-v", "selector=" + selector, program],
            input=data, text=True).returncode == 0
    for selector, expected in cases:
        if accepts(selector) != expected:
            raise SystemExit(f"{engine}: unexpected selector result: {selector!r}")
    for bad in (status + "[GNUPG:] REVKEYSIG fixture\n", status + status,
                status.replace(subkey, primary), status.replace(subkey, "C" * 40)):
        if accepts(subkey + "!", bad):
            raise SystemExit(f"{engine}: signer/status restriction failed")
    print(f"[ok] {engine}: selector length, alphabet, exact subkey and signature controls")
PYAWK
scratch="$(mktemp -d)"
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
# Force OpenPGP even when the host git config uses SSH signing.
git -C "$fixture" config gpg.format openpgp
git -C "$fixture" config gpg.program gpg
git -C "$fixture" config commit.gpgsign false
mkdir -p "$fixture/docs/security" "$fixture/keys" "$fixture/scripts" "$fixture/config/release"
cp "$root/scripts/verify-pinned-tag.sh" "$root/scripts/validate-release-anchors.sh" "$root/scripts/release-decernor.sh" "$fixture/scripts/"
cp "$root/config/release/tagger-identity.txt" "$fixture/config/release/"
gpg --batch --armor --export "$primary" > "$fixture/docs/security/release-signing-keys.asc"
python3 - "$fixture/docs/security/gonimbus-minisign.pub" << 'PY'
import base64
import pathlib
import sys
blob = b'Ed' + b'\x01' * 8 + b'\x01' * 32
pathlib.Path(sys.argv[1]).write_text(
    'untrusted comment: synthetic public key\n' + base64.b64encode(blob).decode() + '\n'
)
PY
"$bin" fingerprint "$fixture/docs/security/release-signing-keys.asc" --class public --kind gpg \
    --format ndjson --path-mode none --gpg-role primary > "$scratch/gpg.ndjson"
"$bin" fingerprint "$fixture/docs/security/gonimbus-minisign.pub" --class public --kind minisign \
    --format ndjson --path-mode none > "$scratch/minisign.ndjson"
python3 - "$scratch" "$fixture/keys" << 'PY'
import json
import pathlib
import sys
base = pathlib.Path(sys.argv[1])
keys = pathlib.Path(sys.argv[2])
g = [json.loads(line) for line in (base / 'gpg.ndjson').read_text().splitlines() if line]
m = [json.loads(line) for line in (base / 'minisign.ndjson').read_text().splitlines() if line and json.loads(line).get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
if len(g) != 1 or len(m) != 1:
    raise SystemExit('error: expected one GPG primary and one minisign fingerprint')
(keys / 'expected-fingerprints.ndjson').write_text(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in (g[0], m[0])))
(keys / 'expected-fingerprints.txt').write_text('gpg ' + g[0]['fingerprint'] + '\nminisign ' + m[0]['fingerprint'] + '\n')
PY
printf 'fixture\n' > "$fixture/file"
printf 'Fixture release\n' > "$scratch/message.txt"
git -C "$fixture" config user.name '3 Leaps Infosec Team'
git -C "$fixture" config user.email infosec@3leaps.net
git -C "$fixture" add file docs/security keys config scripts
git -C "$fixture" commit -qm fixture
good="$(git -C "$fixture" rev-parse HEAD)"
export GONIMBUS_RELEASE_TAG=v1.2.3
export GONIMBUS_TAGGER_NAME='3 Leaps Infosec Team'
export GONIMBUS_TAGGER_EMAIL=infosec@3leaps.net
git_sign_tag() {
    local selector="$1"
    (
        cd "$fixture"
        GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
            git -c gpg.format=openpgp -c gpg.program=gpg -c gpg.openpgp.program=gpg tag -fs -a --cleanup=verbatim \
            -u "$selector" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
    ) > /dev/null
}
(
    cd "$fixture"
    GIT_COMMITTER_NAME='3 Leaps Infosec Team' GIT_COMMITTER_EMAIL=infosec@3leaps.net \
        git -c gpg.format=openpgp -c gpg.program=gpg -c gpg.openpgp.program=gpg tag -s -a --cleanup=verbatim \
        -u "$subkey!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
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
retag() { git_sign_tag "$1"; }
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
        git -c gpg.format=openpgp -c gpg.program=gpg -c gpg.openpgp.program=gpg tag -fs -a --cleanup=verbatim \
        -u "$subkey!" -F "$scratch/message.txt" "$GONIMBUS_RELEASE_TAG"
    tag_verify_object refs/tags/v1.2.3 "$scratch/message.txt"
)
verify > /dev/null
printf 'Fixture release\n' > "$scratch/message.txt"
git_sign_tag "$subkey!"
git -C "$fixture" rm -q docs/security/release-signing-keys.asc
commit_and_expect_fail
gpg --batch --pinentry-mode loopback --passphrase '' --quick-generate-key \
    'Fixture Extra <extra@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
extra="$(gpg --batch --with-colons --fingerprint --list-keys 'Fixture Extra' | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --batch --armor --export "$extra" >> "$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
gpg --batch --armor --export "$extra" > "$fixture/docs/security/release-signing-keys.asc"
commit_and_expect_fail
# Objects are read as stored (GIT_NO_REPLACE_OBJECTS=1): a replacement ref on a
# committed pin blob does not break verification of a valid tag, and replacement
# refs on the pin + anchor blobs (or the commit) cannot authorize an attacker key.
pin_blob="$(git -C "$fixture" --no-replace-objects rev-parse "$good:docs/security/release-signing-keys.asc")"
txt_blob="$(git -C "$fixture" --no-replace-objects rev-parse "$good:keys/expected-fingerprints.txt")"
ndjson_blob="$(git -C "$fixture" --no-replace-objects rev-parse "$good:keys/expected-fingerprints.ndjson")"
git -C "$fixture" replace "$pin_blob" "$(printf 'not a key\n' | git -C "$fixture" hash-object -w --stdin)"
verify > /dev/null
git -C "$fixture" replace -d "$pin_blob" > /dev/null
gpg --batch --pinentry-mode loopback --passphrase '' --quick-add-key "$extra" ed25519 sign 1d > /dev/null 2>&1
extra_sub="$(gpg --batch --with-colons --with-subkey-fingerprint --list-keys "$extra" | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --batch --armor --export "$extra" > "$scratch/extra.asc"
"$bin" fingerprint "$scratch/extra.asc" --class public --kind gpg \
    --format ndjson --path-mode none --gpg-role primary > "$scratch/extra-gpg.ndjson"
python3 - "$scratch" << 'PY'
import json
import pathlib
import sys
base = pathlib.Path(sys.argv[1])
g = [json.loads(line) for line in (base / 'extra-gpg.ndjson').read_text().splitlines() if line]
m = [json.loads(line) for line in (base / 'minisign.ndjson').read_text().splitlines() if line and json.loads(line).get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
(base / 'extra.ndjson').write_text(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in (g[0], m[0])))
(base / 'extra.txt').write_text('gpg ' + g[0]['fingerprint'] + '\nminisign ' + m[0]['fingerprint'] + '\n')
PY
retag "$extra_sub!"
expect_fail verify
git -C "$fixture" replace "$pin_blob" "$(git -C "$fixture" hash-object -w "$scratch/extra.asc")"
git -C "$fixture" replace "$txt_blob" "$(git -C "$fixture" hash-object -w "$scratch/extra.txt")"
git -C "$fixture" replace "$ndjson_blob" "$(git -C "$fixture" hash-object -w "$scratch/extra.ndjson")"
expect_fail verify
# Prove the test fails if GIT_NO_REPLACE_OBJECTS=1 is removed.
sed 's/export GIT_NO_REPLACE_OBJECTS=1/:/g' "$fixture/scripts/verify-pinned-tag.sh" > "$fixture/scripts/unprotected-verify.sh"
sed 's/export GIT_NO_REPLACE_OBJECTS=1/:/g' "$fixture/scripts/release-decernor.sh" > "$fixture/scripts/release-decernor.sh.tmp"
mv "$fixture/scripts/release-decernor.sh.tmp" "$fixture/scripts/release-decernor.sh"
chmod +x "$fixture/scripts/unprotected-verify.sh"
if ! (cd "$fixture" && ./scripts/unprotected-verify.sh) > /dev/null 2>&1; then
    echo 'error: replace-ref negative control did not exercise GIT_NO_REPLACE_OBJECTS' >&2
    exit 1
fi
rm -f "$fixture/scripts/unprotected-verify.sh"
cp "$root/scripts/release-decernor.sh" "$fixture/scripts/release-decernor.sh"
git -C "$fixture" replace -d "$pin_blob" "$txt_blob" "$ndjson_blob" > /dev/null
retag "$subkey!"
verify > /dev/null
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
git_sign_tag "$primary!"
expect_fail verify
git_sign_tag "$subkey!"
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
    GNUPGHOME="$scratch/expired-ring" git -c gpg.format=openpgp -c gpg.program=gpg -c gpg.openpgp.program=gpg \
        verify-tag --raw v1.2.3 > "$scratch/expired-status" 2>&1
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
