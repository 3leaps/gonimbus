#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# shellcheck disable=SC2016
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(cd "$(mktemp -d)" && pwd -P)"
cleanup() {
    gpgconf --homedir "$scratch/home" --kill all > /dev/null 2>&1 || true
    rm -rf "$scratch"
}
trap cleanup EXIT

version="$(tr -d '[:space:]' < "$root/VERSION")"
tag="v$version"
primary="$(awk '$1=="gpg" {print $2}' "$root/keys/expected-fingerprints.txt")"
subkey="$(tr -d '[:space:]' < "$root/config/release/signing-subkey.txt")"
mkdir -m 700 "$scratch/gpg-home"

GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    GONIMBUS_GPG_HOMEDIR="$scratch/gpg-home" \
    "$root/scripts/release-post-tag-selector.sh" > /dev/null

# Missing bang (!) on subkey selector refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="$subkey" \
    GONIMBUS_GPG_HOMEDIR="$scratch/gpg-home" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted subkey selector without trailing !" >&2
    exit 1
fi

# Primary key passed as GONIMBUS_PGP_KEY_ID refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${primary}!" \
    GONIMBUS_GPG_HOMEDIR="$scratch/gpg-home" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted primary key instead of signing subkey" >&2
    exit 1
fi

# Wrong primary fingerprint refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$(printf '%040d' 0)" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    GONIMBUS_GPG_HOMEDIR="$scratch/gpg-home" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted wrong primary fingerprint" >&2
    exit 1
fi

# Version mismatch refuses
if GONIMBUS_RELEASE_TAG="v99.99.99" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    GONIMBUS_GPG_HOMEDIR="$scratch/gpg-home" \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted mismatched tag version" >&2
    exit 1
fi

# Missing GONIMBUS_GPG_HOMEDIR refuses
if GONIMBUS_RELEASE_TAG="$tag" \
    GONIMBUS_GPG_SIGNING_FINGERPRINT="$primary" \
    GONIMBUS_PGP_KEY_ID="${subkey}!" \
    env -u GONIMBUS_GPG_HOMEDIR \
    "$root/scripts/release-post-tag-selector.sh" 2> /dev/null; then
    echo "error: accepted missing GONIMBUS_GPG_HOMEDIR" >&2
    exit 1
fi

# Exercise staged public pin verification + worktree / containment guards in an isolated fixture repo.
fixture="$scratch/repo"
mkdir -p "$fixture/scripts" "$fixture/docs/security" "$fixture/keys" "$fixture/config/release" "$fixture/schemas" "$scratch/home" "$scratch/assets"
chmod 700 "$scratch/home"
cp "$root/scripts/"{release-post-tag-selector.sh,release-tag-common.sh,release-decernor.sh,release-gh.sh,release-insert-anchors.sh,install-release-anchors.sh} "$fixture/scripts/"
cp "$root/config/release/tagger-identity.txt" "$fixture/config/release/"
cp "$root/schemas/fingerprint-record.v0.schema.json" "$fixture/schemas/"
decernor="${GONIMBUS_DECERNOR_BIN:-$(command -v decernor)}"
GONIMBUS_DECERNOR_BIN="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$decernor")"
export GONIMBUS_DECERNOR_BIN

gpg --homedir "$scratch/home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-gen-key 'Synthetic historical <historical@example.invalid>' ed25519 cert 1d > /dev/null 2>&1
hist_primary="$(gpg --homedir "$scratch/home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --homedir "$scratch/home" --batch --pinentry-mode loopback --passphrase '' \
    --quick-add-key "$hist_primary" ed25519 sign 1d > /dev/null 2>&1
hist_subkey="$(gpg --homedir "$scratch/home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --homedir "$scratch/home" --batch --armor --export-options export-minimal --export "$hist_subkey!" > "$fixture/docs/security/release-signing-keys.asc"
python3 - "$scratch/public.pub" << 'PY'
import base64, pathlib, sys
pathlib.Path(sys.argv[1]).write_text('untrusted comment: synthetic public key\n' + base64.b64encode(b'Ed' + b'\x01' * 40).decode() + '\n')
PY
cp "$scratch/public.pub" "$fixture/docs/security/gonimbus-minisign.pub"
printf '1.2.3\n' > "$fixture/VERSION"
printf '%s\n' "$hist_subkey" > "$fixture/config/release/signing-subkey.txt"

git init -q -b main "$fixture"
git -C "$fixture" config user.name fixture
git -C "$fixture" config user.email fixture@example.invalid
git -C "$fixture" add -A
git -C "$fixture" commit -qm pins
GONIMBUS_MINISIGN_PUB="$scratch/public.pub" "$fixture/scripts/release-insert-anchors.sh" > /dev/null
git -C "$fixture" add -A
git -C "$fixture" commit -qm historical
git -C "$fixture" tag v1.2.3

cp "$fixture/keys/expected-fingerprints."{txt,ndjson} "$scratch/assets/"
cp "$fixture/docs/security/release-signing-keys.asc" "$scratch/assets/gonimbus-release-signing-key.asc"
cp "$scratch/public.pub" "$scratch/assets/gonimbus-minisign.pub"

# Advance committed repo state so staged directory check proves independence from current tree.
printf 'gpg %040d\nminisign %064d\n' 0 0 > "$fixture/keys/expected-fingerprints.txt"
printf '9.0.0\n' > "$fixture/VERSION"
git -C "$fixture" add -A
git -C "$fixture" commit -qm 'newer operator state'

export GONIMBUS_RELEASE_TAG=v1.2.3
export GONIMBUS_GPG_SIGNING_FINGERPRINT="$hist_primary"
export GONIMBUS_PGP_KEY_ID="${hist_subkey}!"
export GONIMBUS_GPG_HOMEDIR="$scratch/home"

"$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets" > /dev/null

fail() {
    if "$@" > "$scratch/output" 2>&1; then
        echo "error: invalid selector or homedir accepted: $*" >&2
        exit 1
    fi
}

fail env GONIMBUS_PGP_KEY_ID=0000000000000000000000000000000000000000! "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fail env GONIMBUS_GPG_SIGNING_FINGERPRINT=0000000000000000000000000000000000000000 "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"

mkdir -m 700 "$fixture/in-repo-home"
fail env GONIMBUS_GPG_HOMEDIR="$fixture/in-repo-home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"

fixture_upper_alias="$(dirname "$fixture")/$(basename "$fixture" | tr '[:lower:]' '[:upper:]')/in-repo-home"
if [[ -d "$fixture_upper_alias" ]]; then
    fail env GONIMBUS_GPG_HOMEDIR="$fixture_upper_alias" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fi

git -C "$fixture" worktree add --detach "$scratch/linked-wt" HEAD > /dev/null 2>&1
mkdir -m 700 "$scratch/linked-wt/in-linked-wt-home"
fail env GONIMBUS_GPG_HOMEDIR="$scratch/linked-wt/in-linked-wt-home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
git -C "$fixture" worktree remove --force "$scratch/linked-wt" > /dev/null 2>&1

git -C "$fixture" worktree add --detach "$scratch/linked-wt " HEAD > /dev/null 2>&1
mkdir -m 700 "$scratch/linked-wt /in-linked-wt-home"
fail env GONIMBUS_GPG_HOMEDIR="$scratch/linked-wt /in-linked-wt-home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
git -C "$fixture" worktree remove --force "$scratch/linked-wt " > /dev/null 2>&1

if [[ "$OSTYPE" != msys* && "$OSTYPE" != cygwin* ]]; then
    nl_wt="$scratch/linked"$'\n'"wt"
    git -C "$fixture" worktree add --detach "$nl_wt" HEAD > /dev/null 2>&1
    mkdir -m 700 "$nl_wt/in-linked-wt-home"
    fail env GONIMBUS_GPG_HOMEDIR="$nl_wt/in-linked-wt-home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    git -C "$fixture" worktree remove --force "$nl_wt" > /dev/null 2>&1

    cr_wt="$scratch/linked"$'\r'"wt"
    git -C "$fixture" worktree add --detach "$cr_wt" HEAD > /dev/null 2>&1
    mkdir -m 700 "$cr_wt/in-linked-wt-home"
    fail env GONIMBUS_GPG_HOMEDIR="$cr_wt/in-linked-wt-home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    git -C "$fixture" worktree remove --force "$cr_wt" > /dev/null 2>&1

    trailing_nl_wt="$scratch/linked-trailing"$'\n'
    git -C "$fixture" worktree add --detach "$trailing_nl_wt" HEAD > /dev/null 2>&1
    chmod 700 "$trailing_nl_wt"
    fail env GONIMBUS_GPG_HOMEDIR="$trailing_nl_wt" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    git -C "$fixture" worktree remove --force "$trailing_nl_wt" > /dev/null 2>&1

    ln -s "$scratch/home" "$scratch/home-link\\"
    ln -s "$scratch/home" "$scratch/home-link\\."
    fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link\\/" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link\\/." "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link\\./" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link\\./." "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fi

real_git="$(command -v git)"
mkdir -p "$scratch/bad-git-bin"
cat > "$scratch/bad-git-bin/git" << SH
#!/usr/bin/env bash
for arg in "\$@"; do
    if [[ "\$arg" == "worktree" ]]; then
        exit 1
    fi
done
exec "$real_git" "\$@"
SH
chmod +x "$scratch/bad-git-bin/git"
fail env PATH="$scratch/bad-git-bin:$PATH" GONIMBUS_GPG_HOMEDIR="$scratch/home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"

mkdir -p "$scratch/fake-home/.gnupg" "$scratch/fake-home-upper/.GNUPG"
chmod 700 "$scratch/fake-home/.gnupg" "$scratch/fake-home-upper/.GNUPG"
ln -s "$scratch/home" "$scratch/home-link"
fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link/" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fail env GONIMBUS_GPG_HOMEDIR="$scratch/home-link/." "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fail env HOME="$scratch/fake-home" GONIMBUS_GPG_HOMEDIR="$scratch/fake-home/.gnupg" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fail env USERPROFILE="$scratch/fake-home" GONIMBUS_GPG_HOMEDIR="$scratch/fake-home/.gnupg" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
fail env HOME="$scratch/fake-home-upper" GONIMBUS_GPG_HOMEDIR="$scratch/fake-home-upper/.GNUPG" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"

real_python3="$(command -v python3)"
mkdir -p "$scratch/no-pwd-py-bin"
cat > "$scratch/no-pwd-py-bin/python3" << SH
#!/usr/bin/env bash
if [[ "\${1:-}" == "-I" ]]; then shift; fi
if [[ "\${1:-}" == "-" ]]; then shift; fi
exec "$real_python3" -I -c 'import sys; sys.modules["pwd"] = None; exec(sys.stdin.read())' "\$@"
SH
chmod +x "$scratch/no-pwd-py-bin/python3"
env PATH="$scratch/no-pwd-py-bin:$PATH" USERPROFILE="$scratch/fake-home" GONIMBUS_GPG_HOMEDIR="$scratch/home" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets" > /dev/null
fail env PATH="$scratch/no-pwd-py-bin:$PATH" HOME="" USERPROFILE="$scratch/fake-home" GONIMBUS_GPG_HOMEDIR="$scratch/fake-home/.gnupg" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"

if [[ "$OSTYPE" != msys* && "$OSTYPE" != cygwin* ]]; then
    real_gpgconf="$(command -v gpgconf)"
    win_drive_gpg="C:\\Users\\alice\\AppData\\Roaming\\gnupg"
    win_unc_gpg="\\\\server\\share\\gnupg"
    mkdir -p "$scratch/$win_drive_gpg" "$scratch/$win_unc_gpg" "$scratch/win-gpgconf-bin"
    chmod 700 "$scratch/$win_drive_gpg" "$scratch/$win_unc_gpg"
    cat > "$scratch/win-gpgconf-bin/gpgconf" << SH
#!/usr/bin/env bash
if [[ "\${1:-}" == "--list-dirs" && "\${2:-}" == "homedir" ]]; then
    printf '%s\r\n' "\$MOCK_GPGCONF_HOMEDIR"
    exit 0
fi
exec "$real_gpgconf" "\$@"
SH
    chmod +x "$scratch/win-gpgconf-bin/gpgconf"
    fail env PATH="$scratch/win-gpgconf-bin:$PATH" MOCK_GPGCONF_HOMEDIR="$win_drive_gpg" GONIMBUS_GPG_HOMEDIR="$win_drive_gpg" bash -c 'cd "$1" && "$2" "$3"' _ "$scratch" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    grep -q 'default homedir' "$scratch/output" || { cat "$scratch/output" >&2; exit 1; }
    fail env PATH="$scratch/win-gpgconf-bin:$PATH" MOCK_GPGCONF_HOMEDIR="$win_unc_gpg" GONIMBUS_GPG_HOMEDIR="$win_unc_gpg" bash -c 'cd "$1" && "$2" "$3"' _ "$scratch" "$fixture/scripts/release-post-tag-selector.sh" "$scratch/assets"
    grep -q 'default homedir' "$scratch/output" || { cat "$scratch/output" >&2; exit 1; }
fi

echo "[ok] post-tag selector controls passed"
