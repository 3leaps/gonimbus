#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Post-tag ceremony selector guard: GONIMBUS_RELEASE_TAG matches VERSION,
# GONIMBUS_GPG_SIGNING_FINGERPRINT matches keys/expected-fingerprints.txt, and
# GONIMBUS_PGP_KEY_ID matches config/release/signing-subkey.txt (<subkey>!).
set -euo pipefail
for _git_var in $(compgen -e); do
    case "$_git_var" in
        GIT_COMMITTER_NAME | GIT_COMMITTER_EMAIL | GIT_AUTHOR_NAME | GIT_AUTHOR_EMAIL) ;;
        GIT_*) unset "$_git_var" ;;
    esac
done
unset _git_var
export GIT_NO_REPLACE_OBJECTS=1
die() {
    echo "error: $*" >&2
    exit 1
}
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"

tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    die 'canonical GONIMBUS_RELEASE_TAG required'

version="$(tr -d '[:space:]' < "$root/VERSION")"
[[ "$tag" == "v$version" ]] ||
    die "GONIMBUS_RELEASE_TAG ($tag) does not match VERSION (v$version)"

expected_primary="$(awk '$1=="gpg" {print $2}' "$root/keys/expected-fingerprints.txt")"
[[ "$expected_primary" =~ ^[0-9A-F]{40}$ ]] ||
    die 'missing gpg primary fingerprint in keys/expected-fingerprints.txt'
[[ "${GONIMBUS_GPG_SIGNING_FINGERPRINT:-}" == "$expected_primary" ]] ||
    die 'GONIMBUS_GPG_SIGNING_FINGERPRINT does not match the committed primary fingerprint'

expected_subkey="$(tr -d '[:space:]' < "$root/config/release/signing-subkey.txt")"
[[ "$expected_subkey" =~ ^[0-9A-F]{40}$ ]] ||
    die 'config/release/signing-subkey.txt is malformed'
[[ "${GONIMBUS_PGP_KEY_ID:-}" == "${expected_subkey}!" ]] ||
    die "GONIMBUS_PGP_KEY_ID must be ${expected_subkey}! (exact signing subkey selector)"

echo "[ok] post-tag selector verified for $tag"
