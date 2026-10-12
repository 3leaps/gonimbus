#!/usr/bin/env bash
# Verify an annotated tag using only the committed public pin, in an isolated keyring.
set -euo pipefail
die() {
    echo "error: $*" >&2
    exit 1
}
# --published verifies a tag whose target need not be the current HEAD. Trust
# data is still read only as inert git objects from the tagged commit; no file
# from that commit is ever executed.
published=0
if [[ "${1:-}" == --published ]]; then
    published=1
elif [[ $# -gt 0 ]]; then
    die 'usage: verify-pinned-tag.sh [--published]'
fi
tag="${GONIMBUS_RELEASE_TAG:-}"
[[ "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || die 'canonical release tag required'
object="$(git rev-parse "refs/tags/$tag" 2> /dev/null)" || die 'tag absent'
[[ "$(git cat-file -t "$object")" == tag ]] || die 'annotated tag required'
target="$(git rev-parse "${object}^{}")"
[[ "$published" == 1 || "$target" == "$(git rev-parse HEAD)" ]] || die 'tag target must be HEAD'
[[ "$(git cat-file tag "$object" | sed -n 's/^tag //p' | head -1)" == "$tag" ]] || die 'tag name mismatch'
keyring="$(mktemp -d)"
trap 'gpgconf --homedir "$keyring/home" --kill all > /dev/null 2>&1 || true; rm -rf "$keyring"' EXIT
chmod 700 "$keyring"
# The trust root is the public pin, anchors and tagger identity committed in
# the signed target commit; working-tree copies are never consulted.
committed() {
    local path="$1" out="$2" mode
    mode="$(git ls-tree "$target" -- "$path" | awk '{print $1}')"
    [[ "$mode" == 100644 || "$mode" == 100755 ]] || die "committed $path required in the tagged commit"
    git cat-file blob "$target:$path" > "$out"
    [[ -s "$out" ]] || die "committed $path is empty"
}
pin="$keyring/release-signing-keys.asc"
anchors="$keyring/expected-fingerprints.txt"
committed docs/security/release-signing-keys.asc "$pin"
committed keys/expected-fingerprints.txt "$anchors"
committed config/release/tagger-identity.txt "$keyring/tagger-identity.txt"
"$(dirname "$0")/validate-release-anchors.sh" "$anchors" "$pin" > /dev/null || die 'committed pin and anchors disagree'
tagger="$(git cat-file tag "$object" | sed -n 's/^tagger \(.*\) [0-9][0-9]* [+-][0-9][0-9][0-9][0-9]$/\1/p' | head -1)"
[[ "$tagger" == "$(cat "$keyring/tagger-identity.txt")" ]] || die 'infosec tagger required'
primary="$(awk '$1=="gpg" {print $2}' "$anchors")"
[[ "$primary" =~ ^[0-9A-F]{40}$ ]] || die 'primary anchor malformed'
mkdir -m 700 "$keyring/home"
GNUPGHOME="$keyring/home" gpg --batch --quiet --import "$pin" || die 'pin import failed'
[[ "$(GNUPGHOME="$keyring/home" gpg --batch --with-colons --list-keys | awk -F: '$1=="pub" {count++} END {print count+0}')" == 1 ]] || die 'pin must hold exactly one primary key'
pinned_primary="$(GNUPGHOME="$keyring/home" gpg --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
[[ "$pinned_primary" == "$primary" ]] || die 'pin does not match primary anchor'
# The committed pin defines the one permitted signing subkey; CI has no
# operator selector, so the verifier enforces it from the pin itself.
signing_subkeys="$(GNUPGHOME="$keyring/home" gpg --batch --with-colons --with-subkey-fingerprint --list-keys |
    awk -F: '$1=="sub" {s=1; cap=$12; next} s && $1=="fpr" {if (cap ~ /s/) print $10; s=0}')"
[[ -n "$signing_subkeys" && "$(printf '%s\n' "$signing_subkeys" | wc -l | tr -d ' ')" == 1 ]] ||
    die 'pin must hold exactly one signing subkey'
permitted="$signing_subkeys"
[[ "$permitted" =~ ^[0-9A-F]{40}$ && "$permitted" != "$primary" ]] || die 'pinned signing subkey malformed'
GNUPGHOME="$keyring/home" git -c gpg.program=gpg verify-tag --raw "$object" > "$keyring/status" 2>&1 || {
    awk '$1=="[GNUPG:]" {print "signature status:", $2}' "$keyring/status" >&2
    die 'tag signature invalid under pin'
}
awk -v primary="$primary" -v permitted="$permitted" -v selector="${GONIMBUS_PGP_KEY_ID:-}" '
  $1=="[GNUPG:]" && $2=="VALIDSIG" {
    valid++; subkey=$3; signer_primary=$NF
  }
  $1=="[GNUPG:]" && $2=="GOODSIG" {good++}
  $1=="[GNUPG:]" && $2 ~ /^(EXPKEYSIG|EXPSIG|REVKEYSIG|KEYREVOKED|BADSIG|ERRSIG)$/ {bad++}
  END {
    if (bad || good!=1 || valid!=1 || signer_primary!=primary || subkey==primary || subkey!=permitted) exit 1
    if (selector!="" && (selector !~ /^[0-9A-F]{40}!$/ || subkey "!" != selector)) exit 1
  }
' "$keyring/status" || die 'signing subkey, expiry or revocation check failed'
echo '[ok] signed tag verified against the public pin committed in the tagged commit'
