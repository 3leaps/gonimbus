#!/usr/bin/env bash
# Check the committed fingerprint anchors and that the pinned public primary matches.
set -euo pipefail
# Optional arguments ANCHORS PIN check copies extracted from a commit instead
# of the working tree.
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
anchors="${1:-keys/expected-fingerprints.txt}"
pin="${2:-docs/security/release-signing-keys.asc}"
python3 - "$anchors" << 'PY'
import pathlib
import re
import sys
txt = pathlib.Path(sys.argv[1])
if txt.is_symlink() or not txt.is_file():
    raise SystemExit('error: committed fingerprint anchors missing')
if not re.fullmatch(rb'gpg [0-9A-F]{40}\nminisign [0-9a-f]{64}\n', txt.read_bytes()):
    raise SystemExit('error: fingerprint anchors must be exactly a gpg line then a minisign line')
PY
[[ -f "$pin" && -s "$pin" && ! -L "$pin" ]] || {
    echo 'error: committed public pin missing' >&2
    exit 1
}
temp="$(mktemp -d)"
trap 'gpgconf --homedir "$temp" --kill all > /dev/null 2>&1 || true; rm -rf "$temp"' EXIT
chmod 700 "$temp"
GNUPGHOME="$temp" gpg --batch --quiet --import "$pin"
listing="$(GNUPGHOME="$temp" gpg --batch --with-colons --fingerprint --list-keys)"
[[ "$(awk -F: '$1=="pub" {n++} END {print n+0}' <<< "$listing")" == 1 ]] || {
    echo 'error: pin must contain exactly one public primary' >&2
    exit 1
}
[[ "$(awk -F: '$1=="fpr" {print $10;exit}' <<< "$listing")" == "$(awk '$1=="gpg" {print $2}' "$anchors")" ]] || {
    echo 'error: primary pin differs from fingerprint anchors' >&2
    exit 1
}
echo '[ok] public pin and fingerprint anchors agree'
