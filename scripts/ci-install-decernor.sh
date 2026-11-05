#!/usr/bin/env bash
# Install the digest-pinned Decernor archive from config/release/decernor-ci-sha256.txt.
# Prints the absolute path of the extracted binary. No PATH fallback.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
pin="$root/config/release/decernor-ci-sha256.txt"
[[ -f "$pin" && -s "$pin" && ! -L "$pin" ]] || {
    echo 'error: committed Decernor digest pin missing' >&2
    exit 1
}
read -r digest filename _ < "$pin"
[[ "$digest" =~ ^[0-9a-f]{64}$ ]] || {
    echo 'error: Decernor digest pin is malformed' >&2
    exit 1
}
[[ "$filename" =~ ^decernor_([0-9]+\.[0-9]+\.[0-9]+)_linux_amd64\.tar\.gz$ ]] || {
    echo 'error: Decernor archive name is malformed' >&2
    exit 1
}
version="${BASH_REMATCH[1]}"
dest="${1:-}"
if [[ -z "$dest" ]]; then
    dest="$(mktemp -d "${TMPDIR:-/tmp}/decernor-ci.XXXXXX")"
fi
mkdir -p "$dest"
[[ ! -L "$dest" && -d "$dest" ]] || {
    echo 'error: Decernor install dest must be a directory, not a symlink' >&2
    exit 1
}
url="https://github.com/3leaps/decernor/releases/download/v${version}/${filename}"
curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location --retry 3 \
    "$url" --output "$dest/$filename"
# sha256sum --check prints "file: OK" to stdout; that extra line makes
# GITHUB_ENV assignment invalid. Keep stdout as the binary path only.
if command -v sha256sum > /dev/null 2>&1; then
    (cd "$dest" && sha256sum --check --quiet "$pin") >&2
else
    (cd "$dest" && shasum -a 256 -c "$pin") >&2
fi
tar -xzf "$dest/$filename" -C "$dest" decernor
chmod 0755 "$dest/decernor"
[[ -f "$dest/decernor" && ! -L "$dest/decernor" && -x "$dest/decernor" ]] || {
    echo 'error: extracted Decernor binary is not a regular executable' >&2
    exit 1
}
printf '%s\n' "$(cd "$dest" && pwd -P)/decernor"
