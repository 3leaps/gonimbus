#!/usr/bin/env bash
# Install a public anchor pair without overwrite. Roll back a partial first write.
set -euo pipefail
staging="${1:?staging directory required}"
dest="${2:?destination required}"
shift 2
for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
    [[ -s "$staging/$name" && ! -L "$staging/$name" ]] || {
        echo 'error: incomplete anchor pair' >&2
        exit 1
    }
done
mkdir -p "$dest"
for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
    if [[ -e "$dest/$name" || -L "$dest/$name" ]]; then
        echo "error: refusing to overwrite existing $dest/$name (fingerprint anchors already exist)" >&2
        exit 1
    fi
done
rollback() {
    trap - EXIT INT TERM HUP
    for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
        rm -f "$dest/$name.new" "$dest/$name"
    done
}
trap rollback EXIT
trap 'rollback; exit 130' INT
trap 'rollback; exit 143' TERM
trap 'rollback; exit 129' HUP
for name in expected-fingerprints.txt expected-fingerprints.ndjson; do
    cp "$staging/$name" "$dest/$name.new"
done
mv "$dest/expected-fingerprints.ndjson.new" "$dest/expected-fingerprints.ndjson"
if [[ "${GONIMBUS_TEST_FAIL_ANCHOR_INSTALL:-}" == 1 ]]; then
    echo 'error: interrupted pair installation' >&2
    exit 1
fi
mv "$dest/expected-fingerprints.txt.new" "$dest/expected-fingerprints.txt"
if [[ "$#" -gt 0 ]]; then
    "$@"
fi
trap - EXIT INT TERM HUP
