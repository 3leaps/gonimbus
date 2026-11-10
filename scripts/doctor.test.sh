#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
real_python="$(command -v python3)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

make_stub() {
    local dir="$1" name="$2" body="$3"
    cat > "$dir/$name" << EOF
#!/usr/bin/env bash
$body
EOF
    chmod +x "$dir/$name"
}

populate_good_stubs() {
    local dir="$1"
    mkdir -p "$dir"
    make_stub "$dir" go 'echo "go version go1.26.10 darwin/arm64"'
    make_stub "$dir" goneat 'echo "goneat v0.6.8"'
    make_stub "$dir" decernor 'echo "decernor 0.1.9"'
    make_stub "$dir" limensafe 'echo "limensafe 0.2.0"'
    make_stub "$dir" minisign 'echo "minisign 0.12"'
    make_stub "$dir" gpg 'echo "gpg (GnuPG) 2.4.5"'
    make_stub "$dir" gpgv 'echo "gpgv (GnuPG) 2.4.5"'
    make_stub "$dir" gh 'echo "gh version 2.60.0 (2026-01-01)"'
    make_stub "$dir" shasum 'echo "6.04"'
    make_stub "$dir" python3 'echo "Python 3.12.2"'
    make_stub "$dir" git 'echo "git version 2.44.0"'
    make_stub "$dir" jq 'echo "jq-1.7.1"'
}

good="$scratch/good"
populate_good_stubs "$good"

PATH="$good:/usr/bin:/bin" "$real_python" -I -B "$root/scripts/doctor.py" dev > /dev/null
PATH="$good:/usr/bin:/bin" "$real_python" -I -B "$root/scripts/doctor.py" release > /dev/null

# Old decernor fails release scope but passes dev scope
old_dec="$scratch/old-dec"
populate_good_stubs "$old_dec"
make_stub "$old_dec" decernor 'echo "decernor 0.1.7"'
PATH="$old_dec:/usr/bin:/bin" "$real_python" -I -B "$root/scripts/doctor.py" dev > /dev/null
if PATH="$old_dec:/usr/bin:/bin" "$real_python" -I -B "$root/scripts/doctor.py" release 2> /dev/null; then
    echo "error: doctor release accepted outdated decernor" >&2
    exit 1
fi

# Old go fails dev scope
old_go="$scratch/old-go"
populate_good_stubs "$old_go"
make_stub "$old_go" go 'echo "go version go1.25.0 darwin/arm64"'
if PATH="$old_go:/usr/bin:/bin" "$real_python" -I -B "$root/scripts/doctor.py" dev 2> /dev/null; then
    echo "error: doctor dev accepted outdated go" >&2
    exit 1
fi

# Unknown scope returns exit code 2
set +e
PATH="$good:/usr/bin:/bin" "$real_python" -I -B "$root/scripts/doctor.py" invalid 2> /dev/null
rc=$?
set -e
[[ "$rc" == 2 ]] || {
    echo "error: expected exit code 2 for unknown scope, got $rc" >&2
    exit 1
}

echo "[ok] doctor controls passed"
