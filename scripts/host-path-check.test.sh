#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# host-path-check.py: a build-host directory followed by a separator is
# found in plain files and in every zip member; a short root that is only a
# substring of a longer path (/src in library/alloc/src/) is not; "/" alone is
# ignored.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
check="$root/scripts/host-path-check.py"
python="${PYTHON:-python3}"
"$python" -I -c 'import re, zipfile' || { echo "error: no usable Python ($python); set PYTHON" >&2; exit 1; }
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
ok() { "$python" -I "$check" "$@" >/dev/null 2>&1 || { echo "error: refused: $*" >&2; exit 1; }; }
bad() { if "$python" -I "$check" "$@" >/dev/null 2>&1; then echo "error: accepted: $*" >&2; exit 1; fi; }

printf 'x/rustc/abc/library/alloc/src/string.rs /gonimbus/pkg/x.go /cargo/registry/src/y.rs' >"$scratch/clean"
ok "$scratch/clean" /src /build/.cargo /build/.rustup /build
ok "$scratch/clean" /
h=/home u=/Users
printf 'x%s/user/work/gonimbus/pkg/x.go' "$h" >"$scratch/home"
bad "$scratch/home" /home/user
bad "$scratch/home" "$h/user/work/gonimbus"
printf 'x%s/username/x.go' "$h" >"$scratch/home-longer"
ok "$scratch/home-longer" /home/user
printf 'x\000/src/pkg/x.go' >"$scratch/root"
bad "$scratch/root" /src
printf '"path":"/src/pkg/x.go"' >"$scratch/root-json"
bad "$scratch/root-json" /src
printf 'path+file:///src/pkg#1' >"$scratch/root-url"
bad "$scratch/root-url" /src
printf 'x/alloc/src/pkg/x.go' >"$scratch/root-inner"
ok "$scratch/root-inner" /src
printf 'x C:\\Users\\someone\\x.go' >"$scratch/win"
bad "$scratch/win" 'C:\Users\someone'
win='C:\\Users\\someone'
for form in 'C:\\Users\\someone\\x.go' 'C:\\\\Users\\\\someone\\\\x.go' \
	"path+file:///C:$u/someone/pkg#1" "file:///c:$u/someone/x" \
	'C:%5CUsers%5Csomeone%5Cx' 'C:/Users\\someone/x'; do
	printf '%s' "$form" >"$scratch/win-form"
	bad "$scratch/win-form" "$win"
done
printf 'C:\\Users\\user\\x.go' >"$scratch/win-other"
ok "$scratch/win-other" "$win"
printf '{"p":"\\/home\\/someone\\/x"}' >"$scratch/json-unix"
bad "$scratch/json-unix" /home/someone
"$python" -I - "$scratch" <<'PY'
import json, pathlib, sys, zipfile
d = pathlib.Path(sys.argv[1])
d.joinpath("enc-space-url").write_bytes(b"path+file:///C:/Users/user%20name/pkg#1")
d.joinpath("enc-space-json").write_text(json.dumps({"p": "C:\\Users\\user name\\pkg"}))
d.joinpath("enc-umlaut-json").write_text(json.dumps({"p": "/home/user\u00e9/pkg"}))
d.joinpath("enc-umlaut-url").write_bytes(b"file:///home/user%C3%A9/pkg")
d.joinpath("enc-umlaut-raw").write_bytes("/home/user\u00e9/pkg".encode())
d.joinpath("enc-url-in-json").write_text(json.dumps({"u": "file:///home/user%C3%A9/pkg"}))
d.joinpath("enc-other-user").write_text(json.dumps({"p": "/home/user\u00e9s/pkg"}))
with zipfile.ZipFile(d / "enc.whl", "w") as z:
    z.writestr("pkg-1.dist-info/build.json", json.dumps({"root": "/home/user\u00e9/pkg"}))
PY
bad "$scratch/enc-space-url" 'C:\Users\user name'
bad "$scratch/enc-space-json" 'C:\Users\user name'
for f in enc-umlaut-json enc-umlaut-url enc-umlaut-raw enc-url-in-json enc.whl; do
	bad "$scratch/$f" "/home/user$(printf '\303\251')"
done
ok "$scratch/enc-other-user" "/home/user$(printf '\303\251')"
"$python" -I - "$scratch/clean.whl" "$scratch/dirty.whl" "$h" <<'PY'
import sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w") as z:
    z.writestr("pkg/a.so", b"/rustc/abc/library/core/src/x.rs")
    z.writestr("pkg-1.dist-info/RECORD", b"pkg/a.so,sha256=x,1")
with zipfile.ZipFile(sys.argv[2], "w") as z:
    z.writestr("pkg/a.so", b"clean")
    z.writestr("pkg-1.dist-info/sboms/x.json", f"path+file://{sys.argv[3]}/someone/pkg#1".encode())
PY
ok "$scratch/clean.whl" /src /home/someone
bad "$scratch/dirty.whl" /home/someone
"$python" -I - "$scratch/winjson.whl" <<'PY'
import json, sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w") as z:
    z.writestr("pkg-1.dist-info/build.json", json.dumps({"root": "C:\\Users\\someone\\work\\pkg"}))
PY
bad "$scratch/winjson.whl" "$win"
echo '[ok] host-path-check: directory prefixes in files and zip members; no substring false positives'
