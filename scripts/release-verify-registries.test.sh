#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Local HTTP mock server tests for scripts/release-verify-registries.sh and .py.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/testdata/sha-helpers.sh
source "$root/scripts/testdata/sha-helpers.sh"

bin="${GONIMBUS_DECERNOR_BIN:-$(command -v decernor || true)}"
[[ -n "$bin" && -x "$bin" && "$bin" == /* && ! -L "$bin" ]] || {
    echo 'error: GONIMBUS_DECERNOR_BIN required' >&2
    exit 1
}
export GONIMBUS_DECERNOR_BIN="$bin"

scratch="$(mktemp -d)"
server_pid=""
cleanup() {
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2> /dev/null || true
        wait "$server_pid" 2> /dev/null || true
    fi
    gpgconf --homedir "$scratch/gpg" --kill all > /dev/null 2>&1 || true
    rm -rf "$scratch"
}
trap cleanup EXIT

repo="$scratch/repo"
mkdir -p "$repo/scripts" "$repo/keys" "$repo/docs/security" "$repo/docs/releases" "$scratch/web" "$scratch/published" "$scratch/gpg"
chmod 700 "$scratch/gpg"
cp "$root/scripts/release-verify-registries.sh" "$root/scripts/release-verify-registries.py" \
    "$root/scripts/release-decernor.sh" "$root/scripts/verify-checksums.sh" \
    "$root/scripts/release-verify-signatures.sh" "$repo/scripts/"
chmod +x "$repo/scripts/release-verify-registries.sh" "$repo/scripts/release-verify-registries.py" \
    "$repo/scripts/verify-checksums.sh" "$repo/scripts/release-verify-signatures.sh"
cat > "$repo/scripts/release-verify-published-tag.sh" << 'SH'
#!/usr/bin/env bash
exit 0
SH
chmod +x "$repo/scripts/release-verify-published-tag.sh"

home="$scratch/gpg"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' \
    --quick-generate-key 'Synthetic signing <signing@example.invalid>' ed25519 cert,sign 1d > /dev/null 2>&1
primary="$(gpg --homedir "$home" --batch --with-colons --fingerprint --list-keys | awk -F: '$1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --quiet --pinentry-mode loopback --passphrase '' --quick-add-key "$primary" ed25519 sign 1d > /dev/null 2>&1
subkey="$(gpg --homedir "$home" --batch --with-colons --with-subkey-fingerprint --list-keys | awk -F: '$1=="sub" {s=1;next} s && $1=="fpr" {print $10;exit}')"
gpg --homedir "$home" --batch --armor --export "$primary" > "$repo/docs/security/release-signing-keys.asc"
minisign -G -W -p "$scratch/minisign.pub" -s "$scratch/minisign.key" > /dev/null
cp "$scratch/minisign.pub" "$repo/docs/security/gonimbus-minisign.pub"
"$bin" fingerprint "$repo/docs/security/release-signing-keys.asc" --class public --kind gpg \
    --format ndjson --path-mode none --gpg-role primary > "$scratch/gpg.ndjson"
"$bin" fingerprint "$repo/docs/security/gonimbus-minisign.pub" --class public --kind minisign \
    --format ndjson --path-mode none > "$scratch/minisign.ndjson"
python3 - "$scratch" "$repo/keys" << 'PY'
import json, pathlib, sys
base, keys = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
g = [json.loads(line) for line in (base / 'gpg.ndjson').read_text().splitlines() if line]
m = [json.loads(line) for line in (base / 'minisign.ndjson').read_text().splitlines() if line and json.loads(line).get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
(keys / 'expected-fingerprints.ndjson').write_text(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in (g[0], m[0])))
(keys / 'expected-fingerprints.txt').write_text('gpg ' + g[0]['fingerprint'] + '\nminisign ' + m[0]['fingerprint'] + '\n')
PY

tag="v1.2.3"
printf '# notes %s\n' "$tag" > "$repo/docs/releases/$tag.md"
cp "$root/LICENSE" "$repo/LICENSE"
cp "$root/NOTICE" "$repo/NOTICE"
printf 'module github.com/3leaps/gonimbus\n\ngo 1.26.0\n' > "$repo/go.mod"

git -C "$repo" init -q -b main
git -C "$repo" config commit.gpgsign false
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid add .
git -C "$repo" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm fixture
commit="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" tag "$tag" "$commit"
object="$(printf '%040d' 3)"

pub="$scratch/published"
for name in \
    gonimbus-darwin-arm64 \
    gonimbus-linux-amd64 \
    gonimbus-linux-arm64 \
    gonimbus-windows-amd64.exe \
    gonimbus-windows-arm64.exe; do
    printf 'binary-%s\n' "$name" > "$pub/$name"
done
(cd "$pub" && sha_sum 256 gonimbus-* > SHA256SUMS && sha_sum 512 gonimbus-* > SHA512SUMS)
for m in SHA256SUMS SHA512SUMS; do
    minisign -S -s "$scratch/minisign.key" -m "$pub/$m" -x "$pub/$m.minisig" > /dev/null
    gpg --homedir "$home" --batch --pinentry-mode loopback --passphrase '' \
        --local-user "$subkey!" --armor --detach-sign --output "$pub/$m.asc" "$pub/$m" 2> /dev/null
done
cp "$repo/docs/security/gonimbus-minisign.pub" "$pub/gonimbus-minisign.pub"
cp "$repo/docs/security/release-signing-keys.asc" "$pub/gonimbus-release-signing-key.asc"
cp "$repo/keys/expected-fingerprints.txt" "$pub/expected-fingerprints.txt"
cp "$repo/keys/expected-fingerprints.ndjson" "$pub/expected-fingerprints.ndjson"
cp "$repo/docs/releases/$tag.md" "$pub/release-notes-$tag.md"
printf 'tag=%s\nobject=%s\ncommit=%s\n' "$tag" "$object" "$commit" > "$pub.anchor"

sha_of() {
    awk -v n="$1" '$2 == n {print $1}' "$pub/SHA256SUMS"
}
sha_darwin_arm64="$(sha_of gonimbus-darwin-arm64)"
sha_linux_amd64="$(sha_of gonimbus-linux-amd64)"
sha_linux_arm64="$(sha_of gonimbus-linux-arm64)"
sha_win_amd64="$(sha_of gonimbus-windows-amd64.exe)"
sha_win_arm64="$(sha_of gonimbus-windows-arm64.exe)"

write_fixtures() {
    local web="$scratch/web"
    rm -rf "$web"
    mkdir -p "$web/github.com/3leaps/gonimbus/@v" "$web/lookup/github.com/3leaps" "$web/brew" "$web/scoop"

    printf '{"Version":"%s","Time":"2026-04-20T00:00:00Z"}\n' "$tag" \
        > "$web/github.com/3leaps/gonimbus/@v/$tag.info"
    cp "$repo/go.mod" "$web/github.com/3leaps/gonimbus/@v/$tag.mod"
    cat > "$web/lookup/github.com/3leaps/gonimbus@$tag" << EOF
12345
github.com/3leaps/gonimbus $tag h1:abc123=
github.com/3leaps/gonimbus $tag/go.mod h1:def456=
EOF
    python3 -I - "$web/github.com/3leaps/gonimbus/@v/$tag.zip" "$repo" "$tag" << 'PY'
import pathlib, sys, zipfile
out, repo, tag = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
prefix = f"github.com/3leaps/gonimbus@{tag}/"
with zipfile.ZipFile(out, "w") as zf:
    for rel in ("LICENSE", "NOTICE", "go.mod"):
        zf.writestr(prefix + rel, (repo / rel).read_bytes())
PY

    cat > "$web/brew/gonimbus.rb" << EOF
class Gonimbus < Formula
  desc "High-performance object storage replication"
  homepage "https://github.com/3leaps/gonimbus"
  version "1.2.3"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-darwin-arm64"
      sha256 "$sha_darwin_arm64"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-linux-amd64"
      sha256 "$sha_linux_amd64"
    end
    on_arm do
      url "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-linux-arm64"
      sha256 "$sha_linux_arm64"
    end
  end

  def install
    if OS.mac?
      bin.install "gonimbus-darwin-arm64" => "gonimbus"
    elsif Hardware::CPU.arm?
      bin.install "gonimbus-linux-arm64" => "gonimbus"
    else
      bin.install "gonimbus-linux-amd64" => "gonimbus"
    end
  end
end
EOF

    cat > "$web/scoop/gonimbus.json" << EOF
{
  "version": "1.2.3",
  "description": "High-performance object storage replication",
  "homepage": "https://github.com/3leaps/gonimbus",
  "license": "Apache-2.0",
  "architecture": {
    "64bit": {
      "url": "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-windows-amd64.exe",
      "hash": "$sha_win_amd64",
      "bin": [["gonimbus-windows-amd64.exe", "gonimbus"]]
    },
    "arm64": {
      "url": "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-windows-arm64.exe",
      "hash": "$sha_win_arm64",
      "bin": [["gonimbus-windows-arm64.exe", "gonimbus"]]
    }
  }
}
EOF
}

mkdir -p "$scratch/web/lookup/github.com/3leaps"
write_fixtures

port_file="$scratch/port"
python3 -I - "$scratch/web" "$port_file" << 'PY' &
import http.server, os, pathlib, socketserver, sys

root = pathlib.Path(sys.argv[1]).resolve()
port_file = pathlib.Path(sys.argv[2])

class Handler(http.server.SimpleHTTPRequestHandler):
    def translate_path(self, path: str) -> str:
        clean = path.split("?", 1)[0].lstrip("/")
        return str(root / clean)
    def log_message(self, format: str, *args) -> None:
        pass

with socketserver.TCPServer(("127.0.0.1", 0), Handler) as httpd:
    port_file.write_text(str(httpd.server_address[1]), encoding="utf-8")
    httpd.serve_forever()
PY
server_pid="$!"
for _ in $(seq 1 50); do
    [[ -s "$port_file" ]] && break
    sleep 0.05
done
port="$(cat "$port_file")"
base_url="http://127.0.0.1:$port"

export GONIMBUS_RELEASE_TAG="$tag"
export GONIMBUS_GOPROXY_URL="$base_url"
export GONIMBUS_GOSUMDB_URL="$base_url"
export GONIMBUS_HOMEBREW_FORMULA_URL="$base_url/brew/gonimbus.rb"
export GONIMBUS_SCOOP_MANIFEST_URL="$base_url/scoop/gonimbus.json"

run_verify() {
    (cd "$repo" && ./scripts/release-verify-registries.sh --directory "$pub" "$@")
}

# Refuses GONIMBUS_SKIP_STAGE_CHECKS
if GONIMBUS_SKIP_STAGE_CHECKS=1 run_verify --registry all > "$scratch/out" 2>&1; then
    echo 'error: accepted GONIMBUS_SKIP_STAGE_CHECKS' >&2
    exit 1
fi
grep -q "GONIMBUS_SKIP_STAGE_CHECKS is not permitted" "$scratch/out"

expect_fail() {
    local pattern="$1"
    shift
    if run_verify "$@" > "$scratch/out" 2>&1; then
        echo "error: expected failure matching '$pattern'" >&2
        exit 1
    fi
    grep -qE "$pattern" "$scratch/out" || {
        echo "error: missing expected failure '$pattern'" >&2
        cat "$scratch/out" >&2
        exit 1
    }
}

# Positive pass for all and individual selectors
run_verify --registry all > /dev/null
run_verify --registry go > /dev/null
run_verify --registry brew > /dev/null
run_verify --registry scoop > /dev/null

# Mutation 1: Go module zip tampered NOTICE
write_fixtures
python3 -I - "$scratch/web/github.com/3leaps/gonimbus/@v/$tag.zip" "$repo" "$tag" << 'PY'
import pathlib, sys, zipfile
out, repo, tag = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
prefix = f"github.com/3leaps/gonimbus@{tag}/"
with zipfile.ZipFile(out, "w") as zf:
    zf.writestr(prefix + "LICENSE", (repo / "LICENSE").read_bytes())
    zf.writestr(prefix + "NOTICE", b"tampered notice\n")
    zf.writestr(prefix + "go.mod", (repo / "go.mod").read_bytes())
PY
expect_fail "NOTICE does not match" --registry go

# Mutation 2: Go sumdb missing h1 record
write_fixtures
printf 'empty\n' > "$scratch/web/lookup/github.com/3leaps/gonimbus@$tag"
expect_fail "missing module h1 record" --registry go

# Mutation 3: Homebrew formula wrong sha256
write_fixtures
sed -i.bak "s/$sha_linux_amd64/$(printf '%064d' 0)/" "$scratch/web/brew/gonimbus.rb"
expect_fail "Homebrew formula sha256 mismatch" --registry brew

# Mutation 4: Homebrew formula contains bottle do block
write_fixtures
sed -i.bak 's/license "Apache-2.0"/license "Apache-2.0"\n  bottle do\n  end/' "$scratch/web/brew/gonimbus.rb"
expect_fail "must not contain a bottle do block" --registry brew

# Mutation 5: Homebrew formula uses archive URL instead of raw binary
write_fixtures
sed -i.bak 's/gonimbus-darwin-arm64/gonimbus-darwin-arm64.tar.gz/' "$scratch/web/brew/gonimbus.rb"
expect_fail "must reference raw binary assets, not archive" --registry brew

# Mutation 6: Homebrew formula uses write_env_script
write_fixtures
sed -i.bak 's/bin.install "gonimbus-darwin-arm64" => "gonimbus"/bin.install "gonimbus-darwin-arm64" => "gonimbus"\n    write_env_script/' "$scratch/web/brew/gonimbus.rb"
expect_fail "disallowed statement: write_env_script" --registry brew

# Mutation 6b: Homebrew formula mutates binary via system "strip" inside def install
write_fixtures
sed -i.bak 's/bin.install "gonimbus-linux-amd64" => "gonimbus"/bin.install "gonimbus-linux-amd64" => "gonimbus"\n    system "strip", "#{bin}\/gonimbus"/' "$scratch/web/brew/gonimbus.rb"
expect_fail "disallowed statement: system" --registry brew

# Mutation 6c: Homebrew formula mutates binary via def post_install outside def install
write_fixtures
cat > "$scratch/web/brew/gonimbus.rb" << EOF
class Gonimbus < Formula
  desc "Cloud object storage crawl, inspect, and streaming CLI"
  homepage "https://github.com/3leaps/gonimbus"
  license "Apache-2.0"

  head "https://github.com/3leaps/gonimbus.git", branch: "main"

  on_macos do
    depends_on arch: :arm64

    on_arm do
      url "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-darwin-arm64"
      sha256 "$sha_darwin_arm64"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-linux-amd64"
      sha256 "$sha_linux_amd64"
    end

    on_arm do
      url "https://github.com/3leaps/gonimbus/releases/download/$tag/gonimbus-linux-arm64"
      sha256 "$sha_linux_arm64"
    end
  end

  def install
    bin.install "gonimbus-#{platform_suffix}" => "gonimbus"
  end

  test do
    system bin/"gonimbus", "version"
  end

  private

  def platform_suffix
    return "darwin-arm64" if OS.mac? && Hardware::CPU.arm?

    odie "prebuilt macOS Intel binary is not published for gonimbus #{version}" if OS.mac?
    return "linux-arm64" if Hardware::CPU.arm?

    "linux-amd64"
  end
end
EOF
# Valid homebrew-tap platform_suffix + test do formula passes
run_verify --registry brew > /dev/null

# Adding def post_install to the formula fails closed
sed -i.bak 's/  test do/  def post_install\n    system "strip", bin\/"gonimbus"\n  end\n\n  test do/' "$scratch/web/brew/gonimbus.rb"
expect_fail "disallowed method definition: def post_install" --registry brew

# Adding a disallowed command inside test do fails closed
write_fixtures
sed -i.bak 's/  def install/  test do\n    system "strip", bin\/"gonimbus"\n  end\n\n  def install/' "$scratch/web/brew/gonimbus.rb"
expect_fail "test do contains disallowed statement: system" --registry brew

# Mutation 6d: Homebrew formula desc uses Ruby string interpolation (#{...})
write_fixtures
python3 -I - "$scratch/web/brew/gonimbus.rb" << 'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1])
p.write_text(
    p.read_text().replace(
        'desc "High-performance object storage replication"',
        'desc "#{define_method(:post_install){system(\'strip\',bin/\'gonimbus\')}}"',
    )
)
PY
expect_fail "disallowed statement in class body: desc" --registry brew

# Mutation 7: Scoop manifest wrong hash
write_fixtures
sed -i.bak "s/$sha_win_amd64/$(printf '%064d' 0)/" "$scratch/web/scoop/gonimbus.json"
expect_fail "Scoop manifest architecture.64bit.hash mismatch" --registry scoop

# Mutation 8: Scoop manifest has 3-element bin shim args
write_fixtures
sed -i.bak 's/\["gonimbus-windows-amd64.exe", "gonimbus"\]/["gonimbus-windows-amd64.exe", "gonimbus", "--shim"]/' "$scratch/web/scoop/gonimbus.json"
expect_fail "Scoop manifest bin for 64bit must be" --registry scoop

# Mutation 9: Scoop manifest has extract_dir
write_fixtures
python3 -I - "$scratch/web/scoop/gonimbus.json" << 'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
d = json.loads(p.read_text())
d["extract_dir"] = "gonimbus"
p.write_text(json.dumps(d))
PY
expect_fail "must not contain extract_dir" --registry scoop

# Mutation 10: Scoop manifest has installer.script at root scope
write_fixtures
python3 -I - "$scratch/web/scoop/gonimbus.json" << 'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
d = json.loads(p.read_text())
d["installer"] = {"script": ["[IO.File]::WriteAllBytes((Join-Path $dir $fname), @())"]}
p.write_text(json.dumps(d))
PY
expect_fail "Scoop manifest must not contain installer" --registry scoop

# Mutation 11: Scoop manifest has installer.script under architecture.64bit
write_fixtures
python3 -I - "$scratch/web/scoop/gonimbus.json" << 'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
d = json.loads(p.read_text())
d["architecture"]["64bit"]["installer"] = {"script": ["[IO.File]::WriteAllBytes((Join-Path $dir $fname), @())"]}
p.write_text(json.dumps(d))
PY
expect_fail "Scoop manifest architecture.64bit must not contain installer" --registry scoop

# Mutation 12: Scoop manifest has uninstaller or pre_uninstall
write_fixtures
python3 -I - "$scratch/web/scoop/gonimbus.json" << 'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
d = json.loads(p.read_text())
d["uninstaller"] = {"script": ["Write-Host 'bad'"]}
p.write_text(json.dumps(d))
PY
expect_fail "Scoop manifest must not contain uninstaller" --registry scoop

echo "[ok] release-verify-registries controls passed"
