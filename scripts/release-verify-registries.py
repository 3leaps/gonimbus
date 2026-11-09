#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify published Go module proxy, Homebrew tap, and Scoop bucket against staged release assets."""

from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile

MODULE_PATH = "github.com/3leaps/gonimbus"
REPO_SLUG = "3leaps/gonimbus"
DEFAULT_GOPROXY_URL = "https://proxy.golang.org"
DEFAULT_GOSUMDB_URL = "https://sum.golang.org"
DEFAULT_HOMEBREW_FORMULA_URL = (
    "https://raw.githubusercontent.com/3leaps/homebrew-tap/main/Formula/gonimbus.rb"
)
DEFAULT_SCOOP_MANIFEST_URL = (
    "https://raw.githubusercontent.com/3leaps/scoop-bucket/main/bucket/gonimbus.json"
)
BREW_TARGETS = (
    "gonimbus-darwin-arm64",
    "gonimbus-linux-amd64",
    "gonimbus-linux-arm64",
)
SCOOP_TARGETS = {
    "64bit": "gonimbus-windows-amd64.exe",
    "arm64": "gonimbus-windows-arm64.exe",
}


def die(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_sha256sums(directory: Path) -> dict[str, str]:
    manifest = directory / "SHA256SUMS"
    if not manifest.is_file() or manifest.is_symlink():
        die(f"missing {manifest}")
    digests: dict[str, str] = {}
    for raw in manifest.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        match = re.fullmatch(r"([0-9a-f]{64}) [ *]([^\s/]+)", raw)
        if not match:
            die(f"malformed SHA256SUMS line: {raw}")
        digest, name = match.groups()
        if name in digests:
            die(f"duplicate SHA256SUMS entry: {name}")
        digests[name] = digest
    return digests


def allowed_url(url: str) -> urllib.parse.ParseResult:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "https" and parsed.hostname:
        return parsed
    if parsed.scheme == "http" and parsed.hostname == "127.0.0.1":
        return parsed
    die(f"disallowed registry URL: {url}")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        target = urllib.parse.urljoin(req.full_url, newurl)
        allowed_url(target)
        return super().redirect_request(req, fp, code, msg, headers, target)


def http_bytes(url: str) -> bytes:
    allowed_url(url)
    request = urllib.request.Request(
        url, headers={"User-Agent": "gonimbus-release-verify/1"}
    )
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(request, timeout=30) as response:
            allowed_url(response.geturl())
            return response.read()
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        die(f"HTTP fetch failed for {url}: {exc}")


def http_json(url: str) -> dict[str, object]:
    try:
        data = json.loads(http_bytes(url).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        die(f"invalid JSON from {url}: {exc}")
    if not isinstance(data, dict):
        die(f"JSON root from {url} is not an object")
    return data


ROOT = Path(__file__).resolve().parent.parent
ALLOWED_GIT_ENV = frozenset(
    {
        "GIT_COMMITTER_NAME",
        "GIT_COMMITTER_EMAIL",
        "GIT_AUTHOR_NAME",
        "GIT_AUTHOR_EMAIL",
    }
)


def _git_env() -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_") or key in ALLOWED_GIT_ENV
    }
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_ATTR_NOSYSTEM"] = "1"
    return env


def committed_blob(commit: str, rel_path: str) -> bytes:
    proc = subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(ROOT),
            "cat-file",
            "blob",
            f"{commit}:{rel_path}",
        ],
        capture_output=True,
        check=False,
        env=_git_env(),
    )
    if proc.returncode != 0:
        die(f"missing {rel_path} at commit {commit}")
    return proc.stdout


def verify_license_and_notice(
    license_bytes: bytes, notice_bytes: bytes, commit: str, label: str
) -> None:
    expected_license = committed_blob(commit, "LICENSE")
    expected_notice = committed_blob(commit, "NOTICE")
    if license_bytes != expected_license:
        die(f"{label} LICENSE does not match {commit}:LICENSE")
    if notice_bytes != expected_notice:
        die(f"{label} NOTICE does not match {commit}:NOTICE")
    license_text = license_bytes.decode("utf-8", errors="replace")
    notice_text = notice_bytes.decode("utf-8", errors="replace")
    if "Apache License" not in license_text or "Version 2.0" not in license_text:
        die(f"{label} LICENSE does not contain Apache-2.0 text")
    if (
        "3 Leaps, LLC" not in notice_text
        or "North Carolina Limited Liability Company" not in notice_text
    ):
        die(f"{label} NOTICE does not contain 3 Leaps, LLC North Carolina attribution")


def verify_go(tag: str, commit: str) -> None:
    proxy_base = os.environ.get("GONIMBUS_GOPROXY_URL", DEFAULT_GOPROXY_URL).rstrip("/")
    sumdb_base = os.environ.get("GONIMBUS_GOSUMDB_URL", DEFAULT_GOSUMDB_URL).rstrip("/")

    info_url = f"{proxy_base}/{MODULE_PATH}/@v/{tag}.info"
    mod_url = f"{proxy_base}/{MODULE_PATH}/@v/{tag}.mod"
    zip_url = f"{proxy_base}/{MODULE_PATH}/@v/{tag}.zip"
    sum_url = f"{sumdb_base}/lookup/{MODULE_PATH}@{tag}"

    info = http_json(info_url)
    if info.get("Version") != tag:
        die(
            f"Go proxy .info Version mismatch: expected {tag!r}, got {info.get('Version')!r}"
        )

    mod_bytes = http_bytes(mod_url)
    expected_go_mod = committed_blob(commit, "go.mod")
    if mod_bytes != expected_go_mod:
        die(f"Go proxy .mod does not match {commit}:go.mod")
    mod_text = mod_bytes.decode("utf-8", errors="replace")
    if not re.search(rf"^module\s+{re.escape(MODULE_PATH)}\s*$", mod_text, re.M):
        die(f"Go proxy .mod does not declare module {MODULE_PATH}")

    sum_text = http_bytes(sum_url).decode("utf-8", errors="replace")
    expected_mod_prefix = f"{MODULE_PATH} {tag} h1:"
    expected_gomod_prefix = f"{MODULE_PATH} {tag}/go.mod h1:"
    if not any(line.startswith(expected_mod_prefix) for line in sum_text.splitlines()):
        die(f"sum.golang.org lookup missing module h1 record for {MODULE_PATH}@{tag}")
    if not any(
        line.startswith(expected_gomod_prefix) for line in sum_text.splitlines()
    ):
        die(f"sum.golang.org lookup missing go.mod h1 record for {MODULE_PATH}@{tag}")

    zip_bytes = http_bytes(zip_url)
    prefix = f"{MODULE_PATH}@{tag}/"
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            if not names:
                die("Go module zip is empty")
            for name in names:
                if not name.startswith(prefix) or ".." in Path(name).parts:
                    die(f"unexpected Go module zip entry: {name}")
            lic_entry = f"{prefix}LICENSE"
            notice_entry = f"{prefix}NOTICE"
            gomod_entry = f"{prefix}go.mod"
            for req in (lic_entry, notice_entry, gomod_entry):
                if req not in names:
                    die(f"Go module zip missing required entry: {req}")
            if zf.read(gomod_entry) != expected_go_mod:
                die(f"Go module zip go.mod does not match {commit}:go.mod")
            verify_license_and_notice(
                zf.read(lic_entry), zf.read(notice_entry), commit, "Go module zip"
            )
    except zipfile.BadZipFile as exc:
        die(f"Go proxy .zip is not a valid zip archive: {exc}")

    print(f"[ok] Go module proxy and sumdb verified for {MODULE_PATH}@{tag}")


def verify_brew(tag: str, digests: dict[str, str]) -> None:
    formula_url = os.environ.get(
        "GONIMBUS_HOMEBREW_FORMULA_URL", DEFAULT_HOMEBREW_FORMULA_URL
    )
    text = http_bytes(formula_url).decode("utf-8", errors="replace")
    version = tag.removeprefix("v")

    if not re.search(r"^\s*class\s+Gonimbus\s*<\s*Formula\b", text, re.M):
        die("Homebrew formula does not declare class Gonimbus < Formula")
    if not re.search(r'^\s*license\s+"Apache-2\.0"\s*$', text, re.M):
        die('Homebrew formula does not declare license "Apache-2.0"')
    if re.search(r"^\s*bottle\s+do\b", text, re.M):
        die("Homebrew formula must not contain a bottle do block")

    version_match = re.search(r'^\s*version\s+"([^"]+)"\s*$', text, re.M)
    if version_match is not None and version_match.group(1) != version:
        die(f'Homebrew formula version mismatch: expected "{version}"')

    # Forbid archive URLs (.tar.gz, .tgz, .zip) anywhere in the formula
    all_urls = [m.group(1) for m in re.finditer(r'^\s*url\s+"([^"]+)"\s*$', text, re.M)]
    for u in all_urls:
        if u.endswith((".tar.gz", ".tgz", ".zip")):
            die(f"Homebrew formula must reference raw binary assets, not archive: {u}")

    # Verify each required target URL and immediately following sha256
    expected_urls: set[str] = set()
    for target in BREW_TARGETS:
        expected_digest = digests.get(target)
        if not expected_digest:
            die(f"SHA256SUMS missing entry for {target}")
        expected_url = (
            f"https://github.com/{REPO_SLUG}/releases/download/{tag}/{target}"
        )
        expected_urls.add(expected_url)
        pair_pattern = re.compile(
            rf'url\s+"{re.escape(expected_url)}"\s*\n\s*sha256\s+"([0-9a-fA-F]{{64}})"',
            re.M,
        )
        match = pair_pattern.search(text)
        if match is None:
            die(f"Homebrew formula missing url/sha256 pair for {expected_url}")
        actual_digest = match.group(1).lower()
        if actual_digest != expected_digest:
            die(
                f"Homebrew formula sha256 mismatch for {target}: "
                f"expected {expected_digest}, got {actual_digest}"
            )
    if len(all_urls) != len(BREW_TARGETS) or set(all_urls) != expected_urls:
        die("Homebrew formula url entries do not match the expected 3 release assets")

    cond_atom = r"(?:OS\.(?:mac|linux)\?|Hardware::CPU\.(?:arm|intel)\?)"
    if_re = re.compile(rf"^if\s+{cond_atom}(?:\s*(?:&&|\|\|)\s*{cond_atom})*$")
    elsif_re = re.compile(rf"^elsif\s+{cond_atom}(?:\s*(?:&&|\|\|)\s*{cond_atom})*$")
    bin_install_re = re.compile(
        r'^bin\.install\s+"(gonimbus-(?:darwin-arm64|linux-amd64|linux-arm64))"\s*=>\s*"gonimbus"$'
    )
    allowed_test_lines = {
        'system bin/"gonimbus", "version"',
        'system bin/"gonimbus", "--version"',
        'assert_match version.to_s, shell_output("#{bin}/gonimbus version")',
        'assert_match "\\"version\\":\\"#{version}\\"", shell_output("#{bin}/gonimbus version --json").gsub(/\\s+/, "")',
    }
    allowed_test_unless_lines = {
        'assert_equal stable.checksum.hexdigest, Digest::SHA256.file((bin/"gonimbus").realpath).hexdigest',
    }
    allowed_platform_suffix_lines = {
        'return "darwin-arm64" if OS.mac? && Hardware::CPU.arm?': "gonimbus-darwin-arm64",
        'odie "prebuilt macOS Intel binary is not published for gonimbus #{version}" if OS.mac?': "",
        'return "linux-arm64" if Hardware::CPU.arm?': "gonimbus-linux-arm64",
        '"linux-amd64"': "gonimbus-linux-amd64",
        'return "linux-amd64"': "gonimbus-linux-amd64",
    }

    stack: list[str] = []
    saw_class = False
    found_install = False
    found_platform_suffix = False
    found_test = False
    uses_platform_suffix = False
    installed_targets: set[str] = set()
    suffix_targets: set[str] = set()

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if not stack:
            if not saw_class and stripped == "class Gonimbus < Formula":
                saw_class = True
                stack.append("class")
                continue
            die(f"Homebrew formula contains disallowed top-level statement: {stripped}")

        current = stack[-1]
        if current == "class":
            if stripped == "end":
                stack.pop()
                continue
            if (
                re.fullmatch(r'desc\s+"[A-Za-z0-9 ,._:-]+"', stripped)
                or stripped == f'homepage "https://github.com/{REPO_SLUG}"'
                or stripped == f'version "{version}"'
                or stripped == 'license "Apache-2.0"'
                or stripped
                == f'head "https://github.com/{REPO_SLUG}.git", branch: "main"'
                or stripped == "private"
            ):
                continue
            if stripped in ("on_macos do", "on_linux do"):
                stack.append(stripped)
                continue
            if stripped == "def install":
                if found_install:
                    die("Homebrew formula contains multiple def install blocks")
                found_install = True
                stack.append("def install")
                continue
            if stripped == "def platform_suffix":
                if found_platform_suffix:
                    die("Homebrew formula contains multiple def platform_suffix blocks")
                found_platform_suffix = True
                stack.append("def platform_suffix")
                continue
            if stripped == "test do":
                if found_test:
                    die("Homebrew formula contains multiple test do blocks")
                found_test = True
                stack.append("test do")
                continue
            if stripped.startswith("def "):
                die(
                    f"Homebrew formula contains disallowed method definition: {stripped}"
                )
            die(
                f"Homebrew formula contains disallowed statement in class body: {stripped}"
            )

        if current in ("on_macos do", "on_linux do"):
            if stripped == "end":
                stack.pop()
                continue
            if current == "on_macos do" and stripped == "depends_on arch: :arm64":
                continue
            if stripped in ("on_arm do", "on_intel do"):
                stack.append(stripped)
                continue
            die(
                f"Homebrew formula contains disallowed statement in {current}: {stripped}"
            )

        if current in ("on_arm do", "on_intel do"):
            if stripped == "end":
                stack.pop()
                continue
            url_m = re.fullmatch(r'url\s+"([^"]+)"', stripped)
            if url_m is not None and url_m.group(1) in expected_urls:
                continue
            if re.fullmatch(r'sha256\s+"[0-9a-fA-F]{64}"', stripped):
                continue
            die(
                f"Homebrew formula contains disallowed statement in {current}: {stripped}"
            )

        if current in ("def install", "install_if"):
            if if_re.fullmatch(stripped):
                stack.append("install_if")
                continue
            if elsif_re.fullmatch(stripped) or stripped == "else":
                if current != "install_if":
                    die(
                        f"Homebrew formula def install has unexpected {stripped} outside conditional"
                    )
                continue
            if stripped == "end":
                stack.pop()
                continue
            bin_match = bin_install_re.fullmatch(stripped)
            if bin_match is not None:
                installed_targets.add(bin_match.group(1))
                continue
            if stripped == 'bin.install "gonimbus-#{platform_suffix}" => "gonimbus"':
                uses_platform_suffix = True
                continue
            die(
                f"Homebrew formula def install contains disallowed statement: {stripped}"
            )

        if current == "def platform_suffix":
            if stripped == "end":
                stack.pop()
                continue
            if stripped in allowed_platform_suffix_lines:
                mapped = allowed_platform_suffix_lines[stripped]
                if mapped:
                    suffix_targets.add(mapped)
                continue
            die(
                f"Homebrew formula def platform_suffix contains disallowed statement: {stripped}"
            )

        if current == "test do":
            if stripped == "end":
                stack.pop()
                continue
            if stripped == "unless build.head?":
                stack.append("test_unless")
                continue
            if stripped in allowed_test_lines:
                continue
            die(f"Homebrew formula test do contains disallowed statement: {stripped}")

        if current == "test_unless":
            if stripped == "end":
                stack.pop()
                continue
            if stripped in allowed_test_unless_lines:
                continue
            die(
                f"Homebrew formula test do unless block contains disallowed statement: {stripped}"
            )

    if not saw_class or stack:
        die("Homebrew formula has unbalanced block structure")
    if not found_install:
        die("Homebrew formula missing def install block")
    if uses_platform_suffix:
        if (
            not found_platform_suffix
            or installed_targets
            or suffix_targets != set(BREW_TARGETS)
        ):
            die(
                "Homebrew formula platform_suffix install must define complete platform_suffix helper"
            )
    elif installed_targets != set(BREW_TARGETS):
        die(
            f"Homebrew formula def install must install {sorted(BREW_TARGETS)}, "
            f"got {sorted(installed_targets)}"
        )

    print(f"[ok] Homebrew formula verified for {tag}")


def verify_scoop(tag: str, digests: dict[str, str]) -> None:
    manifest_url = os.environ.get(
        "GONIMBUS_SCOOP_MANIFEST_URL", DEFAULT_SCOOP_MANIFEST_URL
    )
    manifest = http_json(manifest_url)
    version = tag.removeprefix("v")

    if manifest.get("version") != version:
        die(
            f"Scoop manifest version mismatch: expected {version!r}, "
            f"got {manifest.get('version')!r}"
        )

    lic = manifest.get("license")
    lic_id = lic.get("identifier") if isinstance(lic, dict) else lic
    if lic_id != "Apache-2.0":
        die(f"Scoop manifest license must be Apache-2.0, got {lic!r}")

    forbidden_scoop_keys = (
        "extract_dir",
        "pre_install",
        "post_install",
        "installer",
        "uninstaller",
        "pre_uninstall",
        "post_uninstall",
        "psmodule",
        "env_add_path",
        "env_set",
        "shortcuts",
    )
    for forbidden_key in forbidden_scoop_keys:
        if forbidden_key in manifest:
            die(f"Scoop manifest must not contain {forbidden_key}")
    allowed_top_keys = {
        "version",
        "description",
        "homepage",
        "license",
        "architecture",
        "bin",
        "checkver",
        "autoupdate",
    }
    extra_top_keys = set(manifest.keys()) - allowed_top_keys
    if extra_top_keys:
        die(f"Scoop manifest contains disallowed keys: {sorted(extra_top_keys)}")

    arch_map = manifest.get("architecture")
    if not isinstance(arch_map, dict):
        die("Scoop manifest missing architecture object")
    if set(arch_map.keys()) != set(SCOOP_TARGETS.keys()):
        die(
            f"Scoop manifest architecture keys must be {sorted(SCOOP_TARGETS.keys())}, "
            f"got {sorted(arch_map.keys())}"
        )

    top_bin = manifest.get("bin")
    allowed_arch_keys = {"url", "hash", "bin"}
    for arch_key, asset_name in SCOOP_TARGETS.items():
        expected_digest = digests.get(asset_name)
        if not expected_digest:
            die(f"SHA256SUMS missing entry for {asset_name}")
        entry = arch_map.get(arch_key)
        if not isinstance(entry, dict):
            die(f"Scoop manifest architecture.{arch_key} is not an object")
        for forbidden_key in forbidden_scoop_keys:
            if forbidden_key in entry:
                die(
                    f"Scoop manifest architecture.{arch_key} must not contain {forbidden_key}"
                )
        extra_arch_keys = set(entry.keys()) - allowed_arch_keys
        if extra_arch_keys:
            die(
                f"Scoop manifest architecture.{arch_key} contains disallowed keys: "
                f"{sorted(extra_arch_keys)}"
            )
        expected_url = (
            f"https://github.com/{REPO_SLUG}/releases/download/{tag}/{asset_name}"
        )
        actual_url = entry.get("url")
        if actual_url != expected_url:
            die(
                f"Scoop manifest architecture.{arch_key}.url mismatch: "
                f"expected {expected_url!r}, got {actual_url!r}"
            )
        raw_hash = entry.get("hash")
        if not isinstance(raw_hash, str):
            die(f"Scoop manifest architecture.{arch_key}.hash is not a string")
        actual_digest = raw_hash.removeprefix("sha256:").lower()
        if actual_digest != expected_digest:
            die(
                f"Scoop manifest architecture.{arch_key}.hash mismatch: "
                f"expected {expected_digest}, got {actual_digest}"
            )

        bin_spec = entry.get("bin", top_bin)
        if bin_spec != [[asset_name, "gonimbus"]]:
            die(
                f"Scoop manifest bin for {arch_key} must be [[{asset_name!r}, 'gonimbus']], "
                f"got {bin_spec!r}"
            )

    print(f"[ok] Scoop manifest verified for {tag}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", default="dist/published")
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument(
        "--registry", choices=("all", "go", "brew", "scoop"), default="all"
    )
    args = parser.parse_args()

    if not re.fullmatch(
        r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", args.tag
    ):
        die("tag must be canonical vX.Y.Z")
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        die("commit must be 40 lowercase hex")

    directory = Path(args.directory)
    digests = parse_sha256sums(directory)

    if args.registry in ("all", "go"):
        verify_go(args.tag, args.commit)
    if args.registry in ("all", "brew"):
        verify_brew(args.tag, digests)
    if args.registry in ("all", "scoop"):
        verify_scoop(args.tag, digests)


if __name__ == "__main__":
    main()
