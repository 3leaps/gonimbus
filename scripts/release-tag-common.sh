#!/usr/bin/env bash
# Signed-tag ceremony invariants. No signing material is stored in this repo.
set -euo pipefail
# Read git objects as stored: no caller repository or object-store redirects
# (GIT_*), and no replacement refs.
for _git_var in $(compgen -e); do
    case "$_git_var" in
        GIT_COMMITTER_NAME | GIT_COMMITTER_EMAIL | GIT_AUTHOR_NAME | GIT_AUTHOR_EMAIL) ;;
        GIT_*) unset "$_git_var" ;;
    esac
done
unset _git_var
export GIT_NO_REPLACE_OBJECTS=1

tag_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
# shellcheck source=scripts/release-decernor.sh
source "$tag_root/scripts/release-decernor.sh"
# shellcheck source=scripts/release-gh.sh
source "$tag_root/scripts/release-gh.sh"

tag_discard_verify_home() {
    if [[ -n "${TAG_VERIFY_GNUPGHOME:-}" ]]; then
        gpgconf --homedir "$TAG_VERIFY_GNUPGHOME" --kill all > /dev/null 2>&1 || true
        rm -rf "$TAG_VERIFY_GNUPGHOME"
        TAG_VERIFY_GNUPGHOME=""
    fi
}

# Only verification homes are disposable. The operator owns the signing
# keyring and its agent/card state; never remove it or stop its agent.
tag_cleanup_homes() {
    tag_discard_verify_home
}

# Expand ~ and collapse . / .. without following symlinks. Relative paths fail.
tag_canon_path() {
    python3 -c '
import os, sys
raw = sys.argv[1]
p = os.path.expanduser(raw)
if not p.startswith("/"):
    raise SystemExit("relative")
parts = []
for part in p.split("/"):
    if part in ("", "."):
        continue
    if part == "..":
        if parts:
            parts.pop()
        continue
    parts.append(part)
print("/" + "/".join(parts) if parts else "/")
' "$1"
}

# A 404 from the ref API is the only "absent" answer; any other failure stops.
remote_tag_absent() {
    local out
    if out="$(release_gh api "repos/3leaps/gonimbus/git/ref/tags/$GONIMBUS_RELEASE_TAG" 2>&1)"; then
        return 1
    fi
    [[ "$out" == *"HTTP 404"* ]] || tag_die "could not query the remote tag: $out"
}

tag_die() {
    echo "error: $*" >&2
    return 1
}

tag_identity() {
    [[ "${GONIMBUS_TAGGER_NAME:-} <${GONIMBUS_TAGGER_EMAIL:-}>" == "$(cat "$tag_root/config/release/tagger-identity.txt")" ]] || {
        tag_die 'infosec tagger identity required'
        return 1
    }
    [[ "$GONIMBUS_TAGGER_NAME" != *$'\n'* && "$GONIMBUS_TAGGER_NAME" != *$'\r'* &&
        "$GONIMBUS_TAGGER_NAME" != *'<'* && "$GONIMBUS_TAGGER_NAME" != *'>'* ]] || {
        tag_die 'invalid tagger name'
        return 1
    }
    [[ "$GONIMBUS_TAGGER_EMAIL" != *$'\n'* && "$GONIMBUS_TAGGER_EMAIL" != *$'\r'* &&
        "$GONIMBUS_TAGGER_EMAIL" != *'<'* && "$GONIMBUS_TAGGER_EMAIL" != *'>'* ]] || {
        tag_die 'invalid tagger email'
        return 1
    }
}

tag_version() {
    [[ -n "${GONIMBUS_RELEASE_TAG:-}" ]] || {
        tag_die 'GONIMBUS_RELEASE_TAG required'
        return 1
    }
    "$tag_root/scripts/release-guard-tag-version.sh" > /dev/null
}

tag_message_file() {
    [[ -n "${GONIMBUS_TAG_MESSAGE_DIR:-}" && -d "$GONIMBUS_TAG_MESSAGE_DIR" &&
        ! -L "$GONIMBUS_TAG_MESSAGE_DIR" ]] || {
        tag_die 'tag message directory required'
        return 1
    }
    [[ "${GONIMBUS_TAG_MESSAGE_DIR%/}" == */"$GONIMBUS_RELEASE_TAG" ]] || {
        tag_die 'message directory must end in tag'
        return 1
    }
    [[ "$GONIMBUS_TAG_MESSAGE_DIR" == /* && "/$GONIMBUS_TAG_MESSAGE_DIR/" != *'/../'* ]] || {
        tag_die 'absolute message directory without parent traversal required'
        return 1
    }
    local component="${GONIMBUS_TAG_MESSAGE_DIR%/}"
    while [[ "$component" != / ]]; do
        [[ ! -L "$component" ]] || {
            tag_die 'message directory may not contain symlink ancestors'
            return 1
        }
        component="${component%/*}"
        [[ -n "$component" ]] || component=/
    done
    local canonical
    canonical="$(cd "$GONIMBUS_TAG_MESSAGE_DIR" && pwd -P)"
    local probe cur
    for probe in "${GONIMBUS_TAG_MESSAGE_DIR%/}" "$canonical"; do
        cur="$probe"
        while true; do
            if [[ -e "$cur" && "$cur" -ef "$tag_root" ]]; then
                tag_die 'message directory must be outside repository'
                return 1
            fi
            [[ "$cur" != / ]] || break
            cur="$(dirname "$cur")"
        done
    done
    case "$canonical" in "$tag_root" | "$tag_root"/*)
        tag_die 'message directory must be outside repository'
        return 1
        ;;
    esac
    local file="${GONIMBUS_TAG_MESSAGE_DIR%/}/${GONIMBUS_RELEASE_TAG//\//__}.txt"
    if [[ ! -e "$file" && ! -L "$file" && ( -e "${GONIMBUS_TAG_MESSAGE_DIR%/}/message.txt" || -L "${GONIMBUS_TAG_MESSAGE_DIR%/}/message.txt" ) ]]; then
        file="${GONIMBUS_TAG_MESSAGE_DIR%/}/message.txt"
    fi
    [[ -f "$file" && ! -L "$file" && -s "$file" ]] || {
        tag_die 'tag message file required'
        return 1
    }
    "$tag_root/scripts/release-prepare-tag-message.py" --validate-message "$file" || return 1
    printf '%s\n' "$file"
}

tag_origin() {
    local url host
    url="$(git config --get remote.origin.url)"
    case "$url" in
        https://github.com/3leaps/gonimbus | https://github.com/3leaps/gonimbus.git | \
            git@github.com:3leaps/gonimbus | git@github.com:3leaps/gonimbus.git) ;;
        git@*:3leaps/gonimbus | git@*:3leaps/gonimbus.git)
            host="${url#git@}"
            host="${host%%:*}"
            [[ "$(ssh -G "$host" 2> /dev/null | awk '$1=="hostname" {print $2;exit}')" == github.com ]] || {
                tag_die 'origin SSH host must resolve to github.com'
                return 1
            }
            ;;
        *)
            tag_die 'origin must be 3leaps/gonimbus on GitHub'
            return 1
            ;;
    esac
}

tag_checkout() {
    tag_origin
    git fetch --quiet origin '+refs/heads/main:refs/remotes/origin/main'
    [[ -z "$(git status --porcelain --untracked-files=normal)" ]] || tag_die 'clean checkout required'
    [[ "$(git rev-parse HEAD)" == "$(git rev-parse refs/remotes/origin/main)" ]] || tag_die 'HEAD must match fetched origin/main'
}

tag_selector_shape() {
    [[ "${GONIMBUS_PGP_KEY_ID:-}" =~ ^[0-9A-F]{40}!$ ]] || {
        tag_die 'exact uppercase signing subkey fingerprint with ! required'
        return 1
    }
    [[ "${GONIMBUS_GPG_SIGNING_FINGERPRINT:-}" =~ ^[0-9A-F]{40}$ ]] || {
        tag_die 'uppercase primary fingerprint required'
        return 1
    }
}

tag_stat_mode() {
    if stat -c '%a' "$1" > /dev/null 2>&1; then
        stat -c '%a' "$1"
    else
        stat -f '%OLp' "$1"
    fi
}

# Fresh isolated keyring for selector verification. Never reuse an existing
# directory or follow a symlink: mktemp -d, refuse -L, chmod 0700, kill the
# agent on cleanup.
tag_fresh_verify_gnupg_home() {
    local dir mode
    dir="$(mktemp -d /tmp/gonimbus-tag-gnupg.XXXXXX)" || {
        tag_die 'cannot create GnuPG home'
        return 1
    }
    if [[ -L "$dir" || ! -d "$dir" ]]; then
        rm -rf "$dir"
        tag_die 'GnuPG home must be a fresh directory, not a symlink'
        return 1
    fi
    chmod 700 "$dir" || {
        rm -rf "$dir"
        tag_die 'cannot chmod GnuPG home'
        return 1
    }
    mode="$(tag_stat_mode "$dir")"
    [[ "$mode" == "700" ]] || {
        rm -rf "$dir"
        tag_die 'GnuPG home must be mode 0700'
        return 1
    }
    TAG_VERIFY_GNUPGHOME="$dir"
}

# Validate the explicitly selected operator keyring. Canonicalize (~, trailing
# slash, ., ..) before comparing to the default homedir; refuse non-0700
# (do not chmod). Signing uses this home and its existing agent/card setup.
tag_bind_operator_gnupg_home() {
    local raw_home="${GONIMBUS_GPG_HOMEDIR:-}"
    [[ ( "$raw_home" == /* || "$raw_home" == [A-Za-z]:/* || "$raw_home" == [A-Za-z]:\\* || "$raw_home" == \\\\* ) && -d "$raw_home" && ! -L "$raw_home" ]] || {
        tag_die 'external GPG home required'
        return 1
    }
    local resolved
    resolved="$(
        python3 -I - "$raw_home" "$tag_root" "${HOME:-}" << 'PY'
import os
import pathlib
import subprocess
import sys

probe = os.fsencode(sys.argv[1])
seps = (b"/", b"\\") if os.name == "nt" else (b"/",)
dot_seps = (b"/.", b"\\.") if os.name == "nt" else (b"/.",)
dotdot_seps = (b"/..", b"\\..") if os.name == "nt" else (b"/..",)
while True:
    if len(probe) > 1 and probe.endswith(seps):
        probe = probe[:-1]
    elif probe.endswith(dot_seps):
        probe = probe[:-2] or b"/"
    else:
        break
if probe.endswith(dotdot_seps) or os.path.islink(probe):
    print("error: GPG home must not be a symlink", file=sys.stderr)
    sys.exit(1)

def is_win_style(raw: str) -> bool:
    return (
        os.name == "nt"
        or os.path.sep == "\\"
        or raw.startswith("\\\\")
        or (len(raw) >= 3 and raw[0].isalpha() and raw[1:3] in (":/", ":\\"))
    )

def resolve_path(raw: bytes) -> str:
    s = os.fsdecode(raw)
    if os.name != "nt" and is_win_style(s):
        return s
    return os.fsdecode(os.path.realpath(raw))

if not is_win_style(os.fsdecode(probe)):
    cur_check = probe
    while cur_check and cur_check != b"/":
        if os.path.islink(cur_check):
            print("error: GPG home may not contain symlink ancestors", file=sys.stderr)
            sys.exit(1)
        cur_check = os.path.dirname(cur_check)

home = pathlib.Path(resolve_path(os.fsencode(sys.argv[1])))
repo_root = pathlib.Path(resolve_path(os.fsencode(sys.argv[2])))
env_home = sys.argv[3]

def strip_line_terminator(raw: bytes) -> bytes:
    if (os.name == "nt" or os.path.sep == "\\") and raw.endswith(b"\r\n"):
        return raw[:-2]
    if raw.endswith(b"\r\n") and is_win_style(os.fsdecode(raw[:-2])):
        return raw[:-2]
    if raw.endswith(b"\n"):
        return raw[:-1]
    return raw

def same_or_within(path: pathlib.Path, target: pathlib.Path) -> bool:
    p_str = str(path)
    t_str = str(target)
    if is_win_style(p_str) or is_win_style(t_str):
        p_low = p_str.replace("\\", "/").lower().rstrip("/")
        t_low = t_str.replace("\\", "/").lower().rstrip("/")
    else:
        p_low = p_str.lower().rstrip("/")
        t_low = t_str.lower().rstrip("/")
    if p_low == t_low or p_low.startswith(t_low + "/"):
        return True
    for cur in (path, *path.parents):
        try:
            if target.exists() and cur.exists() and os.path.samefile(cur, target):
                return True
            if (
                cur.parent.exists()
                and target.parent.exists()
                and os.path.samefile(cur.parent, target.parent)
                and cur.name.lower() == target.name.lower()
            ):
                return True
        except OSError:
            continue
    return False

repo_roots = {repo_root}
for cwd_candidate in (os.getcwd(), str(repo_root)):
    if not (pathlib.Path(cwd_candidate) / ".git").exists():
        continue
    try:
        wt_records = subprocess.run(
            ["git", "-C", cwd_candidate, "worktree", "list", "--porcelain", "-z"],
            check=True,
            capture_output=True,
        ).stdout.split(b"\0")
    except (OSError, subprocess.CalledProcessError):
        print("error: failed to enumerate repository worktrees", file=sys.stderr)
        sys.exit(1)
    found_worktree = False
    for rec in wt_records:
        if rec.startswith(b"worktree "):
            wt_bytes = rec[len(b"worktree ") :]
            if not wt_bytes:
                print("error: empty worktree path from git worktree list", file=sys.stderr)
                sys.exit(1)
            found_worktree = True
            repo_roots.add(pathlib.Path(os.fsdecode(os.path.realpath(wt_bytes))))
    if not found_worktree:
        print("error: no repository worktrees reported by git worktree list", file=sys.stderr)
        sys.exit(1)
    for git_flag in ("--git-dir", "--git-common-dir"):
        try:
            git_dir_raw = strip_line_terminator(
                subprocess.run(
                    ["git", "-C", cwd_candidate, "rev-parse", "--path-format=absolute", git_flag],
                    check=True,
                    capture_output=True,
                ).stdout
            )
        except (OSError, subprocess.CalledProcessError):
            print(f"error: failed to resolve repository {git_flag}", file=sys.stderr)
            sys.exit(1)
        if not git_dir_raw:
            print(f"error: empty repository {git_flag}", file=sys.stderr)
            sys.exit(1)
        repo_roots.add(pathlib.Path(os.fsdecode(os.path.realpath(git_dir_raw))))

for r_root in repo_roots:
    if same_or_within(home, r_root):
        print("error: GPG home must be outside repository", file=sys.stderr)
        sys.exit(1)

candidates = set()
for home_candidate in (
    env_home,
    os.environ.get("USERPROFILE", ""),
    (os.environ.get("HOMEDRIVE", "") + os.environ.get("HOMEPATH", "")) if os.environ.get("HOMEDRIVE") and os.environ.get("HOMEPATH") else "",
    os.path.expanduser("~"),
):
    if home_candidate and home_candidate != "~":
        candidates.add(pathlib.Path(os.path.realpath(os.path.expanduser(home_candidate))) / ".gnupg")
for appdata_candidate in (os.environ.get("APPDATA", ""), os.environ.get("LOCALAPPDATA", "")):
    if appdata_candidate:
        candidates.add(pathlib.Path(os.path.realpath(os.path.expanduser(appdata_candidate))) / "gnupg")
try:
    import pwd

    acct = pathlib.Path(os.path.realpath(pwd.getpwuid(os.getuid()).pw_dir))
    candidates.add(acct / ".gnupg")
except (ImportError, AttributeError, KeyError, OSError):
    pass
env = {k: v for k, v in os.environ.items() if k != "GNUPGHOME"}
try:
    out = strip_line_terminator(
        subprocess.run(
            ["gpgconf", "--list-dirs", "homedir"],
            env=env,
            check=True,
            capture_output=True,
        ).stdout
    )
    out_str = os.fsdecode(out)
    if is_win_style(out_str) or out.startswith(b"/") or os.path.isabs(out_str):
        candidates.add(pathlib.Path(resolve_path(out)))
        candidates.add(pathlib.Path(out_str))
except (OSError, subprocess.CalledProcessError):
    pass

for cand in candidates:
    if same_or_within(home, cand):
        print("error: GPG home must not be the default homedir", file=sys.stderr)
        sys.exit(1)

sys.stdout.write(str(home) + "x")
PY
    )" || return 1
    resolved="${resolved%x}"
    local mode
    mode="$(tag_stat_mode "$resolved")"
    [[ "$mode" == "700" ]] || {
        tag_die 'GPG home must be mode 0700'
        return 1
    }
    GONIMBUS_GPG_HOMEDIR="$resolved"
}

# Use the operator's keyring directly: hardware-backed signing subkeys cannot
# be exported into a temporary home. Public verification stays isolated.
tag_prepare_signing_home() {
    tag_bind_operator_gnupg_home || return 1
    export GNUPGHOME="$GONIMBUS_GPG_HOMEDIR"
}

tag_key_selector() {
    tag_selector_shape || return 1
    resolve_release_decernor ceremony || {
        tag_die 'trusted Decernor identity/version check failed'
        return 1
    }
    "$tag_root/scripts/release-verify-committed-anchors.sh" > /dev/null || return 1
    [[ "$(awk '$1=="gpg" {print $2}' "$tag_root/keys/expected-fingerprints.txt")" == "$GONIMBUS_GPG_SIGNING_FINGERPRINT" ]] || {
        tag_die 'operator fingerprint differs from committed anchor'
        return 1
    }
    tag_bind_operator_gnupg_home || return 1
    tag_fresh_verify_gnupg_home || return 1
    if ! gpg --homedir "$TAG_VERIFY_GNUPGHOME" --batch --quiet --import \
        "$tag_root/docs/security/release-signing-keys.asc" 2> /dev/null; then
        tag_discard_verify_home
        tag_die 'committed public pin failed to import'
        return 1
    fi
    local listing primary subkey
    if ! listing="$(gpg --homedir "$TAG_VERIFY_GNUPGHOME" --batch --with-colons --fingerprint --with-subkey-fingerprint --list-keys "${GONIMBUS_PGP_KEY_ID%!}" 2> /dev/null)"; then
        tag_discard_verify_home
        tag_die 'signing subkey unavailable'
        return 1
    fi
    primary="$(printf '%s\n' "$listing" | awk -F: '$1=="pub" {p=1;next} p && $1=="fpr" {print $10;exit}')"
    subkey="$(printf '%s\n' "$listing" | awk -F: -v f="${GONIMBUS_PGP_KEY_ID%!}" '$1=="sub" {s=1;cap=$12;next} s && $1=="fpr" {if ($10==f && cap ~ /[sS]/) print $10; s=0}')"
    if [[ "$primary" != "$GONIMBUS_GPG_SIGNING_FINGERPRINT" || "$subkey" != "${GONIMBUS_PGP_KEY_ID%!}" ]]; then
        tag_discard_verify_home
        tag_die 'signing subkey not on pinned primary'
        return 1
    fi
    tag_discard_verify_home
}

tag_expected_message() {
    local file
    file="$(tag_message_file)" || return 1
    cat "$file"
}

tag_verify_object() {
    local object="$1" expected_file="$2" tagger actual_file
    [[ "$(git cat-file -t "$object" 2> /dev/null)" == tag ]] || {
        tag_die 'annotated tag object required'
        return 1
    }
    [[ "$(git cat-file tag "$object" | sed -n 's/^tag //p' | head -1)" == "$GONIMBUS_RELEASE_TAG" ]] || {
        tag_die 'tag name mismatch'
        return 1
    }
    [[ "$(git rev-parse "${object}^{}")" == "$(git rev-parse HEAD)" ]] || {
        tag_die 'tag target mismatch'
        return 1
    }
    [[ "$(git rev-parse HEAD)" == "$(git rev-parse refs/remotes/origin/main)" ]] || {
        tag_die 'tag not at main'
        return 1
    }
    tagger="$(git cat-file tag "$object" | sed -n 's/^tagger \(.*\) [0-9][0-9]* [+-][0-9][0-9][0-9][0-9]$/\1/p' | head -1)"
    [[ "$tagger" == "$GONIMBUS_TAGGER_NAME <$GONIMBUS_TAGGER_EMAIL>" ]] || {
        tag_die 'tagger identity mismatch'
        return 1
    }
    actual_file="$(mktemp)"
    if ! "$tag_root/scripts/release-tag-body.sh" "$object" > "$actual_file"; then
        rm -f "$actual_file"
        return 1
    fi
    if ! cmp -s "$expected_file" "$actual_file"; then
        rm -f "$actual_file"
        tag_die 'signed tag message mismatch'
        return 1
    fi
    rm -f "$actual_file"
}
