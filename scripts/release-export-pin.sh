#!/usr/bin/env bash
# Maintainer-only: explicitly export an approved public key to an absent pin.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
stop() {
    echo "STOP: $1" >&2
    exit 1
}

: "${GONIMBUS_GPG_HOMEDIR:?load the approved GPG home}"
: "${GONIMBUS_PGP_KEY_ID:?load the approved signing-subkey selector}"
: "${GONIMBUS_GPG_SIGNING_FINGERPRINT:?load the approved primary fingerprint}"
: "${GONIMBUS_MINISIGN_PUB:?load the approved minisign public export}"
[[ "$GONIMBUS_GPG_HOMEDIR" == /* && -d "$GONIMBUS_GPG_HOMEDIR" ]] || stop 'approved GPG home must be an absolute directory'
[[ -f "$GONIMBUS_MINISIGN_PUB" && -s "$GONIMBUS_MINISIGN_PUB" && ! -L "$GONIMBUS_MINISIGN_PUB" ]] ||
    stop 'minisign public export must be a nonempty regular file, not a symlink'
python3 - "$GONIMBUS_GPG_HOMEDIR" "$HOME/.gnupg" "$root" << 'PY'
from pathlib import Path
import sys

home, default, repo = (Path(p).resolve() for p in sys.argv[1:4])
if home == default:
    raise SystemExit('STOP: default GPG home is not approved for this export')
if home == repo or repo in home.parents:
    raise SystemExit('STOP: GPG home must be outside the repository')
PY
[[ "$GONIMBUS_PGP_KEY_ID" == *'!' ]] || stop 'signing-subkey selector must end with !'
pin=docs/security/release-signing-keys.asc
[[ ! -e "$pin" && ! -L "$pin" ]] || stop 'public pin already exists; use make release-validate-pin'
[[ ! -L docs/security && (! -e docs/security || -d docs/security) ]] ||
    stop 'public pin directory must be a regular directory, not a symlink'
mkdir -p docs/security || stop 'cannot create public pin directory'
set -C
gpg --homedir "$GONIMBUS_GPG_HOMEDIR" --batch --armor \
    --export "$GONIMBUS_PGP_KEY_ID" > "$pin" || stop 'public GPG export failed; inspect the newly created file before retrying'
"$root/scripts/release-validate-pin.sh"
