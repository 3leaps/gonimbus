#!/usr/bin/env bash
# Create and verify a signed tag locally; publication is a separate command.
set -euo pipefail
# shellcheck source=scripts/release-tag-common.sh
source "$(dirname "$0")/release-tag-common.sh"
cd "$tag_root"
tag_version
tag_identity
tag_key_selector
tag_checkout
remote_tag_absent || tag_die 'remote tag already exists'
[[ -z "$(git show-ref --tags "refs/tags/$GONIMBUS_RELEASE_TAG" || true)" ]] || tag_die 'local tag already exists'
message="$(mktemp)"
trap 'rm -f "$message"' EXIT
tag_expected_message > "$message"
export GIT_COMMITTER_NAME="$GONIMBUS_TAGGER_NAME" GIT_COMMITTER_EMAIL="$GONIMBUS_TAGGER_EMAIL"
git tag -s -a --cleanup=verbatim -u "$GONIMBUS_PGP_KEY_ID" -F "$message" "$GONIMBUS_RELEASE_TAG" HEAD
"$tag_root/scripts/release-verify-tag.sh"
echo '[ok] signed tag is local only'
