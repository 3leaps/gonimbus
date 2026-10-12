#!/usr/bin/env bash
# The tag-triggered release workflow is defined by the tagged commit, so it must
# never hold write authority or create a release. Guard both statically.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
workflow="$root/.github/workflows/release.yml"
python3 - "$workflow" << 'PY'
import re
import sys

text = open(sys.argv[1], encoding='utf-8').read()
errors = []
for match in re.finditer(r'^\s*([a-z-]+):\s*write\b', text, re.MULTILINE):
    errors.append(f'write permission granted: {match.group(1)}')
if re.search(r'^\s*permissions:\s*write-all\b', text, re.MULTILINE):
    errors.append('write-all permissions granted')
for banned in ('action-gh-release', 'gh release ', 'releases/', 'GITHUB_TOKEN: ${{ secrets'):
    if banned in text:
        errors.append(f'release-creating construct present: {banned!r}')
if not re.search(r'^permissions:\s*\n\s+contents:\s*read\b', text, re.MULTILINE):
    errors.append('workflow-level permissions must be contents: read')
if errors:
    raise SystemExit('error: ' + '; '.join(errors))
PY
echo '[ok] release workflow is read-only and never creates a release'
