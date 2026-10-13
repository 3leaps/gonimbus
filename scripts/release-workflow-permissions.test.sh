#!/usr/bin/env bash
# The tag-triggered release workflow is defined by the tagged commit, so it must
# never hold write authority or create a release. Guard both statically, on the
# resolved YAML (flow mappings, quoted keys, aliases and merges included).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
workflow="${1:-$root/.github/workflows/release.yml}"
resolved="$(mktemp)"
trap 'rm -f "$resolved"' EXIT
"$root/scripts/workflow-yaml.sh" "$workflow" > "$resolved"
python3 -I - "$workflow" "$resolved" << 'PY'
import json
import sys

doc = json.load(open(sys.argv[2], encoding='utf-8'))
text = open(sys.argv[1], encoding='utf-8').read()
errors = []


def read_only(perms, where, required):
    if perms is None:
        if required:
            errors.append(f'{where}: permissions must be set to contents: read')
        return
    if perms != {'contents': 'read'}:
        errors.append(f'{where}: permissions must be exactly contents: read, got {json.dumps(perms)}')


read_only(doc.get('permissions'), 'workflow', True)
for name, job in (doc.get('jobs') or {}).items():
    read_only(job.get('permissions'), f'job {name}', False)
    for step in job.get('steps') or []:
        uses = str(step.get('uses', ''))
        if 'action-gh-release' in uses:
            errors.append(f'job {name}: release-creating action {uses}')
for banned in ('gh release ', 'releases/', 'GITHUB_TOKEN: ${{ secrets'):
    if banned in text:
        errors.append(f'release-creating construct present: {banned!r}')
if errors:
    raise SystemExit('error: ' + '; '.join(errors))
PY
echo '[ok] release workflow is read-only and never creates a release'
