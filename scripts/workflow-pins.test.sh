#!/usr/bin/env bash
# Every workflow action is pinned by full commit SHA with a version comment, no
# action pin is on the Node 20 deny-list, container and service images are
# digest-pinned, every workflow is contents: read, and hosted runners use an
# explicit image instead of a moving label.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
python3 - "$root"/.github/workflows/*.yml << 'PY'
import re
import sys

# Pins known to target Node 20 (GitHub forces them onto Node 24 with a
# deprecation warning). Extend when a pinned action is found to run on node20.
NODE20 = {
    'actions/checkout@11d5960a326750d5838078e36cf38b85af677262',
    'actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02',
}
# Larger-runner labels name a runner group, not an image; there is no explicit
# image label to pin them to.
LARGER_RUNNERS = {'windows-latest-arm64-s'}
errors = []
for path in sys.argv[1:]:
    name = path.rsplit('/', 1)[-1]
    text = open(path, encoding='utf-8').read()
    if not re.search(r'^permissions:\s*\n\s+contents:\s*read\s*$', text, re.MULTILINE):
        errors.append(f'{name}: workflow-level permissions must be contents: read')
    if re.search(r'^\s*[a-z-]+:\s*write\b', text, re.MULTILINE) or 'write-all' in text:
        errors.append(f'{name}: write permission granted')
    for number, line in enumerate(text.splitlines(), 1):
        uses = re.match(r'^\s*(?:-\s+)?uses:\s*(\S+)(.*)$', line)
        if uses:
            ref, rest = uses.groups()
            if ref.startswith('./'):
                continue
            if not re.fullmatch(r'[\w.-]+/[\w./-]+@[0-9a-f]{40}', ref):
                errors.append(f'{name}:{number}: action not pinned by full SHA: {ref}')
            elif not re.search(r'#\s*v\d', rest):
                errors.append(f'{name}:{number}: pinned action lacks a version comment: {ref}')
            if ref in NODE20:
                errors.append(f'{name}:{number}: action pin targets Node 20: {ref}')
        image = re.match(r'^\s*image:\s*(\S+)', line)
        if image and not re.search(r'@sha256:[0-9a-f]{64}$', image.group(1)):
            errors.append(f'{name}:{number}: image not digest-pinned: {image.group(1)}')
        label = re.match(r'^\s*(?:-\s+)?(?:runs-on|runner):\s*(\S+)', line)
        if label and label.group(1).endswith('-latest') or (
                label and '-latest-' in label.group(1) and label.group(1) not in LARGER_RUNNERS):
            errors.append(f'{name}:{number}: moving runner label: {label.group(1)}')
if errors:
    raise SystemExit('error: ' + '\n'.join(errors))
PY
echo '[ok] workflows are read-only, SHA- and digest-pinned, off Node 20, on explicit runners'
