#!/usr/bin/env bash
# Every workflow is contents: read at workflow and job level, every action is
# pinned by full commit SHA with a version comment, no action pin is on the
# Node 20 deny-list, container and service images are digest-pinned, and hosted
# runners use an explicit image instead of a moving label. Policy is checked on
# the resolved YAML; version comments are read from the source text.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
shopt -s nullglob
if [[ $# -gt 0 ]]; then
    files=("$@")
else
    files=("$root"/.github/workflows/*.yml "$root"/.github/workflows/*.yaml)
fi
[[ ${#files[@]} -gt 0 ]] || {
    echo 'error: no workflow files found' >&2
    exit 1
}
status=0
for path in "${files[@]}"; do
    resolved="$(mktemp)"
    "$root/scripts/workflow-yaml.sh" "$path" > "$resolved" || {
        rm -f "$resolved"
        status=1
        continue
    }
    python3 -I - "$path" "$resolved" << 'PY' || status=1
import json
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

path = sys.argv[1]
name = path.rsplit('/', 1)[-1]
doc = json.load(open(sys.argv[2], encoding='utf-8'))
text = open(path, encoding='utf-8').read()
errors = []


def read_only(perms, where, required):
    if perms is None:
        if required:
            errors.append(f'{name}: {where} permissions must be set to contents: read')
    elif perms != {'contents': 'read'}:
        errors.append(f'{name}: {where} permissions must be exactly contents: read, got {json.dumps(perms)}')


def labels(value):
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str)]
    if isinstance(value, dict):
        return labels(value.get('labels'))
    return []


def image_ref(value):
    return value.get('image') if isinstance(value, dict) else value


def check_runner(label, where):
    if label.startswith('${{'):
        return
    if (label.endswith('-latest') or '-latest-' in label) and label not in LARGER_RUNNERS:
        errors.append(f'{name}: {where}: moving runner label: {label}')


def check_image(ref, where):
    if ref and not re.search(r'@sha256:[0-9a-f]{64}$', str(ref)):
        errors.append(f'{name}: {where}: image not digest-pinned: {ref}')


read_only(doc.get('permissions'), 'workflow', True)
for job_name, job in (doc.get('jobs') or {}).items():
    where = f'job {job_name}'
    read_only(job.get('permissions'), where, False)
    for label in labels(job.get('runs-on')):
        check_runner(label, where)
    for row in ((job.get('strategy') or {}).get('matrix') or {}).get('include') or []:
        if isinstance(row, dict):
            for label in labels(row.get('runner')):
                check_runner(label, f'{where} matrix')
    check_image(image_ref(job.get('container')), where)
    for svc_name, svc in (job.get('services') or {}).items():
        check_image(image_ref(svc), f'{where} service {svc_name}')
    uses_list = [job.get('uses')] + [s.get('uses') for s in job.get('steps') or []]
    for ref in uses_list:
        if not ref or str(ref).startswith('./'):
            continue
        ref = str(ref)
        if ref.startswith('docker://'):
            check_image(ref[len('docker://'):], where)
            continue
        if not re.fullmatch(r'[\w.-]+/[\w./-]+@[0-9a-f]{40}', ref):
            errors.append(f'{name}: {where}: action not pinned by full SHA: {ref}')
            continue
        if ref in NODE20:
            errors.append(f'{name}: {where}: action pin targets Node 20: {ref}')
        if not re.search(re.escape(ref) + r'\s+#\s*v\d', text):
            errors.append(f'{name}: {where}: pinned action lacks a version comment: {ref}')
if errors:
    raise SystemExit('error: ' + '\n'.join(errors))
PY
    rm -f "$resolved"
done
[[ "$status" == 0 ]] || exit 1
echo '[ok] workflows are read-only, SHA- and digest-pinned, off Node 20, on explicit runners'
