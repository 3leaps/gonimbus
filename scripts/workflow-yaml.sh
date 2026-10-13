#!/usr/bin/env bash
# Print one workflow file as JSON after YAML resolution (anchors, aliases and
# merge keys expanded), so policy checks inspect the mapping GitHub sees rather
# than the text layout.
set -euo pipefail
[[ $# -eq 1 && -f "$1" ]] || {
    echo 'usage: workflow-yaml.sh WORKFLOW_FILE' >&2
    exit 2
}
yq --yaml-fix-merge-anchor-to-spec=true -o=json 'explode(.)' "$1"
