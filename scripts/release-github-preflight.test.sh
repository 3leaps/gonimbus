#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Controls for scripts/release-github-preflight.sh: stubs `gh api` and checks
# that every drifted ruleset or immutable-releases field is rejected.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/fx"

write_good_fixtures() {
	cat >"$scratch/fx/rulesets.json" <<'JSON'
[
  {
    "id": 24954486,
    "name": "Tag Publish Protection",
    "target": "tag",
    "source_type": "Repository",
    "source": "3leaps/gonimbus",
    "enforcement": "active"
  }
]
JSON
	cat >"$scratch/fx/ruleset.json" <<'JSON'
{
  "id": 24954486,
  "name": "Tag Publish Protection",
  "target": "tag",
  "source_type": "Repository",
  "source": "3leaps/gonimbus",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "exclude": [],
      "include": [
        "refs/tags/v*",
        "refs/tags/spec-v*",
        "refs/tags/go/*/v*"
      ]
    }
  },
  "rules": [
    {"type": "creation"},
    {"type": "update"},
    {"type": "deletion"},
    {"type": "non_fast_forward"}
  ],
  "bypass_actors": [
    {
      "actor_id": null,
      "actor_type": "OrganizationAdmin",
      "bypass_mode": "always"
    }
  ]
}
JSON
	cat >"$scratch/fx/immutable.json" <<'JSON'
{"enabled": true, "enforced_by_owner": false}
JSON
}

cat >"$scratch/bin/gh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == api ]] || { echo "stub gh: unexpected $*" >&2; exit 2; }
case "${2:-}" in
repos/3leaps/gonimbus/rulesets\?per_page=100)
	cat "$FIXTURE_DIR/rulesets.json"
	;;
repos/3leaps/gonimbus/rulesets/24954486)
	cat "$FIXTURE_DIR/ruleset.json"
	;;
repos/3leaps/gonimbus/immutable-releases)
	[[ "${FAIL_IMMUTABLE:-0}" == 0 ]] || exit 1
	cat "$FIXTURE_DIR/immutable.json"
	;;
*)
	echo "stub gh: unexpected endpoint ${2:-}" >&2
	exit 2
	;;
esac
SH
chmod +x "$scratch/bin/gh"
export PATH="$scratch/bin:$PATH" FIXTURE_DIR="$scratch/fx"

check() { "$root/scripts/release-github-preflight.sh" >"$scratch/out" 2>&1; }
refused() {
	local why="$1"
	shift
	write_good_fixtures
	"$@"
	if check; then
		echo "error: preflight accepted $why" >&2
		exit 1
	fi
}

mutate_json() {
	local file="$1" expr="$2"
	jq "$expr" "$file" >"$file.tmp" && mv "$file.tmp" "$file"
}

write_good_fixtures
check
grep -q "OrganizationAdmin only" "$scratch/out"

# Read-only token view: GitHub omits `bypass_actors`; preflight still passes and
# notes that bypass_actors was not visible.
write_good_fixtures
mutate_json "$scratch/fx/ruleset.json" 'del(.bypass_actors)'
check
grep -q "bypass_actors: not visible, not checked" "$scratch/out"

# Ruleset drift.
refused "disabled ruleset" mutate_json "$scratch/fx/ruleset.json" '.enforcement = "disabled"'
refused "evaluate ruleset" mutate_json "$scratch/fx/ruleset.json" '.enforcement = "evaluate"'
refused "missing non_fast_forward rule" mutate_json "$scratch/fx/ruleset.json" 'del(.rules[] | select(.type == "non_fast_forward"))'
refused "rule with parameters" mutate_json "$scratch/fx/ruleset.json" '.rules[0].parameters = {"x": 1}'
refused "tag exclusion" mutate_json "$scratch/fx/ruleset.json" '.conditions.ref_name.exclude = ["refs/tags/v0.0.0"]'
refused "missing v* include" mutate_json "$scratch/fx/ruleset.json" '.conditions.ref_name.include = ["refs/tags/spec-v*"]'
refused "wrong source_type" mutate_json "$scratch/fx/ruleset.json" '.source_type = "Organization"'
refused "wrong target" mutate_json "$scratch/fx/ruleset.json" '.target = "branch"'
refused "empty bypass_actors" mutate_json "$scratch/fx/ruleset.json" '.bypass_actors = []'
refused "extra bypass actor" mutate_json "$scratch/fx/ruleset.json" '.bypass_actors += [{"actor_id": 1, "actor_type": "Team", "bypass_mode": "always"}]'
refused "pull_request bypass_mode" mutate_json "$scratch/fx/ruleset.json" '.bypass_actors[0].bypass_mode = "pull_request"'
refused "specific actor_id on OrganizationAdmin" mutate_json "$scratch/fx/ruleset.json" '.bypass_actors[0].actor_id = 1'
refused "missing ruleset in list" mutate_json "$scratch/fx/rulesets.json" '. = []'
refused "duplicate ruleset in list" mutate_json "$scratch/fx/rulesets.json" '. += . '

# Immutable releases drift.
refused "immutable releases disabled" mutate_json "$scratch/fx/immutable.json" '.enabled = false'
write_good_fixtures
if FAIL_IMMUTABLE=1 check; then
	echo "error: preflight accepted failing immutable-releases endpoint" >&2
	exit 1
fi

echo "[ok] GitHub release preflight controls passed"
