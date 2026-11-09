#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Verify the GitHub release settings that live outside the repository:
#
#   1. The "Tag Publish Protection" tag ruleset on 3leaps/gonimbus is active,
#      covers refs/tags/v* with no exclusions, requires creation, update,
#      deletion and non_fast_forward, and (when the caller has admin read access
#      to see bypass_actors) allows only OrganizationAdmin to bypass.
#   2. Immutable releases are enabled on 3leaps/gonimbus.
#
# Read-only: calls `gh api` GET endpoints only. Run by `make release-preflight`
# and standalone via `make release-github-preflight`.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/release-gh.sh
source "$root/scripts/release-gh.sh"

repo="3leaps/gonimbus"
ruleset_name="Tag Publish Protection"

die() {
	echo "error: $*" >&2
	exit 1
}

for tool in gh jq; do
	command -v "$tool" >/dev/null 2>&1 || die "$tool not found in PATH"
done

rulesets_json="$(release_gh api "repos/$repo/rulesets?per_page=100")" ||
	die "could not read rulesets for $repo (check GitHub CLI auth status)"

ruleset_id="$(jq -r --arg name "$ruleset_name" '
	[ .[] | select(.name == $name and .target == "tag") ] |
	if length == 1 then .[0].id else empty end
' <<<"$rulesets_json")"
[[ -n "$ruleset_id" ]] ||
	die "expected exactly one tag ruleset named '$ruleset_name' on $repo"

ruleset="$(release_gh api "repos/$repo/rulesets/$ruleset_id")" ||
	die "could not read ruleset $ruleset_id on $repo"

# GitHub omits `bypass_actors` when the caller lacks repository admin access;
# every other ruleset field is visible to any reader. Validate the ruleset shape
# unconditionally, and check `bypass_actors` when the API returned it.
jq -e --arg repo "$repo" --arg name "$ruleset_name" '
	.name == $name and
	.source_type == "Repository" and
	.source == $repo and
	.target == "tag" and
	.enforcement == "active" and
	(.conditions.ref_name.include | sort) == [
		"refs/tags/go/*/v*",
		"refs/tags/spec-v*",
		"refs/tags/v*"
	] and
	.conditions.ref_name.exclude == [] and
	([.rules[].type] | sort) == [
		"creation",
		"deletion",
		"non_fast_forward",
		"update"
	] and
	([.rules[] | has("parameters")] | all(. == false)) and
	(
		.bypass_actors == null or
		.bypass_actors == [
			{
				"actor_id": null,
				"actor_type": "OrganizationAdmin",
				"bypass_mode": "always"
			}
		]
	)
' <<<"$ruleset" >/dev/null ||
	die "ruleset '$ruleset_name' ($ruleset_id) on $repo does not match the required tag protection policy"

bypass_note="OrganizationAdmin only"
if [[ "$(jq -r '.bypass_actors == null' <<<"$ruleset")" == true ]]; then
	bypass_note="bypass_actors: not visible, not checked"
fi
echo "[ok] ruleset '$ruleset_name' ($ruleset_id): active, refs/tags/v* protected ($bypass_note)"

immutable="$(release_gh api "repos/$repo/immutable-releases")" ||
	die "could not read immutable-releases for $repo"
jq -e '.enabled == true' <<<"$immutable" >/dev/null ||
	die "immutable releases are not enabled on $repo"
echo "[ok] immutable releases: enabled on $repo"

echo "[ok] GitHub release preflight passed for $repo"
