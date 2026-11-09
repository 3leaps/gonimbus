#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Source this file. release_gh runs the GitHub CLI against github.com only:
# an inherited host, repository or enterprise setting that could send a
# request to another forge is refused, and the host is pinned for every call.
release_gh() {
	local name
	for name in GH_HOST GH_REPO GH_ENTERPRISE_TOKEN GITHUB_ENTERPRISE_TOKEN; do
		[[ -z "${!name+x}" ]] || {
			echo "error: $name is set; release steps talk to github.com only" >&2
			return 1
		}
	done
	if [[ "${1:-}" == api ]]; then
		GH_HOST=github.com gh "$@" --hostname github.com
	else
		GH_HOST=github.com gh "$@"
	fi
}
