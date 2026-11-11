#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
# The jq keyword guard refuses a keyword-named binding in a tracked script
# and accepts the tree as committed.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
scratch="$(mktemp -d)"
trap 'rc=$?; rm -rf "$scratch"; exit "$rc"' EXIT
# The tracked files as they are in the working tree.
(cd "$root" && git ls-files -z | xargs -0 tar -cf -) | tar -x -C "$scratch"
git -C "$scratch" init -q
git -C "$scratch" config core.autocrlf false
git -C "$scratch" add -A
"$scratch/scripts/jq-portability.sh" >/dev/null
for name in module import if def; do
	printf 'jq -n --arg %s x %s\n' "$name" "'\$$name'" >"$scratch/scripts/probe.sh"
	git -C "$scratch" add scripts/probe.sh
	if "$scratch/scripts/jq-portability.sh" >"$scratch/out" 2>&1; then
		echo "error: accepted a jq variable named $name" >&2
		exit 1
	fi
	grep -q 'named after a jq keyword' "$scratch/out" || { cat "$scratch/out" >&2; exit 1; }
done
printf 'jq -n --arg module_object x %s\n' "'\$module_object'" >"$scratch/scripts/probe.sh"
git -C "$scratch" add scripts/probe.sh
"$scratch/scripts/jq-portability.sh" >/dev/null
for snippet in \
	'compat=(); maturin build "${compat[@]}"' \
	'compat=(); if false; then compat+=(--zig); fi; maturin build "${compat[@]}"' \
	'compat=(); maturin build "${compat[@]}"; compat+=(--zig)' \
	'compat=(); : "${#compat[@]}"; maturin build "${compat[@]}"' \
	'local a=1 compat=(); maturin build "${compat[@]}"' \
	'local -a a compat; maturin build "${compat[@]}"' \
	'g() { local -a compat; printf "%s\n" "${compat[@]}"; }; g' \
	'false || local -a compat; printf "%s\n" "${compat[@]}"' \
	'case x in x) local -a compat ;; esac; printf "%s\n" "${compat[@]}"' \
	'local -a "compat"; printf "%s\n" "${compat[@]}"' \
	'local -a compat="()"; printf "%s\n" "${compat[@]}"' \
	'local "-a" compat; printf "%s\n" "${compat[@]}"' \
	'local -a co"mp"at; printf "%s\n" "${compat[@]}"' \
	'local -a co\mpat; printf "%s\n" "${compat[@]}"' \
	$'local -a compat=$\x27()\x27; printf "%s\\n" "${compat[@]}"' \
	'local -a compat=\(\); printf "%s\n" "${compat[@]}"' \
	'builtin local -a compat; printf "%s\n" "${compat[@]}"' \
	'command local -a compat; printf "%s\n" "${compat[@]}"' \
	'! local -a compat; printf "%s\n" "${compat[@]}"' \
	'local -a compat; printf "%s\n" "${compat["@"]}"' \
	'local -a compat; printf "%s\n" "${compat[\@]}"' \
	'local -a compat; printf "%s\n" "${compat[@]:0}"' \
	$'compat=(); printf "%s\\n" "${compat[$\x27\\x40\x27]}"' \
	$'compat=(); printf "%s\\n" "${compat[$\x27\\100\x27]}"' \
	$'compat=(); printf "%s\\n" "${compat[$\x27\\x2a\x27]}"' \
	$'compat=(); printf "%s\\n" "${compat[$\x27\\052\x27]}"' \
	'compat=(); printf "%s\n" \${compat[@]+"${compat[@]}"}' \
	'compat=(); printf "%s\n" \\\${compat[@]+"${compat[@]}"}' \
	'compat=(); printf "%s\n" $${compat[@]+"${compat[@]}"}' \
	'compat=(); printf "%s\n" $$$${compat[@]+"${compat[@]}"}' \
	'compat=(); printf "%s\n" $${compat[*]+"${compat[*]}"}' \
	'compat=(); printf "%s\n" $$$${compat[*]+"${compat[*]}"}' \
	$'compat=()\nprintf "%s\\n" $\\\n${compat[@]+"${compat[@]}"}' \
	$'compat=()\nprintf "%s\\n" $\\\n\\\n${compat[@]+"${compat[@]}"}' \
	$'compat=()\nprintf "%s\\n" $\\\n${compat[*]+"${compat[*]}"}' \
	$'compat=()\nprintf "%s\\n" $\\\n\\\n${compat[*]+"${compat[*]}"}' \
	'compat=(); x=`printf "%s\n" \\${compat[@]+"${compat[@]}"}`' \
	'compat=(); x=`printf "%s\n" \\${compat[*]+"${compat[*]}"}`' \
	$'compat=()\nx=`printf "%s\\n" \\\\\\\n${compat[@]+"${compat[@]}"}\n`' \
	$'compat=()\nx=`printf "%s\\n" \\\\\\\n${compat[*]+"${compat[*]}"}\n`' \
	$'compat=()\nx=`printf "%s\\n" \\\\\\\\\\\\\\\n${compat[@]+"${compat[@]}"}\n`' \
	$'compat=()\n# ${compat[@]+"\\\n${compat[@]}"}"' \
	$'compat=()\n: # ${compat[@]+"\\\n${compat[@]}"}"' \
	$'compat=()\nprintf "%s\\n" "\n# ${compat[@]}\n"' \
	$'compat=()\nprintf "%s\\n" \\\n#${compat[@]}' \
	$'compat=()\r#${compat[@]}' \
	$'compat=()\v#${compat[@]}' \
	$'compat=()\f#${compat[@]}' \
	$'compat=()\x1c#${compat[@]}' \
	$'compat=()\x1d#${compat[@]}' \
	$'compat=()\x1e#${compat[@]}' \
	$'compat=()\xc2\x85#${compat[@]}' \
	$'compat=()\xe2\x80\xa8#${compat[@]}' \
	$'compat=()\xe2\x80\xa9#${compat[@]}' \
	$'local \\\n-a compat\nmaturin build "${compat[@]}"' \
	$'compat=()\nmaturin build "${compat[\\\n@]}"'; do
	printf '#!/usr/bin/env bash\nset -euo pipefail\nf() {\n%s\n}\n' "$snippet" >"$scratch/scripts/probe.sh"
	git -C "$scratch" add scripts/probe.sh
	if (cd "$scratch" && "$scratch/scripts/jq-portability.sh") >"$scratch/out" 2>&1; then
		echo "error: accepted unguarded Bash 3.2 empty-array expansion: $snippet" >&2
		exit 1
	fi
	grep -q 'empty array compat expanded without Bash 3.2' "$scratch/out" || { cat "$scratch/out" >&2; exit 1; }
done
printf '#!/usr/bin/env bash\nset -euo pipefail\ncompat=()\nscalar="()"\nprintf "%%s\\n" "$scalar"\nmaturin build ${compat[@]+"${compat[@]}"}\n' >"$scratch/scripts/probe.sh"
git -C "$scratch" add scripts/probe.sh
(cd "$scratch" && "$scratch/scripts/jq-portability.sh") >/dev/null
echo '[ok] jq keyword guard'
