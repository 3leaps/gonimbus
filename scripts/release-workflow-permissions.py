#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Static permission and action-pinning verifier for .github/workflows/release.yml."""

from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

try:
    import yaml
except (
    ModuleNotFoundError
):  # pragma: no cover - fallback when PyYAML is absent in container
    yaml = None

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"

ALLOWED_USES = {
    "actions/checkout": "3d3c42e5aac5ba805825da76410c181273ba90b1",
    "actions/setup-go": "924ae3a1cded613372ab5595356fb5720e22ba16",
    "actions/upload-artifact": "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
}

NODE20_DENY = {
    "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
    "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
}

LARGER_RUNNERS = {"windows-latest-arm64-s"}

FORBIDDEN_STEP_PATTERNS = [
    re.compile(r"\bgh\s+release\b"),
    re.compile(r"\bgh\s+pr\b"),
    re.compile(r"\bgh\s+issue\b"),
    re.compile(r"\bgh\s+workflow\b"),
    re.compile(r"\bgh\s+run\b"),
    re.compile(r"\bgh\s+auth\b"),
    re.compile(r"repos/[^/\s]+/[^/\s]+/releases\b"),
    re.compile(r"softprops/action-gh-release"),
    re.compile(r"actions/create-release"),
    re.compile(r"ncipollo/release-action"),
    re.compile(r"\bcurl\b"),
    re.compile(r"\bwget\b"),
    re.compile(r"\bgit\s+push\b"),
    re.compile(r"\bgit\s+tag\s+-f\b"),
]

ALLOWED_GH_API_LINE = (
    'tag_json="$(gh api "repos/3leaps/gonimbus/git/tags/$EXPECTED_OBJECT")"'
)


class ValidationError(Exception):
    pass


def check_error(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def inspect_yaml_ast(text: str) -> dict:
    check_error("\t" not in text, "workflow contains tab characters")
    check_error("\r" not in text, "workflow contains CR characters")
    if yaml is not None:
        try:
            tokens = list(yaml.scan(text, Loader=yaml.SafeLoader))
        except yaml.YAMLError as exc:
            raise ValidationError(f"invalid YAML tokens: {exc}") from exc
        for tok in tokens:
            check_error(
                not isinstance(tok, (yaml.AnchorToken, yaml.AliasToken)),
                "YAML anchors and aliases are forbidden in workflows",
            )
            if isinstance(tok, yaml.ValueToken):
                pass
        try:
            root = yaml.compose(text, Loader=yaml.SafeLoader)
        except yaml.YAMLError as exc:
            raise ValidationError(f"invalid YAML: {exc}") from exc
        check_error(
            isinstance(root, yaml.MappingNode), "workflow root must be a mapping"
        )

        def walk(node: yaml.Node) -> None:
            check_error(
                node.tag.startswith("tag:yaml.org,2002:"),
                f"non-standard YAML tag: {node.tag}",
            )
            if isinstance(node, yaml.MappingNode):
                check_error(
                    node.flow_style is not True,
                    "flow-style YAML mappings are forbidden in release.yml",
                )
                seen: set[str] = set()
                for key_node, val_node in node.value:
                    check_error(
                        isinstance(key_node, yaml.ScalarNode),
                        "mapping keys must be scalars",
                    )
                    check_error(
                        key_node.tag != "tag:yaml.org,2002:merge"
                        and key_node.value != "<<",
                        "YAML merge keys are forbidden",
                    )
                    check_error(
                        key_node.value not in seen,
                        f"duplicate YAML key: {key_node.value}",
                    )
                    seen.add(key_node.value)
                    walk(key_node)
                    walk(val_node)
            elif isinstance(node, yaml.SequenceNode):
                check_error(
                    node.flow_style is not True,
                    "flow-style YAML sequences are forbidden in release.yml",
                )
                for item in node.value:
                    walk(item)

        walk(root)
        loaded = yaml.safe_load(text)
        check_error(isinstance(loaded, dict), "loaded workflow must be a dict")
        return loaded

    # Fallback when PyYAML is not installed in a container environment
    stripped_expr = re.sub(r"\$\{\{.*?\}\}", "", text)
    in_block_indent: int | None = None
    for line in stripped_expr.splitlines():
        raw_stripped = line.lstrip(" ")
        indent = len(line) - len(raw_stripped)
        if in_block_indent is not None:
            if not raw_stripped or indent > in_block_indent:
                continue
            in_block_indent = None
        code = line.split("#", 1)[0]
        if re.match(r"^\s*run:\s*[|>]", code):
            in_block_indent = indent
            continue
        check_error("<<" not in code or "<<<" in code, "YAML merge keys are forbidden")
        check_error(
            not re.search(r"(^|[\s,:])&[A-Za-z0-9_-]+", code),
            "YAML anchors are forbidden",
        )
        check_error(
            not re.search(r"(^|[\s,:])\*[A-Za-z0-9_-]+", code),
            "YAML aliases are forbidden",
        )
        if not code.lstrip().startswith(("-", "if:", "run:", "[")):
            check_error(
                "{" not in code and "}" not in code,
                "flow-style YAML mappings are forbidden",
            )
    proc = subprocess.run(
        ["yq", "eval", "-o=json", "-"], input=text, text=True, capture_output=True
    )
    check_error(proc.returncode == 0, f"invalid YAML: {proc.stderr.strip()}")
    loaded = json.loads(proc.stdout)
    check_error(isinstance(loaded, dict), "loaded workflow must be a dict")
    return loaded


def validate_step_uses(job_name: str, step: dict, raw_text: str) -> None:
    uses = step.get("uses")
    check_error(
        isinstance(uses, str) and "@" in uses,
        f"{job_name}: invalid uses value {uses!r}",
    )
    action, ref = uses.split("@", 1)
    check_error(
        re.fullmatch(r"[0-9a-f]{40}", ref) is not None,
        f"{job_name}: action {action} is not pinned to a 40-char lowercase commit SHA: {ref}",
    )
    expected = ALLOWED_USES.get(action)
    check_error(
        expected == ref,
        f"{job_name}: action {action} pinned to {ref}, expected {expected}",
    )
    check_error(
        re.search(re.escape(uses) + r"\s+#\s*v\d", raw_text) is not None,
        f"{job_name}: pinned action lacks a version comment: {uses}",
    )
    if action == "actions/checkout":
        with_block = step.get("with")
        check_error(
            isinstance(with_block, dict)
            and with_block.get("persist-credentials") is False,
            f"{job_name}: actions/checkout must set persist-credentials: false",
        )


def validate_run_script(job_name: str, script: str) -> None:
    for pattern in FORBIDDEN_STEP_PATTERNS:
        check_error(
            pattern.search(script) is None,
            f"{job_name}: forbidden command pattern {pattern.pattern} in run script",
        )
    for raw_line in script.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if re.search(r"\bgh\b", line):
            check_error(
                job_name == "verify-signature" and line == ALLOWED_GH_API_LINE,
                f"{job_name}: forbidden gh invocation in run script: {line}",
            )


def validate_workflow_text(text: str) -> None:
    doc = inspect_yaml_ast(text)
    normalized_keys = {"on" if k is True else k for k in doc.keys()}
    expected_top = {"name", "on", "defaults", "permissions", "jobs"}
    check_error(
        normalized_keys == expected_top,
        f"unexpected top-level keys: {sorted(normalized_keys)}",
    )
    check_error(
        doc.get("defaults") == {"run": {"shell": "bash"}},
        "top-level defaults must be exactly {'run': {'shell': 'bash'}}",
    )
    check_error(
        doc.get("permissions") == {"contents": "read"},
        "top-level permissions must be exactly {'contents': 'read'}",
    )
    on_block = doc.get("on", doc.get(True))
    check_error(
        on_block == {"push": {"tags": ["v*"]}},
        "release workflow trigger must be push.tags: ['v*'] only",
    )

    jobs = doc.get("jobs")
    check_error(isinstance(jobs, dict), "jobs must be a mapping")
    check_error(
        set(jobs.keys()) == {"verify-signature", "build"},
        f"jobs must be exactly verify-signature and build, got {sorted(jobs.keys())}",
    )

    for job_name, job in jobs.items():
        check_error(isinstance(job, dict), f"{job_name}: job must be a mapping")
        for forbidden_key in (
            "environment",
            "secrets",
            "strategy",
            "container",
            "services",
            "uses",
        ):
            check_error(
                forbidden_key not in job,
                f"{job_name}: forbidden job key {forbidden_key}",
            )
        job_perms = job.get("permissions")
        check_error(
            job_perms == {"contents": "read"},
            f"{job_name}: job permissions must be exactly {{'contents': 'read'}}",
        )
        check_error(
            job.get("runs-on") == "ubuntu-24.04",
            f"{job_name}: runs-on must be ubuntu-24.04",
        )
        check_error(
            job.get("if") == "startsWith(github.ref, 'refs/tags/v')",
            f"{job_name}: job if must be startsWith(github.ref, 'refs/tags/v')",
        )
        steps = job.get("steps")
        check_error(
            isinstance(steps, list) and len(steps) > 0,
            f"{job_name}: steps must be a non-empty list",
        )
        for step in steps:
            check_error(isinstance(step, dict), f"{job_name}: step must be a mapping")
            check_error(
                "continue-on-error" not in step,
                f"{job_name}: continue-on-error is forbidden",
            )
            with_block = step.get("with")
            if isinstance(with_block, dict):
                check_error(
                    "token" not in with_block,
                    f"{job_name}: custom token in step.with is forbidden",
                )
            if "uses" in step:
                check_error(
                    "env" not in step, f"{job_name}: uses step must not define env"
                )
                validate_step_uses(job_name, step, text)
            if "run" in step:
                check_error(
                    isinstance(step["run"], str), f"{job_name}: run must be a string"
                )
                validate_run_script(job_name, step["run"])

    verify_job = jobs["verify-signature"]
    check_error(
        "env" not in verify_job, "verify-signature must not define job-level env"
    )
    check_error(
        "needs" not in verify_job, "verify-signature must not depend on another job"
    )
    check_error(
        verify_job.get("outputs")
        == {
            "commit": "${{ steps.verify.outputs.commit }}",
            "tag_object": "${{ steps.verify.outputs.tag_object }}",
        },
        "verify-signature outputs must expose commit and tag_object from steps.verify",
    )
    verify_steps = verify_job["steps"]
    check_error(len(verify_steps) == 3, "verify-signature must have exactly 3 steps")
    check_error(
        verify_steps[0].get("with") == {"fetch-depth": 0, "persist-credentials": False},
        "verify-signature checkout must set fetch-depth: 0 and persist-credentials: false",
    )
    check_error(
        "./scripts/ci-install-decernor.sh" in verify_steps[1].get("run", ""),
        "verify-signature step 2 must install checksum-pinned Decernor",
    )
    check_error(
        "env" not in verify_steps[1], "verify-signature step 2 must not define env"
    )
    verify_step = verify_steps[2]
    check_error(
        verify_step.get("id") == "verify",
        "verify-signature step 3 must have id: verify",
    )
    check_error(
        verify_step.get("env")
        == {
            "GH_TOKEN": "${{ github.token }}",
            "GONIMBUS_RELEASE_TAG": "${{ github.ref_name }}",
        },
        "verify-signature step 3 env must be exactly GH_TOKEN and GONIMBUS_RELEASE_TAG",
    )
    verify_script = verify_step.get("run", "")
    for required_snippet in (
        "./scripts/release-restore-tag-ref.sh",
        "GONIMBUS_REQUIRE_TAG=1 ./scripts/release-guard-tag-version.sh",
        "./scripts/verify-pinned-tag.sh",
        "./scripts/release-verify-committed-anchors.sh",
        'EXPECTED_OBJECT="$(git rev-parse "refs/tags/$GONIMBUS_RELEASE_TAG")"',
        ALLOWED_GH_API_LINE,
        '.verification.verified == true and .verification.reason == "valid"',
        'printf \'commit=%s\\n\' "$(git rev-parse HEAD)" >>"$GITHUB_OUTPUT"',
        'printf \'tag_object=%s\\n\' "$EXPECTED_OBJECT" >>"$GITHUB_OUTPUT"',
    ):
        check_error(
            required_snippet in verify_script,
            f"verify-signature script missing required check: {required_snippet}",
        )

    build_job = jobs["build"]
    check_error(
        build_job.get("needs") == "verify-signature",
        "build job must set needs: verify-signature",
    )
    check_error(
        build_job.get("env") == {"CGO_ENABLED": "0"},
        "build job must set env: {'CGO_ENABLED': '0'}",
    )
    build_steps = build_job["steps"]
    check_error(len(build_steps) == 8, "build job must have exactly 8 steps")
    check_error(
        build_steps[0].get("with") == {"persist-credentials": False, "fetch-depth": 0},
        "build checkout must set persist-credentials: false and fetch-depth: 0",
    )
    check_error(
        build_steps[1].get("with") == {"go-version": "1.26.10", "cache": False},
        "build setup-go must set go-version: 1.26.10 and cache: false",
    )
    guard_step = build_steps[2]
    check_error(
        guard_step.get("env")
        == {
            "GH_TOKEN": "${{ github.token }}",
            "VERIFIED_COMMIT": "${{ needs.verify-signature.outputs.commit }}",
            "GONIMBUS_EXPECTED_TAG_OBJECT": "${{ needs.verify-signature.outputs.tag_object }}",
            "GONIMBUS_RELEASE_TAG": "${{ github.ref_name }}",
        },
        "build guard step env must bind VERIFIED_COMMIT and GONIMBUS_EXPECTED_TAG_OBJECT",
    )
    guard_script = guard_step.get("run", "")
    for required_snippet in (
        '[ -n "$VERIFIED_COMMIT" ] && [ "$(git rev-parse HEAD)" = "$VERIFIED_COMMIT" ]',
        "./scripts/release-restore-tag-ref.sh",
    ):
        check_error(
            required_snippet in guard_script,
            f"build guard step missing required check: {required_snippet}",
        )
    for step in build_steps[3:]:
        check_error(
            "env" not in step, "build steps after tag verification must not define env"
        )
    check_error(
        'GONIMBUS_RELEASE_TAG="${GITHUB_REF_NAME}" make release-guard-signing-tag'
        in build_steps[3].get("run", ""),
        "build step 4 must run make release-guard-signing-tag",
    )
    check_error(
        "go test ./..." in build_steps[5].get("run", ""),
        "build step 6 must run go test ./...",
    )
    check_error(
        "make release-build" in build_steps[6].get("run", ""),
        "build step 7 must run make release-build",
    )
    upload_step = build_steps[7]
    check_error(
        upload_step.get("uses")
        == f"actions/upload-artifact@{ALLOWED_USES['actions/upload-artifact']}",
        "build step 8 must use pinned actions/upload-artifact",
    )
    check_error(
        upload_step.get("with")
        == {
            "name": "release-artifacts-${{ github.ref_name }}",
            "path": "dist/release/*",
            "if-no-files-found": "error",
            "retention-days": 30,
        },
        "build upload-artifact with-block does not match expected artifact policy",
    )


def validate_general_workflow_file(path: pathlib.Path) -> None:
    text = path.read_text(encoding="utf-8")
    name = path.name
    check_error("\t" not in text, f"{name}: workflow contains tab characters")
    check_error("\r" not in text, f"{name}: workflow contains CR characters")
    check_error(
        "yq_linux_amd64" not in text,
        f"{name}: runtime curl download of yq is forbidden",
    )
    if yaml is not None:
        doc = yaml.safe_load(text)
    else:
        proc = subprocess.run(
            ["yq", "eval", "-o=json", "-"], input=text, text=True, capture_output=True
        )
        check_error(proc.returncode == 0, f"{name}: invalid YAML")
        doc = json.loads(proc.stdout)
    check_error(isinstance(doc, dict), f"{name}: workflow root must be a mapping")
    check_error(
        doc.get("permissions") == {"contents": "read"},
        f"{name}: workflow permissions must be set to contents: read",
    )
    for job_name, job in (doc.get("jobs") or {}).items():
        where = f"{name}: job {job_name}"
        perms = job.get("permissions")
        if perms is not None:
            check_error(
                perms == {"contents": "read"},
                f"{where} permissions must be exactly contents: read",
            )
        runs_on = job.get("runs-on")
        labels = (
            [runs_on]
            if isinstance(runs_on, str)
            else (runs_on if isinstance(runs_on, list) else [])
        )
        for row in ((job.get("strategy") or {}).get("matrix") or {}).get(
            "include"
        ) or []:
            if isinstance(row, dict) and isinstance(row.get("runner"), str):
                labels.append(row["runner"])
        for label in labels:
            if isinstance(label, str) and not label.startswith("${{"):
                check_error(
                    not (label.endswith("-latest") or "-latest-" in label)
                    or label in LARGER_RUNNERS,
                    f"{where}: moving runner label: {label}",
                )
        container = job.get("container")
        container_img = (
            container.get("image") if isinstance(container, dict) else container
        )
        if container_img:
            check_error(
                re.search(r"@sha256:[0-9a-f]{64}$", str(container_img)) is not None,
                f"{where}: image not digest-pinned: {container_img}",
            )
        for svc_name, svc in (job.get("services") or {}).items():
            svc_img = svc.get("image") if isinstance(svc, dict) else svc
            if svc_img:
                check_error(
                    re.search(r"@sha256:[0-9a-f]{64}$", str(svc_img)) is not None,
                    f"{where} service {svc_name}: image not digest-pinned: {svc_img}",
                )
        uses_list = [job.get("uses")] + [
            s.get("uses") for s in job.get("steps") or [] if isinstance(s, dict)
        ]
        for ref in uses_list:
            if not ref or str(ref).startswith("./"):
                continue
            ref = str(ref)
            if ref.startswith("docker://"):
                check_error(
                    re.search(r"@sha256:[0-9a-f]{64}$", ref[len("docker://") :])
                    is not None,
                    f"{where}: docker action not digest-pinned: {ref}",
                )
                continue
            check_error(
                re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", ref) is not None,
                f"{where}: action not pinned by full SHA: {ref}",
            )
            check_error(
                ref not in NODE20_DENY,
                f"{where}: action pin targets Node 20: {ref}",
            )
            check_error(
                re.search(re.escape(ref) + r"\s+#\s*v\d", text) is not None,
                f"{where}: pinned action lacks a version comment: {ref}",
            )


def run_mutations(base_text: str) -> None:
    mutations: list[tuple[str, str]] = [
        (
            "write-permissions",
            base_text.replace("  contents: read\njobs:", "  contents: write\njobs:", 1),
        ),
        (
            "id-token-write",
            base_text.replace(
                "permissions:\n  contents: read",
                "permissions:\n  contents: read\n  id-token: write",
                1,
            ),
        ),
        (
            "missing-shell-defaults",
            base_text.replace("defaults:\n  run:\n    shell: bash\n", "", 1),
        ),
        (
            "workflow-dispatch-trigger",
            base_text.replace(
                '      - "v*"\n', '      - "v*"\n  workflow_dispatch:\n', 1
            ),
        ),
        (
            "job-level-write-permissions",
            base_text.replace(
                "    runs-on: ubuntu-24.04\n    permissions:\n      contents: read",
                "    runs-on: ubuntu-24.04\n    permissions:\n      contents: write",
                1,
            ),
        ),
        (
            "job-environment",
            base_text.replace(
                "  build:\n    if: startsWith(github.ref, 'refs/tags/v')\n",
                "  build:\n    environment: release\n    if: startsWith(github.ref, 'refs/tags/v')\n",
                1,
            ),
        ),
        (
            "unpinned-checkout",
            base_text.replace(
                "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "actions/checkout@v4",
                1,
            ),
        ),
        (
            "persist-credentials-true",
            base_text.replace(
                "persist-credentials: false", "persist-credentials: true", 1
            ),
        ),
        (
            "missing-cgo-disabled",
            base_text.replace('    env:\n      CGO_ENABLED: "0"\n', "", 1),
        ),
        (
            "gh-release-create",
            base_text.replace("make release-build", "gh release create v0.1.0", 1),
        ),
        (
            "gh-api-releases-post",
            base_text.replace(
                ALLOWED_GH_API_LINE,
                'tag_json="$(gh api -X POST "repos/3leaps/gonimbus/git/tags/$EXPECTED_OBJECT")"',
                1,
            ),
        ),
        (
            "yaml-anchor-alias",
            base_text.replace(
                "permissions:\n  contents: read",
                "permissions: &perm\n  contents: read",
                1,
            ),
        ),
        (
            "flow-mapping",
            base_text.replace(
                "permissions:\n  contents: read",
                "permissions: {contents: read}",
                1,
            ),
        ),
    ]
    for name, mutated in mutations:
        if mutated == base_text:
            raise SystemExit(f"error: mutation {name} did not modify workflow text")
        try:
            validate_workflow_text(mutated)
        except ValidationError:
            continue
        raise SystemExit(f"error: mutation {name} unexpectedly passed validation")
    print(f"[ok] {len(mutations)} negative workflow mutations rejected")


def main(argv: list[str]) -> None:
    args = argv[1:]
    mode = "check"
    target_arg: str | None = None

    if not args:
        mode = "check"
    elif args[0] in ("check", "mutations", "--mutations"):
        mode = "mutations" if args[0] in ("mutations", "--mutations") else "check"
        if len(args) > 2:
            print(
                "error: usage: release-workflow-permissions.py [check|mutations|--mutations] [path]",
                file=sys.stderr,
            )
            sys.exit(1)
        if len(args) == 2:
            target_arg = args[1]
    elif len(args) == 1 and not args[0].startswith("-"):
        mode = "check"
        target_arg = args[0]
    else:
        print(
            "error: usage: release-workflow-permissions.py [check|mutations|--mutations] [path]",
            file=sys.stderr,
        )
        sys.exit(1)

    path = pathlib.Path(target_arg) if target_arg else DEFAULT_WORKFLOW
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"error: cannot read {path}: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        validate_workflow_text(text)
        if target_arg is None:
            workflows_dir = ROOT / ".github" / "workflows"
            for wf in sorted(workflows_dir.glob("*.y*ml")):
                validate_general_workflow_file(wf)
    except ValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    if mode == "check":
        display = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path.name
        print(f"[ok] {display} permissions and action pins verified")
    else:
        run_mutations(text)


if __name__ == "__main__":
    main(sys.argv)
