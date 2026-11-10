#!/usr/bin/env python3
"""Initial design controls, not a physical reader or publication qualifier.

Python stdlib plus goneat (JSON Schema subject matter) is the closed invocation
set. Inputs are repository-owned files. Logical vectors use normalized evidence;
this checker does not validate a production coverage graph or Parquet bytes.
"""

import ast
import copy
import functools
import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "schemas/durable-index/v0"
SCHEMAS = ROOT / "schemas"


class Refusal(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Refusal(reason)


def strict(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_key")
            result[key] = value
        return result

    def invalid(_):
        raise Refusal("invalid_json")

    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)
    except UnicodeDecodeError:
        raise Refusal("invalid_unicode") from None
    except (ValueError, RecursionError):
        raise Refusal("invalid_json") from None

    pending = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if isinstance(item, (dict, list)):
            require(depth < 256, "invalid_json")
        if isinstance(item, float):
            require(math.isfinite(item), "invalid_json")
        elif isinstance(item, str):
            try:
                item.encode("utf-8")
            except UnicodeError:
                raise Refusal("invalid_unicode") from None
        elif isinstance(item, dict):
            for key, child in item.items():
                pending.append((key, depth + 1))
                pending.append((child, depth + 1))
        elif isinstance(item, list):
            for child in item:
                pending.append((child, depth + 1))

    return value


def instant(value):
    require(
        isinstance(value, str)
        and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z", value),
        "invalid_instant",
    )
    try:
        datetime.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise Refusal("invalid_instant") from None
    return value  # fixed-width UTC text preserves all nanoseconds in comparisons


def key(row):
    return row["namespace"].encode(), row["key"].encode()


def scope_key(scope):
    return scope["namespace"].encode(), scope["prefix"].encode()


def contains(scope, namespace, value):
    return scope["namespace"] == namespace and value.startswith(scope["prefix"])


def canonical(scopes):
    require(len(scopes) <= 65536, "capacity")
    unique = sorted({scope_key(scope) for scope in scopes})
    require(all(len(prefix) <= 4096 for _, prefix in unique), "capacity")
    result = []
    for namespace, prefix in unique:
        if not any(ns == namespace and prefix.startswith(p) for ns, p in result):
            result.append((namespace, prefix))
    return [{"namespace": ns.decode(), "prefix": p.decode()} for ns, p in result]


def plan_bytes(set_id, scopes):
    scopes = canonical(scopes)

    def frame(value):
        raw = value.encode()
        return len(raw).to_bytes(4, "big") + raw

    raw = b"durable-index/v0/plan\x00" + frame(set_id)
    raw += len(scopes).to_bytes(4, "big")
    for scope in scopes:
        raw += frame(scope["namespace"]) + frame(scope["prefix"])
    return raw


def digest(set_id, scopes):
    return hashlib.sha256(plan_bytes(set_id, scopes)).hexdigest()


def loss(old, new):
    return any(
        not any(contains(b, a["namespace"], a["prefix"]) for b in new)
        for a in canonical(old)
    )


OBSERVATION_FIELDS = {
    "namespace",
    "key",
    "revision",
    "size_bytes",
    "last_modified",
    "storage_class",
}


def identifier(value):
    return (
        isinstance(value, str)
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value) is not None
    )


@functools.lru_cache(maxsize=4096)
def shaped(raw, schema_name):
    with tempfile.TemporaryDirectory(prefix="index-shape-") as directory:
        path = Path(directory) / "object.json"
        path.write_bytes(raw)
        target, _, fragment = schema_name.partition("#")
        schema_path = BASE / target
        if fragment:
            assert fragment == "/$defs/fieldCatalog"
            original = strict(schema_path.read_bytes())
            wrapper = {
                "$schema": original["$schema"],
                "$defs": original["$defs"],
                "$ref": "#" + fragment,
            }
            schema_path = Path(directory) / "catalog.schema.json"
            schema_path.write_text(json.dumps(wrapper))
        return shape(path, schema_path)


def schema_value(value, schema_name):
    require(
        shaped(json.dumps(value, ensure_ascii=True).encode(), schema_name),
        "invalid_shape",
    )


def source_fields(row):
    require(len(row["key"].encode("utf-8")) <= 4096, "capacity")
    if row["last_modified"] is not None:
        instant(row["last_modified"])


def rows_valid(rows, forbidden_snapshot=None):
    previous = None
    stamps = {}
    for row in rows:
        schema_value(row, "index-row.schema.json")
        source_fields(row)
        k = key(row)
        require(previous != k, "duplicate_key_row")
        require(previous is None or previous < k, "row_order")
        previous = k
        for name in ("first_seen", "last_seen", "last_changed", "deleted"):
            stamp = row[name]
            if stamp is None:
                continue
            instant(stamp["at"])
            require(stamp["snapshot_id"] != forbidden_snapshot, "fabricated_freshness")
            require(
                stamps.get(stamp["snapshot_id"], stamp["at"]) == stamp["at"],
                "stamp_conflict",
            )
            stamps[stamp["snapshot_id"]] = stamp["at"]
        require(
            row["first_seen"]["at"]
            <= min(row["last_seen"]["at"], row["last_changed"]["at"]),
            "lineage_order",
        )
        if row["deleted"] is not None:
            require(
                row["last_seen"]["at"] <= row["deleted"]["at"]
                and row["deleted"] == row["last_changed"],
                "lineage_order",
            )
        names = [a["name"] for a in row["enrichments"]]
        require(names == sorted(set(names)), "enrichment_order")
        for attribute in row["enrichments"]:
            instant(attribute["observed_at"])
            require(row["deleted"] is None, "enrichment_target")
            require(
                row["revision"] is not None
                and attribute["against_revision"] == row["revision"],
                "enrichment_revision",
            )


def acknowledgment_valid(ack):
    require(
        set(ack)
        == {"parent_sha256", "plan_digest", "acknowledged_by", "acknowledged_at"}
        and identifier(ack.get("acknowledged_by")),
        "acknowledgment_actor",
    )
    instant(ack["acknowledged_at"])


def fold(case):
    """Small logical model over authored, normalized inputs; no storage IO."""
    mode, parent = case["mode"], case["parent"]
    require(case["discovery_complete"], "discovery_failed")
    scopes = case["scopes"]
    require(scopes == canonical(scopes), "noncanonical_plan")
    if mode == "baseline":
        require(parent is None and case["generation"] == 0, "invalid_baseline")
        require(case["acknowledgment"] is None, "unexpected_acknowledgment")
        rows, admission = {}, "baseline_not_compared"
    else:
        require(parent is not None, "parent_unavailable")
        require(parent["set_id"] == case["set_id"], "set_mismatch")
        require(parent["snapshot_id"] != case["snapshot_id"], "snapshot_reuse")
        require(
            case["snapshot_id"] not in parent.get("ancestor_snapshot_ids", []),
            "parent_cycle",
        )
        rows_valid(parent["rows"], case["snapshot_id"])
        require(parent["generation"] + 1 == case["generation"], "generation_mismatch")
        require(
            parent["publication_sha256"] == case["admitted_parent_sha256"],
            "stale_parent",
        )
        rows = {key(row): copy.deepcopy(row) for row in parent["rows"]}
        require(len(rows) == len(parent["rows"]), "duplicate_key_row")
        if mode == "enrich":
            require(scopes == parent["scopes"], "enrichment_scope_change")
            require(not case["observations"], "enrichment_observation")
            require(case["observation_at"] is None, "enrichment_observation")
            require(case["acknowledgment"] is None, "unexpected_acknowledgment")
            admission = "inherited"
        elif loss(parent["scopes"], scopes):
            ack = case["acknowledgment"]
            require(ack is not None, "scope_loss")
            acknowledgment_valid(ack)
            require(
                ack["parent_sha256"] == parent["publication_sha256"]
                and ack["plan_digest"] == digest(case["set_id"], scopes),
                "acknowledgment_mismatch",
            )
            admission = "loss_acknowledged"
        else:
            require(case["acknowledgment"] is None, "unexpected_acknowledgment")
            admission = "no_loss"
    stamp = {"snapshot_id": case["snapshot_id"], "at": case["observation_at"]}
    if mode != "enrich":
        instant(stamp["at"])
        for old in rows.values():
            require(
                max(
                    old[name]["at"]
                    for name in ("first_seen", "last_seen", "last_changed", "deleted")
                    if old[name] is not None
                )
                <= stamp["at"],
                "clock_reversal",
            )
        observed = set()
        fields = ("revision", "size_bytes", "last_modified", "storage_class")
        for obs in case["observations"]:
            require(set(obs) == OBSERVATION_FIELDS, "observation_fields")
            candidate = {
                **obs,
                "first_seen": stamp,
                "last_seen": stamp,
                "last_changed": stamp,
                "deleted": None,
                "enrichments": [],
            }
            schema_value(candidate, "index-row.schema.json")
            source_fields(obs)
            k = key(obs)
            require(k not in observed, "duplicate_observation")
            observed.add(k)
            require(
                any(contains(s, obs["namespace"], obs["key"]) for s in scopes),
                "observation_outside_plan",
            )
            old = rows.get(k)
            if old is None:
                row = {
                    **copy.deepcopy(obs),
                    "first_seen": stamp,
                    "last_seen": stamp,
                    "last_changed": stamp,
                    "deleted": None,
                    "enrichments": [],
                }
            else:
                row = copy.deepcopy(old)
                if old["deleted"] is not None or any(old[f] != obs[f] for f in fields):
                    row["last_changed"] = stamp
                if obs["revision"] is None or obs["revision"] != old["revision"]:
                    row["enrichments"] = []
                row.update(copy.deepcopy(obs))
                row["last_seen"], row["deleted"] = stamp, None
            rows[k] = row
        # These scopes are normalized, already-verified I3 evidence in this
        # logical corpus, NOT arbitrary producer assertions of completeness.
        for scope in case["absence_authority"]:
            require(scope in scopes, "coverage_outside_plan")
        for k, row in rows.items():
            if (
                k not in observed
                and row["deleted"] is None
                and any(
                    contains(s, row["namespace"], row["key"])
                    for s in case["absence_authority"]
                )
            ):
                row["deleted"] = stamp
                row["last_changed"] = stamp
                row["enrichments"] = []
    else:
        require(not case["absence_authority"], "enrichment_observation")
    seen_attributes = set()
    for update in case["enrichments"]:
        row = rows.get(key(update))
        require(row is not None and row["deleted"] is None, "enrichment_target")
        attribute = update["attribute"]
        identity = key(update), attribute["name"]
        require(identity not in seen_attributes, "duplicate_enrichment")
        seen_attributes.add(identity)
        require(
            row["revision"] is not None
            and attribute["against_revision"] == row["revision"],
            "enrichment_revision",
        )
        instant(attribute["observed_at"])
        attributes = {item["name"]: item for item in row["enrichments"]}
        old = attributes.get(attribute["name"])
        require(
            old is None or old["observed_at"] <= attribute["observed_at"],
            "stale_enrichment",
        )
        attributes[attribute["name"]] = copy.deepcopy(attribute)
        row["enrichments"] = [attributes[name] for name in sorted(attributes)]
    output = [rows[k] for k in sorted(rows)]
    rows_valid(output)
    return {
        "rows": output,
        "admission": admission,
        "retained_unobserved_rows": sum(
            row["last_seen"]["snapshot_id"] != case["snapshot_id"] for row in output
        ),
    }


def subject_uri(selection):
    raw = b"segmented-snapshot/v0/subject\x00"
    raw += selection["namespace_id"].encode("ascii") + b"\x00"
    raw += selection["snapshot_id"].encode("ascii")
    return "snapshot:" + hashlib.sha256(raw).hexdigest()


def resolve(pin, store):
    require(pin["ref"] in store, "binding_unavailable")
    raw = store[pin["ref"]].encode("utf-8")
    require(
        len(raw) == pin["byte_length"]
        and hashlib.sha256(raw).hexdigest() == pin["sha256"],
        "binding_integrity",
    )
    return strict(raw)


def coverage_scope(scope):
    allowed = {"partition", "prefix"} if "prefix" in scope else {"partition"}
    require(
        set(scope) == allowed and set(scope.get("partition", {})) == {"namespace"},
        "coverage_scope",
    )
    require("prefix" not in scope or bool(scope["prefix"]), "coverage_scope")
    return {
        "namespace": scope["partition"]["namespace"],
        "prefix": scope.get("prefix", ""),
    }


def bindings(case, ancestry=()):
    """Validate exact metadata/plan/coverage bytes against authored source evidence.

    This is the index subgraph, not a physical snapshot or source authentication.
    """
    meta, store = case["metadata"], case["store"]
    schema_value(meta, "index-metadata.schema.json")
    selection = meta["selection"]
    require(selection["snapshot_id"] not in ancestry, "parent_cycle")
    require(len(ancestry) < 64, "capacity")
    plan = resolve(meta["plan"], store)
    schema_value(plan, "index-plan.schema.json")
    require(
        plan["set_id"] == meta["plan"]["id"] == selection["namespace_id"],
        "plan_identity",
    )
    scopes = plan["scopes"]
    require(scopes == canonical(scopes), "noncanonical_plan")
    require(meta["effective_scope_count"] == len(scopes), "scope_count")
    require(meta["plan_digest"] == digest(plan["set_id"], scopes), "plan_digest")
    require(len(meta["coverage"]) == len(scopes), "coverage_count")
    refs = [meta["plan"]["ref"]] + [pin["ref"] for pin in meta["coverage"]]
    require(len(set(refs)) == len(refs), "binding_alias")
    ids = [pin["id"] for pin in meta["coverage"]]
    require(len(ids) == len(set(ids)), "coverage_identity")
    if meta["mode"] == "enrich":
        require("inherited_from" in case, "parent_unavailable")
        prior = case["inherited_from"]
        bindings(prior, ancestry + (selection["snapshot_id"],))
        pm = prior["metadata"]
        require(meta["parent"]["selection"] == pm["selection"], "coverage_inheritance")
        require(
            all(
                meta[name] == pm[name]
                for name in ("plan", "plan_digest", "effective_scope_count", "coverage")
            ),
            "coverage_inheritance",
        )
        require(
            not case["observations"] and not case["enumerated"],
            "enrichment_observation",
        )
        return {"absence_authority": []}
    instant(meta["observation_at"])
    observed = [key(row) for row in case["observations"]]
    require(len(set(observed)) == len(observed), "duplicate_observation")
    for row in case["observations"]:
        require(set(row) == OBSERVATION_FIELDS, "observation_fields")
        source_fields(row)
        require(
            any(contains(scope, row["namespace"], row["key"]) for scope in scopes),
            "observation_outside_plan",
        )
    enumerated = {key(row) for row in case["enumerated"]}
    authority = []
    for scope, pin in zip(scopes, meta["coverage"]):
        coverage = resolve(pin, store)
        schema_value(
            coverage, "../../coverage-attestation/v0/coverage-attestation.schema.json"
        )
        require(coverage["attestation_id"] == pin["id"], "coverage_identity")
        require(
            "supersedes" not in coverage and len(coverage["claims"]) == 1,
            "coverage_scope",
        )
        claim = coverage["claims"][0]
        require(coverage_scope(claim["scope"]) == scope, "coverage_scope")
        require(
            coverage["subject"]
            == {
                "artifact_id": selection["artifact_id"],
                "grain_id": case["grain_id"],
                "subject_uri": subject_uri(selection),
            },
            "coverage_subject",
        )
        require(
            coverage["emitter"]["relation"] == "producer"
            and coverage["emitter"].get("run_id") == selection["snapshot_id"],
            "coverage_emitter",
        )
        require(instant(coverage["as_of"]) == meta["observation_at"], "coverage_time")
        volume = claim.get("volume", {})
        count = sum(
            contains(scope, row["namespace"], row["key"])
            for row in case["observations"]
        )
        require(
            volume.get("unit") == "objects"
            and type(volume.get("observed")) is int
            and volume["observed"] == count
            and (
                "expected" not in volume
                or type(volume["expected"]) is int
                and count <= volume["expected"] <= 9007199254740991
            ),
            "coverage_volume",
        )
        gaps = coverage.get("gaps", [])
        for gap in gaps:
            gs = coverage_scope(gap["scope"])
            require(contains(scope, gs["namespace"], gs["prefix"]), "coverage_gap")
        require(
            coverage["coverage_state"] != "complete" or not gaps,
            "coverage_complete_gap",
        )
        if (
            coverage["coverage_state"] == "complete"
            and claim["basis"] == "confirmed"
            and claim["method"] in {"enumerated", "reconciled"}
        ):
            returned = {
                k for k in enumerated if contains(scope, k[0].decode(), k[1].decode())
            }
            positive = {
                k for k in observed if contains(scope, k[0].decode(), k[1].decode())
            }
            require(returned <= positive, "enumerated_key_omitted")
            authority.append(scope)
    return {"absence_authority": authority}


def bound_transition(case):
    evidence, transition = case["evidence"], copy.deepcopy(case["transition"])
    authority = bindings(evidence)["absence_authority"]
    meta = evidence["metadata"]
    plan = resolve(meta["plan"], evidence["store"])
    require(
        transition["set_id"] == meta["selection"]["namespace_id"]
        and transition["snapshot_id"] == meta["selection"]["snapshot_id"]
        and transition["mode"] == meta["mode"]
        and transition["generation"] == meta["generation"]
        and transition["observation_at"] == meta["observation_at"]
        and transition["scopes"] == plan["scopes"]
        and transition["observations"] == evidence["observations"],
        "metadata_transition",
    )
    parent = transition["parent"]
    if parent is None:
        require(meta["parent"] is None, "metadata_transition")
    else:
        require(meta["parent"] is not None, "metadata_transition")
        require(
            meta["parent"]["selection"]["namespace_id"] == parent["set_id"]
            and meta["parent"]["selection"]["snapshot_id"] == parent["snapshot_id"]
            and meta["parent"]["publication"]["sha256"] == parent["publication_sha256"],
            "metadata_transition",
        )
    ack = meta["loss_acknowledgment"]
    expected_ack = (
        None
        if ack is None
        else {
            "parent_sha256": ack["parent"]["publication"]["sha256"],
            "plan_digest": ack["plan_sha256"],
            "acknowledged_by": ack["acknowledged_by"],
            "acknowledged_at": ack["acknowledged_at"],
        }
    )
    require(
        transition["acknowledgment"] == expected_ack
        and (ack is None or ack["parent"] == meta["parent"]),
        "metadata_transition",
    )
    transition["absence_authority"] = authority
    result = fold(transition)
    require(
        result["admission"] == meta["admission"]
        and result["retained_unobserved_rows"] == meta["retained_unobserved_rows"],
        "metadata_transition",
    )
    return result


LOGICAL_PROFILE = "example.index-logical/v0"


def graph(case):
    """Full byte-bound logical projection, explicitly not a Parquet publication.

    The fixture-only profile consumes NDJSON twins. Every stored byte is real;
    no fabricated Parquet pin, physical decoder or source authentication exists.
    """
    require(case["projection"] == "logical-only/v0", "graph_profile")
    store = case["store"]
    visited, active, all_metadata_refs, member_refs = set(), set(), set(), set()
    profile_spec = (BASE / "fixtures/logical-profile.md").read_text()

    def fetch_binding(pin, metadata_refs, schema=None, member=False):
        require(pin["ref"] in store, "binding_unavailable")
        raw = store[pin["ref"]].encode("utf-8")
        require(
            len(raw) == pin["byte_length"]
            and hashlib.sha256(raw).hexdigest() == pin["sha256"],
            "binding_integrity",
        )
        (member_refs if member else metadata_refs).add(pin["ref"])
        all_metadata_refs.update(metadata_refs)
        require(not all_metadata_refs.intersection(member_refs), "binding_alias")
        require(
            sum(len(store[ref].encode("utf-8")) for ref in metadata_refs) <= 67108864,
            "capacity",
        )
        if schema:
            obj = strict(raw)
            schema_value(obj, schema)
        return obj if schema else raw

    def snapshot(pin):
        # A dependency shared by snapshots counts once in each snapshot's budget.
        # The traversal-wide set is only for metadata/member alias detection.
        metadata_refs = set()

        def fetch(pin, schema=None, member=False):
            return fetch_binding(pin, metadata_refs, schema, member)

        identity = pin["sha256"]
        require(identity not in active, "parent_cycle")
        require(len(active) < 64, "capacity")
        active.add(identity)
        pub = fetch(pin, "index-publication.schema.json")
        selection = pub["selection"]
        require(pub["publication_id"] == pin["id"], "graph_identity")
        require(selection["snapshot_id"] not in visited, "snapshot_reuse")
        visited.add(selection["snapshot_id"])
        manifest = fetch(
            pub["manifest"], "../../segmented-snapshot/v0/snapshot-manifest.schema.json"
        )
        profile = fetch(
            pub["profile"], "../../segmented-snapshot/v0/snapshot-profile.schema.json"
        )
        descriptor = fetch(
            pub["descriptor"], "../../data-artifact/v0/artifact-descriptor.schema.json"
        )
        require(
            pub["state"] == "committed"
            and descriptor["lifecycle"] in {"complete", "partial"},
            "graph_publication",
        )
        require(
            manifest["selection"] == selection
            and pub["manifest"]["id"] == selection["snapshot_id"]
            and pub["descriptor"]["id"]
            == descriptor["artifact_id"]
            == selection["artifact_id"],
            "graph_identity",
        )
        require(
            "contract: durable-index/v0" in manifest["capabilities"], "graph_profile"
        )
        require(
            manifest["profile"] == pub["profile"]
            and profile["profile_id"] == pub["profile"]["id"] == LOGICAL_PROFILE,
            "graph_profile",
        )
        require(
            profile["semantic_spec"]["id"] == LOGICAL_PROFILE
            and fetch(profile["semantic_spec"]).decode("utf-8") == profile_spec,
            "graph_profile",
        )
        require(
            profile["operations"] == ["full_verify"]
            and profile["count_relations"] == ["equal"]
            and profile["physical_orders"] == ["manifest_order"]
            and profile["retention"] == "unsupported"
            and profile["durability_claim"] == "none"
            and profile["streaming"] == "unsupported",
            "graph_profile",
        )
        require(
            manifest["count_relation"] == "equal"
            and manifest["physical_order"] == "manifest_order",
            "graph_profile",
        )
        require("profile_metadata" in manifest, "graph_dependency")
        meta = fetch(manifest["profile_metadata"], "index-metadata.schema.json")
        require(
            meta["selection"] == selection
            and manifest["profile_metadata"]["id"] == selection["snapshot_id"],
            "graph_identity",
        )
        require(
            manifest["coverage_subject_uri"] == subject_uri(selection),
            "coverage_subject",
        )
        schema_raw = fetch(manifest["row_schema"])
        require(
            schema_raw == (BASE / "index-row.schema.json").read_bytes()
            and manifest["row_schema"]["schema_id"]
            == "contract:durable-index/v0/index-row.schema.json",
            "graph_row_schema",
        )
        catalog = fetch(
            manifest["field_catalog"],
            "../../data-artifact/v0/artifact-descriptor.schema.json#/$defs/fieldCatalog",
        )
        require(
            catalog["id"] == manifest["field_catalog"]["id"]
            and catalog["grain"] == manifest["grain_id"],
            "graph_catalog",
        )
        require(
            len(descriptor["grains"]) == len(descriptor["representations"]) == 1,
            "graph_descriptor",
        )
        grain, rep = descriptor["grains"][0], descriptor["representations"][0]
        require(
            grain["id"] == rep["grain"] == manifest["grain_id"]
            and rep["id"] == manifest["representation_id"]
            and grain["field_catalog_ref"] == rep["field_catalog_ref"] == catalog["id"],
            "graph_descriptor",
        )
        require(
            rep["profile"] == descriptor["producer"]["profile"] == LOGICAL_PROFILE
            and rep["uri"] == pub["manifest"]["ref"]
            and rep["format"] == "ndjson"
            and rep["read_path"]["physical_ordering"] == "manifest_order"
            and rep["read_path"]["appendable"] is False,
            "graph_descriptor",
        )
        require(
            descriptor.get("field_catalogs", [catalog]) == [catalog], "graph_catalog"
        )
        fields = {field["name"]: field for field in catalog["fields"]}
        expected_types = {
            "namespace": "string",
            "key": "string",
            "revision": "object",
            "size_bytes": "integer",
            "last_modified": "datetime",
            "storage_class": "string",
            "first_seen": "object",
            "last_seen": "object",
            "last_changed": "object",
            "deleted": "object",
            "enrichments": "array",
        }
        require(
            len(fields) == len(catalog["fields"])
            and set(fields) == set(expected_types)
            and all(
                fields[name]["type"] == kind and fields[name].get("required") is True
                for name, kind in expected_types.items()
            )
            and not catalog.get("withheld_fields")
            and not catalog.get("withheld_field_count"),
            "graph_catalog",
        )
        protection = descriptor["protection"]
        assert protection["default_action"] == "block_export"  # enforced by schema
        rank = {
            "public": 0,
            "internal": 1,
            "controlled": 2,
            "restricted": 3,
            "unknown": 3,
        }
        effective_rank = max(
            [rank[protection.get("default_export_class", "unknown")]]
            + [rank[f["sensitivity"]] for f in fields.values()]
        )
        effective_class = ["public", "internal", "controlled", "restricted"][
            effective_rank
        ]
        require(not case["request_export"], "protected_export")
        members = manifest["members"]
        require(
            len({m["id"] for m in members})
            == len({m["ref"] for m in members})
            == len(members),
            "graph_members",
        )
        require(members or profile["empty_snapshots"], "graph_members")
        require(len(members) <= profile["limits"]["max_members"], "capacity")
        rows = []
        for member in members:
            raw = fetch(member, member=True)
            require(len(raw) <= profile["limits"]["max_member_bytes"], "capacity")
            decoded = [strict(line) for line in raw.splitlines()]
            require(len(decoded) == member["row_count"], "graph_count")
            rows.extend(decoded)
        rows_valid(rows)
        require(
            len(rows)
            == manifest["physical_row_count"]
            == grain["row_count"]
            == rep["row_count"],
            "graph_count",
        )
        # Exact parent is fetched only for transition qualification, not row reconstruction.
        parent = None
        inherited = None
        if meta["parent"] is not None:
            prior = snapshot(meta["parent"]["publication"])
            require(prior["selection"] == meta["parent"]["selection"], "graph_identity")
            require(
                prior["selection"]["namespace_id"] == selection["namespace_id"],
                "set_mismatch",
            )
            parent = {
                "publication_sha256": meta["parent"]["publication"]["sha256"],
                "set_id": prior["selection"]["namespace_id"],
                "snapshot_id": prior["selection"]["snapshot_id"],
                "generation": prior["metadata"]["generation"],
                "scopes": prior["scopes"],
                "rows": prior["rows"],
            }
            inherited = prior["evidence"]
            effective_class = ["public", "internal", "controlled", "restricted"][
                max(rank[effective_class], rank[prior["protection_class"]])
            ]
        require(selection["snapshot_id"] in case["source_evidence"], "graph_evidence")
        source = case["source_evidence"][selection["snapshot_id"]]
        evidence = {
            "metadata": meta,
            "store": store,
            "grain_id": manifest["grain_id"],
            "observations": source["observations"],
            "enumerated": source["enumerated"],
        }
        if inherited is not None:
            evidence["inherited_from"] = inherited
        for binding in [meta["plan"], *meta["coverage"]]:
            fetch(binding)
        scopes = resolve(meta["plan"], store)["scopes"]
        ack = meta["loss_acknowledgment"]
        transition = {
            "mode": meta["mode"],
            "set_id": selection["namespace_id"],
            "snapshot_id": selection["snapshot_id"],
            "generation": meta["generation"],
            "observation_at": meta["observation_at"],
            "discovery_complete": source["discovery_complete"],
            "parent": parent,
            "admitted_parent_sha256": source["admitted_parent_sha256"],
            "scopes": scopes,
            "acknowledgment": None
            if ack is None
            else {
                "parent_sha256": ack["parent"]["publication"]["sha256"],
                "plan_digest": ack["plan_sha256"],
                "acknowledged_by": ack["acknowledged_by"],
                "acknowledged_at": ack["acknowledged_at"],
            },
            "observations": source["observations"],
            "absence_authority": [],
            "enrichments": source["enrichments"],
        }
        expected = bound_transition({"evidence": evidence, "transition": transition})
        require(rows == expected["rows"], "graph_transition")
        require(
            sum(len(store[ref].encode("utf-8")) for ref in metadata_refs)
            <= min(67108864, profile["limits"]["max_metadata_bytes"]),
            "capacity",
        )
        active.remove(identity)
        return {
            "selection": selection,
            "metadata": meta,
            "scopes": scopes,
            "rows": rows,
            "evidence": evidence,
            "protection_class": effective_class,
        }

    result = snapshot(case["publication"])
    return {
        "rows": result["rows"],
        "retained_unobserved_rows": result["metadata"]["retained_unobserved_rows"],
        "generation": result["metadata"]["generation"],
        "protection": {"action": "block_export", "class": result["protection_class"]},
        "evidence_scope": "logical_projection",
    }


def cutover(case):
    record = case["record"]
    require(record is not None, "cutover_required")
    schema_value(record, "index-cutover.schema.json")
    instant(record["accepted_at"])
    require(record["authorized_by"] in case["authorized_actors"], "cutover_actor")
    store = case["store"]
    publications = []
    for side in ("old", "new"):
        endpoint = record[side]
        publication = resolve(endpoint["publication"], store)
        schema_value(publication, "index-publication.schema.json")
        require(
            publication["selection"] == endpoint["selection"]
            and publication["publication_id"] == endpoint["publication"]["id"],
            "cutover_selection",
        )
        publications.append(publication)
    require(
        record["old"]["publication"] != record["new"]["publication"]
        and record["old"]["selection"]["snapshot_id"]
        != record["new"]["selection"]["snapshot_id"],
        "cutover_selection",
    )
    require(record["rollback"] == record["old"]["publication"], "cutover_rollback")
    resolve(record["rollback"], store)
    new = publications[1]
    manifest = resolve(new["manifest"], store)
    require(
        manifest["selection"] == new["selection"]
        and manifest["profile_metadata"]["id"] == new["selection"]["snapshot_id"],
        "cutover_selection",
    )
    meta = resolve(manifest["profile_metadata"], store)
    schema_value(meta, "index-metadata.schema.json")
    require(meta["selection"] == new["selection"], "cutover_selection")
    require(
        meta["mode"] == "baseline"
        and meta["generation"] == 0
        and meta["parent"] is None,
        "cutover_baseline",
    )
    require(
        instant(record["accepted_at"]) >= instant(meta["observation_at"]),
        "cutover_time",
    )
    return {"cutover": "explicit_baseline"}


def cutover_graphs(case):
    result = cutover(case)
    require(
        case["record"]["old"]["publication"] == case["old_graph"]["publication"]
        and case["record"]["new"]["publication"] == case["new_graph"]["publication"],
        "cutover_selection",
    )
    for side in ("old_graph", "new_graph"):
        require(
            all(
                case["store"].get(ref) == raw
                for ref, raw in case[side]["store"].items()
            ),
            "binding_integrity",
        )
        graph(case[side])
    return {**result, "evidence_scope": "logical_projection"}


def shape(path, schema):
    try:
        strict(path.read_bytes())
    except Refusal:
        return False
    result = subprocess.run(
        [
            "goneat",
            "validate",
            "data",
            "--schema-file",
            str(schema),
            "--ref-dir",
            str(BASE),
            "--ref-dir",
            str(SCHEMAS / "segmented-snapshot/v0"),
            "--data",
            str(path),
            "--format",
            "json",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode == 0:
        return True
    try:
        report, end = json.JSONDecoder().raw_decode(result.stderr.lstrip())
        assert result.returncode == 1 and report["valid"] is False and report["errors"]
        assert (
            result.stderr.lstrip()[end:]
            .lstrip()
            .startswith("Error: data validation failed")
        )
        return False
    except (ValueError, KeyError, AssertionError):
        raise RuntimeError(
            "validator execution failed: " + result.stdout + result.stderr
        ) from None


def load_case(filename):
    case = strict((BASE / filename).read_bytes())
    if "baseline" in case:
        baseline = strict((BASE / case["baseline"]).read_bytes())
        assert "baseline" not in baseline, filename
        assert case["replace"] or case.get("store_overlay") or case.get("omit_refs"), (
            filename
        )
        case["input"] = copy.deepcopy(baseline["input"])
        for name, value in case["replace"].items():
            assert baseline["input"].get(name) != value, (filename, name)
            case["input"][name] = value
        for ref, raw in case.get("store_overlay", {}).items():
            assert baseline["input"]["store"].get(ref) != raw, (filename, ref)
            case["input"]["store"][ref] = raw
        for ref in case.get("omit_refs", []):
            assert ref in baseline["input"]["store"], (filename, ref)
            del case["input"]["store"][ref]
    return case


def evaluate(case, namespace=None):
    scope = globals() if namespace is None else namespace
    try:
        if case["runner"] == "parsing":
            return {"value": scope["strict"](bytes.fromhex(case["input"]["hex"]))}
        runner = {
            "transitions": "fold",
            "bindings": "bindings",
            "cutover": "cutover",
            "rows": "row_members",
            "bound_transitions": "bound_transition",
            "graphs": "graph",
            "cutover_graphs": "cutover_graphs",
        }[case["runner"]]
        return scope[runner](copy.deepcopy(case["input"]))
    except scope["Refusal"] as error:
        return {"refusal": str(error)}


def row_members(case):
    rows = [row for member in case["members"] for row in member]
    rows_valid(rows)
    return {"row_count": len(rows)}


def declared_reasons():
    tree = ast.parse(Path(__file__).read_text())
    reasons = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            arg = (
                1
                if node.func.id == "require"
                else 0
                if node.func.id == "Refusal"
                else None
            )
            if (
                arg is not None
                and len(node.args) > arg
                and isinstance(node.args[arg], ast.Constant)
            ):
                reasons.add(node.args[arg].value)
    return reasons


def fixture_tables(inventory, filenames):
    expected_reasons = {
        load_case(f)["expected"]["refusal"]
        for f in filenames
        if "refusal" in load_case(f)["expected"]
    }
    standard = (ROOT / "docs/standards/durable-index-contract.md").read_text()
    table = set(
        re.findall(
            r"^\| `([a-z_]+)`\s*\|",
            standard.split("## Logical checker refusal reasons", 1)[1],
            re.M,
        )
    )
    assert declared_reasons() == table == expected_reasons, (
        declared_reasons() - expected_reasons,
        table ^ declared_reasons(),
    )
    readme = (BASE / "rejects/README.md").read_text()
    listed = set(re.findall(r"^\| `([^`]+)`\s*\|", readme, re.M))
    files = {entry["file"] for entry in inventory["shape"] if not entry["valid"]}
    files |= {f for f in filenames if "refusal" in load_case(f)["expected"]}
    assert listed == files, listed ^ files
    indexed = {
        f: (layer.strip(), reason)
        for f, layer, reason in re.findall(
            r"^\| `([^`]+)`\s*\| ([^|]+)\| `([^`]+)`\s*\|", readme, re.M
        )
    }
    expected_index = {
        e["file"]: ("schema", e["schema"]) for e in inventory["shape"] if not e["valid"]
    }
    expected_index.update(
        {
            f: (load_case(f)["runner"], load_case(f)["expected"]["refusal"])
            for f in filenames
            if "refusal" in load_case(f)["expected"]
        }
    )
    assert indexed == expected_index, "reject README labels differ from fixtures"
    for filename in files:
        if filename.startswith("rejects/semantic/"):
            reason = load_case(filename)["expected"]["refusal"]
            assert Path(filename).parent.name.startswith(f"reject-{reason}--")
    vectors = (BASE / "vectors/README.md").read_text()
    listed_vectors = set(re.findall(r"^\| `([^`]+)`\s*\|", vectors, re.M))
    assert listed_vectors == {
        f for f in inventory["plans"] + filenames if f.startswith("vectors/")
    }


def plan_mutation_controls():
    # The cross-namespace vector must distinguish three tempting shortcuts.
    source = Path(__file__).read_text().split("def plan_mutation_controls(", 1)[0]
    case = load_case("vectors/plans/multiple-namespaces.json")
    for name in ("namespace-sort", "namespace-containment", "namespace-loss"):
        changes = []

        class Weaken(ast.NodeTransformer):
            def visit_Assign(self, node):
                self.generic_visit(node)
                if name == "namespace-sort" and any(
                    isinstance(target, ast.Name) and target.id == "unique"
                    for target in node.targets
                ):
                    node.value = ast.parse(
                        "sorted({scope_key(scope) for scope in scopes}, "
                        "key=lambda pair: pair[1])",
                        mode="eval",
                    ).body
                    changes.append(name)
                return node

            def visit_Compare(self, node):
                self.generic_visit(node)
                if (
                    name == "namespace-containment"
                    and ast.unparse(node) == "ns == namespace"
                ):
                    changes.append(name)
                    return ast.copy_location(ast.Constant(True), node)
                return node

            def visit_FunctionDef(self, node):
                self.generic_visit(node)
                if name == "namespace-loss" and node.name == "contains":
                    node.body = [
                        ast.parse("return value.startswith(scope['prefix'])").body[0]
                    ]
                    changes.append(name)
                return node

        tree = ast.fix_missing_locations(Weaken().visit(ast.parse(source)))
        assert len(changes) == 1, (name, changes)
        namespace = {"__file__": __file__, "__name__": "index_plan_mutation"}
        exec(compile(tree, __file__, "exec"), namespace)
        if name == "namespace-loss":
            comparison = case["comparisons"][0]
            assert comparison["loss"] and not namespace["loss"](
                case["canonical"], comparison["next"]
            ), name
        else:
            assert namespace["canonical"](case["input"]) != case["canonical"], name
        print(f"[ok] plan mutation detected: {name}")


def mutation_controls(filenames):
    # Fixed AST mutations, never fixture-selected executable code or commands.
    source = Path(__file__).read_text().split("def mutation_controls(", 1)[0]
    mutations = [
        ("fabricated_freshness", "retained-current-stamp"),
        ("observation_fields", "observation-first_seen"),
        ("lineage_order", "first-after-last"),
        ("scope_count", "wrong-count"),
        ("plan_digest", "wrong-plan-digest"),
        ("coverage_count", "missing-coverage"),
        ("enumerated_key_omitted", "hidden-key-filtered"),
        ("invalid_json", "overflow"),
        ("capacity", "multibyte-key"),
        ("prefix-byte-capacity", "multibyte-prefix"),
        ("cutover_actor", "unrecognized-actor"),
        ("scope-loss", "equal-count-loss"),
        ("stale_parent", "stale-parent"),
        ("partial-authority", "partial-does-not-authorize"),
        ("revision-clearing", "revision-change-clears-enrichment"),
        ("graph_identity", "graph-manifest-selection"),
        ("graph_transition", "graph-unobserved-row-change"),
        ("graph_count", "graph-wrong-member-count"),
        ("binding_integrity", "graph-altered-parent-bytes"),
        ("graph_catalog", "graph-catalog-omitted-field"),
        ("protected_export", "graph-protected-export"),
        ("protection-inheritance", "inherited-protection"),
        ("cutover_selection", "graph-cutover-reused-selection"),
        ("cutover_time", "graph-cutover-before-observation"),
        ("metadata-ancestry", "metadata-at-limit"),
        ("nesting-depth", "depth-257"),
    ]
    for name, witness in mutations:
        changes = []

        class Weaken(ast.NodeTransformer):
            def visit_Assign(self, node):
                self.generic_visit(node)
                if name == "protection-inheritance" and any(
                    isinstance(t, ast.Name) and t.id == "effective_rank"
                    for t in node.targets
                ):
                    node.value = ast.Constant(0)
                    changes.append(name)
                if name == "metadata-ancestry" and any(
                    isinstance(t, ast.Name) and t.id == "metadata_refs"
                    for t in node.targets
                ):
                    node.value = ast.Name(id="all_metadata_refs", ctx=ast.Load())
                    changes.append(name)
                return node

            def visit_Call(self, node):
                self.generic_visit(node)
                if (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "require"
                    and len(node.args) == 2
                    and isinstance(node.args[1], ast.Constant)
                    and node.args[1].value == name
                ):
                    node.args[0] = ast.Constant(True)
                    changes.append(name)
                if (
                    name == "nesting-depth"
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "require"
                    and ast.unparse(node.args[0]) == "depth < 256"
                ):
                    node.args[0] = ast.Constant(True)
                    changes.append(name)
                if (
                    name == "prefix-byte-capacity"
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "require"
                    and ast.unparse(node.args[0])
                    == "all((len(prefix) <= 4096 for _, prefix in unique))"
                ):
                    node.args[0] = ast.Constant(True)
                    changes.append(name)
                return node

            def visit_FunctionDef(self, node):
                self.generic_visit(node)
                if name == "scope-loss" and node.name == "loss":
                    node.body = [ast.Return(ast.Constant(False))]
                    changes.append(name)
                return node

            def visit_If(self, node):
                self.generic_visit(node)
                test = ast.unparse(node.test)
                if (
                    name == "revision-clearing"
                    and test
                    == "obs['revision'] is None or obs['revision'] != old['revision']"
                ):
                    node.test = ast.Constant(False)
                    changes.append(name)
                if name == "partial-authority" and test.startswith(
                    "coverage['coverage_state'] == 'complete' and claim['basis']"
                ):
                    node.test.values.pop(0)
                    changes.append(name)
                return node

        tree = ast.fix_missing_locations(Weaken().visit(ast.parse(source)))
        assert changes, name
        namespace = {"__file__": __file__, "__name__": "index_mutation"}
        exec(compile(tree, __file__, "exec"), namespace)
        matches = [
            f
            for f in filenames
            if Path(f).stem == witness or Path(f).parent.name.endswith("--" + witness)
        ]
        assert len(matches) == 1, (name, matches)
        case = load_case(matches[0])
        # Exceptions propagate and fail the gate. An incidental second refusal
        # is not a successful witness for removal of the intended guard.
        outcome = evaluate(case, namespace)
        if "refusal" in case["expected"]:
            assert "refusal" not in outcome, ("mutation still refused", name, outcome)
        else:
            alternatives = {
                "partial-authority": {
                    "absence_authority": [
                        {"namespace": "objects", "prefix": "alpha/"},
                        {"namespace": "objects", "prefix": "beta/"},
                    ]
                },
                "revision-clearing": {"refusal": "enrichment_revision"},
                "protection-inheritance": {
                    **case["expected"],
                    "protection": {"action": "block_export", "class": "public"},
                },
                "metadata-ancestry": {"refusal": "capacity"},
            }
            assert outcome == alternatives[name], (
                "wrong mutation outcome",
                name,
                outcome,
            )
        print(f"[ok] mutation detected: {name}")


def main():
    inventory = strict((BASE / "cases.json").read_bytes())
    actual = {
        p.relative_to(BASE).as_posix()
        for folder in ("examples", "rejects", "vectors")
        for p in (BASE / folder).rglob("*.json")
    }
    listed = {entry["file"] for entry in inventory["shape"]}
    listed.update(
        inventory["plans"] + inventory["transitions"] + inventory["scenarios"]
    )
    assert listed == actual, listed ^ actual
    assert len(listed) == len(inventory["shape"]) + len(inventory["plans"]) + len(
        inventory["transitions"]
    ) + len(inventory["scenarios"])
    for entry in inventory["shape"]:
        if not entry["valid"]:
            kind = Path(entry["file"]).name.split("--")[0].removeprefix("reject-")
            original = strict((BASE / "examples" / f"{kind}.json").read_bytes())
            rejected = strict((BASE / entry["file"]).read_bytes())
            missing = object()
            changed = [
                k
                for k in original.keys() | rejected.keys()
                if original.get(k, missing) != rejected.get(k, missing)
            ]
            assert len(changed) == 1, (entry["file"], changed)
        assert shape(BASE / entry["file"], BASE / entry["schema"]) == entry["valid"], (
            entry
        )
    metadata = strict((BASE / "examples/index-metadata.json").read_bytes())
    example_bindings = [(metadata["plan"], "index-plan.json")]
    example_bindings += [
        (pin, f"coverage-{i}.json") for i, pin in enumerate(metadata["coverage"])
    ]
    for pin, filename in example_bindings:
        raw = (BASE / "examples" / filename).read_bytes()
        assert pin["byte_length"] == len(raw)
        assert pin["sha256"] == hashlib.sha256(raw).hexdigest()
    for filename in inventory["plans"]:
        case = strict((BASE / filename).read_bytes())
        assert canonical(case["input"]) == case["canonical"], filename
        assert plan_bytes(case["set_id"], case["input"]).hex() == case["bytes_hex"], (
            filename
        )
        assert digest(case["set_id"], case["input"]) == case["sha256"], filename
        for member in case.get("membership", []):
            assert (
                contains(member["scope"], member["namespace"], member["key"])
                == member["contains"]
            ), filename
        for comparison in case["comparisons"]:
            assert loss(case["canonical"], comparison["next"]) == comparison["loss"], (
                filename
            )
    with tempfile.TemporaryDirectory(prefix="durable-index-controls-") as tmp:
        path = Path(tmp) / "row.json"
        filenames = inventory["transitions"] + inventory["scenarios"]
        for filename in filenames:
            case = load_case(filename)
            outcome = evaluate(case)
            assert outcome == case["expected"], (filename, outcome, case["expected"])
            for row in outcome.get("rows", []):
                path.write_text(json.dumps(row))
                assert shape(path, BASE / "index-row.schema.json"), filename
            print(f"[ok] {filename}")
    fixture_tables(inventory, filenames)
    plan_mutation_controls()
    mutation_controls(filenames)
    for opening, closing in [("[", "]"), ('{"a":', "}")]:
        # Exact container-depth boundary, including the iterative scalar walk.
        strict((opening * 256 + "0" + closing * 256).encode())
        try:
            strict((opening * 257 + "0" + closing * 257).encode())
        except Refusal as error:
            assert str(error) == "invalid_json"
        else:
            raise AssertionError("nesting limit")
    for raw, reason in [
        (b'{"a":1,"a":2}', "duplicate_key"),
        (b'{"a":NaN}', "invalid_json"),
        (b'{"a":"\\ud800"}', "invalid_unicode"),
    ]:
        try:
            strict(raw)
        except Refusal as error:
            assert str(error) == reason
        else:
            raise AssertionError(reason)
    print(
        f"Durable index DESIGN controls PASS: {len(inventory['shape'])} shapes, "
        f"{len(inventory['plans'])} plan vectors, {len(filenames)} scenarios. "
        "Complete logical projection graphs checked; no Parquet/R2 or runtime qualification claim."
    )


if __name__ == "__main__":
    main()
