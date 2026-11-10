---
id: "DDR-0004"
title: "Durable index row and plan encoding"
status: "proposed"
date: "2026-10-18"
last_updated: "2026-10-18"
deciders: ["@3leapsdave", "entarch"]
scope: "Durable index object-inventory/v0 and parquet-inventory/v0"
tags: ["encoding", "parquet", "identity", "data"]
---

# DDR-0004: Durable Index Row and Plan Encoding

## Status

Proposed for design review. The physical corpus and independent logical/physical
parity witness remain adoption gates before locking this profile. They are not
prerequisites for publishing an explicitly experimental logical-only corpus.

## Logical profile

`index-row.schema.json` specifies the logical object. Nulls are explicit; an
absent required property is not a null. No unlisted row properties or columns are
permitted. Object keys are scalar Unicode strings ordered by unsigned UTF-8
bytes. Object-key path interpretation belongs outside this profile.

The first published encoding is Parquet. It uses the following exact column
names and structures in listed order. `STRING` means BYTE_ARRAY with STRING
logical annotation (legacy UTF8 compatibility annotation allowed). Parquet's
STRING ordering is unsigned byte-wise. The mapping follows the
[Apache Parquet logical types specification](https://parquet.apache.org/docs/file-format/types/logicaltypes/).

| Logical field | Physical column / group                                                                                              | Repetition                                 |
| ------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| namespace     | STRING `namespace`                                                                                                   | required                                   |
| key           | STRING `key`                                                                                                         | required                                   |
| revision      | group `revision`: required STRING `kind`, required STRING `value`                                                    | optional group; null means absent group    |
| size_bytes    | Physical INT64 `size_bytes`; logical annotation absent or INT(64, signed=true); never unsigned, decimal or timestamp | required; 0 through 2^53−1                 |
| last_modified | STRING `last_modified`                                                                                               | optional                                   |
| storage_class | STRING `storage_class`                                                                                               | optional                                   |
| first_seen    | group `first_seen`: required STRING `snapshot_id`, required STRING `at`                                              | required                                   |
| last_seen     | same fields in group `last_seen`                                                                                     | required                                   |
| last_changed  | same fields in group `last_changed`                                                                                  | required                                   |
| deleted       | same fields in group `deleted`                                                                                       | optional                                   |
| enrichments   | standard three-level LIST group `enrichments` / repeated group `list` / required group `element`                     | required list; empty is distinct from null |

Each enrichment element has required STRING `name`, optional STRING `value`,
required STRING `operation`, required STRING `observed_at`, and required group
`against_revision` with required STRING `kind` and `value`. No null elements or
null list. Elements are unique and sorted by attribute name. Optional value maps
to the explicit logical null, not omission of the attribute.

Timestamps retain the standard's exact nine-digit UTC string form. A producer
may expose derived timestamp columns in another representation; it must not
silently replace these fields with a lower precision or narrower-range type.
Unknown revision is a null group, never an empty token or inferred digest.

The initial profile admits unencrypted self-contained Parquet files, data page
v1 or v2, PLAIN and RLE_DICTIONARY value encoding, dictionary PLAIN, and the
standard RLE/bit-packed definition and repetition levels. Codecs are UNCOMPRESSED
and ZSTD. Readers claiming the full profile must support both codecs and both
page versions; narrower reader evidence states the exact exercised subset.
Unknown physical/logical types, codecs, external column chunks and encryption
are unsupported, not a reason to reinterpret bytes. Extra statistics, bloom
filters and page indexes do not confer lookup capabilities or relax protection.

Row groups and member boundaries cannot change primary-key uniqueness or order.
An empty snapshot has zero members and zero rows. The profile does not require
byte-identical Parquet emission across writers: every new physical encoding has
its own exact digest, while the decoded logical row set can remain equal.

## Metadata

The snapshot manifest's `profile_metadata` binding targets `index_metadata`.
The current manifest is not referenced back from that object. This avoids a
physical digest cycle. Its selection identifies the snapshot; its parent binding
identifies the exact predecessor publication. A baseline has no predecessor.

The plan's semantic digest uses explicit length framing, not a JSON serializer
or concatenation of unframed strings. The standard defines the bytes. The test
corpus records both framed hex and SHA-256, including empty, Unicode and
ambiguous-concatenation pairs. A metadata binding always hashes stored bytes;
it never substitutes the semantic digest.

## Compatibility

Scope normalization, timestamp precision, row comparison, unknown-value meaning
and enrichment provenance are part of the profile version. Changes require a
new supported profile/version and explicit reader behavior. A producer cannot
claim this profile merely because it emits valid Parquet or matching counts.

## Review gates

Reviewers must exercise the nested null/empty mapping, maximum integer and
nanosecond timestamps by comparing a physical reader with independently authored
logical twins and an independently implemented logical checker before profile
lock. This establishes semantic parity, not two independent Parquet decoders.
Protection of statistics and namespace values, exact parent admission and
recovery are separate checks. See the
[conformance requirements](../standards/durable-index-conformance.md).

Physical twins MUST exercise both permitted `size_bytes` annotation forms.
