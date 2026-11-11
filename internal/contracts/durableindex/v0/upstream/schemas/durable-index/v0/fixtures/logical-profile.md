# Corpus-only logical projection profile

`example.index-logical/v0` is a test transport, never a production index profile.
It stores independently authored logical twins as UTF-8 NDJSON and binds their
actual bytes in a projected segmented-snapshot graph. The index metadata retains
the intended production encoding identifier; this projection deliberately does
not exercise that physical encoding. A successful result is labeled
`logical_projection`, never R2 or a conforming Parquet publication.

The sole operation is full_verify. Every dependency, row count, member order,
selection, plan, coverage, parent and logical transition is checked. This fixture
profile supports flat exhaustive members, no hidden dependencies, no retention,
streaming, durability or export claim. The row schema is the exact pinned
index-row schema. The complete field catalog declares every required logical
field; nullable object values retain their logical type. All graph content,
keys, prefixes, diagnostics and statistics inherit block_export and the most
restrictive descriptor/catalog/parent classification. No field-level allow can
override that default. Authenticated export release is outside this corpus.

Source enumeration and admission evidence are authored inputs, not assertions
in the publication or a proof of provider completeness. Observations and
expected rows are separately authored. A whole graph is a logical oracle for
independently implemented readers, not production writer output.
