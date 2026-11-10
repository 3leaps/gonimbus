# Synthetic snapshot fixture profile

This profile exists only to exercise the contract corpus. It claims no producer
durability or implemented retention mechanism. Fixture variants may bind a
declaration-only retention specification, advertise provisional output, or use
subset counts; the baseline declares no retention/streaming and equal counts.
Its NDJSON members contain public synthetic rows. Keys are unique and ordered by
manifest member order. A full scan or verification checks every member. Selective
verification checks only named members, including order/uniqueness within the
checked subset. No lookup or projection is supported, and no rows are emitted by
this repository's fixture validator.

All required files are listed in the manifest/publication bindings. No parent,
external schema reference or implicit sidecar is needed. Catalog metadata is
public; an actual export gate still enforces the data-artifact protection model.
