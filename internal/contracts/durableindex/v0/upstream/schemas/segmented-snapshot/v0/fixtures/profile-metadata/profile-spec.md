# Synthetic profile metadata variant

This fixture-only profile retains the base NDJSON row/count/read rules. It adds
one required metadata object containing exactly kind, metadata_id, selection and
a string label. Its kind is example_profile_metadata. Its identity equals the
binding id and its selection equals the manifest selection. No additional
metadata dependency kinds are supported. The metadata bytes count against the
aggregate metadata budget and must be verified on full and selective reads.
No production, retention or durability claim is made.
