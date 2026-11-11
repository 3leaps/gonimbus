# Authored vectors

Inputs and expected results are fixed data. Binding stores carry the exact UTF-8
JSON bytes addressed by their pins. Parser vectors carry exact input bytes as
hex. Logical member vectors preserve member boundaries and input order. No
fixture is evidence of a physical Parquet decoder or provider completeness.

| File                                                              | Layer             |
| ----------------------------------------------------------------- | ----------------- |
| `vectors/bindings/baseline.json`                                  | bindings          |
| `vectors/bindings/hidden-key-included.json`                       | bindings          |
| `vectors/bindings/inferred-does-not-authorize.json`               | bindings          |
| `vectors/bindings/inherited-provenance.json`                      | bindings          |
| `vectors/bindings/partial-does-not-authorize.json`                | bindings          |
| `vectors/bound_transitions/baseline.json`                         | bound_transitions |
| `vectors/bound_transitions/observe.json`                          | bound_transitions |
| `vectors/cutover/cutover-time-equal.json`                         | cutover           |
| `vectors/cutover/explicit-new-set.json`                           | cutover           |
| `vectors/graphs/baseline.json`                                    | graphs            |
| `vectors/graphs/cutover-same-set.json`                            | cutover_graphs    |
| `vectors/graphs/cutover-time-equal.json`                          | cutover_graphs    |
| `vectors/graphs/cutover.json`                                     | cutover_graphs    |
| `vectors/graphs/enrich.json`                                      | graphs            |
| `vectors/graphs/inherited-protection.json`                        | graphs            |
| `vectors/graphs/metadata-at-limit.json`                           | graphs            |
| `vectors/graphs/observe.json`                                     | graphs            |
| `vectors/parsing/duplicate.json`                                  | parsing           |
| `vectors/parsing/invalid-utf8.json`                               | parsing           |
| `vectors/parsing/nan.json`                                        | parsing           |
| `vectors/parsing/overflow.json`                                   | parsing           |
| `vectors/parsing/surrogate.json`                                  | parsing           |
| `vectors/parsing/syntax.json`                                     | parsing           |
| `vectors/plans/canonical-prefixes.json`                           | plan framing      |
| `vectors/plans/empty.json`                                        | plan framing      |
| `vectors/plans/frame-a-bc.json`                                   | plan framing      |
| `vectors/plans/frame-ab-c.json`                                   | plan framing      |
| `vectors/plans/multiple-namespaces.json`                           | plan framing      |
| `vectors/plans/raw-prefix.json`                                   | plan framing      |
| `vectors/plans/reader-preserves-prefix-bytes.json`                | plan framing      |
| `vectors/plans/unicode-and-no-cleanup.json`                       | plan framing      |
| `vectors/rows/exact-multibyte-ceiling.json`                       | rows              |
| `vectors/rows/two-members.json`                                   | rows              |
| `vectors/transitions/ack-retains-omitted-row.json`                | transitions       |
| `vectors/transitions/broader-prefix-no-loss.json`                 | transitions       |
| `vectors/transitions/empty-baseline.json`                         | transitions       |
| `vectors/transitions/enrichment-only.json`                        | transitions       |
| `vectors/transitions/explicit-baseline.json`                      | transitions       |
| `vectors/transitions/observe-and-tombstone.json`                  | transitions       |
| `vectors/transitions/partial-preserves-absent.json`               | transitions       |
| `vectors/transitions/producer-upgrade-carries-token.json`         | transitions       |
| `vectors/transitions/raw-prefix-authorizes-alphabet-absence.json` | transitions       |
| `vectors/transitions/raw-prefix-observes-alphabet.json`           | transitions       |
| `vectors/transitions/repeated-shrink-still-stale.json`            | transitions       |
| `vectors/transitions/resurrection.json`                           | transitions       |
| `vectors/transitions/retained-tombstone.json`                     | transitions       |
| `vectors/transitions/revision-change-clears-enrichment.json`      | transitions       |
| `vectors/transitions/tombstone-clears-enrichment.json`            | transitions       |
