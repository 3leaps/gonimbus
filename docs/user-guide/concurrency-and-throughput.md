# Concurrency and Throughput

This guide explains how Gonimbus bounds and adapts concurrent provider work for
large object-storage operations.

Drafting note for v0.3.3: this page is the provider-generalized home for the
GON-048 concurrency model. S3 is the concrete first implementation; GCS and
future providers should extend this same model instead of creating separate
provider-specific throughput guides.

## Operator Model

`transfer reflow --parallel` is a requested ceiling, not a guaranteed active
worker count. Gonimbus resolves an effective ceiling from the requested value
and local resource caps, then adaptive mode adjusts active copy concurrency
within that ceiling.

Technical draft slot:

- requested ceiling vs effective ceiling
- memory and file-descriptor resource caps
- clamp warning behavior
- adaptive default-on behavior
- `--no-adaptive` fixed mode
- throttling backoff and connection-error freeze
- retries staying inside the active concurrency budget

## Provider Transport Interaction

For S3-compatible destinations, transfer reflow opts into HTTP transport sizing
from the effective ceiling. That keeps the SDK default path unchanged for
unrelated commands while reducing connection churn during high-throughput
reflow runs.

Technical draft slot:

- why idle connection pool sizing matters
- why `MaxConnsPerHost` is a hard resource backstop
- why transport limits derive from the effective ceiling, not the raw request
- how future providers should map this model onto their own client transports

## Output Fields

Transfer reflow run and summary records include additive concurrency fields for
operator audit. Draft final wording from the implemented schema:

- `adaptive_enabled`
- `concurrency_floor`
- `concurrency_initial`
- `concurrency_ceiling_requested`
- `concurrency_ceiling_effective`
- `concurrency_ceiling_reason`
- `concurrency_final`
- throttle/backoff/freeze counters
