# Concurrency and Throughput

This guide explains how Gonimbus bounds and adapts concurrent provider work for
large object-storage operations.

This page is the **provider-generalized** home for Gonimbus's concurrency model.
S3 is the concrete first implementation; GCS and future providers extend this
same model rather than introducing separate provider-specific throughput guides.

## Operator Model

`transfer reflow --parallel` is a **requested ceiling, not a guaranteed active
worker count.** A run resolves an _effective ceiling_ from the requested value
and local resource limits, then — in adaptive mode (the default) — varies the
_active_ copy concurrency within that ceiling in response to how the destination
behaves.

```
effective_ceiling = min(--parallel, memory_cap, fd_cap)
floor ≤ active concurrency ≤ effective_ceiling
```

### Requested vs effective ceiling (the resource cap)

The adaptive controller is a _soft_ bound — it only backs off when it receives a
throttle signal. An endpoint that degrades by latency, or refuses connections,
may never return a throttle, so the controller would otherwise ramp to the raw
requested ceiling. A hard, **resource-derived cap** prevents that from harming
the host:

- **Memory cap.** Each active copy can buffer up to one retry buffer
  (16 MiB) in memory. The cap allots a fraction (25%) of the available memory
  budget — container/cgroup-aware where discoverable, otherwise a conservative
  default — divided by the retry-buffer size. This keeps peak buffer memory
  bounded by `active × 16 MiB`, not `ceiling × 16 MiB`.
- **File-descriptor cap.** Derived from the process soft `RLIMIT_NOFILE` minus
  headroom, divided by the descriptors a single copy needs. This prevents
  connection/FD exhaustion at high concurrency.

The lower of the two (and the requested value) wins. Because the cap is computed
**at startup, independent of throttle feedback**, even `--parallel 9385` against
a never-throttling endpoint settles at a value the host can survive.

### The clamp is always visible

When the effective ceiling is below the requested value, the run reports **why**
— it never silently runs slower. The `concurrency_ceiling_reason` field carries
`requested` (no clamp) or `resource_capped:memory:<source>` / `resource_capped:fd`
(or both), and a human-readable notice is emitted to stderr. A lower-than-expected
concurrency is therefore explained, not inferred from throughput.

### Adaptive control (AIMD)

Within the effective ceiling, adaptive mode uses additive-increase /
multiplicative-decrease — the same shape as TCP congestion control:

- **Start cautiously.** Active concurrency begins at `min(16, effective_ceiling)`,
  not the ceiling.
- **Throttle → halve.** On a throttle signal (e.g. HTTP `503 SlowDown`) from
  either the source or destination provider, active concurrency is halved (floor
  of 1) and a cooldown begins.
- **Clean streak → step up.** After the cooldown, a sustained run of successful
  operations raises concurrency one step at a time — a gradual ramp back toward
  the ceiling, not an immediate snap-back.
- **Connection errors freeze, they don't cut.** A genuine network outage is not
  congestion, so connection errors never _decrease_ concurrency. But because a
  connection-error burst at high concurrency is often self-inflicted (FD/pool
  pressure), a connection-error streak **freezes** the additive increase rather
  than letting it ramp into a self-made wall.
- **Retries stay inside the budget.** A retried operation holds its concurrency
  slot for the duration — total in-flight work (active copies plus their retries)
  never exceeds the current concurrency, so retries during a throttle storm
  cannot amplify load.

The settled concurrency a run converges to is, in effect, the **empirically
discovered sustainable rate** for that endpoint and host — surfaced in the run
summary (below) so you can right-size future runs.

### Fixed mode

`--no-adaptive` disables the controller and runs at a fixed concurrency. That
fixed value is still the **effective** (resource-capped) ceiling — a fixed
`--parallel 9385` is clamped the same way — so fixed mode is deterministic
without being a footgun.

## Provider Transport Interaction

High concurrency only helps if the underlying client reuses connections. For
S3-compatible destinations, `transfer reflow` opts into HTTP transport sizing
derived from the **effective ceiling**:

- **Idle connection pool.** The default SDK transport keeps only a small idle
  pool per host, so above that count every worker re-establishes a TCP+TLS
  connection per request — a handshake tax that grows with concurrency. Sizing
  the idle pool from the effective ceiling lets workers reuse connections.
- **Hard connection backstop.** A per-host maximum-connections limit remains a
  deliberate, resource-aware cap — the FD safety net beneath the controller.
- **Sized from the effective ceiling, not the raw request.** Pool limits track
  the resource-capped ceiling, so the transport never holds more idle
  connections (FDs) than the host budget allows — the same amplification guard as
  the controller, by another door.
- **Scoped to reflow.** The tuning applies on the high-throughput reflow path;
  unrelated commands (`inspect`, `crawl`, …) keep the SDK default transport, so
  there is no surprise behavior change elsewhere.

**For future providers:** the model is provider-neutral. A new provider plugs in
by (1) exposing a throttle signal the controller can observe and (2) sizing its
own client transport (connection pool / concurrency limits) from the same
effective ceiling. S3 is the reference implementation of that contract; GCS
(planned) and others map onto it rather than re-deriving a throughput story.

## Output Fields

`transfer reflow` run and summary records carry additive concurrency fields for
operator audit (all backwards-compatible; existing consumers are unaffected):

| Field                                  | Meaning                                                                   |
| -------------------------------------- | ------------------------------------------------------------------------- |
| `adaptive_enabled`                     | Whether adaptive control was active (`false` under `--no-adaptive`).      |
| `concurrency_floor`                    | The minimum active concurrency (1).                                       |
| `concurrency_initial`                  | Active concurrency at run start (`min(16, effective_ceiling)`).           |
| `concurrency_ceiling_requested`        | The `--parallel` value requested.                                         |
| `concurrency_ceiling_effective`        | The resource-capped ceiling actually used.                                |
| `concurrency_ceiling_reason`           | `requested`, or `resource_capped:memory:<source>` / `resource_capped:fd`. |
| `concurrency_final`                    | Active concurrency when the run ended.                                    |
| `concurrency_max_active`               | The peak active concurrency reached.                                      |
| `concurrency_throttle_backoffs`        | Count of multiplicative decreases (throttle events).                      |
| `concurrency_additive_increases`       | Count of step-up increases.                                               |
| `concurrency_connection_error_freezes` | Count of increase-freezes from connection-error streaks.                  |

Reading these together — e.g. _requested 256 → effective 256 → final 48, 6
throttle backoffs_ — tells you the endpoint's sustainable rate and whether the
host resource cap (not the endpoint) was the limiting factor.
