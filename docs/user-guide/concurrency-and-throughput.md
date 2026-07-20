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

- **Memory cap.** Each active copy can buffer up to one retry buffer in memory.
  The cap is the resolved **memory budget** divided by the **per-copy retry-buffer
  cap** (16 MiB by default, shrunk to the budget when the budget is smaller). The
  budget is either derived as a fraction (25%) of the detected memory limit or
  supplied explicitly with `--memory-budget`. This keeps peak buffer memory
  bounded by `active × retry_buffer_cap`, not `ceiling × 16 MiB`. See
  [Memory limit detection and the operator budget](#memory-limit-detection-and-the-operator-budget).
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
concurrency is therefore explained, not inferred from throughput. When the memory
clamp comes from an operator-supplied budget, the source reads
`resource_capped:memory:operator_budget` rather than a detection source, so an
operator-chosen bound is never mistaken for a host constraint.

### Memory limit detection and the operator budget

The memory limit that derives the budget is resolved by probing **every** viable
candidate and binding the **lowest positive** one:

| Candidate              | Source label              | Notes                                             |
| ---------------------- | ------------------------- | ------------------------------------------------- |
| Container/cgroup limit | `cgroup_v2` / `cgroup_v1` | Linux; the hard limit the kernel will enforce.    |
| Explicit runtime limit | `runtime`                 | `GOMEMLIMIT` / `debug.SetMemoryLimit`.            |
| Detected physical RAM  | `physical_ram`            | macOS `sysctl`, Linux `meminfo`, Windows API.     |
| No candidate succeeded | `detection_unavailable`   | Conservative 1 GiB default — never host capacity. |

**Lowest binds — an explicit limit may tighten, never widen.** Setting
`GOMEMLIMIT` above a container's hard limit does not authorize the transfer to
size itself against the larger number; the cgroup limit still binds. Equally, a
failed probe never falls back to "assume all host RAM": when nothing is
detectable, the conservative default applies and the records say
`detection_unavailable` so the clamp is attributable.

`GOMEMLIMIT` is an **interim runtime bound, not the production control.** It
governs the Go heap soft target, not the descriptors, provider-SDK buffers, or
transport pools a transfer also consumes. Use `--memory-budget` to state the
transfer's governed memory directly.

#### `--memory-budget`

```bash
gonimbus transfer reflow --stdin \
  --dest 's3://dest-bucket/reflowed/' \
  --parallel 256 --memory-budget 8GiB < reflow-input.jsonl
```

- **What it governs.** Transfer retry buffering and concurrency sizing — _not_
  total process memory and not provider-SDK allocations.
- **Config key.** `memory_budget` in the config file (schema-declared; SI or IEC
  size strings, e.g. `512MiB`, `8GiB`, or raw bytes).
- **Precedence.** Explicit flag → config key → derived (25% of the detected
  limit) when omitted.
- **Bounds.** Minimum 64 MiB, maximum 4 TiB. Values outside the bounds, or that
  fail to parse, are **refused before any provider construction or destination
  mutation** — an invalid budget fails the run, it does not degrade it.
- **Clamped to the detected limit.** A budget above the detected memory limit is
  reduced to that limit and recorded as `operator_clamped_to_limit`. When
  detection is unavailable the operator value is authoritative (still
  sanity-bounded) — the run never silently assumes maximum host RAM.
- **Resume-persistent.** The resolved budget is carried in checkpoint
  configuration and restored on `--resume` / `--resume-run`, so a resumed run
  does not silently re-derive a different budget on a differently sized host.
- **The retry cap shrinks to the budget, the budget never rises to the cap.**
  When the budget is below the 16 MiB default retry-buffer size, the per-copy cap
  becomes the budget; objects above the cap spool instead of buffering. The
  records and the copy-path allocator share this one resolved bound
  (`retry_buffer_cap_bytes`).

### Memory admission

Copy buffer bytes are admitted by a **FIFO reservation ledger** before the
concurrency token is taken and before any provider action, using the same
arithmetic the allocator uses: a known source size reserves
`min(size, retry_buffer_cap)`; an unknown or absent size reserves the full cap.
Reservations are released exactly once on every terminal path, admission is
cancellable, and the queue is head-of-line ordered so a large request cannot be
starved by a stream of small ones.

**The ceiling stays conservative and static.** The effective memory ceiling is
still `budget ÷ retry_buffer_cap` computed at startup — the ledger does not
raise it when the workload turns out to be small-object. Reservations are
bounded by construction to fit inside the budget, so the pressure telemetry
below is **observation only**: it explains where a run spent time waiting on
admission; it does not license a higher concurrency ceiling than the static
arithmetic allows. Any future relaxation of that division is a separate,
explicitly evaluated change.

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
without being a footgun. Fixed mode has no ramp: it runs _at_ the effective
ceiling, never below it.

### The configuration invariant

Every resolved concurrency configuration — whether it comes from the CLI, the
embedded library's `Config.Concurrency` (including the zero value, which
resolves to resource-derived defaults), or direct `NewConcurrencyLimiter`
construction — is normalized to one invariant before any work starts:

```
1 ≤ floor ≤ initial ≤ effective_ceiling ≤ requested_ceiling
```

The consequences worth knowing:

- **No run executes different concurrency than its records report.** Pool
  size, limiter behavior, and the run/summary fields all derive from the same
  normalized configuration.
- **Throttle recovery can never exceed the effective ceiling.** Multiplicative
  decrease recovers to `max(floor, current/2)`; because the floor is clamped
  to the effective ceiling, backoff recovery cannot push observed concurrency
  above what the records claim.
- **Partial configurations resolve safely.** A library configuration missing
  its initial value runs at `min(16, effective)` when adaptive, or at the
  effective ceiling when fixed — never silently serial.

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

| Field                                  | Meaning                                                                                                                                          |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `execution_path`                       | Which execution path ran the transfer: `engine` (library engine) or `cli-pool`. See "Execution paths" in the reflow guide.                       |
| `adaptive_enabled`                     | Whether adaptive control was active (`false` under `--no-adaptive`).                                                                             |
| `concurrency_floor`                    | The normalized minimum active concurrency (default 1; embedded library configurations may set a higher floor, clamped to the effective ceiling). |
| `concurrency_initial`                  | Active concurrency at run start: adaptive mode `min(16, effective_ceiling)` subject to the normalized floor; fixed mode the effective ceiling.   |
| `concurrency_ceiling_requested`        | The `--parallel` value requested.                                                                                                                |
| `concurrency_ceiling_effective`        | The resource-capped ceiling actually used.                                                                                                       |
| `concurrency_ceiling_reason`           | `requested`, or `resource_capped:memory:<source>` / `resource_capped:fd`.                                                                        |
| `concurrency_final`                    | Active concurrency when the run ended.                                                                                                           |
| `concurrency_max_active`               | The peak active concurrency reached.                                                                                                             |
| `concurrency_throttle_backoffs`        | Count of multiplicative decreases (throttle events).                                                                                             |
| `concurrency_additive_increases`       | Count of step-up increases.                                                                                                                      |
| `concurrency_connection_error_freezes` | Count of increase-freezes from connection-error streaks.                                                                                         |
| `concurrency_time_avg_active`          | Time-averaged active concurrency over the run's execution window (see below).                                                                    |

Reading these together — e.g. _requested 256 → effective 256 → final 48, 6
throttle backoffs_ — tells you the endpoint's sustainable rate and whether the
host resource cap (not the endpoint) was the limiting factor.

### Occupancy (`concurrency_time_avg_active`)

`concurrency_max_active` is a peak; occupancy is the **average**. It is
limiter-active provider-operation-seconds divided by the run's execution window,
rounded to three decimals. Both execution paths reset that window immediately
before emitting the run record, so setup, preflight, and provider construction
are excluded identically — the run record's startup sample is therefore always
`0`, and the **summary** value is the completed-run diagnostic.

A low time-average under a high effective ceiling means the pipe was bound by
something other than the ceiling. It is a signal to read together with the
throttle/freeze counters and the memory-pressure fields below — not standalone
proof of a starved producer.

### Memory fields

These are populated when the ceiling was memory-resolved (probe-resolved runs
always carry limit, budget, and sources). They are omitted for configurations
constructed directly with an explicit effective ceiling and no probe — the
ceiling was not memory-derived, and the records say so rather than implying an
arithmetic that did not run.

| Field                           | Meaning                                                                             |
| ------------------------------- | ----------------------------------------------------------------------------------- |
| `memory_limit_bytes`            | The detected memory limit that bound the run.                                       |
| `memory_limit_source`           | `cgroup_v2`, `cgroup_v1`, `runtime`, `physical_ram`, or `detection_unavailable`.    |
| `memory_budget_requested_bytes` | The operator-supplied budget, when one was given (omitted for derived budgets).     |
| `memory_budget_effective_bytes` | The budget actually used after clamping.                                            |
| `memory_budget_source`          | `derived` (25% of the limit), `operator`, or `operator_clamped_to_limit`.           |
| `retry_buffer_cap_bytes`        | The resolved per-copy in-memory retry-buffer bound the copy paths allocate against. |
| `memory_reserved_peak_bytes`    | Peak outstanding copy-buffer reservation held by the admission ledger.              |
| `memory_reservation_waits`      | Count of copies that had to wait for admission.                                     |
| `memory_reservation_wait_ms`    | Total admission-wait time across the run.                                           |

**Interpreting pressure.** Waits with a peak sitting near the effective budget
mean the run was bound by _memory admission_ — the fix is a larger
`--memory-budget` (if the host genuinely has the headroom) or a smaller
`--parallel`, not more workers. Waits near zero with a low
`concurrency_time_avg_active` point the other way: the producer, source
provider, or endpoint was the bound, and memory was not the limiting factor.
These fields are diagnostic; they do not change the ceiling the run executes
under.
