// Independent logical reader for contract: durable-index/v0.
//
// Contract compliance tooling only (PDR-0006 Rule 2c). It is written from the
// standard's text, not from the repository's Python checker, and shares no code
// with it. It reads the authored fixtures and reports whether its own results
// agree with their expected outcomes. A disagreement is a finding against the
// fixture, the standard or one of the checkers; it is resolved by the contract
// text. This is not a reference implementation or a library.
//
// Validation interpreter: Node.js >= 22.6 with built-in type stripping (PDR-0006
// Rule 2c); conformance is judged on Node.js. Standard-library `node:` modules
// only, no install step. Type annotations are documentation: Node.js strips
// them and continuous integration runs no type checker. Bun may run the same
// source as a developer convenience, but its result is not conformance evidence.
//
// Usage: node --experimental-strip-types scripts/durable-index-reader.ts

import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const VECTORS = join(ROOT, "schemas/durable-index/v0/vectors");

type Scope = { namespace: string; prefix: string };

const utf8 = new TextEncoder();

// Unsigned UTF-8 byte order (I2). JavaScript string comparison uses UTF-16 code
// units, which disagrees with UTF-8 byte order for supplementary characters, so
// compare encoded bytes.
function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

function compareScopes(a: Scope, b: Scope): number {
  return (
    compareBytes(utf8.encode(a.namespace), utf8.encode(b.namespace)) ||
    compareBytes(utf8.encode(a.prefix), utf8.encode(b.prefix))
  );
}

function startsWithBytes(value: Uint8Array, prefix: Uint8Array): boolean {
  if (prefix.length > value.length) return false;
  for (let i = 0; i < prefix.length; i++) {
    if (value[i] !== prefix[i]) return false;
  }
  return true;
}

// A row belongs to a scope when its namespace is equal and its key begins with
// the prefix by exact UTF-8 bytes. No cleanup, trimming or normalization (I2).
function contains(scope: Scope, namespace: string, key: string): boolean {
  return (
    scope.namespace === namespace && startsWithBytes(utf8.encode(key), utf8.encode(scope.prefix))
  );
}

// One scope covers another when they share a namespace and the other's prefix
// starts with this one's.
function covers(outer: Scope, inner: Scope): boolean {
  return contains(outer, inner.namespace, inner.prefix);
}

// Sorted, duplicate-free and prefix-minimal (I2). Individual prefixes are never
// rewritten.
function canonical(scopes: Scope[]): Scope[] {
  const sorted = [...scopes].sort(compareScopes);
  const out: Scope[] = [];
  for (const scope of sorted) {
    if (!out.some((kept) => covers(kept, scope))) out.push(scope);
  }
  return out;
}

function u32(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n, false);
  return b;
}

function framed(text: string): Uint8Array[] {
  const bytes = utf8.encode(text);
  return [u32(bytes.length), bytes];
}

// The semantic plan digest input (I2): domain tag and zero byte, framed set id,
// four-byte scope count, then each canonical scope's framed namespace and prefix.
function planBytes(setId: string, scopes: Scope[]): Uint8Array {
  const parts: Uint8Array[] = [
    utf8.encode("durable-index/v0/plan"),
    new Uint8Array([0]),
    ...framed(setId),
    u32(scopes.length),
  ];
  for (const scope of scopes) parts.push(...framed(scope.namespace), ...framed(scope.prefix));
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");
const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

// Scope loss (I5): every old prefix must be covered by some new prefix in its
// namespace. Counts are irrelevant.
function loses(previous: Scope[], next: Scope[]): boolean {
  return previous.some((old) => !next.some((n) => covers(n, old)));
}

type Result = { file: string; agree: boolean; detail?: string };

function same(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function checkPlan(file: string): Result {
  const v = JSON.parse(readFileSync(join(VECTORS, "plans", file), "utf8"));
  const canon = canonical(v.input);
  if (!same(canon, v.canonical)) return { file, agree: false, detail: "canonical" };
  const bytes = planBytes(v.set_id, canon);
  if (hex(bytes) !== v.bytes_hex) return { file, agree: false, detail: "bytes_hex" };
  if (sha256(bytes) !== v.sha256) return { file, agree: false, detail: "sha256" };
  for (const [i, c] of (v.comparisons ?? []).entries()) {
    if (loses(canon, canonical(c.next)) !== c.loss) {
      return { file, agree: false, detail: `comparison ${i}` };
    }
  }
  for (const [i, m] of (v.membership ?? []).entries()) {
    if (contains(m.scope, m.namespace, m.key) !== m.contains) {
      return { file, agree: false, detail: `membership ${i}` };
    }
  }
  return { file: `plans/${file}`, agree: true };
}

// ---------------------------------------------------------------------------
// Transitions (I4 rows and lineage, I5 successors, I6 enrichment)

// Erasable syntax only (PDR-0006 Rule 2c): Node.js type stripping removes
// annotations but refuses syntax that emits code, such as this constructor
// parameter property written the long way.
class Refusal extends Error {
  reason: string;
  constructor(reason: string) {
    super(reason);
    this.reason = reason;
  }
}

function refuse(condition: unknown, reason: string): asserts condition {
  if (!condition) throw new Refusal(reason);
}

type Stamp = { snapshot_id: string; at: string };
type Revision = { kind: string; value: string } | null;
type Attribute = {
  name: string;
  value: string | null;
  operation: string;
  observed_at: string;
  against_revision: { kind: string; value: string };
};
type Row = {
  namespace: string;
  key: string;
  revision: Revision;
  size_bytes: number;
  last_modified: string | null;
  storage_class: string | null;
  first_seen: Stamp;
  last_seen: Stamp;
  last_changed: Stamp;
  deleted: Stamp | null;
  enrichments: Attribute[];
};

const SOURCE_FIELDS = [
  "namespace",
  "key",
  "revision",
  "size_bytes",
  "last_modified",
  "storage_class",
] as const;
const MAX_TEXT_BYTES = 4096;
const MAX_SIZE = 2 ** 53 - 1;

// Fixed-width UTC text: exactly nine fraction digits, real Gregorian dates,
// years 0001-9999, seconds 00-59 (I4). Fixed width makes text order equal time
// order, so instants compare as strings without losing nanoseconds.
const INSTANT = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.\d{9}Z$/;

function checkInstant(value: unknown): string {
  refuse(typeof value === "string", "invalid_instant");
  const m = INSTANT.exec(value);
  refuse(m, "invalid_instant");
  const [y, mo, d, h, mi, s] = m.slice(1).map(Number);
  refuse(y >= 1 && mo >= 1 && mo <= 12 && h <= 23 && mi <= 59 && s <= 59, "invalid_instant");
  const leap = (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1];
  refuse(d >= 1 && d <= days, "invalid_instant");
  return value;
}

function compareRows(a: { namespace: string; key: string }, b: { namespace: string; key: string }) {
  return (
    compareBytes(utf8.encode(a.namespace), utf8.encode(b.namespace)) ||
    compareBytes(utf8.encode(a.key), utf8.encode(b.key))
  );
}

const rowId = (r: { namespace: string; key: string }) => JSON.stringify([r.namespace, r.key]);

function checkText(value: string) {
  refuse(utf8.encode(value).length <= MAX_TEXT_BYTES, "capacity");
}

function checkSource(o: Record<string, unknown>) {
  refuse(typeof o.namespace === "string" && typeof o.key === "string", "invalid_shape");
  checkText(o.namespace as string);
  checkText(o.key as string);
  const size = o.size_bytes;
  refuse(
    Number.isInteger(size) && (size as number) >= 0 && (size as number) <= MAX_SIZE,
    "invalid_shape",
  );
  if (o.last_modified !== null) checkInstant(o.last_modified);
}

// Enrichments on a row: unique names, sorted by UTF-8 bytes, each bound to the
// row's non-null revision, and only on an active row (I6). A repeated name on a
// row is an ordering fault: names must be strictly increasing.
function checkEnrichments(row: Row) {
  const names = row.enrichments.map((e) => e.name);
  for (let i = 1; i < names.length; i++) {
    refuse(compareBytes(utf8.encode(names[i - 1]), utf8.encode(names[i])) < 0, "enrichment_order");
  }
  for (const e of row.enrichments) {
    checkInstant(e.observed_at);
    refuse(row.deleted === null, "enrichment_target");
    refuse(row.revision !== null && same(e.against_revision, row.revision), "enrichment_revision");
  }
}

// Internal consistency of one row's lineage (I4).
function checkLineage(row: Row) {
  for (const s of [row.first_seen, row.last_seen, row.last_changed, row.deleted]) {
    if (s) checkInstant(s.at);
  }
  refuse(
    row.first_seen.at <= row.last_seen.at && row.first_seen.at <= row.last_changed.at,
    "lineage_order",
  );
  if (row.deleted) {
    refuse(row.deleted.at >= row.last_seen.at, "lineage_order");
    refuse(same(row.deleted, row.last_changed), "lineage_order");
  }
}

const ROW_FIELDS = [
  ...SOURCE_FIELDS,
  "first_seen",
  "last_seen",
  "last_changed",
  "deleted",
  "enrichments",
];

// Rows are closed objects: exactly the row fields, nothing more (I4).
function checkRowShape(row: Row) {
  const keys = Object.keys(row);
  refuse(
    keys.length === ROW_FIELDS.length && ROW_FIELDS.every((f) => keys.includes(f)),
    "invalid_shape",
  );
}

function sourceChanged(row: Row, obs: Record<string, unknown>): boolean {
  return SOURCE_FIELDS.slice(2).some((f) => !same(row[f], obs[f]));
}

type Ack = {
  parent_sha256: string;
  plan_digest: string;
  acknowledged_by: string;
  acknowledged_at: string;
} | null;

type Transition = {
  mode: "baseline" | "observe" | "enrich";
  set_id: string;
  snapshot_id: string;
  generation: number;
  observation_at: string | null;
  discovery_complete: boolean;
  parent: null | {
    publication_sha256: string;
    set_id: string;
    snapshot_id: string;
    generation: number;
    scopes: Scope[];
    rows: Row[];
    ancestor_snapshot_ids?: string[];
  };
  admitted_parent_sha256: string | null;
  scopes: Scope[];
  acknowledgment: Ack;
  observations: Record<string, unknown>[];
  absence_authority: Scope[];
  enrichments: { namespace: string; key: string; attribute: Attribute }[];
};

type Outcome = {
  rows: Row[];
  admission: string;
  retained_unobserved_rows: number;
};

function transition(t: Transition): Outcome {
  // Plan scopes are bounded in UTF-8 bytes (I2) before any use of them.
  for (const scope of t.scopes) {
    checkText(scope.namespace);
    checkText(scope.prefix);
  }
  // Plan identity (I2): the wire plan must already be canonical; readers refuse
  // noncanonical lists rather than repair them.
  refuse(same(t.scopes, canonical(t.scopes)), "noncanonical_plan");

  if (t.mode === "baseline") {
    refuse(
      t.parent === null && t.generation === 0 && t.acknowledgment === null,
      "invalid_baseline",
    );
  } else {
    refuse(t.parent !== null, "parent_unavailable");
  }
  const parent = t.parent;
  const parentRows = parent ? parent.rows : [];

  if (parent) {
    // A successor naming its own snapshot as parent reuses that snapshot; an
    // ancestor further back that repeats it is a cycle (I5).
    refuse(parent.snapshot_id !== t.snapshot_id, "snapshot_reuse");
    refuse(!(parent.ancestor_snapshot_ids ?? []).includes(t.snapshot_id), "parent_cycle");
    refuse(parent.set_id === t.set_id, "set_mismatch");
    refuse(t.generation === parent.generation + 1, "generation_mismatch");
    refuse(t.admitted_parent_sha256 === parent.publication_sha256, "stale_parent");
  }

  // Inherited rows: strictly ordered, unique, and individually valid (I4).
  for (let i = 1; i < parentRows.length; i++) {
    const c = compareRows(parentRows[i - 1], parentRows[i]);
    refuse(c !== 0, "duplicate_key_row");
    refuse(c < 0, "row_order");
  }
  const seenTimes = new Map<string, string>();
  for (const row of parentRows) {
    checkRowShape(row);
    checkSource(row);
    checkLineage(row);
    checkEnrichments(row);
    for (const s of [row.first_seen, row.last_seen, row.last_changed, row.deleted]) {
      if (!s) continue;
      refuse(s.snapshot_id !== t.snapshot_id, "fabricated_freshness");
      const prior = seenTimes.get(s.snapshot_id);
      refuse(prior === undefined || prior === s.at, "stamp_conflict");
      seenTimes.set(s.snapshot_id, s.at);
    }
  }

  if (t.mode === "enrich") {
    refuse(parent !== null, "parent_unavailable");
    return enrich(t, parent, parentRows);
  }

  // Observation successor or baseline.
  refuse(t.discovery_complete, "discovery_failed");
  const at = checkInstant(t.observation_at);
  const now: Stamp = { snapshot_id: t.snapshot_id, at };
  for (const row of parentRows) {
    for (const s of [row.first_seen, row.last_seen, row.last_changed, row.deleted]) {
      if (s) refuse(s.at <= at, "clock_reversal");
    }
  }
  for (const scope of t.absence_authority) {
    refuse(
      t.scopes.some((p) => covers(p, scope)),
      "coverage_outside_plan",
    );
  }

  const observed = new Map<string, Record<string, unknown>>();
  for (const obs of t.observations) {
    refuse(
      Object.keys(obs).length === SOURCE_FIELDS.length && SOURCE_FIELDS.every((f) => f in obs),
      "observation_fields",
    );
    checkSource(obs);
    refuse(
      t.scopes.some((s) => contains(s, obs.namespace as string, obs.key as string)),
      "observation_outside_plan",
    );
    const id = rowId(obs as Row);
    refuse(!observed.has(id), "duplicate_observation");
    observed.set(id, obs);
  }

  // Admission (I5 steps 3-4).
  let admission: string;
  if (t.mode === "baseline") {
    admission = "baseline_not_compared";
  } else if (parent !== null && loses(parent.scopes, t.scopes)) {
    const ack = t.acknowledgment;
    refuse(ack !== null, "scope_loss");
    refuse(
      ack.parent_sha256 === parent.publication_sha256 &&
        ack.plan_digest === sha256(planBytes(t.set_id, t.scopes)),
      "acknowledgment_mismatch",
    );
    refuse(
      typeof ack.acknowledged_by === "string" && ack.acknowledged_by.length > 0,
      "acknowledgment_actor",
    );
    checkInstant(ack.acknowledged_at);
    admission = "loss_acknowledged";
  } else {
    refuse(t.acknowledgment === null, "unexpected_acknowledgment");
    admission = "no_loss";
  }

  const byId = new Map(parentRows.map((r) => [rowId(r), r]));
  const out: Row[] = [];
  const ids = new Set([...byId.keys(), ...observed.keys()]);
  for (const id of ids) {
    const prior = byId.get(id);
    const obs = observed.get(id);
    if (obs) {
      if (!prior) {
        out.push({
          ...(Object.fromEntries(SOURCE_FIELDS.map((f) => [f, obs[f]])) as Pick<
            Row,
            (typeof SOURCE_FIELDS)[number]
          >),
          first_seen: now,
          last_seen: now,
          last_changed: now,
          deleted: null,
          enrichments: [],
        });
        continue;
      }
      const reappeared = prior.deleted !== null;
      const changed = reappeared || sourceChanged(prior, obs);
      const revisionKept = obs.revision !== null && same(obs.revision, prior.revision);
      out.push({
        ...prior,
        ...(Object.fromEntries(SOURCE_FIELDS.map((f) => [f, obs[f]])) as object),
        last_seen: now,
        last_changed: changed ? now : prior.last_changed,
        deleted: null,
        enrichments: revisionKept && !reappeared ? prior.enrichments : [],
      } as Row);
      continue;
    }
    // Not observed: the row is inherited from the parent. Tombstone it only
    // under absence authority; otherwise retain it unchanged.
    if (prior === undefined) continue;
    const authorized = t.absence_authority.some((s) => contains(s, prior.namespace, prior.key));
    if (authorized && prior.deleted === null) {
      out.push({ ...prior, last_changed: now, deleted: now, enrichments: [] });
    } else {
      out.push(prior);
    }
  }

  applyEnrichments(out, t.enrichments);
  out.sort(compareRows);
  return {
    rows: out,
    admission,
    retained_unobserved_rows: out.filter((r) => r.last_seen.snapshot_id !== t.snapshot_id).length,
  };
}

// Apply attribute updates to active rows whose current revision they match.
function applyEnrichments(rows: Row[], updates: Transition["enrichments"]) {
  const byId = new Map(rows.map((r) => [rowId(r), r]));
  // One update per row/attribute; a repeat is refused, never last-write-wins (I6).
  const targets = updates.map((u) => JSON.stringify([u.namespace, u.key, u.attribute.name]));
  refuse(new Set(targets).size === targets.length, "duplicate_enrichment");
  for (const u of updates) {
    const row = byId.get(rowId(u));
    refuse(row !== undefined && row.deleted === null, "enrichment_target");
    refuse(
      row.revision !== null && same(u.attribute.against_revision, row.revision),
      "enrichment_revision",
    );
    checkInstant(u.attribute.observed_at);
    const existing = row.enrichments.find((e) => e.name === u.attribute.name);
    if (existing) refuse(u.attribute.observed_at >= existing.observed_at, "stale_enrichment");
    row.enrichments = [
      ...row.enrichments.filter((e) => e.name !== u.attribute.name),
      { ...u.attribute },
    ].sort((a, b) => compareBytes(utf8.encode(a.name), utf8.encode(b.name)));
  }
  for (const row of rows) checkEnrichments(row);
}

// An enrich successor changes only enrichments (I6).
function enrich(
  t: Transition,
  parent: NonNullable<Transition["parent"]>,
  parentRows: Row[],
): Outcome {
  refuse(t.observation_at === null && t.observations.length === 0, "enrichment_observation");
  refuse(same(t.scopes, parent.scopes) && t.acknowledgment === null, "enrichment_scope_change");
  const rows = parentRows.map((r) => ({
    ...r,
    enrichments: [...r.enrichments],
  }));
  applyEnrichments(rows, t.enrichments);
  return {
    rows,
    admission: "inherited",
    retained_unobserved_rows: rows.length,
  };
}

function evaluate(t: Transition): Outcome | { refusal: string } {
  try {
    return transition(structuredClone(t));
  } catch (e) {
    if (e instanceof Refusal) return { refusal: e.reason };
    throw e;
  }
}

const SCHEMAS = join(ROOT, "schemas/durable-index/v0");

function checkTransitionVector(file: string): Result {
  const v = JSON.parse(readFileSync(join(VECTORS, "transitions", file), "utf8"));
  const got = evaluate(v.input);
  return same(got, v.expected)
    ? { file: `transitions/${file}`, agree: true }
    : {
        file: `transitions/${file}`,
        agree: false,
        detail: describe(got, v.expected),
      };
}

function checkTransitionReject(dir: string): Result {
  const c = JSON.parse(readFileSync(join(SCHEMAS, "rejects/semantic", dir, "case.json"), "utf8"));
  const base = JSON.parse(readFileSync(join(SCHEMAS, c.baseline), "utf8"));
  const got = evaluate({ ...base.input, ...c.replace });
  return same(got, c.expected)
    ? { file: dir, agree: true }
    : { file: dir, agree: false, detail: describe(got, c.expected) };
}

function describe(got: unknown, want: unknown): string {
  const g = (got as { refusal?: string }).refusal ?? "accept";
  const w = (want as { refusal?: string }).refusal ?? "accept";
  return g === w ? "rows or counts differ" : `got ${g}, want ${w}`;
}

// Disagreements this reader keeps on purpose, each an open finding against the
// standard or a fixture, never resolved by copying the other checker. Each entry
// must name a current fixture and still disagree exactly as recorded: an entry
// that agrees, disagrees differently or matches no fixture fails the run, so a
// fix or a renamed fixture is noticed rather than silently absorbed. Every entry
// must be resolved before the next tagged release (PDR-0006 Rule 2c), so this
// table is empty at tag.
const KNOWN_DISAGREEMENTS: Record<string, string> = {};

const usedDisagreements = new Set<string>();

function report(label: string, results: Result[]): number {
  let failures = 0;
  let known = 0;
  for (const r of results) {
    const expected = KNOWN_DISAGREEMENTS[r.file];
    if (expected !== undefined) usedDisagreements.add(r.file);
    if (expected === undefined) {
      if (!r.agree) {
        console.error(`[!!] ${label} disagree: ${r.file} (${r.detail})`);
        failures++;
      }
    } else if (r.agree || r.detail !== expected) {
      console.error(
        `[!!] ${label} known disagreement changed: ${r.file} (${r.detail ?? "now agrees"})`,
      );
      failures++;
    } else {
      console.log(`[--] ${label} known disagreement: ${r.file} (${r.detail})`);
      known++;
    }
  }
  const agree = results.filter((r) => r.agree).length;
  console.log(`  ${label}: ${agree}/${results.length} agree, ${known} known disagreement(s)`);
  return failures;
}

function main(): number {
  const json = (dir: string) =>
    readdirSync(dir)
      .filter((f) => f.endsWith(".json"))
      .sort();
  const rejectDirs = readdirSync(join(SCHEMAS, "rejects/semantic"))
    .filter((d) => {
      const c = JSON.parse(readFileSync(join(SCHEMAS, "rejects/semantic", d, "case.json"), "utf8"));
      return c.runner === "transitions";
    })
    .sort();
  console.log("durable-index independent reader (logical; not a reference implementation)");
  let failures = 0;
  failures += report("plan vectors", json(join(VECTORS, "plans")).map(checkPlan));
  failures += report(
    "transition vectors",
    json(join(VECTORS, "transitions")).map(checkTransitionVector),
  );
  failures += report("transition rejects", rejectDirs.map(checkTransitionReject));
  for (const entry of Object.keys(KNOWN_DISAGREEMENTS)) {
    if (!usedDisagreements.has(entry)) {
      console.error(`[!!] known disagreement matches no current fixture: ${entry}`);
      failures++;
    }
  }
  return failures === 0 ? 0 : 1;
}

process.exitCode = main();
