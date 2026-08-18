import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";

import {
  ObjectProgressStore,
  ProgressReporter,
  parseProgressSnapshot,
  progressClaimKey,
  progressPointerKey,
  stableJsonBytes,
  type ProgressClaim,
  type ProgressInput,
  type ProgressObjectStore,
  type ProgressTrack,
} from "../src/index.js";

const input: ProgressInput = {
  revision: "a".repeat(40),
  contract_sha256: "b".repeat(64),
};

function track(
  completed: number,
  plan_id = "plan-1",
  total = 10,
): ProgressTrack {
  return {
    key: "items",
    plan_id,
    status: "running",
    completed,
    total,
    unit: "items",
    source_updated_at: "2026-08-18T12:00:00.000Z",
  };
}

class MemoryObjects implements ProgressObjectStore {
  readonly bucketId = "memory/progress";
  readonly files = new Map<string, Uint8Array>();

  async read(key: string): Promise<Uint8Array | null> {
    return this.files.get(key) ?? null;
  }

  async list(prefix: string): Promise<readonly string[]> {
    return [...this.files.keys()]
      .filter((key) => key.startsWith(prefix))
      .sort();
  }

  async write(key: string, content: Uint8Array): Promise<void> {
    this.files.set(key, Uint8Array.from(content));
  }
}

test("cross-language fixture is canonical", async () => {
  const raw = await readFile("fixtures/progress-v1.json");
  const snapshot = parseProgressSnapshot(JSON.parse(raw.toString("utf8")));
  assert.deepEqual(stableJsonBytes(snapshot), raw);
});

test("snapshot validation rejects regression-shaped counts", () => {
  assert.throws(
    () =>
      parseProgressSnapshot({
        schema_version: 1,
        run_id: "run",
        attempt_id: "attempt-1",
        sequence: 1,
        updated_at: "2026-08-18T12:00:00Z",
        input,
        state: "running",
        tracks: [{ ...track(11), total: 10 }],
      }),
    /must not exceed/u,
  );
});

test("reporter publishes ordered content-addressed snapshots", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects, "project");
  const moments = [
    new Date("2026-08-18T12:00:00Z"),
    new Date("2026-08-18T12:00:10Z"),
    new Date("2026-08-18T12:00:31Z"),
  ];
  const reporter = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => {
      const value = moments.shift();
      if (value === undefined) throw new Error("test clock exhausted");
      return value;
    },
  });
  reporter.plan([track(1)]);
  const first = await reporter.flush({ force: true });
  assert.equal(first?.snapshot.sequence, 1);
  assert.match(first?.reference.key ?? "", /sha256-[a-f0-9]{64}/u);

  reporter.update(track(2));
  assert.equal(await reporter.flush(), null);
  const second = await reporter.flush();
  assert.equal(second?.snapshot.sequence, 2);
  assert.deepEqual(second?.snapshot.previous, first?.reference);
  assert.deepEqual(await store.loadLatest("run"), second);
});

test("competing reporters cannot overwrite the same sequence", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const reporters = await Promise.all(
    ["attempt-1", "attempt-2"].map((attemptId) =>
      ProgressReporter.create({
        runId: "run",
        attemptId,
        input,
        store,
        clock: () => new Date("2026-08-18T12:00:00Z"),
      }),
    ),
  );
  for (const reporter of reporters) reporter.plan([track(1)]);

  const results = await Promise.allSettled(
    reporters.map((reporter) => reporter.flush({ force: true })),
  );
  assert.equal(
    results.filter((result) => result.status === "rejected").length,
    1,
  );
  assert.equal((await store.loadLatest("run"))?.snapshot.sequence, 1);
});

test("reporter restores progress for a replacement attempt", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const first = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  first.plan([track(6)]);
  const firstStored = await first.flush({ force: true });
  assert.ok(firstStored);

  const second = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-2",
    jobId: "job-2",
    input,
    store,
    clock: () => new Date("2026-08-18T12:01:00Z"),
  });
  assert.deepEqual(second.tracks, [track(6)]);
  second.update(track(7));
  const secondStored = await second.flush({ force: true });
  assert.equal(secondStored?.snapshot.sequence, 2);
  assert.equal(secondStored?.snapshot.attempt_id, "attempt-2");
});

test("terminal state is preserved after restart", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const first = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  first.plan([{ ...track(10), status: "completed" }]);
  first.setState("completed");
  assert.ok(await first.flush({ force: true }));

  const replacement = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-2",
    input,
    store,
    clock: () => new Date("2026-08-18T12:01:00Z"),
  });
  assert.equal(await replacement.flush({ force: true }), null);
  assert.throws(
    () => replacement.setState("running"),
    /terminal progress state/u,
  );
});

test("same plan rejects regression and a new plan resets counts", async () => {
  const reporter = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store: new ObjectProgressStore(new MemoryObjects()),
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  reporter.plan([track(5)]);
  assert.throws(() => reporter.update(track(4)), /cannot move backwards/u);
  assert.throws(
    () => reporter.update({ ...track(5), total: 11 }),
    /total cannot change/u,
  );
  assert.throws(
    () =>
      reporter.plan([
        { key: "later", plan_id: "plan-1", status: "pending" },
        track(4),
      ]),
    /cannot move backwards/u,
  );
  assert.deepEqual(
    reporter.tracks.map((item) => item.key),
    ["items"],
  );
  reporter.update(track(0, "plan-2", 20));
  assert.deepEqual(reporter.tracks, [track(0, "plan-2", 20)]);
});

test("pointer metadata must match its snapshot", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const reporter = await ProgressReporter.create({
    runId: "other-run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  reporter.plan([track(1)]);
  const stored = await reporter.flush({ force: true });
  assert.ok(stored);
  await objects.write(
    progressPointerKey("", "run"),
    stableJsonBytes({
      schema_version: 1,
      run_id: "run",
      sequence: stored.snapshot.sequence,
      updated_at: stored.snapshot.updated_at,
      snapshot: stored.reference,
    }),
  );

  await assert.rejects(store.loadLatest("run"), /snapshot run_id mismatch/u);
});

test("orphan sequence claim restores a missing pointer", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const reporter = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  reporter.plan([track(1)]);
  const stored = await reporter.flush({ force: true });
  assert.ok(stored);
  objects.files.delete(progressPointerKey("", "run"));

  assert.deepEqual(await store.loadLatest("run"), stored);
});

test("competing sequence claims are rejected", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const reporter = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  reporter.plan([track(1)]);
  const stored = await reporter.flush({ force: true });
  assert.ok(stored);
  const competing: ProgressClaim = {
    schema_version: 1,
    run_id: "run",
    attempt_id: "attempt-2",
    sequence: 1,
    created_at: stored.snapshot.updated_at,
    snapshot: stored.reference,
  };
  await objects.write(
    progressClaimKey("", competing),
    stableJsonBytes(competing),
  );

  await assert.rejects(
    store.loadLatest("run"),
    /competing progress sequence claims/u,
  );
});

test("stable JSON sorts object keys", () => {
  assert.equal(
    Buffer.from(stableJsonBytes({ z: 1, a: { y: 2, b: 3 } })).toString("utf8"),
    '{\n  "a": {\n    "b": 3,\n    "y": 2\n  },\n  "z": 1\n}\n',
  );
});

test("object store detects corrupt immutable snapshot", async () => {
  const objects = new MemoryObjects();
  const store = new ObjectProgressStore(objects);
  const reporter = await ProgressReporter.create({
    runId: "run",
    attemptId: "attempt-1",
    input,
    store,
    clock: () => new Date("2026-08-18T12:00:00Z"),
  });
  reporter.plan([track(1)]);
  const stored = await reporter.flush({ force: true });
  assert.ok(stored);
  objects.files.set(stored.reference.key, Buffer.from("{}\n"));
  await assert.rejects(store.loadLatest("run"), /byte count mismatch/u);
});
