import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";

import {
  CheckpointCoordinator,
  checkpointBundleKey,
  checkpointClaimKey,
  checkpointPointerKey,
  createCheckpointBundle,
  parseCheckpointManifest,
  stableCheckpointJsonBytes,
  verifyCheckpointBundle,
  type CheckpointAdapter,
  type CheckpointBoundary,
  type CheckpointClaim,
  type CheckpointManifest,
  type CheckpointObjectStore,
  type CheckpointReceipt,
  type CheckpointReceiptStore,
} from "../src/index.js";

const PLAN_SHA256 = "a".repeat(64);
const CREATED_AT = "2026-08-19T12:00:00.000Z";

function boundary(sequence: number): CheckpointBoundary {
  return {
    name: "batch",
    sequence,
    reached_at: CREATED_AT,
    metadata: { completed: sequence },
  };
}

class TextAdapter implements CheckpointAdapter<{ restored: string }> {
  readonly spec = {
    name: "text",
    version: 1,
    resume_mode: "exact" as const,
  };
  value: string;

  constructor(value: string) {
    this.value = value;
  }

  async save(): Promise<readonly { path: string; bytes: Uint8Array }[]> {
    return [
      { path: "nested/state.txt", bytes: Buffer.from(this.value, "utf8") },
      { path: "empty.bin", bytes: new Uint8Array() },
    ];
  }

  async restore(
    _manifest: CheckpointManifest,
    payloads: ReadonlyMap<string, Uint8Array>,
  ): Promise<{ restored: string }> {
    const state = payloads.get("nested/state.txt");
    if (state === undefined) throw new Error("state payload is missing");
    this.value = Buffer.from(state).toString("utf8");
    return { restored: this.value };
  }
}

class MemoryCheckpointObjects implements CheckpointObjectStore {
  readonly bucketId = "memory/checkpoints";
  readonly files = new Map<string, Uint8Array>();
  pointerWrites = 0;
  failNextClaim = false;
  failPointerWrites = false;

  async read(path: string): Promise<Uint8Array | null> {
    const value = this.files.get(path);
    return value === undefined ? null : Uint8Array.from(value);
  }

  async writeImmutable(path: string, bytes: Uint8Array): Promise<void> {
    if (this.failNextClaim && path.includes("/claims/")) {
      this.failNextClaim = false;
      throw new Error("forced claim failure");
    }
    const existing = this.files.get(path);
    if (
      existing !== undefined &&
      !Buffer.from(existing).equals(Buffer.from(bytes))
    ) {
      throw new Error(`immutable object differs: ${path}`);
    }
    this.files.set(path, Uint8Array.from(bytes));
  }

  async writePointerHint(path: string, bytes: Uint8Array): Promise<void> {
    if (this.failPointerWrites) throw new Error("forced pointer failure");
    this.pointerWrites += 1;
    this.files.set(path, Uint8Array.from(bytes));
  }

  async list(prefix: string): Promise<readonly string[]> {
    return [...this.files.keys()]
      .filter((key) => key.startsWith(prefix))
      .sort();
  }
}

class MemoryReceipts implements CheckpointReceiptStore {
  readonly receipts: CheckpointReceipt[] = [];

  async publish(receipt: CheckpointReceipt): Promise<void> {
    this.receipts.push(receipt);
  }
}

test("Python and TypeScript checkpoint bundles have identical bytes", async () => {
  const expectedBundle = await readFile(
    "fixtures/checkpoint-v1/checkpoint.hfjob",
  );
  const expectedManifest = parseCheckpointManifest(
    JSON.parse(await readFile("fixtures/checkpoint-v1/manifest.json", "utf8")),
  );
  const created = createCheckpointBundle({
    runId: "fixture-run",
    attemptId: "attempt-1",
    adapter: { name: "fixture", version: 1, resume_mode: "exact" },
    planSha256: PLAN_SHA256,
    boundary: {
      name: "batch",
      sequence: 1,
      reached_at: "2026-08-19T12:00:00Z",
      metadata: { completed: 1 },
    },
    previousCheckpointSha256: null,
    payloads: [
      { path: "a.txt", bytes: Buffer.from("first") },
      { path: "empty.bin", bytes: new Uint8Array() },
    ],
    createdAt: "2026-08-19T12:00:00Z",
  });
  assert.deepEqual(created.manifest, expectedManifest);
  assert.deepEqual(Buffer.from(created.bytes), expectedBundle);
});

test("checkpoint bundle is deterministic and verifies multiple payloads", () => {
  const options = {
    runId: "run",
    attemptId: "attempt-1",
    adapter: { name: "text", version: 1, resume_mode: "exact" as const },
    planSha256: PLAN_SHA256,
    boundary: boundary(1),
    previousCheckpointSha256: null,
    payloads: [
      { path: "z.txt", bytes: Buffer.from("last") },
      { path: "a.txt", bytes: Buffer.from("first") },
    ],
    createdAt: CREATED_AT,
  };
  const first = createCheckpointBundle(options);
  const second = createCheckpointBundle(options);
  assert.deepEqual(first.bytes, second.bytes);
  assert.deepEqual(
    [...verifyCheckpointBundle(first.bytes).payloads.keys()],
    ["a.txt", "z.txt"],
  );
});

test("coordinator restores claims when the pointer hint is missing", async () => {
  const store = new MemoryCheckpointObjects();
  const receipts = new MemoryReceipts();
  const first = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-1",
    jobId: "job-1",
    planSha256: PLAN_SHA256,
    store,
    receiptStore: receipts,
    clock: () => new Date(CREATED_AT),
  });
  const source = new TextAdapter("one");
  await first.commit(boundary(1), source);
  source.value = "two";
  const head = await first.finish(boundary(2), source);
  store.files.delete(checkpointPointerKey("", "run"));

  const second = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-2",
    jobId: "job-2",
    planSha256: PLAN_SHA256,
    store,
    receiptStore: receipts,
    clock: () => new Date(CREATED_AT),
  });
  const target = new TextAdapter("");
  const restored = await second.restoreLatest(target);

  assert.equal(restored?.checkpoint.sha256, head.sha256);
  assert.equal(target.value, "two");
  assert.equal(store.pointerWrites, 3);
  assert.deepEqual(
    receipts.receipts.map((receipt) => receipt.kind),
    ["terminal", "restore"],
  );
});

test("an uploaded bundle without a claim does not become committed", async () => {
  const store = new MemoryCheckpointObjects();
  store.failNextClaim = true;
  const interrupted = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-1",
    planSha256: PLAN_SHA256,
    store,
    clock: () => new Date(CREATED_AT),
  });
  await assert.rejects(
    interrupted.commit(boundary(1), new TextAdapter("one")),
    /forced claim failure/u,
  );
  assert.ok(
    [...store.files.keys()].some((key) => key.endsWith("checkpoint.hfjob")),
  );

  const replacement = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-2",
    planSha256: PLAN_SHA256,
    store,
  });
  assert.equal(await replacement.restoreLatest(new TextAdapter("")), null);
});

test("a pointer-hint failure does not lose a claimed checkpoint", async () => {
  const store = new MemoryCheckpointObjects();
  store.failPointerWrites = true;
  const first = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-1",
    planSha256: PLAN_SHA256,
    store,
    clock: () => new Date(CREATED_AT),
  });
  const reference = await first.commit(boundary(1), new TextAdapter("one"));
  assert.equal(store.files.has(checkpointPointerKey("", "run")), false);

  const replacement = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-2",
    planSha256: PLAN_SHA256,
    store,
  });
  const restored = await replacement.restoreLatest(new TextAdapter(""));
  assert.equal(restored?.checkpoint.sha256, reference.sha256);
});

test("coordinator rejects different claims for one sequence", async () => {
  const store = new MemoryCheckpointObjects();
  const coordinator = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-1",
    planSha256: PLAN_SHA256,
    store,
    clock: () => new Date(CREATED_AT),
  });
  await coordinator.commit(boundary(1), new TextAdapter("one"));

  const other = createCheckpointBundle({
    runId: "run",
    attemptId: "attempt-2",
    adapter: { name: "text", version: 1, resume_mode: "exact" },
    planSha256: PLAN_SHA256,
    boundary: boundary(1),
    previousCheckpointSha256: null,
    payloads: [{ path: "state.txt", bytes: Buffer.from("other") }],
    createdAt: CREATED_AT,
  });
  const sha256 = await import("node:crypto").then(({ createHash }) =>
    createHash("sha256").update(other.bytes).digest("hex"),
  );
  const reference = {
    bucket: store.bucketId,
    key: checkpointBundleKey("", "run", sha256),
    sha256,
    bytes: other.bytes.byteLength,
  };
  await store.writeImmutable(reference.key, other.bytes);
  const claim: CheckpointClaim = {
    schema_version: 1,
    run_id: "run",
    attempt_id: "attempt-2",
    sequence: 1,
    plan_sha256: PLAN_SHA256,
    previous_checkpoint_sha256: null,
    checkpoint: reference,
    created_at: CREATED_AT,
  };
  await store.writeImmutable(
    checkpointClaimKey("", claim),
    stableCheckpointJsonBytes(claim),
  );

  const replacement = CheckpointCoordinator.create({
    runId: "run",
    attemptId: "attempt-3",
    planSha256: PLAN_SHA256,
    store,
  });
  await assert.rejects(
    replacement.restoreLatest(new TextAdapter("")),
    /conflicting checkpoint claims/u,
  );
});

test("bundle rejects traversal and tampering", () => {
  assert.throws(
    () =>
      createCheckpointBundle({
        runId: "run",
        attemptId: "attempt-1",
        adapter: { name: "text", version: 1, resume_mode: "exact" },
        planSha256: PLAN_SHA256,
        boundary: boundary(1),
        previousCheckpointSha256: null,
        payloads: [{ path: "../state", bytes: Buffer.from("x") }],
        createdAt: CREATED_AT,
      }),
    /safe relative POSIX/u,
  );
  const bundle = createCheckpointBundle({
    runId: "run",
    attemptId: "attempt-1",
    adapter: { name: "text", version: 1, resume_mode: "exact" },
    planSha256: PLAN_SHA256,
    boundary: boundary(1),
    previousCheckpointSha256: null,
    payloads: [{ path: "state", bytes: Buffer.from("x") }],
    createdAt: CREATED_AT,
  });
  const tampered = Uint8Array.from(bundle.bytes);
  tampered[tampered.length - 1] = 0;
  assert.throws(() => verifyCheckpointBundle(tampered), /SHA-256 mismatch/u);
});
