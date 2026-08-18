import { createHash } from "node:crypto";

export const PROGRESS_SCHEMA_VERSION = 1 as const;
const MAX_TRACKS = 256;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const REPO_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*\/[A-Za-z0-9][A-Za-z0-9._-]*$/u;
const SHA256 = /^[a-f0-9]{64}$/u;
const RFC3339 =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/u;
const PUBLICATION_LOCKS = new Map<string, AsyncLock>();

export type ProgressStatus =
  | "pending"
  | "running"
  | "waiting"
  | "blocked"
  | "completed"
  | "failed"
  | "cancelled";

const TERMINAL_STATUSES = new Set<ProgressStatus>([
  "completed",
  "failed",
  "cancelled",
]);

export type ProgressInput = {
  revision: string;
  contract_sha256: string;
};

export type ArtifactRef = {
  bucket: string;
  key: string;
  sha256: string;
  bytes: number;
};

export type ProgressTrack = {
  key: string;
  plan_id: string;
  status: ProgressStatus;
  label?: string;
  completed?: number;
  total?: number;
  unit?: string;
  source_updated_at?: string;
};

export type ProgressSnapshot = {
  schema_version: 1;
  run_id: string;
  attempt_id: string;
  job_id?: string;
  sequence: number;
  updated_at: string;
  input: ProgressInput;
  state: ProgressStatus;
  previous?: ArtifactRef;
  tracks: readonly ProgressTrack[];
};

export type ProgressPointer = {
  schema_version: 1;
  run_id: string;
  sequence: number;
  updated_at: string;
  snapshot: ArtifactRef;
};

export type ProgressClaim = {
  schema_version: 1;
  run_id: string;
  attempt_id: string;
  sequence: number;
  created_at: string;
  snapshot: ArtifactRef;
};

export type StoredProgress = {
  snapshot: ProgressSnapshot;
  reference: ArtifactRef;
};

export interface ProgressStore {
  loadLatest(runId: string): Promise<StoredProgress | null>;
  loadReference(reference: ArtifactRef): Promise<ProgressSnapshot>;
  publish(snapshot: ProgressSnapshot): Promise<StoredProgress>;
}

export interface ProgressObjectStore {
  readonly bucketId: string;
  read(key: string): Promise<Uint8Array | null>;
  list(prefix: string): Promise<readonly string[]>;
  write(key: string, content: Uint8Array): Promise<void>;
}

export function progressPointerKey(prefix: string, runId: string): string {
  requireSafeId(runId, "run_id");
  return joinKey(
    normalizePrefix(prefix),
    "operations",
    runId,
    "progress",
    "current.json",
  );
}

export function progressSnapshotKey(
  prefix: string,
  runId: string,
  digest: string,
): string {
  requireSafeId(runId, "run_id");
  if (!SHA256.test(digest))
    throw new Error("digest must be 64 lowercase hex characters");
  return joinKey(
    normalizePrefix(prefix),
    "operations",
    runId,
    "progress",
    "snapshots",
    `sha256-${digest}`,
    "progress.json",
  );
}

export function progressClaimPrefix(
  prefix: string,
  runId: string,
  sequence: number,
): string {
  requireSafeId(runId, "run_id");
  requireInteger(sequence, "sequence", 1);
  return joinKey(
    normalizePrefix(prefix),
    "operations",
    runId,
    "progress",
    "claims",
    `sequence-${sequence.toString().padStart(16, "0")}`,
  );
}

export function progressClaimKey(prefix: string, claim: ProgressClaim): string {
  return joinKey(
    progressClaimPrefix(prefix, claim.run_id, claim.sequence),
    `${claim.attempt_id}.json`,
  );
}

export class ObjectProgressStore implements ProgressStore {
  readonly #objects: ProgressObjectStore;
  readonly #prefix: string;
  readonly #publicationLock: AsyncLock;

  constructor(objects: ProgressObjectStore, prefix = "") {
    requireRepoId(objects.bucketId, "bucketId");
    this.#objects = objects;
    this.#prefix = normalizePrefix(prefix);
    this.#publicationLock = sharedPublicationLock(
      objects.bucketId,
      this.#prefix,
    );
  }

  async loadLatest(runId: string): Promise<StoredProgress | null> {
    const raw = await this.#objects.read(
      progressPointerKey(this.#prefix, runId),
    );
    const pointer = raw === null ? null : parseProgressPointer(parseJson(raw));
    return this.#reconcileLatest(pointer, runId);
  }

  async loadReference(reference: ArtifactRef): Promise<ProgressSnapshot> {
    validateArtifact(reference);
    if (reference.bucket !== this.#objects.bucketId) {
      throw new Error("progress snapshot Bucket mismatch");
    }
    const raw = await this.#objects.read(reference.key);
    if (raw === null)
      throw new Error(`progress snapshot is missing: ${reference.key}`);
    verifyBytes(raw, reference);
    return parseProgressSnapshot(parseJson(raw));
  }

  publish(snapshot: ProgressSnapshot): Promise<StoredProgress> {
    return this.#publicationLock.run(() => this.#publish(snapshot));
  }

  async #publish(snapshot: ProgressSnapshot): Promise<StoredProgress> {
    const validated = parseProgressSnapshot(snapshot);
    const latest = await this.loadLatest(validated.run_id);
    validatePublication(validated, latest);
    const raw = stableJsonBytes(validated);
    const digest = sha256(raw);
    const key = progressSnapshotKey(this.#prefix, validated.run_id, digest);
    const reference: ArtifactRef = {
      bucket: this.#objects.bucketId,
      key,
      sha256: digest,
      bytes: raw.byteLength,
    };
    const existing = await this.#objects.read(key);
    if (existing === null) {
      await this.#objects.write(key, raw);
    } else if (!bytesEqual(existing, raw)) {
      throw new Error("immutable progress snapshot differs");
    }
    const stored = await this.#objects.read(key);
    if (stored === null)
      throw new Error("uploaded progress snapshot is missing");
    verifyBytes(stored, reference);
    const claim: ProgressClaim = {
      schema_version: PROGRESS_SCHEMA_VERSION,
      run_id: validated.run_id,
      attempt_id: validated.attempt_id,
      sequence: validated.sequence,
      created_at: validated.updated_at,
      snapshot: reference,
    };
    const claimKey = progressClaimKey(this.#prefix, claim);
    const claimRaw = stableJsonBytes(claim);
    const existingClaim = await this.#objects.read(claimKey);
    if (existingClaim === null) {
      await this.#objects.write(claimKey, claimRaw);
    } else if (!bytesEqual(existingClaim, claimRaw)) {
      throw new Error("immutable progress claim differs");
    }
    requireSingleClaim(
      await this.#loadClaims(validated.run_id, validated.sequence),
      claim,
    );
    await this.#writePointer({
      schema_version: PROGRESS_SCHEMA_VERSION,
      run_id: validated.run_id,
      sequence: validated.sequence,
      updated_at: validated.updated_at,
      snapshot: reference,
    });
    return { snapshot: validated, reference };
  }

  async #reconcileLatest(
    pointer: ProgressPointer | null,
    runId: string,
  ): Promise<StoredProgress | null> {
    let current: StoredProgress | null = null;
    if (pointer !== null) {
      if (pointer.run_id !== runId)
        throw new Error("progress pointer run_id mismatch");
      const snapshot = await this.loadReference(pointer.snapshot);
      validatePointerSnapshot(pointer, snapshot);
      current = { snapshot, reference: pointer.snapshot };
      validateClaim(
        requireSingleClaim(await this.#loadClaims(runId, snapshot.sequence)),
        current,
      );
    }
    while (true) {
      const sequence = current === null ? 1 : current.snapshot.sequence + 1;
      const claims = await this.#loadClaims(runId, sequence);
      if (claims.length === 0) return current;
      const claim = requireSingleClaim(claims);
      const child: StoredProgress = {
        snapshot: await this.loadReference(claim.snapshot),
        reference: claim.snapshot,
      };
      validateClaim(claim, child);
      validatePublication(child.snapshot, current);
      await this.#writePointer({
        schema_version: PROGRESS_SCHEMA_VERSION,
        run_id: child.snapshot.run_id,
        sequence: child.snapshot.sequence,
        updated_at: child.snapshot.updated_at,
        snapshot: child.reference,
      });
      current = child;
    }
  }

  async #loadClaims(runId: string, sequence: number): Promise<ProgressClaim[]> {
    const prefix = `${progressClaimPrefix(this.#prefix, runId, sequence)}/`;
    const paths = await this.#objects.list(prefix);
    return Promise.all(
      [...paths]
        .filter((path) => path.startsWith(prefix) && path.endsWith(".json"))
        .sort()
        .map(async (path) => {
          const raw = await this.#objects.read(path);
          if (raw === null)
            throw new Error(`progress claim is missing: ${path}`);
          return parseProgressClaim(parseJson(raw));
        }),
    );
  }

  async #writePointer(pointer: ProgressPointer): Promise<void> {
    const pointerRaw = stableJsonBytes(pointer);
    const pointerKey = progressPointerKey(this.#prefix, pointer.run_id);
    await this.#objects.write(pointerKey, pointerRaw);
    const verifiedPointer = await this.#objects.read(pointerKey);
    if (verifiedPointer === null || !bytesEqual(verifiedPointer, pointerRaw)) {
      throw new Error("uploaded progress pointer verification failed");
    }
  }
}

export type ProgressReporterOptions = {
  runId: string;
  attemptId: string;
  jobId?: string;
  input: ProgressInput;
  store: ProgressStore;
  flushIntervalMs?: number;
  clock?: () => Date;
};

export class ProgressReporter {
  readonly #runId: string;
  readonly #attemptId: string;
  readonly #jobId: string | undefined;
  readonly #input: ProgressInput;
  readonly #store: ProgressStore;
  readonly #flushIntervalMs: number;
  readonly #clock: () => Date;
  readonly #tracks = new Map<string, ProgressTrack>();
  #latest: StoredProgress | null;
  #sequence: number;
  #state: ProgressStatus = "running";
  #dirty: boolean;
  #changeSequence = 0;
  #lastFlushMs: number | null;
  #flushChain: Promise<StoredProgress | null> = Promise.resolve(null);

  private constructor(
    options: ProgressReporterOptions,
    latest: StoredProgress | null,
  ) {
    requireSafeId(options.runId, "runId");
    requireSafeId(options.attemptId, "attemptId");
    if (options.jobId !== undefined)
      requireBoundedString(options.jobId, "jobId", 200);
    validateInput(options.input);
    const flushIntervalMs = options.flushIntervalMs ?? 30_000;
    if (!Number.isFinite(flushIntervalMs) || flushIntervalMs < 0) {
      throw new Error("flushIntervalMs must be a nonnegative finite number");
    }
    this.#runId = options.runId;
    this.#attemptId = options.attemptId;
    this.#jobId = options.jobId;
    this.#input = { ...options.input };
    this.#store = options.store;
    this.#flushIntervalMs = flushIntervalMs;
    this.#clock = options.clock ?? (() => new Date());
    this.#latest = latest;
    this.#sequence = latest?.snapshot.sequence ?? 0;
    const sameInput =
      latest !== null && equalInput(latest.snapshot.input, options.input);
    if (sameInput) {
      for (const track of latest.snapshot.tracks)
        this.#tracks.set(track.key, track);
      this.#state = latest.snapshot.state;
      this.#dirty =
        !TERMINAL_STATUSES.has(latest.snapshot.state) &&
        (latest.snapshot.attempt_id !== options.attemptId ||
          latest.snapshot.job_id !== options.jobId);
    } else {
      this.#state = "running";
      this.#dirty = true;
    }
    this.#lastFlushMs =
      latest === null ? null : Date.parse(latest.snapshot.updated_at);
  }

  static async create(
    options: ProgressReporterOptions,
  ): Promise<ProgressReporter> {
    return new ProgressReporter(
      options,
      await options.store.loadLatest(options.runId),
    );
  }

  get tracks(): readonly ProgressTrack[] {
    return [...this.#tracks.values()].sort((left, right) =>
      left.key.localeCompare(right.key),
    );
  }

  plan(tracks: readonly ProgressTrack[]): void {
    if (tracks.length === 0)
      throw new Error("plan requires at least one track");
    const keys = new Set<string>();
    const candidateTracks = new Map(this.#tracks);
    for (const candidate of tracks) {
      const track = parseProgressTrack(candidate);
      if (keys.has(track.key))
        throw new Error("planned track keys must be unique");
      keys.add(track.key);
      const current = candidateTracks.get(track.key);
      if (current !== undefined) validateTrackTransition(current, track);
      candidateTracks.set(track.key, track);
    }
    if (candidateTracks.size > MAX_TRACKS)
      throw new Error(`tracks must not exceed ${MAX_TRACKS}`);
    this.#tracks.clear();
    for (const [key, track] of candidateTracks) this.#tracks.set(key, track);
    this.#markDirty();
  }

  update(candidate: ProgressTrack): void {
    const track = parseProgressTrack(candidate);
    const current = this.#tracks.get(track.key);
    if (current === undefined)
      throw new Error(`unknown progress track: ${track.key}`);
    validateTrackTransition(current, track);
    if (!equalTrack(current, track)) {
      this.#tracks.set(track.key, track);
      this.#markDirty();
    }
  }

  setState(state: ProgressStatus): void {
    requireProgressStatus(state, "state");
    if (TERMINAL_STATUSES.has(this.#state) && state !== this.#state) {
      throw new Error("terminal progress state cannot change");
    }
    if (state !== this.#state) {
      this.#state = state;
      this.#markDirty();
    }
  }

  heartbeat(): Promise<StoredProgress | null> {
    this.#markDirty();
    return this.flush();
  }

  flush(options: { force?: boolean } = {}): Promise<StoredProgress | null> {
    const force = options.force ?? false;
    const next = this.#flushChain
      .catch(() => null)
      .then(() => this.#flushNow(force));
    this.#flushChain = next;
    return next;
  }

  async #flushNow(force: boolean): Promise<StoredProgress | null> {
    if (!this.#dirty) return null;
    if (this.#tracks.size === 0) {
      throw new Error("at least one progress track is required before flush");
    }
    const now = this.#clock();
    if (!Number.isFinite(now.getTime()))
      throw new Error("clock must return a valid Date");
    if (
      !force &&
      this.#lastFlushMs !== null &&
      now.getTime() - this.#lastFlushMs < this.#flushIntervalMs
    ) {
      return null;
    }
    const changeSequence = this.#changeSequence;
    const snapshot: ProgressSnapshot = parseProgressSnapshot({
      schema_version: PROGRESS_SCHEMA_VERSION,
      run_id: this.#runId,
      attempt_id: this.#attemptId,
      ...(this.#jobId === undefined ? {} : { job_id: this.#jobId }),
      sequence: this.#sequence + 1,
      updated_at: now.toISOString(),
      input: this.#input,
      state: this.#state,
      ...(this.#latest === null ? {} : { previous: this.#latest.reference }),
      tracks: this.tracks,
    });
    const stored = await this.#store.publish(snapshot);
    this.#latest = stored;
    this.#sequence = snapshot.sequence;
    this.#lastFlushMs = now.getTime();
    if (this.#changeSequence === changeSequence) this.#dirty = false;
    return stored;
  }

  #markDirty(): void {
    this.#dirty = true;
    this.#changeSequence += 1;
  }
}

export function parseProgressSnapshot(value: unknown): ProgressSnapshot {
  const record = requireRecord(value, "progress snapshot");
  requireExactKeys(
    record,
    [
      "schema_version",
      "run_id",
      "attempt_id",
      "sequence",
      "updated_at",
      "input",
      "state",
      "tracks",
    ],
    ["job_id", "previous"],
  );
  requireLiteralOne(record.schema_version, "schema_version");
  const runId = requireSafeId(record.run_id, "run_id");
  const attemptId = requireSafeId(record.attempt_id, "attempt_id");
  const sequence = requireInteger(record.sequence, "sequence", 1);
  const updatedAt = requireTimestamp(record.updated_at, "updated_at");
  const input = parseProgressInput(record.input);
  const state = requireProgressStatus(record.state, "state");
  if (
    !Array.isArray(record.tracks) ||
    record.tracks.length < 1 ||
    record.tracks.length > MAX_TRACKS
  ) {
    throw new Error(`tracks must contain 1 to ${MAX_TRACKS} items`);
  }
  const tracks = record.tracks
    .map(parseProgressTrack)
    .sort((left, right) => left.key.localeCompare(right.key));
  if (new Set(tracks.map((track) => track.key)).size !== tracks.length) {
    throw new Error("track keys must be unique");
  }
  return {
    schema_version: PROGRESS_SCHEMA_VERSION,
    run_id: runId,
    attempt_id: attemptId,
    ...(record.job_id === undefined
      ? {}
      : { job_id: requireBoundedString(record.job_id, "job_id", 200) }),
    sequence,
    updated_at: updatedAt,
    input,
    state,
    ...(record.previous === undefined
      ? {}
      : { previous: parseArtifact(record.previous) }),
    tracks,
  };
}

export function parseProgressPointer(value: unknown): ProgressPointer {
  const record = requireRecord(value, "progress pointer");
  requireExactKeys(
    record,
    ["schema_version", "run_id", "sequence", "updated_at", "snapshot"],
    [],
  );
  requireLiteralOne(record.schema_version, "schema_version");
  return {
    schema_version: PROGRESS_SCHEMA_VERSION,
    run_id: requireSafeId(record.run_id, "run_id"),
    sequence: requireInteger(record.sequence, "sequence", 1),
    updated_at: requireTimestamp(record.updated_at, "updated_at"),
    snapshot: parseArtifact(record.snapshot),
  };
}

export function parseProgressClaim(value: unknown): ProgressClaim {
  const record = requireRecord(value, "progress claim");
  requireExactKeys(
    record,
    [
      "schema_version",
      "run_id",
      "attempt_id",
      "sequence",
      "created_at",
      "snapshot",
    ],
    [],
  );
  requireLiteralOne(record.schema_version, "schema_version");
  return {
    schema_version: PROGRESS_SCHEMA_VERSION,
    run_id: requireSafeId(record.run_id, "run_id"),
    attempt_id: requireSafeId(record.attempt_id, "attempt_id"),
    sequence: requireInteger(record.sequence, "sequence", 1),
    created_at: requireTimestamp(record.created_at, "created_at"),
    snapshot: parseArtifact(record.snapshot),
  };
}

export function parseProgressTrack(value: unknown): ProgressTrack {
  const record = requireRecord(value, "progress track");
  requireExactKeys(
    record,
    ["key", "plan_id", "status"],
    ["label", "completed", "total", "unit", "source_updated_at"],
  );
  const result: ProgressTrack = {
    key: requireSafeId(record.key, "key"),
    plan_id: requireSafeId(record.plan_id, "plan_id"),
    status: requireProgressStatus(record.status, "status"),
    ...(record.label === undefined
      ? {}
      : { label: requireBoundedString(record.label, "label", 200) }),
    ...(record.completed === undefined
      ? {}
      : { completed: requireInteger(record.completed, "completed", 0) }),
    ...(record.total === undefined
      ? {}
      : { total: requireInteger(record.total, "total", 0) }),
    ...(record.unit === undefined
      ? {}
      : { unit: requireBoundedString(record.unit, "unit", 64) }),
    ...(record.source_updated_at === undefined
      ? {}
      : {
          source_updated_at: requireTimestamp(
            record.source_updated_at,
            "source_updated_at",
          ),
        }),
  };
  validateTrackCounts(result);
  return result;
}

export function stableJsonBytes(value: unknown): Uint8Array {
  return Buffer.from(
    `${JSON.stringify(canonicalize(value), null, 2)}\n`,
    "utf8",
  );
}

class AsyncLock {
  #tail: Promise<void> = Promise.resolve();

  run<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#tail.then(operation, operation);
    this.#tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

function sharedPublicationLock(bucketId: string, prefix: string): AsyncLock {
  const key = `${bucketId}\n${prefix}`;
  const existing = PUBLICATION_LOCKS.get(key);
  if (existing !== undefined) return existing;
  const created = new AsyncLock();
  PUBLICATION_LOCKS.set(key, created);
  return created;
}

function parseProgressInput(value: unknown): ProgressInput {
  const record = requireRecord(value, "progress input");
  requireExactKeys(record, ["revision", "contract_sha256"], []);
  const result = {
    revision: requireBoundedString(record.revision, "revision", 200),
    contract_sha256: requireString(record.contract_sha256, "contract_sha256"),
  };
  validateInput(result);
  return result;
}

function parseArtifact(value: unknown): ArtifactRef {
  const record = requireRecord(value, "artifact");
  requireExactKeys(record, ["bucket", "key", "sha256", "bytes"], []);
  const result = {
    bucket: requireString(record.bucket, "bucket"),
    key: requireString(record.key, "key"),
    sha256: requireString(record.sha256, "sha256"),
    bytes: requireInteger(record.bytes, "bytes", 1),
  };
  validateArtifact(result);
  return result;
}

function validateInput(input: ProgressInput): void {
  requireBoundedString(input.revision, "revision", 200);
  if (!SHA256.test(input.contract_sha256)) {
    throw new Error("contract_sha256 must be 64 lowercase hex characters");
  }
}

function validateArtifact(reference: ArtifactRef): void {
  requireRepoId(reference.bucket, "bucket");
  if (
    reference.key.length < 1 ||
    reference.key.length > 1024 ||
    reference.key.startsWith("/")
  ) {
    throw new Error(
      "key must be a relative POSIX path with at most 1024 characters",
    );
  }
  const parts = reference.key.split("/");
  if (
    parts.some((part) => part === "" || part === "." || part === "..") ||
    reference.key.includes("\\")
  ) {
    throw new Error("key contains an unsafe path component");
  }
  if (!SHA256.test(reference.sha256))
    throw new Error("sha256 must be 64 lowercase hex characters");
  if (!parts.includes(`sha256-${reference.sha256}`)) {
    throw new Error("key must contain a sha256-<digest> segment");
  }
  requireInteger(reference.bytes, "bytes", 1);
}

function validateTrackCounts(track: ProgressTrack): void {
  if (track.total !== undefined && track.completed === undefined) {
    throw new Error("completed is required when total is set");
  }
  if (track.completed !== undefined && track.unit === undefined) {
    throw new Error("unit is required when completed is set");
  }
  if (
    track.completed !== undefined &&
    track.total !== undefined &&
    track.completed > track.total
  ) {
    throw new Error("completed must not exceed total");
  }
  if (
    track.status === "completed" &&
    track.total !== undefined &&
    track.completed !== track.total
  ) {
    throw new Error("a completed track must reach its total");
  }
}

function validateTrackTransition(
  previous: ProgressTrack,
  current: ProgressTrack,
): void {
  if (previous.key !== current.key)
    throw new Error("progress track key cannot change");
  if (previous.plan_id !== current.plan_id) return;
  if (previous.unit !== current.unit)
    throw new Error("progress track unit cannot change within a plan");
  if (previous.total !== current.total)
    throw new Error("progress track total cannot change within a plan");
  if (
    previous.completed !== undefined &&
    current.completed !== undefined &&
    current.completed < previous.completed
  ) {
    throw new Error("progress track completed count cannot move backwards");
  }
  if (
    TERMINAL_STATUSES.has(previous.status) &&
    !equalTrack(previous, current)
  ) {
    throw new Error("terminal progress track cannot change within a plan");
  }
}

function validatePointerSnapshot(
  pointer: ProgressPointer,
  snapshot: ProgressSnapshot,
): void {
  if (snapshot.run_id !== pointer.run_id) {
    throw new Error("progress pointer snapshot run_id mismatch");
  }
  if (snapshot.sequence !== pointer.sequence) {
    throw new Error("progress pointer snapshot sequence mismatch");
  }
  if (snapshot.updated_at !== pointer.updated_at) {
    throw new Error("progress pointer snapshot timestamp mismatch");
  }
}

function validateClaim(claim: ProgressClaim, stored: StoredProgress): void {
  const snapshot = stored.snapshot;
  if (claim.run_id !== snapshot.run_id) {
    throw new Error("progress claim snapshot run_id mismatch");
  }
  if (claim.attempt_id !== snapshot.attempt_id) {
    throw new Error("progress claim snapshot attempt_id mismatch");
  }
  if (claim.sequence !== snapshot.sequence) {
    throw new Error("progress claim snapshot sequence mismatch");
  }
  if (claim.created_at !== snapshot.updated_at) {
    throw new Error("progress claim snapshot timestamp mismatch");
  }
  if (!equalArtifact(claim.snapshot, stored.reference)) {
    throw new Error("progress claim snapshot reference mismatch");
  }
}

function requireSingleClaim(
  claims: readonly ProgressClaim[],
  expected?: ProgressClaim,
): ProgressClaim {
  if (claims.length === 0)
    throw new Error("progress sequence claim is missing");
  if (claims.length > 1) {
    throw new Error("competing progress sequence claims detected");
  }
  const claim = claims[0];
  if (claim === undefined)
    throw new Error("progress sequence claim is missing");
  if (expected !== undefined && !equalClaim(claim, expected)) {
    throw new Error("progress sequence is claimed by another attempt");
  }
  return claim;
}

function validatePublication(
  snapshot: ProgressSnapshot,
  latest: StoredProgress | null,
): void {
  if (latest === null) {
    if (snapshot.sequence !== 1)
      throw new Error("first progress sequence must be 1");
    if (snapshot.previous !== undefined) {
      throw new Error("first progress snapshot must not have a predecessor");
    }
    return;
  }
  if (snapshot.sequence !== latest.snapshot.sequence + 1) {
    throw new Error("progress sequence must increase by exactly one");
  }
  if (!equalArtifact(snapshot.previous, latest.reference)) {
    throw new Error("progress predecessor does not match current snapshot");
  }
}

function equalInput(left: ProgressInput, right: ProgressInput): boolean {
  return (
    left.revision === right.revision &&
    left.contract_sha256 === right.contract_sha256
  );
}

function equalTrack(left: ProgressTrack, right: ProgressTrack): boolean {
  return (
    JSON.stringify(canonicalize(left)) === JSON.stringify(canonicalize(right))
  );
}

function equalClaim(left: ProgressClaim, right: ProgressClaim): boolean {
  return (
    JSON.stringify(canonicalize(left)) === JSON.stringify(canonicalize(right))
  );
}

function equalArtifact(
  left: ArtifactRef | undefined,
  right: ArtifactRef,
): boolean {
  return (
    left !== undefined &&
    left.bucket === right.bucket &&
    left.key === right.key &&
    left.sha256 === right.sha256 &&
    left.bytes === right.bytes
  );
}

function parseJson(raw: Uint8Array): unknown {
  try {
    return JSON.parse(Buffer.from(raw).toString("utf8"));
  } catch (error) {
    throw new Error("progress document must contain valid JSON", {
      cause: error,
    });
  }
}

function canonicalize(value: unknown): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("JSON numbers must be finite");
    return value;
  }
  if (Array.isArray(value)) return value.map(canonicalize);
  if (isRecord(value)) {
    const result: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const item = value[key];
      if (item !== undefined) result[key] = canonicalize(item);
    }
    return result;
  }
  throw new Error("value must be JSON serializable");
}

function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${name} must be an object`);
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requireExactKeys(
  record: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): void {
  const allowed = new Set([...required, ...optional]);
  const missing = required.filter((key) => !(key in record));
  const unexpected = Object.keys(record).filter((key) => !allowed.has(key));
  if (missing.length > 0)
    throw new Error(`missing fields: ${missing.sort().join(", ")}`);
  if (unexpected.length > 0) {
    throw new Error(`unexpected fields: ${unexpected.sort().join(", ")}`);
  }
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== "string") throw new Error(`${name} must be a string`);
  return value;
}

function requireBoundedString(
  value: unknown,
  name: string,
  maximum: number,
): string {
  const result = requireString(value, name);
  if (result.length < 1 || result.length > maximum) {
    throw new Error(`${name} must contain 1 to ${maximum} characters`);
  }
  return result;
}

function requireSafeId(value: unknown, name: string): string {
  const result = requireString(value, name);
  if (!SAFE_ID.test(result))
    throw new Error(`${name} must be a safe identifier`);
  return result;
}

function requireRepoId(value: unknown, name: string): string {
  const result = requireString(value, name);
  if (!REPO_ID.test(result))
    throw new Error(`${name} must use namespace/name form`);
  return result;
}

function requireInteger(value: unknown, name: string, minimum: number): number {
  if (
    !Number.isSafeInteger(value) ||
    typeof value !== "number" ||
    value < minimum
  ) {
    throw new Error(`${name} must be an integer >= ${minimum}`);
  }
  return value;
}

function requireTimestamp(value: unknown, name: string): string {
  const result = requireString(value, name);
  if (!RFC3339.test(result) || !Number.isFinite(Date.parse(result))) {
    throw new Error(`${name} must be an RFC 3339 timestamp`);
  }
  return result;
}

function requireProgressStatus(value: unknown, name: string): ProgressStatus {
  if (
    value !== "pending" &&
    value !== "running" &&
    value !== "waiting" &&
    value !== "blocked" &&
    value !== "completed" &&
    value !== "failed" &&
    value !== "cancelled"
  ) {
    throw new Error(`${name} must be a valid progress status`);
  }
  return value;
}

function requireLiteralOne(value: unknown, name: string): 1 {
  if (value !== 1) throw new Error(`${name} must be 1`);
  return 1;
}

function normalizePrefix(prefix: string): string {
  const normalized = prefix.replace(/^\/+|\/+$/gu, "");
  if (normalized === "") return "";
  if (
    normalized
      .split("/")
      .some((part) => part === "" || part === "." || part === "..")
  ) {
    throw new Error("prefix must be a safe relative POSIX path");
  }
  return normalized;
}

function joinKey(...parts: readonly string[]): string {
  return parts.filter((part) => part !== "").join("/");
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

function verifyBytes(content: Uint8Array, reference: ArtifactRef): void {
  if (content.byteLength !== reference.bytes) {
    throw new Error("progress snapshot byte count mismatch");
  }
  if (sha256(content) !== reference.sha256) {
    throw new Error("progress snapshot SHA-256 mismatch");
  }
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.byteLength === right.byteLength &&
    Buffer.from(left).equals(Buffer.from(right))
  );
}
