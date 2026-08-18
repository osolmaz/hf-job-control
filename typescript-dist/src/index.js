import { createHash } from "node:crypto";
export const PROGRESS_SCHEMA_VERSION = 1;
const MAX_TRACKS = 256;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const REPO_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*\/[A-Za-z0-9][A-Za-z0-9._-]*$/u;
const SHA256 = /^[a-f0-9]{64}$/u;
const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-](\d{2}):(\d{2}))$/u;
const PUBLICATION_LOCKS = new Map();
export class TransientProgressError extends Error {
    constructor(message, options = {}) {
        super(message, options);
        this.name = "TransientProgressError";
    }
}
const TERMINAL_STATUSES = new Set([
    "completed",
    "failed",
    "cancelled",
]);
export function progressPointerKey(prefix, runId) {
    requireSafeId(runId, "run_id");
    return joinKey(normalizePrefix(prefix), "operations", runId, "progress", "current.json");
}
export function progressSnapshotKey(prefix, runId, digest) {
    requireSafeId(runId, "run_id");
    if (!SHA256.test(digest))
        throw new Error("digest must be 64 lowercase hex characters");
    return joinKey(normalizePrefix(prefix), "operations", runId, "progress", "snapshots", `sha256-${digest}`, "progress.json");
}
export function progressClaimPrefix(prefix, runId, sequence) {
    requireSafeId(runId, "run_id");
    requireInteger(sequence, "sequence", 1);
    return joinKey(normalizePrefix(prefix), "operations", runId, "progress", "claims", `sequence-${sequence.toString().padStart(16, "0")}`);
}
export function progressClaimKey(prefix, claim) {
    return joinKey(progressClaimPrefix(prefix, claim.run_id, claim.sequence), `${claim.attempt_id}.json`);
}
export class ObjectProgressStore {
    #objects;
    #prefix;
    #publicationLock;
    constructor(objects, prefix = "") {
        requireRepoId(objects.bucketId, "bucketId");
        this.#objects = objects;
        this.#prefix = normalizePrefix(prefix);
        this.#publicationLock = sharedPublicationLock(objects.bucketId, this.#prefix);
    }
    async loadLatest(runId) {
        const raw = await this.#objects.read(progressPointerKey(this.#prefix, runId));
        const pointer = raw === null ? null : parseProgressPointer(parseJson(raw));
        return this.#reconcileLatest(pointer, runId);
    }
    async loadReference(reference) {
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
    publish(snapshot) {
        return this.#publicationLock.run(() => this.#publish(snapshot));
    }
    async #publish(snapshot) {
        const validated = parseProgressSnapshot(snapshot);
        const latest = await this.loadLatest(validated.run_id);
        validatePublication(validated, latest);
        const raw = stableJsonBytes(validated);
        const digest = sha256(raw);
        const key = progressSnapshotKey(this.#prefix, validated.run_id, digest);
        const reference = {
            bucket: this.#objects.bucketId,
            key,
            sha256: digest,
            bytes: raw.byteLength,
        };
        const existing = await this.#objects.read(key);
        if (existing === null) {
            await this.#objects.write(key, raw);
        }
        else if (!bytesEqual(existing, raw)) {
            throw new Error("immutable progress snapshot differs");
        }
        const stored = await this.#objects.read(key);
        if (stored === null)
            throw new Error("uploaded progress snapshot is missing");
        verifyBytes(stored, reference);
        const claim = {
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
        }
        else if (!bytesEqual(existingClaim, claimRaw)) {
            throw new Error("immutable progress claim differs");
        }
        requireSingleClaim(await this.#loadClaims(validated.run_id, validated.sequence), claim);
        await this.#writePointer({
            schema_version: PROGRESS_SCHEMA_VERSION,
            run_id: validated.run_id,
            sequence: validated.sequence,
            updated_at: validated.updated_at,
            snapshot: reference,
        });
        return { snapshot: validated, reference };
    }
    async #reconcileLatest(pointer, runId) {
        let current = null;
        if (pointer !== null) {
            if (pointer.run_id !== runId)
                throw new Error("progress pointer run_id mismatch");
            const snapshot = await this.loadReference(pointer.snapshot);
            validatePointerSnapshot(pointer, snapshot);
            current = { snapshot, reference: pointer.snapshot };
            validateClaim(requireSingleClaim(await this.#loadClaims(runId, snapshot.sequence)), current);
        }
        while (true) {
            const sequence = current === null ? 1 : current.snapshot.sequence + 1;
            const claims = await this.#loadClaims(runId, sequence);
            if (claims.length === 0)
                return current;
            const claim = requireSingleClaim(claims);
            if (claim.run_id !== runId) {
                throw new Error("progress claim run_id mismatch");
            }
            const child = {
                snapshot: await this.loadReference(claim.snapshot),
                reference: claim.snapshot,
            };
            if (child.snapshot.run_id !== runId) {
                throw new Error("progress claim snapshot run_id mismatch");
            }
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
    async #loadClaims(runId, sequence) {
        const prefix = `${progressClaimPrefix(this.#prefix, runId, sequence)}/`;
        const paths = await this.#objects.list(prefix);
        return Promise.all([...paths]
            .filter((path) => path.startsWith(prefix) && path.endsWith(".json"))
            .sort()
            .map(async (path) => {
            const raw = await this.#objects.read(path);
            if (raw === null)
                throw new Error(`progress claim is missing: ${path}`);
            return parseProgressClaim(parseJson(raw));
        }));
    }
    async #writePointer(pointer) {
        const pointerRaw = stableJsonBytes(pointer);
        const pointerKey = progressPointerKey(this.#prefix, pointer.run_id);
        await this.#objects.write(pointerKey, pointerRaw);
        const verifiedPointer = await this.#objects.read(pointerKey);
        if (verifiedPointer === null || !bytesEqual(verifiedPointer, pointerRaw)) {
            throw new Error("uploaded progress pointer verification failed");
        }
    }
}
export class ProgressReporter {
    #runId;
    #attemptId;
    #jobId;
    #input;
    #store;
    #flushIntervalMs;
    #publishAttempts;
    #retryDelayMs;
    #clock;
    #sleep;
    #tracks = new Map();
    #latest;
    #sequence;
    #state = "running";
    #dirty;
    #changeSequence = 0;
    #lastFlushMs;
    #flushChain = Promise.resolve(null);
    constructor(options, latest) {
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
        const publishAttempts = options.publishAttempts ?? 3;
        const retryDelayMs = options.retryDelayMs ?? 2_000;
        if (!Number.isSafeInteger(publishAttempts) || publishAttempts < 1) {
            throw new Error("publishAttempts must be an integer >= 1");
        }
        if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
            throw new Error("retryDelayMs must be a nonnegative finite number");
        }
        this.#store = options.store;
        this.#flushIntervalMs = flushIntervalMs;
        this.#publishAttempts = publishAttempts;
        this.#retryDelayMs = retryDelayMs;
        this.#clock = options.clock ?? (() => new Date());
        this.#sleep = options.sleep ?? sleep;
        this.#latest = latest;
        this.#sequence = latest?.snapshot.sequence ?? 0;
        const sameInput = latest !== null && equalInput(latest.snapshot.input, options.input);
        if (sameInput) {
            for (const track of latest.snapshot.tracks)
                this.#tracks.set(track.key, track);
            this.#state = latest.snapshot.state;
            this.#dirty =
                !TERMINAL_STATUSES.has(latest.snapshot.state) &&
                    (latest.snapshot.attempt_id !== options.attemptId ||
                        latest.snapshot.job_id !== options.jobId);
        }
        else {
            this.#state = "running";
            this.#dirty = true;
        }
        this.#lastFlushMs =
            latest === null ? null : Date.parse(latest.snapshot.updated_at);
    }
    static async create(options) {
        validateReporterOptions(options);
        return new ProgressReporter(options, await options.store.loadLatest(options.runId));
    }
    get tracks() {
        return [...this.#tracks.values()].sort((left, right) => left.key.localeCompare(right.key));
    }
    plan(tracks) {
        if (tracks.length === 0)
            throw new Error("plan requires at least one track");
        const keys = new Set();
        const candidateTracks = new Map(this.#tracks);
        for (const candidate of tracks) {
            const track = parseProgressTrack(candidate);
            if (keys.has(track.key))
                throw new Error("planned track keys must be unique");
            keys.add(track.key);
            const current = candidateTracks.get(track.key);
            if (current !== undefined)
                validateTrackTransition(current, track);
            candidateTracks.set(track.key, track);
        }
        if (candidateTracks.size > MAX_TRACKS)
            throw new Error(`tracks must not exceed ${MAX_TRACKS}`);
        this.#tracks.clear();
        for (const [key, track] of candidateTracks)
            this.#tracks.set(key, track);
        this.#markDirty();
    }
    update(candidate) {
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
    setState(state) {
        requireProgressStatus(state, "state");
        if (TERMINAL_STATUSES.has(this.#state) && state !== this.#state) {
            throw new Error("terminal progress state cannot change");
        }
        if (state !== this.#state) {
            this.#state = state;
            this.#markDirty();
        }
    }
    heartbeat() {
        this.#markDirty();
        return this.flush();
    }
    flush(options = {}) {
        const force = options.force ?? false;
        const next = this.#flushChain
            .catch(() => null)
            .then(() => this.#flushNow(force));
        this.#flushChain = next;
        return next;
    }
    async #flushNow(force) {
        if (!this.#dirty)
            return null;
        if (this.#tracks.size === 0) {
            throw new Error("at least one progress track is required before flush");
        }
        const now = this.#clock();
        if (!Number.isFinite(now.getTime()))
            throw new Error("clock must return a valid Date");
        if (!force &&
            this.#lastFlushMs !== null &&
            now.getTime() - this.#lastFlushMs < this.#flushIntervalMs) {
            return null;
        }
        const changeSequence = this.#changeSequence;
        const snapshot = parseProgressSnapshot({
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
        const stored = await this.#publishWithRetry(snapshot);
        this.#latest = stored;
        this.#sequence = snapshot.sequence;
        this.#lastFlushMs = now.getTime();
        if (this.#changeSequence === changeSequence)
            this.#dirty = false;
        return stored;
    }
    async #publishWithRetry(snapshot) {
        let lastError;
        for (let attempt = 0; attempt < this.#publishAttempts; attempt += 1) {
            try {
                return await this.#store.publish(snapshot);
            }
            catch (error) {
                lastError = error;
                if (!(error instanceof TransientProgressError))
                    throw error;
                if (attempt + 1 < this.#publishAttempts) {
                    await this.#sleep(this.#retryDelayMs);
                }
            }
        }
        throw lastError instanceof Error
            ? lastError
            : new Error("progress publication failed", { cause: lastError });
    }
    #markDirty() {
        this.#dirty = true;
        this.#changeSequence += 1;
    }
}
export function parseProgressSnapshot(value) {
    const record = requireRecord(value, "progress snapshot");
    requireExactKeys(record, [
        "schema_version",
        "run_id",
        "attempt_id",
        "sequence",
        "updated_at",
        "input",
        "state",
        "tracks",
    ], ["job_id", "previous"]);
    requireLiteralOne(record.schema_version, "schema_version");
    const runId = requireSafeId(record.run_id, "run_id");
    const attemptId = requireSafeId(record.attempt_id, "attempt_id");
    const sequence = requireInteger(record.sequence, "sequence", 1);
    const updatedAt = requireTimestamp(record.updated_at, "updated_at");
    const input = parseProgressInput(record.input);
    const state = requireProgressStatus(record.state, "state");
    if (!Array.isArray(record.tracks) ||
        record.tracks.length < 1 ||
        record.tracks.length > MAX_TRACKS) {
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
export function parseProgressPointer(value) {
    const record = requireRecord(value, "progress pointer");
    requireExactKeys(record, ["schema_version", "run_id", "sequence", "updated_at", "snapshot"], []);
    requireLiteralOne(record.schema_version, "schema_version");
    return {
        schema_version: PROGRESS_SCHEMA_VERSION,
        run_id: requireSafeId(record.run_id, "run_id"),
        sequence: requireInteger(record.sequence, "sequence", 1),
        updated_at: requireTimestamp(record.updated_at, "updated_at"),
        snapshot: parseArtifact(record.snapshot),
    };
}
export function parseProgressClaim(value) {
    const record = requireRecord(value, "progress claim");
    requireExactKeys(record, [
        "schema_version",
        "run_id",
        "attempt_id",
        "sequence",
        "created_at",
        "snapshot",
    ], []);
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
export function parseProgressTrack(value) {
    const record = requireRecord(value, "progress track");
    requireExactKeys(record, ["key", "plan_id", "status"], ["label", "completed", "total", "unit", "source_updated_at"]);
    const result = {
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
                source_updated_at: requireTimestamp(record.source_updated_at, "source_updated_at"),
            }),
    };
    validateTrackCounts(result);
    return result;
}
export function stableJsonBytes(value) {
    return Buffer.from(`${JSON.stringify(canonicalize(value), null, 2)}\n`, "utf8");
}
function validateReporterOptions(options) {
    requireSafeId(options.runId, "runId");
    requireSafeId(options.attemptId, "attemptId");
    if (options.jobId !== undefined)
        requireBoundedString(options.jobId, "jobId", 200);
    validateInput(options.input);
    const flushIntervalMs = options.flushIntervalMs ?? 30_000;
    if (!Number.isFinite(flushIntervalMs) || flushIntervalMs < 0) {
        throw new Error("flushIntervalMs must be a nonnegative finite number");
    }
    const publishAttempts = options.publishAttempts ?? 3;
    if (!Number.isSafeInteger(publishAttempts) || publishAttempts < 1) {
        throw new Error("publishAttempts must be an integer >= 1");
    }
    const retryDelayMs = options.retryDelayMs ?? 2_000;
    if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
        throw new Error("retryDelayMs must be a nonnegative finite number");
    }
}
function sleep(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
class AsyncLock {
    #tail = Promise.resolve();
    run(operation) {
        const result = this.#tail.then(operation, operation);
        this.#tail = result.then(() => undefined, () => undefined);
        return result;
    }
}
function sharedPublicationLock(bucketId, prefix) {
    const key = `${bucketId}\n${prefix}`;
    const existing = PUBLICATION_LOCKS.get(key);
    if (existing !== undefined)
        return existing;
    const created = new AsyncLock();
    PUBLICATION_LOCKS.set(key, created);
    return created;
}
function parseProgressInput(value) {
    const record = requireRecord(value, "progress input");
    requireExactKeys(record, ["revision", "contract_sha256"], []);
    const result = {
        revision: requireBoundedString(record.revision, "revision", 200),
        contract_sha256: requireString(record.contract_sha256, "contract_sha256"),
    };
    validateInput(result);
    return result;
}
function parseArtifact(value) {
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
function validateInput(input) {
    requireBoundedString(input.revision, "revision", 200);
    if (!SHA256.test(input.contract_sha256)) {
        throw new Error("contract_sha256 must be 64 lowercase hex characters");
    }
}
function validateArtifact(reference) {
    requireRepoId(reference.bucket, "bucket");
    if (reference.key.length < 1 ||
        reference.key.length > 1024 ||
        reference.key.startsWith("/")) {
        throw new Error("key must be a relative POSIX path with at most 1024 characters");
    }
    const parts = reference.key.split("/");
    if (parts.some((part) => part === "" || part === "." || part === "..") ||
        reference.key.includes("\\")) {
        throw new Error("key contains an unsafe path component");
    }
    if (!SHA256.test(reference.sha256))
        throw new Error("sha256 must be 64 lowercase hex characters");
    if (!parts.includes(`sha256-${reference.sha256}`)) {
        throw new Error("key must contain a sha256-<digest> segment");
    }
    requireInteger(reference.bytes, "bytes", 1);
}
function validateTrackCounts(track) {
    if (track.total !== undefined && track.completed === undefined) {
        throw new Error("completed is required when total is set");
    }
    if (track.completed !== undefined && track.unit === undefined) {
        throw new Error("unit is required when completed is set");
    }
    if (track.completed !== undefined &&
        track.total !== undefined &&
        track.completed > track.total) {
        throw new Error("completed must not exceed total");
    }
    if (track.status === "completed" &&
        track.total !== undefined &&
        track.completed !== track.total) {
        throw new Error("a completed track must reach its total");
    }
}
function validateTrackTransition(previous, current) {
    if (previous.key !== current.key)
        throw new Error("progress track key cannot change");
    if (previous.plan_id !== current.plan_id)
        return;
    if (previous.unit !== current.unit)
        throw new Error("progress track unit cannot change within a plan");
    if (previous.total !== current.total)
        throw new Error("progress track total cannot change within a plan");
    if (previous.completed !== undefined && current.completed === undefined) {
        throw new Error("progress track completed count cannot be removed");
    }
    if (previous.completed !== undefined &&
        current.completed !== undefined &&
        current.completed < previous.completed) {
        throw new Error("progress track completed count cannot move backwards");
    }
    if (TERMINAL_STATUSES.has(previous.status) &&
        !equalTrack(previous, current)) {
        throw new Error("terminal progress track cannot change within a plan");
    }
}
function validatePointerSnapshot(pointer, snapshot) {
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
function validateClaim(claim, stored) {
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
function requireSingleClaim(claims, expected) {
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
function validatePublication(snapshot, latest) {
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
function equalInput(left, right) {
    return (left.revision === right.revision &&
        left.contract_sha256 === right.contract_sha256);
}
function equalTrack(left, right) {
    return (JSON.stringify(canonicalize(left)) === JSON.stringify(canonicalize(right)));
}
function equalClaim(left, right) {
    return (JSON.stringify(canonicalize(left)) === JSON.stringify(canonicalize(right)));
}
function equalArtifact(left, right) {
    return (left !== undefined &&
        left.bucket === right.bucket &&
        left.key === right.key &&
        left.sha256 === right.sha256 &&
        left.bytes === right.bytes);
}
function parseJson(raw) {
    try {
        return JSON.parse(Buffer.from(raw).toString("utf8"));
    }
    catch (error) {
        throw new Error("progress document must contain valid JSON", {
            cause: error,
        });
    }
}
function canonicalize(value) {
    if (value === null || typeof value === "string" || typeof value === "boolean")
        return value;
    if (typeof value === "number") {
        if (!Number.isFinite(value))
            throw new Error("JSON numbers must be finite");
        return value;
    }
    if (Array.isArray(value))
        return value.map(canonicalize);
    if (isRecord(value)) {
        const result = {};
        for (const key of Object.keys(value).sort()) {
            const item = value[key];
            if (item !== undefined)
                result[key] = canonicalize(item);
        }
        return result;
    }
    throw new Error("value must be JSON serializable");
}
function requireRecord(value, name) {
    if (!isRecord(value))
        throw new Error(`${name} must be an object`);
    return value;
}
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function requireExactKeys(record, required, optional) {
    const allowed = new Set([...required, ...optional]);
    const missing = required.filter((key) => !(key in record));
    const unexpected = Object.keys(record).filter((key) => !allowed.has(key));
    if (missing.length > 0)
        throw new Error(`missing fields: ${missing.sort().join(", ")}`);
    if (unexpected.length > 0) {
        throw new Error(`unexpected fields: ${unexpected.sort().join(", ")}`);
    }
}
function requireString(value, name) {
    if (typeof value !== "string")
        throw new Error(`${name} must be a string`);
    return value;
}
function requireBoundedString(value, name, maximum) {
    const result = requireString(value, name);
    if (result.length < 1 || result.length > maximum) {
        throw new Error(`${name} must contain 1 to ${maximum} characters`);
    }
    return result;
}
function requireSafeId(value, name) {
    const result = requireString(value, name);
    if (!SAFE_ID.test(result))
        throw new Error(`${name} must be a safe identifier`);
    return result;
}
function requireRepoId(value, name) {
    const result = requireString(value, name);
    if (!REPO_ID.test(result))
        throw new Error(`${name} must use namespace/name form`);
    return result;
}
function requireInteger(value, name, minimum) {
    if (!Number.isSafeInteger(value) ||
        typeof value !== "number" ||
        value < minimum) {
        throw new Error(`${name} must be an integer >= ${minimum}`);
    }
    return value;
}
function requireTimestamp(value, name) {
    const result = requireString(value, name);
    const match = RFC3339.exec(result);
    if (match === null || !Number.isFinite(Date.parse(result))) {
        throw new Error(`${name} must be an RFC 3339 timestamp`);
    }
    const year = timestampPart(match, 1, name);
    const month = timestampPart(match, 2, name);
    const day = timestampPart(match, 3, name);
    const hour = timestampPart(match, 4, name);
    const minute = timestampPart(match, 5, name);
    const second = timestampPart(match, 6, name);
    const offsetHour = match[7] === undefined ? 0 : timestampPart(match, 7, name);
    const offsetMinute = match[8] === undefined ? 0 : timestampPart(match, 8, name);
    const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
    if (year < 1 ||
        month < 1 ||
        month > 12 ||
        day < 1 ||
        day > daysInMonth ||
        hour > 23 ||
        minute > 59 ||
        second > 59 ||
        offsetHour > 23 ||
        offsetMinute > 59) {
        throw new Error(`${name} must be an RFC 3339 timestamp`);
    }
    return result;
}
function timestampPart(match, index, name) {
    const value = match[index];
    if (value === undefined)
        throw new Error(`${name} must be an RFC 3339 timestamp`);
    return Number(value);
}
function requireProgressStatus(value, name) {
    if (value !== "pending" &&
        value !== "running" &&
        value !== "waiting" &&
        value !== "blocked" &&
        value !== "completed" &&
        value !== "failed" &&
        value !== "cancelled") {
        throw new Error(`${name} must be a valid progress status`);
    }
    return value;
}
function requireLiteralOne(value, name) {
    if (value !== 1)
        throw new Error(`${name} must be 1`);
    return 1;
}
function normalizePrefix(prefix) {
    const normalized = prefix.replace(/^\/+|\/+$/gu, "");
    if (normalized === "")
        return "";
    if (normalized
        .split("/")
        .some((part) => part === "" || part === "." || part === "..")) {
        throw new Error("prefix must be a safe relative POSIX path");
    }
    return normalized;
}
function joinKey(...parts) {
    return parts.filter((part) => part !== "").join("/");
}
function sha256(content) {
    return createHash("sha256").update(content).digest("hex");
}
function verifyBytes(content, reference) {
    if (content.byteLength !== reference.bytes) {
        throw new Error("progress snapshot byte count mismatch");
    }
    if (sha256(content) !== reference.sha256) {
        throw new Error("progress snapshot SHA-256 mismatch");
    }
}
function bytesEqual(left, right) {
    return (left.byteLength === right.byteLength &&
        Buffer.from(left).equals(Buffer.from(right)));
}
//# sourceMappingURL=index.js.map