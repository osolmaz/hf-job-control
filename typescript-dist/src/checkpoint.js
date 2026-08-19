import { createHash } from "node:crypto";
const BUNDLE_MAGIC = Buffer.from("HFJOB1\n", "ascii");
const MANIFEST_LENGTH_BYTES = 8;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const ADAPTER_NAME = /^[a-z][a-z0-9_-]{0,63}$/u;
const REPO_ID = /^[A-Za-z0-9][A-Za-z0-9._-]*\/[A-Za-z0-9][A-Za-z0-9._-]*$/u;
const SHA256 = /^[0-9a-f]{64}$/u;
const CLAIM_PATH = /\/checkpoints\/claims\/sequence-(\d{16})\/([^/]+)\.json$/u;
export function checkpointBundleKey(prefix, runId, sha256) {
    requireSafeId(runId, "run_id");
    requireSha256(sha256, "sha256");
    return joinPrefix(prefix, `${runId}/checkpoints/sha256-${sha256}/checkpoint.hfjob`);
}
export function checkpointClaimPrefix(prefix, runId) {
    requireSafeId(runId, "run_id");
    return joinPrefix(prefix, `${runId}/checkpoints/claims/`);
}
export function checkpointClaimKey(prefix, claim) {
    requireSafeId(claim.run_id, "run_id");
    requireSafeId(claim.attempt_id, "attempt_id");
    requirePositiveInteger(claim.sequence, "sequence");
    return joinPrefix(prefix, `${claim.run_id}/checkpoints/claims/sequence-${String(claim.sequence).padStart(16, "0")}/${claim.attempt_id}.json`);
}
export function checkpointPointerKey(prefix, runId) {
    requireSafeId(runId, "run_id");
    return joinPrefix(prefix, `${runId}/current.json`);
}
export function createCheckpointBundle(options) {
    const payloads = normalizePayloads(options.payloads);
    const manifest = parseCheckpointManifest({
        schema_version: 1,
        run_id: options.runId,
        attempt_id: options.attemptId,
        adapter: options.adapter,
        plan_sha256: options.planSha256,
        boundary: options.boundary,
        previous_checkpoint_sha256: options.previousCheckpointSha256,
        payloads: payloads.map((payload) => ({
            path: payload.path,
            bytes: payload.bytes.byteLength,
            sha256: sha256Bytes(payload.bytes),
        })),
        created_at: options.createdAt,
    });
    const manifestBytes = stableCheckpointJsonBytes(manifest);
    const header = Buffer.alloc(BUNDLE_MAGIC.byteLength + MANIFEST_LENGTH_BYTES);
    BUNDLE_MAGIC.copy(header, 0);
    header.writeBigUInt64BE(BigInt(manifestBytes.byteLength), BUNDLE_MAGIC.byteLength);
    const bytes = Buffer.concat([
        header,
        Buffer.from(manifestBytes),
        ...payloads.map((payload) => Buffer.from(payload.bytes)),
    ]);
    return { bytes: new Uint8Array(bytes), manifest };
}
export function verifyCheckpointBundle(bytes) {
    const source = Buffer.from(bytes);
    if (source.byteLength < BUNDLE_MAGIC.byteLength + MANIFEST_LENGTH_BYTES) {
        throw new Error("checkpoint bundle is truncated");
    }
    if (!source.subarray(0, BUNDLE_MAGIC.byteLength).equals(BUNDLE_MAGIC)) {
        throw new Error("checkpoint bundle magic mismatch");
    }
    const length = source.readBigUInt64BE(BUNDLE_MAGIC.byteLength);
    if (length < 2n || length > 16n * 1024n * 1024n) {
        throw new Error("checkpoint manifest length is invalid");
    }
    const manifestLength = Number(length);
    const manifestStart = BUNDLE_MAGIC.byteLength + MANIFEST_LENGTH_BYTES;
    const manifestEnd = manifestStart + manifestLength;
    if (manifestEnd > source.byteLength) {
        throw new Error("checkpoint bundle manifest is truncated");
    }
    const manifest = parseCheckpointManifest(parseJson(source.subarray(manifestStart, manifestEnd), "checkpoint manifest"));
    const payloads = new Map();
    let offset = manifestEnd;
    for (const payload of manifest.payloads) {
        const end = offset + payload.bytes;
        if (end > source.byteLength) {
            throw new Error(`checkpoint payload byte count mismatch: ${payload.path}`);
        }
        const content = new Uint8Array(source.subarray(offset, end));
        if (sha256Bytes(content) !== payload.sha256) {
            throw new Error(`checkpoint payload SHA-256 mismatch: ${payload.path}`);
        }
        payloads.set(payload.path, content);
        offset = end;
    }
    if (offset !== source.byteLength) {
        throw new Error("checkpoint bundle has trailing bytes");
    }
    return { manifest, payloads };
}
export function parseCheckpointManifest(value) {
    const record = requireRecord(value, "checkpoint manifest");
    requireExactKeys(record, [
        "schema_version",
        "run_id",
        "attempt_id",
        "adapter",
        "plan_sha256",
        "boundary",
        "previous_checkpoint_sha256",
        "payloads",
        "created_at",
    ], []);
    requireLiteralOne(record.schema_version, "schema_version");
    const payloadValues = requireArray(record.payloads, "payloads");
    const payloads = payloadValues.map(parsePayloadReference);
    const paths = payloads.map((payload) => payload.path);
    if (paths.some((path, index) => index > 0 && path <= (paths[index - 1] ?? ""))) {
        throw new Error("checkpoint payloads must be sorted with unique paths");
    }
    return {
        schema_version: 1,
        run_id: requireSafeId(record.run_id, "run_id"),
        attempt_id: requireSafeId(record.attempt_id, "attempt_id"),
        adapter: parseAdapterSpec(record.adapter),
        plan_sha256: requireSha256(record.plan_sha256, "plan_sha256"),
        boundary: parseBoundary(record.boundary),
        previous_checkpoint_sha256: record.previous_checkpoint_sha256 === null
            ? null
            : requireSha256(record.previous_checkpoint_sha256, "previous_checkpoint_sha256"),
        payloads,
        created_at: requireTimestamp(record.created_at, "created_at"),
    };
}
export function parseCheckpointClaim(value) {
    const record = requireRecord(value, "checkpoint claim");
    requireExactKeys(record, [
        "schema_version",
        "run_id",
        "attempt_id",
        "sequence",
        "plan_sha256",
        "previous_checkpoint_sha256",
        "checkpoint",
        "created_at",
    ], []);
    requireLiteralOne(record.schema_version, "schema_version");
    return {
        schema_version: 1,
        run_id: requireSafeId(record.run_id, "run_id"),
        attempt_id: requireSafeId(record.attempt_id, "attempt_id"),
        sequence: requirePositiveInteger(record.sequence, "sequence"),
        plan_sha256: requireSha256(record.plan_sha256, "plan_sha256"),
        previous_checkpoint_sha256: record.previous_checkpoint_sha256 === null
            ? null
            : requireSha256(record.previous_checkpoint_sha256, "previous_checkpoint_sha256"),
        checkpoint: parseCheckpointReference(record.checkpoint),
        created_at: requireTimestamp(record.created_at, "created_at"),
    };
}
export function parseCheckpointPointer(value) {
    const record = requireRecord(value, "checkpoint pointer");
    requireExactKeys(record, [
        "schema_version",
        "run_id",
        "sequence",
        "plan_sha256",
        "checkpoint",
        "updated_at",
    ], []);
    requireLiteralOne(record.schema_version, "schema_version");
    return {
        schema_version: 1,
        run_id: requireSafeId(record.run_id, "run_id"),
        sequence: requirePositiveInteger(record.sequence, "sequence"),
        plan_sha256: requireSha256(record.plan_sha256, "plan_sha256"),
        checkpoint: parseCheckpointReference(record.checkpoint),
        updated_at: requireTimestamp(record.updated_at, "updated_at"),
    };
}
export function parseCheckpointReceipt(value) {
    const record = requireRecord(value, "checkpoint receipt");
    requireExactKeys(record, [
        "schema_version",
        "kind",
        "run_id",
        "attempt_id",
        "plan_sha256",
        "sequence",
        "checkpoint",
        "adapter",
        "created_at",
    ], ["job_id", "evidence"]);
    requireLiteralOne(record.schema_version, "schema_version");
    const kind = record.kind;
    if (kind !== "restore" && kind !== "terminal") {
        throw new Error("checkpoint receipt kind is invalid");
    }
    return {
        schema_version: 1,
        kind,
        run_id: requireSafeId(record.run_id, "run_id"),
        attempt_id: requireSafeId(record.attempt_id, "attempt_id"),
        ...(record.job_id === undefined
            ? {}
            : { job_id: requireBoundedString(record.job_id, "job_id", 200) }),
        plan_sha256: requireSha256(record.plan_sha256, "plan_sha256"),
        sequence: requirePositiveInteger(record.sequence, "sequence"),
        checkpoint: parseCheckpointReference(record.checkpoint),
        adapter: parseAdapterSpec(record.adapter),
        created_at: requireTimestamp(record.created_at, "created_at"),
        ...(record.evidence === undefined
            ? {}
            : { evidence: requireJsonRecord(record.evidence, "evidence") }),
    };
}
export function stableCheckpointJsonBytes(value) {
    return Buffer.from(`${JSON.stringify(canonicalize(value), null, 2)}\n`, "utf8");
}
export class CheckpointCoordinator {
    #runId;
    #attemptId;
    #jobId;
    #planSha256;
    #store;
    #receiptStore;
    #prefix;
    #clock;
    #head = null;
    constructor(options) {
        this.#runId = options.runId;
        this.#attemptId = options.attemptId;
        this.#jobId = options.jobId;
        this.#planSha256 = options.planSha256;
        this.#store = options.store;
        this.#receiptStore = options.receiptStore;
        this.#prefix = options.prefix;
        this.#clock = options.clock;
    }
    static create(options) {
        requireSafeId(options.runId, "run_id");
        requireSafeId(options.attemptId, "attempt_id");
        requireSha256(options.planSha256, "plan_sha256");
        requireRepoId(options.store.bucketId, "bucketId");
        if (options.jobId !== undefined)
            requireBoundedString(options.jobId, "job_id", 200);
        return new CheckpointCoordinator({
            runId: options.runId,
            attemptId: options.attemptId,
            ...(options.jobId === undefined ? {} : { jobId: options.jobId }),
            planSha256: options.planSha256,
            store: options.store,
            ...(options.receiptStore === undefined
                ? {}
                : { receiptStore: options.receiptStore }),
            prefix: normalizePrefix(options.prefix ?? ""),
            clock: options.clock ?? (() => new Date()),
        });
    }
    async restoreLatest(adapter) {
        validateAdapterSpec(adapter.spec);
        if (adapter.spec.resume_mode === "restart" ||
            adapter.spec.resume_mode === "unsupported") {
            throw new Error(`adapter ${adapter.spec.name} does not support checkpoint restore`);
        }
        const chain = await this.#loadChain();
        if (chain.length === 0)
            return null;
        const head = chain.at(-1);
        if (head === undefined)
            throw new Error("checkpoint chain head is missing");
        const bytes = await this.#readReference(head.checkpoint);
        const verified = verifyCheckpointBundle(bytes);
        this.#validateManifest(verified.manifest, adapter.spec, head.sequence);
        if (verified.manifest.previous_checkpoint_sha256 !==
            head.previous_checkpoint_sha256) {
            throw new Error("checkpoint head predecessor does not match its claim");
        }
        const evidence = await adapter.restore(verified.manifest, verified.payloads);
        this.#head = { claim: head, manifest: verified.manifest };
        await this.#repairPointer(head);
        const result = {
            checkpoint: head.checkpoint,
            manifest: verified.manifest,
            evidence,
        };
        await this.#publishReceipt({
            kind: "restore",
            claim: head,
            adapter: adapter.spec,
            evidence: toJsonRecord(evidence, "restore evidence"),
        });
        return result;
    }
    async commit(boundaryValue, adapter) {
        const boundary = parseBoundary(boundaryValue);
        validateAdapterSpec(adapter.spec);
        const expectedSequence = (this.#head?.claim.sequence ?? 0) + 1;
        if (boundary.sequence !== expectedSequence) {
            throw new Error(`checkpoint boundary sequence must be ${String(expectedSequence)}`);
        }
        const payloads = await adapter.save(boundary);
        const createdAt = this.#now();
        const bundle = createCheckpointBundle({
            runId: this.#runId,
            attemptId: this.#attemptId,
            adapter: adapter.spec,
            planSha256: this.#planSha256,
            boundary,
            previousCheckpointSha256: this.#head?.claim.checkpoint.sha256 ?? null,
            payloads,
            createdAt,
        });
        const digest = sha256Bytes(bundle.bytes);
        const reference = parseCheckpointReference({
            bucket: this.#store.bucketId,
            key: checkpointBundleKey(this.#prefix, this.#runId, digest),
            sha256: digest,
            bytes: bundle.bytes.byteLength,
        });
        await this.#store.writeImmutable(reference.key, bundle.bytes);
        const uploaded = await this.#readReference(reference);
        verifyCheckpointBundle(uploaded);
        const claim = parseCheckpointClaim({
            schema_version: 1,
            run_id: this.#runId,
            attempt_id: this.#attemptId,
            sequence: expectedSequence,
            plan_sha256: this.#planSha256,
            previous_checkpoint_sha256: this.#head?.claim.checkpoint.sha256 ?? null,
            checkpoint: reference,
            created_at: createdAt,
        });
        const claimKey = checkpointClaimKey(this.#prefix, claim);
        const claimBytes = stableCheckpointJsonBytes(claim);
        await this.#store.writeImmutable(claimKey, claimBytes);
        const storedClaim = await this.#store.read(claimKey);
        if (storedClaim === null || !equalBytes(storedClaim, claimBytes)) {
            throw new Error("checkpoint claim read-back mismatch");
        }
        const chain = await this.#loadChain();
        const committed = chain.at(-1);
        if (committed === undefined ||
            committed.sequence !== expectedSequence ||
            !equalReference(committed.checkpoint, reference)) {
            throw new Error("checkpoint claim did not become the unique chain head");
        }
        this.#head = { claim, manifest: bundle.manifest };
        await this.#repairPointer(claim);
        return reference;
    }
    async finish(boundary, adapter) {
        const checkpoint = await this.commit(boundary, adapter);
        const head = this.#head;
        if (head === null)
            throw new Error("final checkpoint head is missing");
        await this.#publishReceipt({
            kind: "terminal",
            claim: head.claim,
            adapter: adapter.spec,
        });
        return checkpoint;
    }
    async #loadChain() {
        const keys = await this.#store.list(checkpointClaimPrefix(this.#prefix, this.#runId));
        const claims = [];
        for (const key of keys) {
            if (!CLAIM_PATH.test(`/${key}`))
                continue;
            const raw = await this.#store.read(key);
            if (raw === null)
                throw new Error(`checkpoint claim disappeared: ${key}`);
            const claim = parseCheckpointClaim(parseJson(raw, "checkpoint claim"));
            if (claim.run_id !== this.#runId ||
                claim.plan_sha256 !== this.#planSha256) {
                throw new Error("checkpoint claim identity mismatch");
            }
            if (key !== checkpointClaimKey(this.#prefix, claim)) {
                throw new Error(`checkpoint claim path mismatch: ${key}`);
            }
            claims.push(claim);
        }
        const bySequence = new Map();
        for (const claim of claims) {
            const values = bySequence.get(claim.sequence) ?? [];
            values.push(claim);
            bySequence.set(claim.sequence, values);
        }
        const sequenceNumbers = [...bySequence.keys()].sort((left, right) => left - right);
        if (sequenceNumbers.length === 0)
            return [];
        if (sequenceNumbers[0] !== 1) {
            throw new Error("checkpoint chain must start at sequence 1");
        }
        const chain = [];
        let previousSha256 = null;
        for (const [index, sequence] of sequenceNumbers.entries()) {
            if (sequence !== index + 1) {
                throw new Error("checkpoint claim sequence gap");
            }
            const candidates = bySequence.get(sequence);
            if (candidates === undefined || candidates.length === 0) {
                throw new Error("checkpoint claim sequence is empty");
            }
            const reference = candidates[0]?.checkpoint;
            if (reference === undefined) {
                throw new Error("checkpoint claim reference is missing");
            }
            if (candidates.some((candidate) => !equalReference(candidate.checkpoint, reference) ||
                candidate.previous_checkpoint_sha256 !== previousSha256)) {
                throw new Error(`conflicting checkpoint claims at sequence ${String(sequence)}`);
            }
            const claim = candidates[0];
            if (claim === undefined)
                throw new Error("checkpoint claim is missing");
            if (claim.previous_checkpoint_sha256 !== previousSha256) {
                throw new Error("checkpoint predecessor claim mismatch");
            }
            chain.push(claim);
            previousSha256 = reference.sha256;
        }
        return chain;
    }
    #validateManifest(manifest, adapter, sequence) {
        if (manifest.run_id !== this.#runId ||
            manifest.plan_sha256 !== this.#planSha256) {
            throw new Error("checkpoint manifest identity mismatch");
        }
        if (!equalAdapter(manifest.adapter, adapter)) {
            throw new Error("checkpoint adapter mismatch");
        }
        if (manifest.boundary.sequence !== sequence) {
            throw new Error("checkpoint boundary sequence mismatch");
        }
    }
    async #readReference(reference) {
        if (reference.bucket !== this.#store.bucketId) {
            throw new Error("checkpoint bucket mismatch");
        }
        const raw = await this.#store.read(reference.key);
        if (raw === null)
            throw new Error(`checkpoint object is missing: ${reference.key}`);
        if (raw.byteLength !== reference.bytes) {
            throw new Error("checkpoint object byte count mismatch");
        }
        if (sha256Bytes(raw) !== reference.sha256) {
            throw new Error("checkpoint object SHA-256 mismatch");
        }
        return raw;
    }
    async #repairPointer(claim) {
        const pointer = parseCheckpointPointer({
            schema_version: 1,
            run_id: this.#runId,
            sequence: claim.sequence,
            plan_sha256: this.#planSha256,
            checkpoint: claim.checkpoint,
            updated_at: this.#now(),
        });
        const path = checkpointPointerKey(this.#prefix, this.#runId);
        const bytes = stableCheckpointJsonBytes(pointer);
        try {
            await this.#store.writePointerHint(path, bytes);
            const readBack = await this.#store.read(path);
            if (readBack === null || !equalBytes(readBack, bytes))
                return;
        }
        catch {
            // Claims remain authoritative when the optional pointer hint cannot update.
        }
    }
    async #publishReceipt(options) {
        if (this.#receiptStore === undefined)
            return;
        const receipt = {
            schema_version: 1,
            kind: options.kind,
            run_id: this.#runId,
            attempt_id: this.#attemptId,
            ...(this.#jobId === undefined ? {} : { job_id: this.#jobId }),
            plan_sha256: this.#planSha256,
            sequence: options.claim.sequence,
            checkpoint: options.claim.checkpoint,
            adapter: options.adapter,
            created_at: this.#now(),
            ...(options.evidence === undefined ? {} : { evidence: options.evidence }),
        };
        await this.#receiptStore.publish(receipt);
    }
    #now() {
        const value = this.#clock();
        if (!Number.isFinite(value.getTime()))
            throw new Error("clock returned an invalid Date");
        return value.toISOString();
    }
}
function normalizePayloads(payloads) {
    const result = payloads
        .map((payload) => ({
        path: requirePayloadPath(payload.path),
        bytes: Uint8Array.from(payload.bytes),
    }))
        .sort((left, right) => left.path.localeCompare(right.path));
    if (new Set(result.map((payload) => payload.path)).size !== result.length) {
        throw new Error("checkpoint payload paths must be unique");
    }
    return result;
}
function parsePayloadReference(value) {
    const record = requireRecord(value, "checkpoint payload");
    requireExactKeys(record, ["path", "bytes", "sha256"], []);
    return {
        path: requirePayloadPath(record.path),
        bytes: requireNonnegativeInteger(record.bytes, "bytes"),
        sha256: requireSha256(record.sha256, "sha256"),
    };
}
function parseAdapterSpec(value) {
    const record = requireRecord(value, "adapter");
    requireExactKeys(record, ["name", "version", "resume_mode"], []);
    const result = {
        name: requireString(record.name, "name"),
        version: requirePositiveInteger(record.version, "version"),
        resume_mode: requireResumeMode(record.resume_mode),
    };
    validateAdapterSpec(result);
    return result;
}
function validateAdapterSpec(value) {
    if (!ADAPTER_NAME.test(value.name)) {
        throw new Error("adapter name must be a lowercase identifier");
    }
    requirePositiveInteger(value.version, "adapter version");
    requireResumeMode(value.resume_mode);
}
function parseBoundary(value) {
    const record = requireRecord(value, "boundary");
    requireExactKeys(record, ["name", "sequence", "reached_at", "metadata"], []);
    return {
        name: requireBoundedString(record.name, "name", 100),
        sequence: requireNonnegativeInteger(record.sequence, "sequence"),
        reached_at: requireTimestamp(record.reached_at, "reached_at"),
        metadata: requireJsonRecord(record.metadata, "metadata"),
    };
}
export function parseCheckpointReference(value) {
    const record = requireRecord(value, "checkpoint reference");
    requireExactKeys(record, ["bucket", "key", "sha256", "bytes"], []);
    const sha256 = requireSha256(record.sha256, "sha256");
    const key = requireObjectPath(record.key, "key");
    if (!key.includes(`sha256-${sha256}`)) {
        throw new Error("checkpoint key must contain its SHA-256");
    }
    return {
        bucket: requireRepoId(record.bucket, "bucket"),
        key,
        sha256,
        bytes: requirePositiveInteger(record.bytes, "bytes"),
    };
}
function requirePayloadPath(value) {
    const path = requireString(value, "payload path");
    if (path.length === 0 ||
        path.length > 1024 ||
        path.startsWith("/") ||
        path.includes("\\") ||
        path.split("/").some((part) => part === "" || part === "." || part === "..")) {
        throw new Error("checkpoint payload path must be a safe relative POSIX path");
    }
    return path;
}
function requireObjectPath(value, name) {
    const path = requireString(value, name);
    if (path.length === 0 ||
        path.length > 1024 ||
        path.startsWith("/") ||
        path.includes("\\") ||
        path.split("/").some((part) => part === "" || part === "." || part === "..")) {
        throw new Error(`${name} must be a safe relative POSIX path`);
    }
    return path;
}
function requireRecord(value, name) {
    if (!isRecord(value))
        throw new Error(`${name} must be an object`);
    return value;
}
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function requireJsonRecord(value, name) {
    const canonical = canonicalize(requireRecord(value, name));
    if (!isJsonRecord(canonical))
        throw new Error(`${name} must be an object`);
    return canonical;
}
function isJsonRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function toJsonRecord(value, name) {
    return requireJsonRecord(value, name);
}
function requireArray(value, name) {
    if (!Array.isArray(value))
        throw new Error(`${name} must be an array`);
    return value;
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
function requireLiteralOne(value, name) {
    if (value !== 1)
        throw new Error(`${name} must be 1`);
    return 1;
}
function requireString(value, name) {
    if (typeof value !== "string")
        throw new Error(`${name} must be a string`);
    return value;
}
function requireBoundedString(value, name, maximum) {
    const result = requireString(value, name);
    if (result.length === 0 || result.length > maximum) {
        throw new Error(`${name} must contain 1 to ${String(maximum)} characters`);
    }
    return result;
}
function requireSafeId(value, name) {
    const result = requireString(value, name);
    if (!SAFE_ID.test(result))
        throw new Error(`${name} must be a safe identifier`);
    return result;
}
function requireSha256(value, name) {
    const result = requireString(value, name);
    if (!SHA256.test(result))
        throw new Error(`${name} must be 64 lowercase hex characters`);
    return result;
}
function requireRepoId(value, name) {
    const result = requireString(value, name);
    if (!REPO_ID.test(result))
        throw new Error(`${name} must be a namespace/name identifier`);
    return result;
}
function requireNonnegativeInteger(value, name) {
    if (!Number.isSafeInteger(value) || typeof value !== "number" || value < 0) {
        throw new Error(`${name} must be a nonnegative safe integer`);
    }
    return value;
}
function requirePositiveInteger(value, name) {
    const result = requireNonnegativeInteger(value, name);
    if (result < 1)
        throw new Error(`${name} must be a positive safe integer`);
    return result;
}
function requireTimestamp(value, name) {
    const result = requireString(value, name);
    const parsed = new Date(result);
    if (!Number.isFinite(parsed.getTime()) ||
        !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/u.test(result)) {
        throw new Error(`${name} must be an RFC 3339 UTC timestamp`);
    }
    return result;
}
function requireResumeMode(value) {
    if (value !== "exact" &&
        value !== "boundary" &&
        value !== "restart" &&
        value !== "unsupported") {
        throw new Error("resume_mode is invalid");
    }
    return value;
}
function parseJson(raw, name) {
    try {
        return JSON.parse(Buffer.from(raw).toString("utf8"));
    }
    catch (error) {
        throw new Error(`${name} must contain valid JSON`, { cause: error });
    }
}
function canonicalize(value) {
    if (value === null ||
        typeof value === "string" ||
        typeof value === "boolean") {
        return value;
    }
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
function sha256Bytes(value) {
    return createHash("sha256").update(value).digest("hex");
}
function equalBytes(left, right) {
    return Buffer.from(left).equals(Buffer.from(right));
}
function equalReference(left, right) {
    return (left.bucket === right.bucket &&
        left.key === right.key &&
        left.sha256 === right.sha256 &&
        left.bytes === right.bytes);
}
function equalAdapter(left, right) {
    return (left.name === right.name &&
        left.version === right.version &&
        left.resume_mode === right.resume_mode);
}
function normalizePrefix(value) {
    const result = value.replace(/^\/+|\/+$/gu, "");
    if (result.includes("\\") ||
        result.split("/").some((part) => part === "." || part === "..")) {
        throw new Error("prefix must be a safe relative POSIX path");
    }
    return result;
}
function joinPrefix(prefix, path) {
    return prefix.length === 0 ? path : `${prefix}/${path}`;
}
//# sourceMappingURL=checkpoint.js.map