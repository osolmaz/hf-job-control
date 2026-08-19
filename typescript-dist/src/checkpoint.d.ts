export type JsonValue = null | boolean | number | string | JsonValue[] | {
    [key: string]: JsonValue;
};
export type CheckpointResumeMode = "exact" | "boundary" | "restart" | "unsupported";
export type CheckpointBoundary = {
    name: string;
    sequence: number;
    reached_at: string;
    metadata: Readonly<Record<string, JsonValue>>;
};
export type CheckpointAdapterSpec = {
    name: string;
    version: number;
    resume_mode: CheckpointResumeMode;
};
export type CheckpointPayload = {
    path: string;
    bytes: Uint8Array;
};
export type CheckpointPayloadReference = {
    path: string;
    bytes: number;
    sha256: string;
};
export type CheckpointManifest = {
    schema_version: 1;
    run_id: string;
    attempt_id: string;
    adapter: CheckpointAdapterSpec;
    plan_sha256: string;
    boundary: CheckpointBoundary;
    previous_checkpoint_sha256: string | null;
    payloads: readonly CheckpointPayloadReference[];
    created_at: string;
};
export type CheckpointReference = {
    bucket: string;
    key: string;
    sha256: string;
    bytes: number;
};
export type CheckpointClaim = {
    schema_version: 1;
    run_id: string;
    attempt_id: string;
    sequence: number;
    plan_sha256: string;
    previous_checkpoint_sha256: string | null;
    checkpoint: CheckpointReference;
    created_at: string;
};
export type CheckpointPointer = {
    schema_version: 1;
    run_id: string;
    sequence: number;
    plan_sha256: string;
    checkpoint: CheckpointReference;
    updated_at: string;
};
export type CheckpointReceipt = {
    schema_version: 1;
    kind: "restore" | "terminal";
    run_id: string;
    attempt_id: string;
    job_id?: string;
    plan_sha256: string;
    sequence: number;
    checkpoint: CheckpointReference;
    adapter: CheckpointAdapterSpec;
    created_at: string;
    evidence?: Readonly<Record<string, JsonValue>>;
};
export type VerifiedCheckpointBundle = {
    manifest: CheckpointManifest;
    payloads: ReadonlyMap<string, Uint8Array>;
};
export type RestoreResult<RestoreEvidence> = {
    checkpoint: CheckpointReference;
    manifest: CheckpointManifest;
    evidence: RestoreEvidence;
};
export interface CheckpointAdapter<RestoreEvidence extends Readonly<Record<string, JsonValue>>> {
    readonly spec: CheckpointAdapterSpec;
    save(boundary: CheckpointBoundary): Promise<readonly CheckpointPayload[]>;
    restore(manifest: CheckpointManifest, payloads: ReadonlyMap<string, Uint8Array>): Promise<RestoreEvidence>;
}
export interface CheckpointObjectStore {
    readonly bucketId: string;
    read(path: string): Promise<Uint8Array | null>;
    writeImmutable(path: string, bytes: Uint8Array): Promise<void>;
    writePointerHint(path: string, bytes: Uint8Array): Promise<void>;
    list(prefix: string): Promise<readonly string[]>;
}
export interface CheckpointReceiptStore {
    publish(receipt: CheckpointReceipt): Promise<void>;
}
export declare function checkpointBundleKey(prefix: string, runId: string, sha256: string): string;
export declare function checkpointClaimPrefix(prefix: string, runId: string): string;
export declare function checkpointClaimKey(prefix: string, claim: Pick<CheckpointClaim, "run_id" | "attempt_id" | "sequence">): string;
export declare function checkpointPointerKey(prefix: string, runId: string): string;
export declare function createCheckpointBundle(options: {
    runId: string;
    attemptId: string;
    adapter: CheckpointAdapterSpec;
    planSha256: string;
    boundary: CheckpointBoundary;
    previousCheckpointSha256: string | null;
    payloads: readonly CheckpointPayload[];
    createdAt: string;
}): {
    bytes: Uint8Array;
    manifest: CheckpointManifest;
};
export declare function verifyCheckpointBundle(bytes: Uint8Array): VerifiedCheckpointBundle;
export declare function parseCheckpointManifest(value: unknown): CheckpointManifest;
export declare function parseCheckpointClaim(value: unknown): CheckpointClaim;
export declare function parseCheckpointPointer(value: unknown): CheckpointPointer;
export declare function parseCheckpointReceipt(value: unknown): CheckpointReceipt;
export declare function stableCheckpointJsonBytes(value: unknown): Uint8Array;
export declare class CheckpointCoordinator {
    #private;
    private constructor();
    static create(options: {
        runId: string;
        attemptId: string;
        jobId?: string;
        planSha256: string;
        store: CheckpointObjectStore;
        receiptStore?: CheckpointReceiptStore;
        prefix?: string;
        clock?: () => Date;
    }): CheckpointCoordinator;
    restoreLatest<RestoreEvidence extends Readonly<Record<string, JsonValue>>>(adapter: CheckpointAdapter<RestoreEvidence>): Promise<RestoreResult<RestoreEvidence> | null>;
    commit<RestoreEvidence extends Readonly<Record<string, JsonValue>>>(boundaryValue: CheckpointBoundary, adapter: CheckpointAdapter<RestoreEvidence>): Promise<CheckpointReference>;
    finish<RestoreEvidence extends Readonly<Record<string, JsonValue>>>(boundary: CheckpointBoundary, adapter: CheckpointAdapter<RestoreEvidence>): Promise<CheckpointReference>;
}
export declare function parseCheckpointReference(value: unknown): CheckpointReference;
//# sourceMappingURL=checkpoint.d.ts.map