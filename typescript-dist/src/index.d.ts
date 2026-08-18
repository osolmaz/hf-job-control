export declare const PROGRESS_SCHEMA_VERSION: 1;
export declare class TransientProgressError extends Error {
    constructor(message: string, options?: ErrorOptions);
}
export type ProgressStatus = "pending" | "running" | "waiting" | "blocked" | "completed" | "failed" | "cancelled";
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
export declare function progressPointerKey(prefix: string, runId: string): string;
export declare function progressSnapshotKey(prefix: string, runId: string, digest: string): string;
export declare function progressClaimPrefix(prefix: string, runId: string, sequence: number): string;
export declare function progressClaimKey(prefix: string, claim: ProgressClaim): string;
export declare class ObjectProgressStore implements ProgressStore {
    #private;
    constructor(objects: ProgressObjectStore, prefix?: string);
    loadLatest(runId: string): Promise<StoredProgress | null>;
    loadReference(reference: ArtifactRef): Promise<ProgressSnapshot>;
    publish(snapshot: ProgressSnapshot): Promise<StoredProgress>;
}
export type ProgressReporterOptions = {
    runId: string;
    attemptId: string;
    jobId?: string;
    input: ProgressInput;
    store: ProgressStore;
    flushIntervalMs?: number;
    publishAttempts?: number;
    retryDelayMs?: number;
    clock?: () => Date;
    sleep?: (milliseconds: number) => Promise<void>;
};
export declare class ProgressReporter {
    #private;
    private constructor();
    static create(options: ProgressReporterOptions): Promise<ProgressReporter>;
    get tracks(): readonly ProgressTrack[];
    plan(tracks: readonly ProgressTrack[]): void;
    update(candidate: ProgressTrack): void;
    setState(state: ProgressStatus): void;
    heartbeat(): Promise<StoredProgress | null>;
    flush(options?: {
        force?: boolean;
    }): Promise<StoredProgress | null>;
}
export declare function parseProgressSnapshot(value: unknown): ProgressSnapshot;
export declare function parseProgressPointer(value: unknown): ProgressPointer;
export declare function parseProgressClaim(value: unknown): ProgressClaim;
export declare function parseProgressTrack(value: unknown): ProgressTrack;
export declare function stableJsonBytes(value: unknown): Uint8Array;
//# sourceMappingURL=index.d.ts.map