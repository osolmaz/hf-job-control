---
title: Add durable TypeScript job resume
author: Onur Solmaz <2453968+osolmaz@users.noreply.github.com>
date: 2026-08-19
tags: [hugging-face, jobs, checkpoints, typescript, resume]
---

# Add durable TypeScript job resume

## Purpose

Long-running TypeScript Jobs need the same logical-run and checkpoint guarantees
that HF Job Control already gives Python workers. A physical Hugging Face Job
must be disposable. A replacement must verify a small checkpoint, resume the
same fixed work plan, and continue only work that has no durable result.

This plan extends the [control protocol](2026-07-24-protocol.md) and the
[progress reporting plan](2026-08-18-progress-reporting-plan.md). Progress will
continue to describe committed application work. Checkpoints will make that
work resumable.

The first adopters are xTap Pool enrichment and OurModels publication. Their
application plans are maintained in their own repositories:

- [xTap Pool small worker checkpoints](https://github.com/osolmaz/xtap-pool/blob/main/docs/2026-08-19-small-worker-checkpoints-plan.md)
- [OurModels resumable publication](https://github.com/osolmaz/ourmodels/blob/main/docs/2026-08-19-resumable-publication-plan.md)

## Requirements

The TypeScript package must:

- Keep one logical `run_id` across physical attempts.
- Give each attempt a separate `attempt_id` and record the Hugging Face
  `JOB_ID` when available.
- Register one immutable launch specification for the logical run.
- Bind every checkpoint to one adapter identity and one immutable work plan.
- Package checkpoint payloads as content-addressed bundles.
- Verify outer bytes, the outer SHA-256, the inner manifest, and every payload
  before restore.
- Link checkpoints into an ordered predecessor chain.
- Make immutable sequence claims the authoritative checkpoint history.
- Treat the mutable current pointer as a startup shortcut only.
- Reject identity and predecessor conflicts, wrong sizes, and digest mismatches.
- Publish a checkpoint claim before progress can report the related work as
  committed.
- Recover from immutable claims when the pointer is missing, stale, or wrong.
- Preserve immutable receipts for restore and lifecycle decisions.
- Keep provider clients and application-specific state outside the package.
- Use strict TypeScript without `any`, unchecked casts, or unvalidated external
  input.

## Boundaries

The package owns identities, manifests, bundles, verification, pointer recovery,
launch-spec checks, and receipts. The application owns the safe boundary, work
plan, payload format, result publication, and final output.

This work does not create a scheduler, service, database, Dataset, or Bucket. An
application supplies its approved object store. The xTap Pool and OurModels
integrations will use their existing Buckets.

The package will not decide scientific stopping, choose application work, merge
application results, calculate an overall percentage, or infer a finish time.

## Resume architecture

Each logical run has one immutable plan. Results and checkpoint bundles are
immutable, content-addressed objects. Immutable sequence claims form the
authoritative checkpoint history, while `current.json` gives the worker a fast
starting point. Recovery verifies the claim chain and corrects a missing or stale
pointer hint.

This design does not require compare-and-swap support from a Bucket. Claims use
attempt-specific immutable paths, and a conflicting claim stops the run before
more work starts. Where a git-backed repository already provides
`parentCommit`, its canonical public pointer keeps that stronger check.

## Public TypeScript API

Add the following public types to the TypeScript package:

```ts
export type Boundary = {
  sequence: number;
  phase: string;
  completed: number;
  total?: number;
  unit?: string;
};

export type AdapterSpec = {
  name: string;
  version: string;
  resume_mode: "exact" | "boundary";
};

export type CheckpointPayload = {
  path: string;
  bytes: Uint8Array;
};

export type CheckpointManifest = {
  schema_version: 1;
  run_id: string;
  attempt_id: string;
  adapter: AdapterSpec;
  plan_sha256: string;
  boundary: Boundary;
  previous_checkpoint_sha256: string | null;
  payloads: Array<{
    path: string;
    bytes: number;
    sha256: string;
  }>;
  created_at: string;
};

export interface CheckpointAdapter<RestoreEvidence> {
  readonly spec: AdapterSpec;
  save(boundary: Boundary): Promise<readonly CheckpointPayload[]>;
  restore(
    manifest: CheckpointManifest,
    payloads: ReadonlyMap<string, Uint8Array>,
  ): Promise<RestoreEvidence>;
}
```

The package also exposes a provider-neutral object store:

```ts
export interface CheckpointObjectStore {
  read(path: string): Promise<Uint8Array | null>;
  writeImmutable(path: string, bytes: Uint8Array): Promise<void>;
  writePointerHint(path: string, bytes: Uint8Array): Promise<void>;
  list(prefix: string): Promise<readonly string[]>;
}
```

`writeImmutable` accepts an existing object only when its bytes are equal.
Claims use attempt-specific paths, so they do not depend on an atomic
create-only operation from the Bucket. `writePointerHint` remains mutable and
must be followed by an exact read-back. Correctness never depends on that hint.

## Checkpoint bundle

The `.hfjob` v1 format changes in place for new logical runs. It contains one
canonical JSON manifest and zero or more payloads under validated relative
paths.

```text
manifest.json
payloads/<application path>
```

The Python and TypeScript packages use the same manifest fields and deterministic
archive bytes. The new format applies only to new logical runs. Existing v1
bundles remain audit evidence and are not restored by the new coordinator. A
separate one-time converter is required if an application must resume one of
those old runs.

Bundle creation follows this order:

1. Validate identities, adapter, boundary, plan digest, and predecessor.
2. Validate every payload path and reject traversal or duplicates.
3. Calculate every payload byte count and SHA-256.
4. Serialize the canonical manifest.
5. Build the deterministic bundle.
6. Calculate the bundle byte count and SHA-256.
7. Upload it under its content-addressed key.
8. Download it and verify exact bytes.

The artifact path remains:

```text
<run_id>/checkpoints/sha256-<bundle_sha256>/checkpoint.hfjob
```

## Sequence claims and pointer hint

One immutable claim records which bundle an attempt committed for a sequence:

```ts
export type CheckpointClaim = {
  schema_version: 1;
  run_id: string;
  attempt_id: string;
  sequence: number;
  plan_sha256: string;
  checkpoint: CheckpointReference;
  created_at: string;
};
```

Claims use this path:

```text
<run_id>/checkpoints/claims/sequence-<sequence>/<attempt_id>.json
```

A sequence is valid only when it has one claim, or when all claims contain the
same checkpoint reference. Different references for one sequence stop the run.
This rule is safe on Buckets that have no compare-and-swap write. The canonical
non-concurrent schedule remains the first admission control, while claims detect
any stale or unexpected second writer.

Applications may also write a small current pointer:

```ts
export type CheckpointPointer = {
  schema_version: 1;
  run_id: string;
  sequence: number;
  plan_sha256: string;
  checkpoint: CheckpointReference;
  updated_at: string;
};
```

The pointer is only a startup shortcut. A missing, stale, or corrupt pointer
cannot remove committed work because restore verifies the claim history.

## Coordinator

Add these concrete return types:

```ts
export type CheckpointReference = {
  key: string;
  bytes: number;
  sha256: string;
};

export type RestoreResult<RestoreEvidence> = {
  checkpoint: CheckpointReference;
  manifest: CheckpointManifest;
  evidence: RestoreEvidence;
};
```

Add a `CheckpointCoordinator` with these operations:

```ts
create(options): Promise<CheckpointCoordinator>
restoreLatest(adapter): Promise<RestoreResult | null>
commit(boundary, adapter): Promise<CheckpointReference>
finish(boundary, adapter): Promise<CheckpointReference>
```

`create` validates `RUN_ID`, `ATTEMPT_ID`, optional `JOB_ID`, adapter identity,
plan digest, launch-spec digest, and store configuration.

`restoreLatest`:

1. Loads the immutable plan.
2. Reads the pointer hint when it exists.
3. Lists and validates sequence claims.
4. Finds the highest unique claimed sequence.
5. Verifies the complete predecessor chain back to the one root claim whose
   predecessor is null. More than one root stops the run.
6. Downloads and verifies the head bundle and every payload.
7. Verifies the run and adapter identities, the plan digest, the predecessor,
   and the boundary.
8. Calls the adapter only after all generic checks pass.
9. Returns restore evidence for the application receipt.
10. Repairs a stale pointer hint after successful restore.

`commit` calls the adapter only at an application-declared safe boundary. It
uploads and verifies the bundle, writes and verifies the immutable sequence
claim, updates the pointer hint, and returns the exact checkpoint reference.
The application may then publish progress for that sequence.

`finish` performs one normal final checkpoint commit and then publishes the
terminal receipt. A terminal receipt never precedes its final checkpoint claim.

## Result-before-checkpoint ordering

HF Job Control cannot make application output and a checkpoint one atomic
remote write. The application must use this order:

1. Publish the immutable result object.
2. Download and verify the result.
3. Add its exact reference to local checkpoint state.
4. Commit and verify the checkpoint bundle.
5. Publish and verify its immutable sequence claim.
6. Update the pointer hint.
7. Publish progress from that checkpoint sequence.

If the result exists but the checkpoint does not, the application must identify
that result by deterministic batch identity and apply it during recovery. The
package will expose helper validation for content-addressed references but will
not interpret result contents.

## Launch specification

The TypeScript package will read and validate the same launch-spec wire format
as Python. The first attempt registers canonical bytes. Every later attempt
must match its digest before restore or new work.

Secrets remain absent from launch JSON. The specification records secret names,
not secret values.

## Receipts

TypeScript will publish immutable receipts for:

- Launch-spec registration.
- Checkpoint restore.
- Applied lifecycle generations.
- Terminal completion.

A restore receipt names the logical run, physical attempt, physical Job when
available, checkpoint digest, boundary sequence, adapter, plan digest, and
application restore evidence. The terminal receipt names the final claimed
checkpoint.

## Files

Add or extend:

```text
typescript/src/checkpoint.ts
typescript/src/checkpoint-adapter.ts
typescript/src/checkpoint-bundle.ts
typescript/src/checkpoint-coordinator.ts
typescript/src/checkpoint-recovery.ts
typescript/src/object-checkpoint-store.ts
typescript/src/launch-spec.ts
typescript/src/receipt.ts
typescript/src/index.ts
src/hf_job_control/checkpoint.py
src/hf_job_control/models.py
src/hf_job_control/stores.py
schemas/checkpoint-manifest-v1.schema.json
```

Add shared fixtures for the manifest, deterministic bundle, claim, pointer, and
receipt formats. Python and TypeScript must accept and produce the same canonical
bytes. Keep bundled schema copies byte-identical to the root schemas.

## Failure behavior

The worker must stop before application restore or new work when it finds:

- Invalid external JSON.
- A wrong run, attempt, adapter, or plan identity.
- A launch-spec mismatch.
- A missing or corrupt bundle.
- A wrong byte count or digest.
- An invalid payload path.
- A broken predecessor chain or more than one root.
- Different checkpoint references claimed for one sequence.
- A checkpoint sequence that disagrees with the application boundary.

A pointer-hint failure does not invalidate a committed claim. Transient reads
and idempotent immutable writes use bounded retries. Validation, identity,
credential, and claim conflicts do not retry blindly.

## Verification

Unit and shared-fixture tests must cover:

- Bundle creation and deterministic verification.
- Exact and boundary adapter round trips.
- Empty and multi-file payloads.
- Wrong run, attempt, adapter, or plan identity, plus wrong sizes or digests.
- Path traversal and duplicate payload names.
- Equal claims and conflicting claims for one sequence.
- Pointer hints that are missing or stale, or contain invalid bytes.
- Empty history, one valid null-predecessor root, and multiple-root rejection.
- Interrupted publication at every bundle, claim, or pointer-hint write.
- Broken checkpoint chains.
- Launch-spec parity with Python.
- Restore and terminal receipts with physical Job identity.
- Progress publication after, and never before, checkpoint publication.

Run the repository checks:

```bash
uv sync --all-groups
npm ci
npm run check
npm audit
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest --cov --cov-report=term-missing
uv run pip-audit
uvx slophammer-py==0.4.0 dry .
uvx slophammer-py==0.4.0 check .
uv run python scripts/check-mutation.py --min-kill-rate 70
npx -y @simpledoc/simpledoc check
```

## Delivery

1. Change the Python and TypeScript v1 models, schemas, deterministic bundles,
   and shared fixtures together.
2. Add immutable sequence claims, pointer hints, the coordinator, recovery,
   launch-spec checks, and receipts.
3. Prove Python and TypeScript wire parity and mark old v1 bundles audit-only.
4. Run a local finite worker through forced stops at every remote-write boundary.
5. Publish a package release only after all local checks pass.
6. Integrate xTap Pool against the released package.
7. Integrate OurModels against the same release.
8. Run application canaries only after their changed remote Job contracts have
   approval.

## Acceptance criteria

The shared work is complete when:

- Python and TypeScript validate the same common wire fixtures.
- A physical TypeScript Job can restore a verified checkpoint from another
  attempt in the same logical run.
- A wrong launch specification or plan cannot resume.
- Interrupted bundle, claim, or pointer-hint publication recovers from the
  unique verified claim chain.
- A missing or wrong pointer hint cannot hide committed work.
- Progress never claims work beyond the referenced checkpoint.
- Every restore and terminal action has immutable evidence.
- The generic package contains no xTap Pool or OurModels domain logic.
- The complete repository checks and remote CPU canary pass.
