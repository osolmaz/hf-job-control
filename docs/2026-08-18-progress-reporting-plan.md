---
title: Progress reporting plan
author: Onur Solmaz <2453968+osolmaz@users.noreply.github.com>
date: 2026-08-18
---

# Progress reporting plan

## Purpose

An operator must be able to inspect a running Job and see its current phase,
exact committed progress, remaining work, and the best supported finish-time
range. This must work after a physical Job restart or monitor-session loss
without downloading large databases or thousands of cache shards.

HF Job Control will provide the shared progress protocol and worker libraries.
Applications will measure their own work. Monitoring systems will calculate
rates and finish-time ranges from those facts.

This plan extends the lifecycle work in the
[initial implementation plan](2026-07-24-implementation-plan.md). It does not
change the control protocol described in the
[protocol document](2026-07-24-protocol.md).

## Requirements

The implementation must:

- Represent every measurable phase as a separate progress track.
- Report only committed work that can survive a restart.
- Keep progress continuous across physical Job attempts in one logical run.
- Identify the exact input revision and producer contract for every work plan.
- Reject counter regression within an unchanged plan.
- Start a new estimation epoch when the input, total, unit, or work plan changes.
- Publish a durable update at least every 30 seconds while committed work moves.
- Preserve enough recent samples to recover rate estimates after monitor loss.
- Use each application's existing approved storage.
- Support Python and TypeScript workers from one repository and one schema.
- Keep credentials out of progress records and diagnostic output.
- Leave finish-time calculation to the monitoring system.
- Keep receipts, checkpoints, output manifests, and hashes as completion proof.

## Boundaries

Progress reporting is normal application telemetry. It must be useful to any
operator and must not depend on Pi or Pi Workflows.

The implementation will not add:

- A central progress service or Bucket.
- A network API that workers must call.
- A Pi-specific schema, dependency, command, or storage path.
- A provider client inside Pi Workflows.
- An application-specific field in the shared protocol.
- An overall percentage made by adding unrelated units.
- A finish time when the remaining work or measured rate is unknown.

The progress record is operational evidence. It does not replace an immutable
receipt or prove that final output is valid.

## Ownership

The work spans three repositories.

### HF Job Control

`osolmaz/hf-job-control` owns:

- The language-neutral progress schema.
- Python and TypeScript models and validation.
- Progress reporter behavior.
- Store interfaces and Hugging Face Bucket adapters.
- Ordering, throttling, retry, and recovery rules.
- Cross-language fixtures and remote canary coverage.

### xTap Pool

`osolmaz/xtap-pool` owns:

- Phase definitions for enrichment and publication.
- Counts read from its SQLite database and immutable segments.
- Its progress storage prefix.
- Reconciliation between progress and durable outputs after restart.

### OurModels

`osolmaz/ourmodels` owns:

- Phase definitions for publication.
- Exact task plans for association, claim extraction, and verification.
- Counts read from its cache, snapshot builder, and publisher.
- Its progress storage prefix.
- Reconciliation between progress and durable outputs after restart.

Pi Workflows needs no application integration. The regular Pi model can read a
small progress record, map its tracks to `pi-workflows.progress.v1`, and submit
them through the existing `workflow` tool.

## Progress format

Add `schemas/progress-v1.schema.json` to HF Job Control. Keep version 1 as the
current contract and change it in place under the repository's hard-cutover
policy.

A snapshot has this shape:

```json
{
  "schema_version": 1,
  "run_id": "xtap-enrichment-restoration",
  "attempt_id": "6a846e74e55292eada79c642",
  "job_id": "6a846e74e55292eada79c642",
  "sequence": 42,
  "updated_at": "2026-08-18T14:40:00Z",
  "input": {
    "revision": "b8b3e6999541bfc6",
    "contract_sha256": "2a2814ad4162457c"
  },
  "state": "running",
  "tracks": [
    {
      "key": "registry-scan",
      "plan_id": "registry-scan-b8b3e699",
      "status": "running",
      "completed": 12000,
      "total": 25451,
      "unit": "candidates"
    }
  ]
}
```

The final schema will define these stable concepts:

- `run_id`: Logical work identity across physical attempts.
- `attempt_id`: One physical execution attempt.
- `job_id`: Provider Job identity when available.
- `sequence`: Strictly increasing publication order for the logical run.
- `updated_at`: UTC observation time.
- `input`: Exact input and producer-contract identity.
- `state`: Run-level operational state.
- `tracks`: Independent measurable workstreams.
- `key`: Stable track identity.
- `plan_id`: Identity of one fixed denominator and method.
- `status`: Pending, running, waiting, blocked, completed, failed, or cancelled.
- `completed`: Committed completed work.
- `total`: Planned work when known.
- `unit`: One stable unit for the plan.

A track may omit `completed` and `total` when the phase has no measurable
workload. A track with `completed` must also have a valid unit. A track with a
total must satisfy `0 <= completed <= total`.

The schema will use bounded strings, finite safe numbers, UTC timestamps, and
strict external-input validation. It will reject unknown top-level fields and
secret-like keys.

## Plan and epoch rules

A plan is one fixed workload under one input and producer contract.

Within one `plan_id`:

- The unit cannot change.
- The total cannot decrease or change meaning.
- The completed count cannot decrease.
- Completion is terminal.
- A physical retry continues from the last committed count.

A new source snapshot, denominator, unit, or method creates a new `plan_id`.
This starts a new estimation epoch instead of making progress appear to move
backward.

Some work is discovered by an earlier phase. The later phase remains pending
without a total until discovery finishes. The worker then fixes its plan and
publishes the denominator. The monitoring system must show that the finish time
is unavailable before this point.

## Storage layout

Each application writes to its existing Bucket:

```text
operations/<run-id>/progress/current.json
operations/<run-id>/progress/snapshots/<sha256>.json
```

A snapshot is immutable and content-addressed. `current.json` atomically points
to the latest snapshot and includes its path, byte count, and SHA-256. Each
snapshot references its predecessor.

The predecessor chain gives a replacement monitor enough samples to rebuild a
rate estimate. It also preserves evidence when a mutable pointer is replaced.
The implementation will bound normal reads to the current snapshot and a small
recent window.

Progress storage follows this order:

1. Commit and verify application output.
2. Create and validate the next progress snapshot.
3. Upload the immutable snapshot.
4. Verify its remote size and SHA-256.
5. Replace `current.json` atomically.

A pointer must never reference an unverified snapshot.

## Library interfaces

### Python

Add typed models and a `ProgressReporter` to `hf_job_control`.

The reporter will provide operations equivalent to:

```python
reporter.plan(tracks)
reporter.set("registry-scan", completed=12000, total=25451)
reporter.complete("registry-scan")
reporter.flush()
```

### TypeScript

Publish a TypeScript package from the same repository. It will expose the same
models, validation rules, and reporter behavior.

The TypeScript interface will provide operations equivalent to:

```typescript
await reporter.plan(tracks);
await reporter.set("registry-scan", { completed: 12000, total: 25451 });
await reporter.complete("registry-scan");
await reporter.flush();
```

### Shared behavior

Both implementations will:

- Validate all input before changing state.
- Serialize concurrent updates.
- Deduplicate equivalent updates.
- Throttle routine publication to 30 seconds.
- Flush at safe boundaries and normal exit.
- Retry bounded transient storage failures.
- Reject stale and out-of-order writes.
- Restore and verify the latest snapshot before continuing.
- Reconcile reported progress with committed application state.
- Expose storage through a small interface for local and Bucket tests.

The reporter will not install signal handlers that can interrupt application
cleanup. Each worker will call `flush()` from its existing lifecycle boundary.
An exit that cannot run cleanup can lose one reporting interval, but it cannot
lose committed application output.

## Run status integration

Add an optional typed `progress` field to `RunStatus` and
`run-status-v1.schema.json`. Keep arbitrary scientific metrics in `metrics`.
Progress must no longer be encoded as unvalidated metric keys.

A worker that uses the full controller can publish progress with its normal
boundary status. A standalone batch worker can use `ProgressReporter` without
adopting pause and resume in the same change. Both paths use the same schema and
storage rules.

## xTap Pool integration

Instrument these phases:

1. Restore database bytes.
2. Verify the restored database.
3. Discover raw segments.
4. Replay attempt segments.
5. Replay registry segments.
6. Replay receipt segments.
7. Plan queue work.
8. Commit enrichment units.
9. Scan registry candidates.
10. Build and verify SQLite output.
11. Upload database bytes.
12. Publish and verify the index pointer.

Queue and scan counts will come from the live SQLite database. Segment counts
will come from verified immutable segment identities. Database and upload
progress will use bytes.

Only completed transactions and verified uploads increment progress. In-flight
inference calls do not count.

When the raw snapshot changes, xTap creates new queue and scan plans. This keeps
the previous plan intact and prevents apparent regression.

## OurModels integration

Instrument these phases:

1. Restore durable cache shards.
2. Load source posts.
3. Discover model groups.
4. Plan association tasks.
5. Commit associations.
6. Plan claim extraction tasks.
7. Commit extracted claims.
8. Plan verification tasks.
9. Commit verified claims.
10. Assemble snapshot units.
11. Build and verify SQLite output.
12. Upload database bytes.
13. Publish and verify the manifest.
14. Verify the production publication.

Association totals can be fixed from the frozen input projection. Claim totals
are fixed after association results identify eligible units. Verification totals
are fixed after extraction creates the claims to verify.

Cache counts will be rebuilt from the verified manifest during resume. The
monitor will no longer need to download every cache shard to determine current
progress.

## Finish-time calculation

Workers report facts only. They do not publish a guessed rate or finish time.

A monitoring system calculates a range from successive snapshots with the same
`plan_id`, total, and unit. It must:

- Wait for at least two positive progress samples.
- Ignore stale, regressing, or cross-plan samples.
- Use a recent robust rate window.
- Show confidence based on sample count and rate stability.
- Reset the estimate when the plan changes.
- Mark stalled work when the heartbeat is stale.
- Omit a finish time when the denominator or positive rate is unavailable.

An end-to-end finish time is available only when every remaining serial phase
has a supported duration estimate. Independent tracks remain separate and are
never added as if their units matched.

## Failure and resume behavior

After restart, the worker will:

1. Restore and verify its application checkpoint.
2. Restore and verify the latest progress snapshot.
3. Recompute committed counts from durable application state.
4. Reject progress ahead of committed output.
5. Advance progress when committed output is ahead of the last snapshot.
6. Start a new physical attempt under the same logical run and plans.

An interrupted progress upload leaves the previous pointer valid. A corrupt
snapshot, hash mismatch, invalid plan transition, or progress ahead of durable
work fails closed before new work begins.

## Verification

### HF Job Control

Add tests for:

- JSON Schema and model parity.
- Python and TypeScript fixture parity.
- Monotonic counters and sequence numbers.
- Valid plan changes and invalid regression.
- Concurrent updates and ordered writes.
- Throttling and explicit flushes.
- Atomic pointer replacement.
- Interrupted and repeated uploads.
- Stale pointer rejection.
- Cross-attempt continuation.
- Checkpoint reconciliation.
- Secret-like field rejection.
- Corrupt content and checksum rejection.
- Terminal-state behavior.

Run the repository's full required quality suite. Then run a remote CPU canary
that proves normal progress, forced interruption, resume, and completion.

### Applications

Each application will add unit tests for every phase and counter source. Each
will run its existing complete quality suite and a real pause-resume canary.
The canary must prove that:

- Progress matches committed output.
- A new attempt resumes without counter regression.
- No more than one reporting interval is missing after forced termination.
- A replacement monitor rebuilds a rate from the snapshot chain.
- Final progress and the final receipt agree.

## Delivery sequence

1. Add the format and implementation plan to HF Job Control.
2. Implement the shared schema, models, stores, and reporter.
3. Implement and verify Python and TypeScript parity.
4. Run the local suite and remote CPU canary.
5. Publish a new minor HF Job Control release.
6. Add xTap phase instrumentation and tests.
7. Add OurModels phase instrumentation and tests.
8. Let currently running Jobs finish under their unchanged contracts.
9. Deploy the instrumented application revisions.
10. Run one bounded canary for each application.
11. Verify progress and finish-time recovery across a physical restart.
12. Move each schedule to its validated instrumented revision.
13. Remove cap-based pseudo-progress and large-artifact inspection from normal monitoring.

## Current Job boundary

The running xTap restoration and OurModels publication Jobs must not be
modified or restarted to add telemetry. Their source and execution contracts
are already fixed.

Instrumentation will apply to later Jobs after current receipts and outputs
validate. This preserves the approved contracts and avoids losing useful work.

## Acceptance criteria

The work is complete when:

- HF Job Control has one documented progress schema and matching Python and
  TypeScript libraries.
- Both language implementations accept and reject the same fixtures.
- The remote canary survives interruption and resumes exact progress.
- xTap reports every listed phase from committed state.
- OurModels reports every listed phase from committed state.
- A monitor reads one small pointer and recent snapshots for each Job.
- Every fixed plan exposes exact completed and total values.
- Estimates reset correctly after a plan change.
- Stale progress is visible.
- Final progress agrees with receipts, manifests, and durable output hashes.
- No application imports Pi Workflows or writes a Pi-specific progress record.
- Current production Jobs complete without an instrumentation-driven restart.
