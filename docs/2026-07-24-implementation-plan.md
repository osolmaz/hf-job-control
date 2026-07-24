---
title: "HF Job Control implementation plan"
author: "Onur Solmaz <2453968+osolmaz@users.noreply.github.com>"
date: "2026-07-24"
---

# HF Job Control implementation plan

## Purpose

HF Job Control will be a Python library and command-line tool for long-running
Hugging Face Jobs. A job will check for commands at boundaries where it can
safely save its work. An operator will be able to let the job continue, pause
it for later, stop it cleanly, or abort it after a failure.

Hugging Face Jobs currently supports inspection and logs alongside labels,
statistics, and cancellation. Cancellation does not give a training loop enough time to save an
exact checkpoint. A detached job also cannot have its command or environment
changed after launch. HF Job Control will provide cooperative control inside
the submitted program while leaving physical job scheduling to Hugging Face.

The first user will be a PyTorch training job that evaluates every half epoch.
The design must also work for target generation, evaluation, data processing,
and other batch jobs whose safe boundary is a completed unit of work.

## Design boundary

The library will own the control mechanism. Each job will continue to own its
scientific and application-specific behavior.

The shared code will handle:

- Exact reads from a versioned control store.
- Schema validation and monotonic command generations.
- Idempotent command application.
- Content-addressed checkpoint references.
- Applied-control receipts and observed status.
- Retries with validation and safe failure behavior.
- Logical run identity across physical job attempts.

A job adapter will define its safe boundary, checkpoint contents, restore
procedure, and finalization behavior. It will also decide which metrics to
publish. The library will never decide that a model has converged or choose a
checkpoint for scientific use.

## Initial architecture

The first implementation will use four parts.

### Worker library

The `hf_job_control` Python package will run inside the submitted job. The job
will call it only after saving and publishing work at a declared safe boundary.
The library will read the latest desired state, validate it, write a receipt,
and return a typed decision to the caller.

The library will not terminate the process directly. The job adapter will
finalize its own outputs and exit with the status required by the decision.
This keeps framework cleanup under the job's control.

### Operator CLI

The `hf-job-control` command will create logical runs, inspect their state, and
publish commands with optimistic concurrency. It will also verify checkpoints,
show physical attempts, and run the end-to-end canary.

New logical run IDs will come from `@osolmaz/petname`. The generated ID will be
stored once and reused for every physical attempt. Hugging Face's server job ID
will remain the identity of one execution attempt.

### Hugging Face storage

A private Hugging Face dataset will hold small desired-state documents. Dataset
commits provide history and an immutable revision for every command that a job
observes.

Large checkpoints will live in a private Hugging Face Bucket. Every checkpoint
key will contain its SHA-256 digest. Published keys will never be overwritten.
A project repository will hold observed status and receipts together with
metrics, manifests, and submitted script checksums.

### Metrics

W&B may receive live metrics and provide convergence graphs and alerts. It will
remain optional. The job must still be controllable and auditable when W&B is
unavailable, and W&B will not be the source of control state or resume state.

## Control model

Each logical run will have one current control document at
`controls/<run_id>.json`. The first schema will support these actions:

- `run` allows the next unit of work to start.
- `pause` saves state and ends the current physical job successfully.
- `stop` finalizes the logical run at the next safe boundary.
- `abort` records a failed run and exits with an error.

Every update will increment `generation`. The publisher will use the current
dataset revision as the parent commit, so concurrent writes cannot silently
replace each other. A worker may observe the same generation more than once and
must apply it only once. It may skip intermediate generations when it sees a
newer valid desired state.

A control document may include a `resume_from` reference. The reference will
name a Bucket, a relative content-addressed key, an exact byte count, and a
SHA-256 digest. Unknown fields, unsupported schema versions, mutable artifact
references, and mismatched run IDs will be rejected.

## Observed state

Desired state and observed state will stay separate. The control dataset says
what should happen. Project status says what the job actually did.

A logical run will move through observed states such as `created`, `running`,
`pausing`, `paused`, `stopping`, `completed`, `aborted`, or `failed`. Status will
also record the active physical attempt, previous attempts, the latest safe
boundary, and the newest usable checkpoint.

Every accepted command will produce an applied-control receipt containing:

- The control repository and path with the exact revision and file digest.
- The run ID and action with its generation.
- Observation and application times.
- The boundary and checkpoint used when applying it.
- The outcome and physical job ID.

A receipt will be written before a command changes the process lifecycle. This
order makes retries and postmortems unambiguous.

## Safe-boundary protocol

The worker integration will follow one fixed order.

1. Finish the current unit of work.
2. Calculate and publish the registered metrics.
3. Save all state needed by the job's declared resume mode.
4. Upload the checkpoint and verify its byte count and SHA-256.
5. Publish observed status containing the checkpoint reference.
6. Resolve the control dataset branch to an exact commit.
7. Read and validate the control file from that commit.
8. Write the applied-control receipt.
9. Return the action to the job adapter.

A temporary control-store failure will be retried with bounded backoff. If the
store remains unavailable, the worker will use the checkpoint it has already
published and end in a safe paused state. It will not continue an open-ended run
without control for an unlimited period.

A malformed current command will be recorded as rejected. The worker will
pause after publishing the rejection because silently continuing could ignore
an operator's attempted stop.

## Resume model

Resume will start a new physical Hugging Face Job under the same logical run ID.
Hugging Face does not resume a terminated job in place.

A paused run will expose its latest verified checkpoint in observed status. The
operator CLI will perform the following steps for `hf-job-control resume`:

1. Require the logical run to be paused.
2. Read the current status and checkpoint manifest from exact revisions.
3. Download or stream the checkpoint and verify its size and digest.
4. Confirm that its format and submitted-code identity match the registered
   launch specification.
5. Publish a new `run` generation containing `resume_from`.
6. Return the exact control revision to the operator.

The operator then uses `hf-job-control launch` with the immutable launch
specification. Publishing desired state and launching compute cannot be one
atomic Hub operation, so keeping these as separate commands makes a failed
launch visible and safely retryable.

At startup, the worker will verify `resume_from` before constructing mutable
training state. The job adapter will restore the checkpoint and return resume
evidence that records the restored boundary, global position, and state digest.
The worker will publish this evidence before allowing more work.

## Resume capabilities

Jobs will declare one of four resume modes.

`exact` restores all state needed to reproduce an uninterrupted execution from
the boundary. A PyTorch training adapter will include model parameters,
optimizer and scheduler state, gradient-scaling state when present, Python and
Torch RNG state, CUDA RNG state, sampler or shuffle state, data position,
global step, best-score state, and stopping counters.

`boundary` resumes from the last committed unit without promising identical
in-process state. Generation jobs may use a completed shard or row range as the
boundary. Evaluation jobs may use the last committed prediction batch.

`restart` starts the job again from its immutable inputs because checkpointing
would cost more than repeating the work. `unsupported` rejects pause and resume
commands before they can imply a guarantee the job cannot meet.

The generic package will define a `CheckpointAdapter` interface. It will not
try to serialize arbitrary application state by itself.

## Launch specification

Every logical run will have an immutable launch specification containing the
image, command, hardware flavor, explicit timeout, mounted storage, public
environment values, secret names, and submitted script digest. Secret values
will never enter the manifest.

Hugging Face Jobs has a 30-minute default timeout. The launcher will always set
an explicit operational timeout long enough for the run. That timeout is a
platform safety limit and will not serve as the scientific training horizon.
The status watcher will warn when an attempt approaches its deadline.

Each launch will include `run_id=<run_id>` as a Hugging Face label and set
`RUN_ID=<run_id>` in the environment. The worker will reject a launch where
these identities disagree with the control document.

## Planned Python API

The worker API stays small:

```python
controller.start(checkpoint_adapter)

decision = controller.boundary(
    boundary=boundary,
    adapter=checkpoint_adapter,
    metrics=metrics,
)
if decision.should_exit:
    finalize_outputs()
    controller.finish(decision)
```

`boundary` asks the adapter to save its payload, uploads the verified bundle,
publishes status, reads control, and writes the receipt. A caller cannot request
a lifecycle decision without first providing a safe boundary and checkpoint
adapter.

Storage will sit behind narrow `ControlStore`, `ArtifactStore`, and
`StatusStore` interfaces. The first implementations will use
`huggingface_hub`. A `MetricSink` interface will support W&B without making it a
required dependency.

## Planned CLI

The first CLI will cover the full operator workflow:

```text
hf-job-control create
hf-job-control show RUN_ID
hf-job-control watch RUN_ID
hf-job-control pause RUN_ID
hf-job-control stop RUN_ID
hf-job-control abort RUN_ID
hf-job-control resume RUN_ID
hf-job-control launch RUN_ID launch.json
hf-job-control verify RUN_ID
hf-job-control canary RUN_ID --package-ref PACKAGE
```

Mutating commands will show the current generation and require optimistic
concurrency. Commands intended for an interactive terminal will ask for
confirmation before stopping or aborting a run. Automation will need an
explicit non-interactive flag.

`watch` will combine desired state, observed project status, and physical HF Job
status. W&B links may appear when configured, but the CLI will remain useful
without W&B.

## Package and repository layout

The project will require Python 3.11 or newer. It will use `uv`, typed public
interfaces, Ruff, mypy, Pytest, and Slophammer's Python checks. Releases will
publish a provenance-backed wheel from a GitHub Release.

The proposed source layout is:

```text
src/hf_job_control/
  cli.py
  controller.py
  models.py
  stores.py
  checkpoint.py
  metrics.py
  launch.py
  canary.py
tests/
  unit/
  integration/
  remote/
```

Submitted jobs will pin an exact package version and wheel SHA-256. A mutable
Git branch will never be a runtime dependency.

## Test strategy

Unit tests will cover schema validation, generation ordering, idempotency,
concurrent publishing, action handling, and checkpoint verification. Store
interfaces will have contract tests that run against an in-memory fake and a
temporary Hub repository.

A deterministic training fixture will compare uninterrupted execution with a
pause and resume. The restored run must produce the same model state, optimizer
state, scheduler state, data order, RNG outputs, and subsequent losses.

A remote CPU canary will use a private test control path and short boundaries.
It will exercise `run`, `pause`, resumed `run`, `stop`, and `abort`. The canary
will verify receipts, terminal states, checkpoint hashes, attempt identity, and
that a command for another run ID has no effect.

Fault tests will cover unavailable control storage, malformed commands, stale
and repeated generations, interrupted uploads, launch failure after resume is
published, and W&B failure. No H200 training job will adopt the package until
the CPU canary and deterministic resume test pass.

## Delivery stages

The first stage will establish the Python package, schemas, store interfaces,
and read-only inspection commands. It will migrate the existing publisher only
after compatibility tests pass.

The second stage will add worker polling, receipts, checkpoint manifests, and
the canary adapter. This stage ends when all control transitions pass locally
and on a remote CPU Job.

The third stage will implement exact PyTorch resume and integrate one training
job. The integration will repeat its existing construction and representation
checks plus the small-fit checks before remote training.

Later work will add boundary adapters for generation and evaluation. Data
processing will receive its own boundary adapter as well. Those adapters will share the control protocol while keeping their
checkpoint formats independent.

## Acceptance criteria

The first production release will be ready when:

- Jobs contain no copied Hub polling or receipt code.
- All actions pass the remote CPU canary.
- Exact PyTorch pause and resume matches uninterrupted training.
- Every applied command has an immutable receipt.
- Every resume payload passes exact size and SHA-256 checks with compatible formats.
- Stale and repeated generations behave safely, as do malformed or cross-run commands.
- W&B failure leaves control and status working while resume remains available.
- Launch specifications always set an explicit HF Job timeout.
- No command changes an unrelated Hugging Face Job.
- Documentation gives one complete create, pause, resume and stop example.

## Exclusions

The first release will not schedule arbitrary workflows or replace a cluster
orchestrator. It will not provide a universal serializer for application state,
choose scientific stopping rules, manage multi-node distributed training, or
host a long-running control service.

Those boundaries keep the package small enough to audit. If usage grows into
many dependent workflows running every day, the project can add an orchestration
adapter or move execution to a platform that already owns the full job
lifecycle.
