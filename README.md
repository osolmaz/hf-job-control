# HF Job Control

HF Job Control is a Python library and CLI for cooperative control of detached
Hugging Face Jobs. A running job saves its work at a safe boundary before it
responds to `run`, `pause`, `stop`, or `abort`.

The control history stays in a versioned Hugging Face dataset. Checkpoints use
content-addressed keys in a Hugging Face Bucket, while a separate dataset stores
observed status and immutable receipts.

## Installation

HF Job Control requires Python 3.11 or newer. Install a released tag with `uv`:

```bash
uv tool install "hf-job-control @ git+https://github.com/osolmaz/hf-job-control@<tag>"
```

A submitted job should pin the same tag and verify the built wheel's SHA-256.

## Operator workflow

Set the private Hub resources used by the run:

```bash
export HF_JOB_CONTROL_REPO=owner/jobs-control
export HF_JOB_STATUS_REPO=owner/job-status
export HF_JOB_ARTIFACT_BUCKET=owner/job-artifacts
```

Create a logical run. The default ID comes from `@osolmaz/petname`:

```bash
RUN_ID="$(hf-job-control create --reason "Start training" | jq -r '.control.run_id')"
```

Launch one physical attempt from a checked-in launch specification:

```bash
hf-job-control launch "$RUN_ID" launch.json
```

A pause takes effect after the next safe checkpoint:

```bash
hf-job-control pause "$RUN_ID" --reason "Release the worker"
hf-job-control watch "$RUN_ID"
```

Resume publishes `run` with the verified paused checkpoint. Launching again
creates a new physical job under the same logical run ID:

```bash
hf-job-control resume "$RUN_ID" --reason "Continue from the checkpoint"
hf-job-control launch "$RUN_ID" launch.json
```

Stop completes the logical run at its next safe boundary:

```bash
hf-job-control stop "$RUN_ID" --reason "The registered metric has converged"
hf-job-control watch "$RUN_ID"
```

Pass `--expected-generation` to a mutating command when another operator or
process could write concurrently.

## Remote canary

The built-in canary runs a small counter on `cpu-basic`. It exercises the same
Hub reads, checkpoint uploads, status writes, and receipts as a real worker:

```bash
hf-job-control canary "$RUN_ID" \
  --status-repo "$HF_JOB_STATUS_REPO" \
  --artifact-bucket "$HF_JOB_ARTIFACT_BUCKET" \
  --package-ref "hf-job-control @ git+https://github.com/osolmaz/hf-job-control@<tag>"
```

Use the normal `pause`, `resume`, `launch`, and `stop` commands against the
canary run.

## Worker integration

A worker supplies a checkpoint adapter and calls the controller at each safe
boundary:

```python
from hf_job_control import (
    Boundary,
    Controller,
    ControllerConfig,
    HubBucketArtifactStore,
    HubControlStore,
    HubStatusStore,
)

controller = Controller(
    ControllerConfig.from_environment(),
    control_store=HubControlStore("owner/jobs-control"),
    status_store=HubStatusStore("owner/job-status"),
    artifact_store=HubBucketArtifactStore("owner/job-artifacts"),
)
controller.start(checkpoint_adapter)

for step in training_steps:
    train(step)
    if is_safe_boundary(step):
        decision = controller.boundary(
            boundary=Boundary(name="half-epoch", sequence=step),
            adapter=checkpoint_adapter,
            metrics={"exact": evaluate()},
        )
        if decision.should_exit:
            finalize_outputs()
            controller.finish(decision)
            raise SystemExit(decision.exit_code)
```

The adapter writes and restores the application's checkpoint payload. The
controller verifies the bundle and Hub revisions, tracks command generations,
and writes both receipts and observed state.

## Resume guarantees

Adapters declare one resume mode. `exact` restores every state item needed to
match uninterrupted execution. `boundary` restarts from the last committed
unit. `restart` repeats the job from immutable inputs, and `unsupported`
rejects resume requests.

For PyTorch training, an exact adapter normally includes model parameters,
optimizer and scheduler state, mixed-precision state, random-number generator
state, data order, global step, and model-selection counters.

## Monitoring

`hf-job-control watch` reads durable project status. W&B can receive the same
metrics through `WandbMetricSink`, but W&B is optional and never controls
checkpoint or resume state.

## Safety

Hugging Face Jobs defaults to a 30-minute timeout. Every launch specification
must set an explicit timeout long enough for the workload.

The controller writes a checkpoint before reading control. It writes an
applied-control receipt before changing lifecycle state. If control remains
unavailable after retries, the worker pauses instead of continuing forever.

See the [protocol](docs/2026-07-24-protocol.md) for file layouts and action semantics. The
[implementation plan](docs/2026-07-24-implementation-plan.md) records the design and test
requirements.

## License

[MIT](LICENSE)
