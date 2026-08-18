from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from hf_job_control.models import (
    Action,
    AdapterSpec,
    AppliedControlReceipt,
    ArtifactRef,
    Boundary,
    CheckpointManifest,
    ControlDocument,
    LaunchSpec,
    ResumeMode,
    RunState,
    RunStatus,
)
from hf_job_control.progress import (
    ProgressClaim,
    ProgressInput,
    ProgressPointer,
    ProgressSnapshot,
    ProgressStatus,
    ProgressTrack,
)

ROOT = Path(__file__).parents[1]
SCHEMAS = {
    path.name: json.loads(path.read_text(encoding="utf-8"))
    for path in (ROOT / "schemas").glob("*.json")
}
REGISTRY = Registry().with_resources(
    (schema["$id"], Resource.from_contents(schema)) for schema in SCHEMAS.values()
)


def validate(schema_name: str, value: object) -> None:
    schema = SCHEMAS[schema_name]
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
        registry=REGISTRY,
    ).validate(value)


def test_progress_schemas_match_models() -> None:
    now = datetime.now(UTC)
    snapshot = ProgressSnapshot(
        run_id="run",
        attempt_id="attempt-1",
        sequence=1,
        updated_at=now,
        input=ProgressInput(revision="a" * 40, contract_sha256="b" * 64),
        state=ProgressStatus.RUNNING,
        tracks=(
            ProgressTrack(
                key="items",
                plan_id="plan-1",
                status=ProgressStatus.RUNNING,
                completed=1,
                total=2,
                unit="items",
            ),
        ),
    )
    validate("progress-v1.schema.json", snapshot.to_dict())
    reference = ArtifactRef(
        bucket="owner/bucket",
        key=f"runs/run/snapshots/sha256-{'c' * 64}/progress.json",
        sha256="c" * 64,
        bytes=10,
    )
    validate(
        "progress-claim-v1.schema.json",
        ProgressClaim(
            run_id="run",
            attempt_id="attempt-1",
            sequence=1,
            created_at=now,
            snapshot=reference,
        ).to_dict(),
    )
    validate(
        "progress-pointer-v1.schema.json",
        ProgressPointer(
            run_id="run",
            sequence=1,
            updated_at=now,
            snapshot=reference,
        ).to_dict(),
    )


def test_control_schema_matches_model() -> None:
    validate(
        "control-v1.schema.json",
        ControlDocument(run_id="run", generation=1, action=Action.RUN).to_dict(),
    )


def test_receipt_schema_matches_model() -> None:
    now = datetime.now(UTC)
    receipt = AppliedControlReceipt(
        run_id="run",
        attempt_id="attempt-1",
        control_repo="owner/control",
        control_revision="a" * 40,
        control_path="controls/run.json",
        control_sha256="b" * 64,
        generation=1,
        action=Action.RUN,
        observed_at=now,
        applied_at=now,
        outcome="started",
        boundary=Boundary(name="start", sequence=0),
    )
    validate("applied-control-v1.schema.json", receipt.to_dict())


def test_status_schema_matches_model() -> None:
    status = RunStatus(
        run_id="run",
        attempt_id="attempt-1",
        state=RunState.RUNNING,
        updated_at=datetime.now(UTC),
        last_applied_generation=1,
        last_action=Action.RUN,
        metrics={"loss": 0.5},
        progress=ProgressSnapshot(
            run_id="run",
            attempt_id="attempt-1",
            sequence=1,
            updated_at=datetime.now(UTC),
            input=ProgressInput(revision="a" * 40, contract_sha256="b" * 64),
            state=ProgressStatus.RUNNING,
            tracks=(
                ProgressTrack(
                    key="items",
                    plan_id="plan-1",
                    status=ProgressStatus.RUNNING,
                ),
            ),
        ),
    )
    validate("run-status-v1.schema.json", status.to_dict())


def test_checkpoint_schema_matches_model() -> None:
    manifest = CheckpointManifest(
        run_id="run",
        attempt_id="attempt-1",
        adapter=AdapterSpec(name="test", version=1, resume_mode=ResumeMode.EXACT),
        boundary=Boundary(name="batch", sequence=1),
        payload_sha256="a" * 64,
        payload_bytes=10,
        created_at=datetime.now(UTC),
    )
    validate("checkpoint-manifest-v1.schema.json", manifest.to_dict())


def test_launch_schema_matches_model() -> None:
    spec = LaunchSpec(
        image="python:3.13",
        command=("python", "worker.py"),
        flavor="cpu-basic",
        timeout="10m",
    )
    validate("launch-spec-v1.schema.json", spec.to_dict())
