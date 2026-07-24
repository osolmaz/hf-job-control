from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from hf_job_control.models import (
    Action,
    AdapterSpec,
    AppliedControlReceipt,
    Boundary,
    CheckpointManifest,
    ControlDocument,
    LaunchSpec,
    ResumeMode,
    RunState,
    RunStatus,
)

ROOT = Path(__file__).parents[1]


def validate(schema_name: str, value: object) -> None:
    schema = json.loads((ROOT / "schemas" / schema_name).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(value)


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
