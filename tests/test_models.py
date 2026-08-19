from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from hf_job_control.models import (
    Action,
    AdapterSpec,
    ArtifactRef,
    Boundary,
    CheckpointClaim,
    CheckpointManifest,
    CheckpointPayloadRef,
    CheckpointPointer,
    CheckpointReceipt,
    CheckpointReceiptKind,
    ControlDocument,
    ResumeMode,
    RunState,
    RunStatus,
    format_datetime,
    parse_datetime,
    parse_json_object,
    stable_json_bytes,
)
from hf_job_control.progress import (
    ProgressInput,
    ProgressSnapshot,
    ProgressStatus,
    ProgressTrack,
)

DIGEST = "a" * 64


def artifact() -> ArtifactRef:
    return ArtifactRef(
        bucket="owner/bucket",
        key=f"run/checkpoints/sha256-{DIGEST}/checkpoint.hfjob",
        sha256=DIGEST,
        bytes=10,
    )


def test_control_round_trip_with_resume() -> None:
    original = ControlDocument(
        run_id="202607240525-nova-bandicoot",
        generation=3,
        action=Action.RUN,
        reason="Resume after maintenance",
        resume_from=artifact(),
    )

    restored = ControlDocument.from_dict(parse_json_object(stable_json_bytes(original.to_dict())))

    assert restored == original


@pytest.mark.parametrize("action", [Action.PAUSE, Action.STOP, Action.ABORT])
def test_resume_reference_requires_run(action: Action) -> None:
    with pytest.raises(ValueError, match="only valid with action run"):
        ControlDocument(
            run_id="valid-run",
            generation=2,
            action=action,
            resume_from=artifact(),
        )


@pytest.mark.parametrize(
    ("key", "message"),
    [
        ("/absolute", "relative POSIX"),
        ("parent/../escape", "unsafe path"),
        ("windows\\path", "POSIX"),
        ("missing/digest", "sha256-<digest>"),
    ],
)
def test_artifact_rejects_unsafe_keys(key: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ArtifactRef(bucket="owner/bucket", key=key, sha256=DIGEST, bytes=1)


def test_external_fields_are_strict() -> None:
    value = ControlDocument(run_id="run", generation=1, action=Action.RUN).to_dict()
    value["surprise"] = True

    with pytest.raises(ValueError, match=r"unknown=\['surprise'\]"):
        ControlDocument.from_dict(value)


def test_control_checks_expected_run_id() -> None:
    value = ControlDocument(run_id="run-a", generation=1, action=Action.RUN).to_dict()

    with pytest.raises(ValueError, match="run_id mismatch"):
        ControlDocument.from_dict(value, expected_run_id="run-b")


def test_stable_json_rejects_nonfinite_numbers() -> None:
    with pytest.raises(ValueError, match="JSON compliant"):
        stable_json_bytes({"loss": float("nan")})


def test_datetime_round_trip() -> None:
    value = datetime(2026, 7, 24, 5, 25, tzinfo=UTC)

    assert parse_datetime(format_datetime(value), "when") == value


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        format_datetime(datetime.now(UTC).replace(tzinfo=None))


def test_boundary_and_status_round_trip() -> None:
    boundary = Boundary(name="half-epoch", sequence=7, metadata={"exact": 0.81})
    status = RunStatus(
        run_id="run",
        attempt_id="attempt-1",
        state=RunState.RUNNING,
        updated_at=datetime.now(UTC),
        last_applied_generation=2,
        last_action=Action.RUN,
        job_id="job-123",
        boundary=boundary,
        checkpoint=artifact(),
        message="healthy",
    )

    assert RunStatus.from_dict(status.to_dict()) == status


def test_status_rejects_progress_for_another_attempt() -> None:
    progress = ProgressSnapshot(
        run_id="run",
        attempt_id="attempt-2",
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
    )
    with pytest.raises(ValueError, match="attempt_id must match"):
        RunStatus(
            run_id="run",
            attempt_id="attempt-1",
            state=RunState.RUNNING,
            updated_at=datetime.now(UTC),
            last_applied_generation=1,
            last_action=Action.RUN,
            progress=progress,
        )


def test_adapter_spec_rejects_unstable_name() -> None:
    with pytest.raises(ValueError, match="lowercase identifier"):
        AdapterSpec(name="Bad Adapter", version=1, resume_mode=ResumeMode.EXACT)


def checkpoint_payload() -> CheckpointPayloadRef:
    return CheckpointPayloadRef(path="state.bin", bytes=10, sha256=DIGEST)


def checkpoint_manifest() -> CheckpointManifest:
    return CheckpointManifest(
        run_id="run",
        attempt_id="attempt-1",
        adapter=AdapterSpec(name="state", version=1, resume_mode=ResumeMode.EXACT),
        plan_sha256=DIGEST,
        boundary=Boundary(name="batch", sequence=1),
        previous_checkpoint_sha256=None,
        payloads=(checkpoint_payload(),),
        created_at=datetime.now(UTC),
    )


def test_checkpoint_models_round_trip() -> None:
    manifest = checkpoint_manifest()
    assert CheckpointManifest.from_dict(manifest.to_dict()) == manifest
    claim = CheckpointClaim(
        run_id="run",
        attempt_id="attempt-1",
        sequence=1,
        plan_sha256=DIGEST,
        previous_checkpoint_sha256=None,
        checkpoint=artifact(),
        created_at=datetime.now(UTC),
    )
    assert CheckpointClaim.from_dict(claim.to_dict()) == claim
    pointer = CheckpointPointer(
        run_id="run",
        sequence=1,
        plan_sha256=DIGEST,
        checkpoint=artifact(),
        updated_at=datetime.now(UTC),
    )
    assert CheckpointPointer.from_dict(pointer.to_dict()) == pointer
    receipt = CheckpointReceipt(
        kind=CheckpointReceiptKind.RESTORE,
        run_id="run",
        attempt_id="attempt-2",
        job_id="job-2",
        plan_sha256=DIGEST,
        sequence=1,
        checkpoint=artifact(),
        adapter=manifest.adapter,
        created_at=datetime.now(UTC),
        evidence={"restored": True},
    )
    assert CheckpointReceipt.from_dict(receipt.to_dict()) == receipt


@pytest.mark.parametrize("path", ["", "/absolute", "a/../b", "a\\b"])
def test_checkpoint_payload_rejects_unsafe_path(path: str) -> None:
    with pytest.raises(ValueError, match="safe relative POSIX"):
        CheckpointPayloadRef(path=path, bytes=0, sha256=DIGEST)


def test_checkpoint_manifest_rejects_unsorted_or_duplicate_payloads() -> None:
    second = CheckpointPayloadRef(path="a.bin", bytes=0, sha256=DIGEST)
    with pytest.raises(ValueError, match="sorted"):
        replace(
            checkpoint_manifest(),
            payloads=(checkpoint_payload(), second),
        )
    with pytest.raises(ValueError, match="unique"):
        replace(
            checkpoint_manifest(),
            payloads=(checkpoint_payload(), checkpoint_payload()),
        )


def test_checkpoint_claim_rejects_invalid_predecessor() -> None:
    with pytest.raises(ValueError, match="previous_checkpoint_sha256"):
        CheckpointClaim(
            run_id="run",
            attempt_id="attempt-1",
            sequence=1,
            plan_sha256=DIGEST,
            previous_checkpoint_sha256="bad",
            checkpoint=artifact(),
            created_at=datetime.now(UTC),
        )
