from __future__ import annotations

from pathlib import Path

import pytest

import hf_job_control.canary as canary
from hf_job_control.models import Action, ControlDocument, ControlSnapshot, RunState
from hf_job_control.stores import LocalArtifactStore, MemoryControlStore, MemoryStatusStore


class AutoStopControlStore(MemoryControlStore):
    def __init__(self) -> None:
        super().__init__()
        self.publish(
            ControlDocument(run_id="run", generation=1, action=Action.RUN),
            expected_generation=0,
        )
        self.fetches = 0

    def fetch(self, run_id: str) -> ControlSnapshot:
        self.fetches += 1
        if self.fetches == 2:
            self.publish(
                ControlDocument(run_id="run", generation=2, action=Action.STOP),
                expected_generation=1,
            )
        return super().fetch(run_id)


def test_live_worker_shape_stops_at_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = AutoStopControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    monkeypatch.setenv("RUN_ID", "run")
    monkeypatch.setenv("ATTEMPT_ID", "attempt-1")
    monkeypatch.setenv("PLAN_SHA256", "a" * 64)
    monkeypatch.setattr(canary, "HubControlStore", lambda _repo: controls)
    monkeypatch.setattr(
        canary,
        "HubStatusStore",
        lambda _repo, prefix="canary-runs": statuses,
    )
    monkeypatch.setattr(canary, "HubBucketArtifactStore", lambda _bucket: artifacts)

    result = canary.run_worker(
        control_repo="owner/control",
        status_repo="owner/status",
        artifact_bucket="owner/bucket",
        status_prefix="canary-runs",
        interval_seconds=0,
        max_boundaries=3,
    )

    assert result == 0
    status = statuses.fetch_status("run")
    assert status is not None
    assert status.state is RunState.COMPLETED
    assert status.boundary is not None
    assert status.boundary.sequence == 1


def test_canary_safety_limit_fails_without_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = MemoryControlStore()
    controls.publish(
        ControlDocument(run_id="run", generation=1, action=Action.RUN),
        expected_generation=0,
    )
    monkeypatch.setenv("RUN_ID", "run")
    monkeypatch.setenv("ATTEMPT_ID", "attempt-1")
    monkeypatch.setenv("PLAN_SHA256", "a" * 64)
    monkeypatch.setattr(canary, "HubControlStore", lambda _repo: controls)
    monkeypatch.setattr(
        canary,
        "HubStatusStore",
        lambda _repo, prefix="canary-runs": MemoryStatusStore(),
    )
    monkeypatch.setattr(
        canary,
        "HubBucketArtifactStore",
        lambda _bucket: LocalArtifactStore(tmp_path),
    )

    with pytest.raises(RuntimeError, match="safety boundary limit"):
        canary.run_worker(
            control_repo="owner/control",
            status_repo="owner/status",
            artifact_bucket="owner/bucket",
            status_prefix="canary-runs",
            interval_seconds=0,
            max_boundaries=1,
        )


def test_canary_main_requires_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUN_ID", raising=False)
    monkeypatch.delenv("ATTEMPT_ID", raising=False)
    monkeypatch.delenv("PLAN_SHA256", raising=False)
    monkeypatch.setattr(canary, "parse_args", lambda: object())

    with pytest.raises(ValueError, match="RUN_ID, ATTEMPT_ID, and PLAN_SHA256"):
        canary.main()
