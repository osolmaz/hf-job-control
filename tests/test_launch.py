from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from huggingface_hub import HfApi

from hf_job_control.launch import HubJobLauncher
from hf_job_control.models import Action, ControlDocument, LaunchSpec
from hf_job_control.stores import MemoryControlStore


class FakeApi:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def run_job(self, **kwargs: object) -> SimpleNamespace:
        self.kwargs = kwargs
        return SimpleNamespace(id="job-123", url="https://huggingface.co/jobs/job-123")


def spec(*, secret_names: tuple[str, ...] = ()) -> LaunchSpec:
    return LaunchSpec(
        image="python:3.13",
        command=("python", "worker.py"),
        flavor="cpu-basic",
        timeout="10m",
        environment={"MODE": "canary"},
        secret_names=secret_names,
        labels={"kind": "test"},
    )


def run_store(action: Action = Action.RUN) -> MemoryControlStore:
    store = MemoryControlStore()
    store.publish(
        ControlDocument(run_id="run", generation=1, action=action),
        expected_generation=0,
    )
    return store


def test_launch_adds_identity_and_resolves_secrets() -> None:
    api = FakeApi()
    launcher = HubJobLauncher(run_store(), api=cast(HfApi, api))

    job = launcher.launch(
        "run",
        spec(secret_names=("HF_TOKEN",)),
        attempt_id="attempt-1",
        secret_values={"HF_TOKEN": "secret"},
    )

    assert job.job_id == "job-123"
    assert api.kwargs is not None
    assert api.kwargs["env"] == {
        "ATTEMPT_ID": "attempt-1",
        "MODE": "canary",
        "RUN_ID": "run",
    }
    assert api.kwargs["labels"] == {
        "attempt_id": "attempt-1",
        "kind": "test",
        "run_id": "run",
    }
    assert api.kwargs["secrets"] == {"HF_TOKEN": "secret"}
    assert api.kwargs["timeout"] == "10m"


def test_launch_rejects_non_run_control() -> None:
    launcher = HubJobLauncher(run_store(Action.PAUSE), api=cast(HfApi, FakeApi()))

    with pytest.raises(ValueError, match="desired action is pause"):
        launcher.launch("run", spec(), attempt_id="attempt-1")


def test_launch_requires_secret_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_SECRET", raising=False)
    launcher = HubJobLauncher(run_store(), api=cast(HfApi, FakeApi()))

    with pytest.raises(ValueError, match="missing secret value"):
        launcher.launch(
            "run",
            spec(secret_names=("MISSING_SECRET",)),
            attempt_id="attempt-1",
        )


def test_launch_spec_round_trip() -> None:
    original = spec(secret_names=("HF_TOKEN",))

    assert LaunchSpec.from_dict(original.to_dict()) == original


def test_launch_spec_reserves_identity_environment() -> None:
    with pytest.raises(ValueError, match="assigned by the launcher"):
        LaunchSpec(
            image="python",
            command=("true",),
            flavor="cpu-basic",
            timeout="10m",
            environment={"RUN_ID": "bad"},
        )
