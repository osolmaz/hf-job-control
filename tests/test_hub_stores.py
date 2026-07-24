from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace
from typing import BinaryIO, cast

import pytest
from huggingface_hub import HfApi
from huggingface_hub.errors import EntryNotFoundError

import hf_job_control.stores as stores_module
from hf_job_control.models import (
    Action,
    AppliedControlReceipt,
    Boundary,
    ControlDocument,
    LaunchSpec,
    RunState,
    RunStatus,
    stable_json_bytes,
    utc_now,
)
from hf_job_control.stores import HubBucketArtifactStore, HubControlStore, HubStatusStore


class FakeApi:
    def __init__(self) -> None:
        self.head = "1" * 40
        self.created: list[dict[str, object]] = []
        self.existing = False

    def repo_info(self, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(sha=self.head)

    def create_commit(self, **kwargs: object) -> SimpleNamespace:
        self.created.append(kwargs)
        self.head = f"{len(self.created) + 1:040x}"
        return SimpleNamespace(oid=self.head)

    def file_exists(self, **_kwargs: object) -> bool:
        return self.existing


class MemoryWriter(io.BytesIO):
    def __init__(self, values: dict[str, bytes], path: str) -> None:
        super().__init__()
        self.values = values
        self.path = path

    def close(self) -> None:
        if not self.closed:
            self.values[self.path] = self.getvalue()
        super().close()


class FakeFileSystem:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.values

    def open(self, path: str, mode: str) -> BinaryIO:
        if mode == "rb":
            return io.BytesIO(self.values[path])
        return MemoryWriter(self.values, path)


def test_hub_control_fetch_and_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeApi()
    current = ControlDocument(run_id="run", generation=1, action=Action.RUN)
    local = tmp_path / "control.json"
    local.write_bytes(stable_json_bytes(current.to_dict()))
    monkeypatch.setattr(stores_module, "hf_hub_download", lambda **_kwargs: str(local))
    store = HubControlStore("owner/control", api=cast(HfApi, api))

    assert store.fetch("run").control == current
    next_control = ControlDocument(run_id="run", generation=2, action=Action.PAUSE)
    snapshot = store.publish(next_control, expected_generation=1)

    assert snapshot.control == next_control
    assert api.created[0]["parent_commit"] == "1" * 40


def test_hub_control_registers_immutable_launch_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeApi()
    store = HubControlStore("owner/control", api=cast(HfApi, api))
    spec = LaunchSpec(
        image="python:3.13",
        command=("python", "worker.py"),
        flavor="cpu-basic",
        timeout="10m",
    )

    published = store.register_launch_spec("run", spec)

    assert published.path == "launch-specs/run.json"
    api.existing = True
    different = tmp_path / "launch.json"
    different.write_bytes(
        stable_json_bytes(
            LaunchSpec(
                image="python:3.13",
                command=("python", "worker.py"),
                flavor="cpu-basic",
                timeout="20m",
            ).to_dict()
        )
    )
    monkeypatch.setattr(stores_module, "hf_hub_download", lambda **_kwargs: str(different))
    with pytest.raises(RuntimeError, match="immutable launch specification differs"):
        store.register_launch_spec("run", spec)


def test_hub_control_create_and_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeApi()

    def missing(**_kwargs: object) -> str:
        raise EntryNotFoundError("missing")

    monkeypatch.setattr(stores_module, "hf_hub_download", missing)
    store = HubControlStore("owner/control", api=cast(HfApi, api))

    with pytest.raises(ValueError, match="missing control"):
        store.fetch("run")
    snapshot = store.publish(
        ControlDocument(run_id="run", generation=1, action=Action.RUN),
        expected_generation=0,
    )
    assert snapshot.control.generation == 1


def test_hub_status_store_reads_and_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = FakeApi()
    status = RunStatus(
        run_id="run",
        attempt_id="attempt-1",
        state=RunState.RUNNING,
        updated_at=utc_now(),
        last_applied_generation=1,
        last_action=Action.RUN,
    )
    local = tmp_path / "status.json"
    local.write_bytes(stable_json_bytes(status.to_dict()))
    monkeypatch.setattr(stores_module, "hf_hub_download", lambda **_kwargs: str(local))
    store = HubStatusStore("owner/status", api=cast(HfApi, api))

    assert store.fetch_status("run") == status
    published = store.publish_status(status)
    assert published.path == "runs/run/status.json"

    receipt = AppliedControlReceipt(
        run_id="run",
        attempt_id="attempt-1",
        control_repo="owner/control",
        control_revision="2" * 40,
        control_path="controls/run.json",
        control_sha256="a" * 64,
        generation=1,
        action=Action.RUN,
        observed_at=utc_now(),
        applied_at=utc_now(),
        outcome="started",
        boundary=Boundary(name="start", sequence=0),
    )
    receipt_doc = store.publish_receipt(receipt)
    assert receipt_doc.path.endswith("generation-00000001.json")


def test_hub_status_store_missing_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    api = FakeApi()

    def missing(**_kwargs: object) -> str:
        raise EntryNotFoundError("missing")

    monkeypatch.setattr(stores_module, "hf_hub_download", missing)
    store = HubStatusStore("owner/status", api=cast(HfApi, api))
    assert store.fetch_status("run") is None


def test_hub_bucket_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fs = FakeFileSystem()
    monkeypatch.setattr(stores_module, "HfFileSystem", lambda **_kwargs: fs)
    store = HubBucketArtifactStore("owner/bucket")
    source = tmp_path / "checkpoint"
    source.write_bytes(b"checkpoint")

    reference = store.put_checkpoint("run", source)
    destination = tmp_path / "download"
    store.get_checkpoint(reference, destination)

    assert destination.read_bytes() == b"checkpoint"
    assert store.put_checkpoint("run", source) == reference
