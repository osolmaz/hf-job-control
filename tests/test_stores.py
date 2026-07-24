from __future__ import annotations

from pathlib import Path

import pytest

from hf_job_control.models import Action, ControlDocument
from hf_job_control.stores import LocalArtifactStore, MemoryControlStore


def test_memory_control_store_enforces_generation() -> None:
    store = MemoryControlStore()
    first = ControlDocument(run_id="run", generation=1, action=Action.RUN)
    snapshot = store.publish(first, expected_generation=0)

    assert snapshot.control == first
    with pytest.raises(RuntimeError, match="expected generation"):
        store.publish(
            ControlDocument(run_id="run", generation=2, action=Action.PAUSE),
            expected_generation=0,
        )
    with pytest.raises(ValueError, match="advance by one"):
        store.publish(
            ControlDocument(run_id="run", generation=3, action=Action.PAUSE),
            expected_generation=1,
        )


def test_local_artifact_store_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"checkpoint")
    store = LocalArtifactStore(tmp_path / "bucket")

    reference = store.put_checkpoint("run", source)
    destination = tmp_path / "downloaded"
    store.get_checkpoint(reference, destination)

    assert destination.read_bytes() == b"checkpoint"
    assert store.put_checkpoint("run", source) == reference


def test_local_artifact_store_detects_corruption(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"checkpoint")
    store = LocalArtifactStore(tmp_path / "bucket")
    reference = store.put_checkpoint("run", source)
    (store.root / reference.key).write_bytes(b"corrupt")

    with pytest.raises(ValueError, match="byte count mismatch"):
        store.get_checkpoint(reference, tmp_path / "downloaded")
