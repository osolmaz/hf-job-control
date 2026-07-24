from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from hf_job_control.checkpoint import create_bundle, read_manifest, restore_bundle
from hf_job_control.models import (
    AdapterSpec,
    Boundary,
    CheckpointManifest,
    JsonObject,
    ResumeMode,
)


@dataclass
class TextAdapter:
    value: str
    name: str = "text"

    @property
    def spec(self) -> AdapterSpec:
        return AdapterSpec(name=self.name, version=1, resume_mode=ResumeMode.EXACT)

    def save(self, destination: Path, boundary: Boundary) -> JsonObject:
        destination.write_text(self.value, encoding="utf-8")
        return {"saved": self.value, "sequence": boundary.sequence}

    def restore(self, source: Path, manifest: CheckpointManifest) -> JsonObject:
        self.value = source.read_text(encoding="utf-8")
        return {"restored": self.value, "sequence": manifest.boundary.sequence}


def test_checkpoint_bundle_round_trip(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    source = TextAdapter("state-42")
    boundary = Boundary(name="batch", sequence=42)

    created = create_bundle(
        destination=bundle,
        run_id="run",
        attempt_id="attempt-1",
        boundary=boundary,
        adapter=source,
    )
    target = TextAdapter("")
    restored, evidence = restore_bundle(bundle=bundle, expected_run_id="run", adapter=target)

    assert read_manifest(bundle) == created
    assert restored == created
    assert target.value == "state-42"
    assert evidence == {"restored": "state-42", "sequence": 42}


def test_checkpoint_rejects_adapter_mismatch(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    create_bundle(
        destination=bundle,
        run_id="run",
        attempt_id="attempt-1",
        boundary=Boundary(name="batch", sequence=1),
        adapter=TextAdapter("state"),
    )

    with pytest.raises(ValueError, match="adapter mismatch"):
        restore_bundle(
            bundle=bundle,
            expected_run_id="run",
            adapter=TextAdapter("", name="other"),
        )


def test_checkpoint_rejects_payload_tampering(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    create_bundle(
        destination=bundle,
        run_id="run",
        attempt_id="attempt-1",
        boundary=Boundary(name="batch", sequence=1),
        adapter=TextAdapter("state"),
    )
    manifest = read_manifest(bundle)
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("manifest.json", json.dumps(manifest.to_dict()))
        archive.writestr("payload.bin", b"tampered")

    with pytest.raises(ValueError, match="byte count mismatch"):
        restore_bundle(bundle=bundle, expected_run_id="run", adapter=TextAdapter(""))


def test_checkpoint_rejects_extra_entries(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    with zipfile.ZipFile(bundle, "w") as archive:
        archive.writestr("manifest.json", "{}")
        archive.writestr("payload.bin", b"")
        archive.writestr("extra", b"")

    with pytest.raises(ValueError, match="entries mismatch"):
        read_manifest(bundle)
