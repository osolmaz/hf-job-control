from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hf_job_control.checkpoint import create_bundle, read_manifest, restore_bundle
from hf_job_control.models import AdapterSpec, Boundary, CheckpointManifest, JsonObject, ResumeMode

PLAN_SHA256 = "a" * 64
CREATED_AT = datetime(2026, 8, 19, 12, tzinfo=UTC)


@dataclass
class TextAdapter:
    value: str
    name: str = "text"

    @property
    def spec(self) -> AdapterSpec:
        return AdapterSpec(name=self.name, version=1, resume_mode=ResumeMode.EXACT)

    def save(self, destination: Path, boundary: Boundary) -> None:
        del boundary
        (destination / "nested").mkdir()
        (destination / "nested/state.txt").write_text(self.value, encoding="utf-8")
        (destination / "empty.bin").write_bytes(b"")

    def restore(self, source: Path, manifest: CheckpointManifest) -> JsonObject:
        self.value = (source / "nested/state.txt").read_text(encoding="utf-8")
        return {"restored": self.value, "sequence": manifest.boundary.sequence}


def create_text_bundle(
    path: Path, adapter: TextAdapter, *, sequence: int = 1
) -> CheckpointManifest:
    return create_bundle(
        destination=path,
        run_id="run",
        attempt_id="attempt-1",
        plan_sha256=PLAN_SHA256,
        boundary=Boundary(name="batch", sequence=sequence, reached_at=CREATED_AT),
        previous_checkpoint_sha256=None,
        adapter=adapter,
        created_at=CREATED_AT,
    )


def test_checkpoint_bundle_round_trip(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    source = TextAdapter("state-42")
    created = create_text_bundle(bundle, source, sequence=42)
    target = TextAdapter("")
    restored, evidence = restore_bundle(
        bundle=bundle,
        expected_run_id="run",
        expected_plan_sha256=PLAN_SHA256,
        adapter=target,
    )

    assert read_manifest(bundle) == created
    assert restored == created
    assert [payload.path for payload in created.payloads] == ["empty.bin", "nested/state.txt"]
    assert target.value == "state-42"
    assert evidence == {"restored": "state-42", "sequence": 42}


def test_checkpoint_bundle_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first.hfjob"
    second = tmp_path / "second.hfjob"
    create_text_bundle(first, TextAdapter("state"))
    create_text_bundle(second, TextAdapter("state"))
    assert first.read_bytes() == second.read_bytes()


def test_checkpoint_rejects_adapter_and_plan_mismatch(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    create_text_bundle(bundle, TextAdapter("state"))

    with pytest.raises(ValueError, match="adapter mismatch"):
        restore_bundle(
            bundle=bundle,
            expected_run_id="run",
            expected_plan_sha256=PLAN_SHA256,
            adapter=TextAdapter("", name="other"),
        )
    with pytest.raises(ValueError, match="plan_sha256 mismatch"):
        restore_bundle(
            bundle=bundle,
            expected_run_id="run",
            expected_plan_sha256="b" * 64,
            adapter=TextAdapter(""),
        )


def test_checkpoint_rejects_payload_tampering(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    create_text_bundle(bundle, TextAdapter("state"))
    tampered = bytearray(bundle.read_bytes())
    tampered[-1] ^= 1
    bundle.write_bytes(tampered)

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        restore_bundle(
            bundle=bundle,
            expected_run_id="run",
            expected_plan_sha256=PLAN_SHA256,
            adapter=TextAdapter(""),
        )


def test_checkpoint_rejects_trailing_bytes(tmp_path: Path) -> None:
    bundle = tmp_path / "checkpoint.hfjob"
    create_text_bundle(bundle, TextAdapter("state"))
    bundle.write_bytes(bundle.read_bytes() + b"trailing")

    with pytest.raises(ValueError, match="trailing bytes"):
        read_manifest(bundle)
