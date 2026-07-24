"""Checkpoint adapter and streaming bundle format."""

from __future__ import annotations

import hashlib
import tempfile
import zipfile
from pathlib import Path
from typing import IO, Protocol

from hf_job_control.models import (
    AdapterSpec,
    Boundary,
    CheckpointManifest,
    JsonObject,
    parse_json_object,
    stable_json_bytes,
    utc_now,
)

MANIFEST_NAME = "manifest.json"
PAYLOAD_NAME = "payload.bin"
CHUNK_SIZE = 1024 * 1024


class CheckpointAdapter(Protocol):
    """Job-specific checkpoint serialization and restoration."""

    @property
    def spec(self) -> AdapterSpec:
        """Return the stable adapter identity and resume guarantee."""

    def save(self, destination: Path, boundary: Boundary) -> JsonObject:
        """Write one checkpoint payload and return small manifest metadata."""

    def restore(self, source: Path, manifest: CheckpointManifest) -> JsonObject:
        """Restore one verified payload and return resume evidence."""


def create_bundle(
    *,
    destination: Path,
    run_id: str,
    attempt_id: str,
    boundary: Boundary,
    adapter: CheckpointAdapter,
) -> CheckpointManifest:
    """Create a self-describing checkpoint bundle without loading its payload."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hf-job-control-save-") as temp_dir:
        payload_path = Path(temp_dir) / PAYLOAD_NAME
        metadata = adapter.save(payload_path, boundary)
        payload_sha256, payload_bytes = _hash_file(payload_path)
        manifest = CheckpointManifest(
            run_id=run_id,
            attempt_id=attempt_id,
            adapter=adapter.spec,
            boundary=boundary,
            payload_sha256=payload_sha256,
            payload_bytes=payload_bytes,
            created_at=utc_now(),
            metadata=metadata,
        )
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr(MANIFEST_NAME, stable_json_bytes(manifest.to_dict()))
            archive.write(payload_path, PAYLOAD_NAME)
    return manifest


def read_manifest(bundle: Path) -> CheckpointManifest:
    """Read and validate a checkpoint bundle manifest."""

    with zipfile.ZipFile(bundle) as archive:
        _validate_entries(archive)
        return CheckpointManifest.from_dict(parse_json_object(archive.read(MANIFEST_NAME)))


def restore_bundle(
    *,
    bundle: Path,
    expected_run_id: str,
    adapter: CheckpointAdapter,
) -> tuple[CheckpointManifest, JsonObject]:
    """Verify and restore a checkpoint bundle through its matching adapter."""

    with tempfile.TemporaryDirectory(prefix="hf-job-control-restore-") as temp_dir:
        payload_path = Path(temp_dir) / PAYLOAD_NAME
        with zipfile.ZipFile(bundle) as archive:
            _validate_entries(archive)
            manifest = CheckpointManifest.from_dict(parse_json_object(archive.read(MANIFEST_NAME)))
            _validate_manifest_identity(manifest, expected_run_id, adapter)
            with archive.open(PAYLOAD_NAME) as source, payload_path.open("wb") as destination:
                payload_sha256, payload_bytes = _copy_and_hash(source, destination)
        if payload_bytes != manifest.payload_bytes:
            raise ValueError("checkpoint payload byte count mismatch")
        if payload_sha256 != manifest.payload_sha256:
            raise ValueError("checkpoint payload SHA-256 mismatch")
        evidence = adapter.restore(payload_path, manifest)
    return manifest, evidence


def _validate_manifest_identity(
    manifest: CheckpointManifest,
    expected_run_id: str,
    adapter: CheckpointAdapter,
) -> None:
    if manifest.run_id != expected_run_id:
        raise ValueError(f"checkpoint run_id mismatch: {manifest.run_id!r} != {expected_run_id!r}")
    if manifest.adapter != adapter.spec:
        raise ValueError(f"checkpoint adapter mismatch: {manifest.adapter!r} != {adapter.spec!r}")


def _validate_entries(archive: zipfile.ZipFile) -> None:
    names = set(archive.namelist())
    expected = {MANIFEST_NAME, PAYLOAD_NAME}
    if names != expected:
        raise ValueError(f"checkpoint bundle entries mismatch: {sorted(names)}")


def _hash_file(path: Path) -> tuple[str, int]:
    with path.open("rb") as source:
        return _hash_stream(source)


def _hash_stream(source: IO[bytes]) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _copy_and_hash(source: IO[bytes], destination: IO[bytes]) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(CHUNK_SIZE):
        destination.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size
