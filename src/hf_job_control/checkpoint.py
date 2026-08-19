"""Checkpoint adapter and deterministic bundle format."""

from __future__ import annotations

import hashlib
import tempfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import IO, Protocol

from hf_job_control.models import (
    AdapterSpec,
    Boundary,
    CheckpointManifest,
    CheckpointPayloadRef,
    JsonObject,
    parse_json_object,
    stable_json_bytes,
    utc_now,
)

BUNDLE_MAGIC = b"HFJOB1\n"
MANIFEST_LENGTH_BYTES = 8
CHUNK_SIZE = 1024 * 1024


class CheckpointAdapter(Protocol):
    """Job-specific checkpoint serialization and restoration."""

    @property
    def spec(self) -> AdapterSpec:
        """Return the stable adapter identity and resume guarantee."""

    def save(self, destination: Path, boundary: Boundary) -> None:
        """Write zero or more payload files below the destination directory."""

    def restore(self, source: Path, manifest: CheckpointManifest) -> JsonObject:
        """Restore verified payload files and return resume evidence."""


def create_bundle(
    *,
    destination: Path,
    run_id: str,
    attempt_id: str,
    plan_sha256: str,
    boundary: Boundary,
    previous_checkpoint_sha256: str | None,
    adapter: CheckpointAdapter,
    created_at: datetime | None = None,
) -> CheckpointManifest:
    """Create a deterministic checkpoint bundle from adapter payload files."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hf-job-control-save-") as temp_dir:
        payload_root = Path(temp_dir) / "payloads"
        payload_root.mkdir()
        adapter.save(payload_root, boundary)
        payloads = tuple(_payload_refs(payload_root))
        manifest = CheckpointManifest(
            run_id=run_id,
            attempt_id=attempt_id,
            adapter=adapter.spec,
            plan_sha256=plan_sha256,
            boundary=boundary,
            previous_checkpoint_sha256=previous_checkpoint_sha256,
            payloads=payloads,
            created_at=utc_now() if created_at is None else created_at,
        )
        manifest_bytes = stable_json_bytes(manifest.to_dict())
        with destination.open("wb") as bundle:
            bundle.write(BUNDLE_MAGIC)
            bundle.write(len(manifest_bytes).to_bytes(MANIFEST_LENGTH_BYTES, "big"))
            bundle.write(manifest_bytes)
            for payload in payloads:
                with (payload_root / payload.path).open("rb") as source:
                    _copy_exact(source, bundle, payload.bytes)
    return manifest


def read_manifest(bundle: Path) -> CheckpointManifest:
    """Read and validate a checkpoint bundle manifest."""

    with bundle.open("rb") as source:
        manifest = _read_manifest(source)
        _verify_payload_stream(source, manifest, None)
    return manifest


def restore_bundle(
    *,
    bundle: Path,
    expected_run_id: str,
    expected_plan_sha256: str,
    adapter: CheckpointAdapter,
) -> tuple[CheckpointManifest, JsonObject]:
    """Verify and restore a checkpoint bundle through its matching adapter."""

    with tempfile.TemporaryDirectory(prefix="hf-job-control-restore-") as temp_dir:
        payload_root = Path(temp_dir) / "payloads"
        payload_root.mkdir()
        with bundle.open("rb") as source:
            manifest = _read_manifest(source)
            _validate_manifest_identity(
                manifest,
                expected_run_id=expected_run_id,
                expected_plan_sha256=expected_plan_sha256,
                adapter=adapter,
            )
            _verify_payload_stream(source, manifest, payload_root)
        evidence = adapter.restore(payload_root, manifest)
    return manifest, evidence


def _payload_refs(root: Path) -> Iterator[CheckpointPayloadRef]:
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root).as_posix()
        digest, size = _hash_file(path)
        yield CheckpointPayloadRef(path=relative, bytes=size, sha256=digest)


def _read_manifest(source: IO[bytes]) -> CheckpointManifest:
    if source.read(len(BUNDLE_MAGIC)) != BUNDLE_MAGIC:
        raise ValueError("checkpoint bundle magic mismatch")
    length_raw = source.read(MANIFEST_LENGTH_BYTES)
    if len(length_raw) != MANIFEST_LENGTH_BYTES:
        raise ValueError("checkpoint bundle is missing the manifest length")
    manifest_length = int.from_bytes(length_raw, "big")
    if manifest_length < 2 or manifest_length > 16 * 1024 * 1024:
        raise ValueError("checkpoint manifest length is invalid")
    manifest_raw = source.read(manifest_length)
    if len(manifest_raw) != manifest_length:
        raise ValueError("checkpoint bundle manifest is truncated")
    return CheckpointManifest.from_dict(parse_json_object(manifest_raw))


def _verify_payload_stream(
    source: IO[bytes], manifest: CheckpointManifest, destination: Path | None
) -> None:
    for payload in manifest.payloads:
        target = None
        if destination is not None:
            target_path = destination / payload.path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target = target_path.open("wb")
        try:
            digest, size = _copy_and_hash_exact(source, target, payload.bytes)
        finally:
            if target is not None:
                target.close()
        if size != payload.bytes:
            raise ValueError(f"checkpoint payload byte count mismatch: {payload.path}")
        if digest != payload.sha256:
            raise ValueError(f"checkpoint payload SHA-256 mismatch: {payload.path}")
    if source.read(1):
        raise ValueError("checkpoint bundle has trailing bytes")


def _validate_manifest_identity(
    manifest: CheckpointManifest,
    *,
    expected_run_id: str,
    expected_plan_sha256: str,
    adapter: CheckpointAdapter,
) -> None:
    if manifest.run_id != expected_run_id:
        raise ValueError(f"checkpoint run_id mismatch: {manifest.run_id!r} != {expected_run_id!r}")
    if manifest.plan_sha256 != expected_plan_sha256:
        raise ValueError("checkpoint plan_sha256 mismatch")
    if manifest.adapter != adapter.spec:
        raise ValueError(f"checkpoint adapter mismatch: {manifest.adapter!r} != {adapter.spec!r}")


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


def _copy_exact(source: IO[bytes], destination: IO[bytes], size: int) -> None:
    remaining = size
    while remaining > 0:
        chunk = source.read(min(CHUNK_SIZE, remaining))
        if not chunk:
            raise ValueError("checkpoint payload changed while bundling")
        destination.write(chunk)
        remaining -= len(chunk)


def _copy_and_hash_exact(
    source: IO[bytes], destination: IO[bytes] | None, expected_size: int
) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    remaining = expected_size
    while remaining > 0:
        chunk = source.read(min(CHUNK_SIZE, remaining))
        if not chunk:
            break
        if destination is not None:
            destination.write(chunk)
        digest.update(chunk)
        size += len(chunk)
        remaining -= len(chunk)
    return digest.hexdigest(), size
