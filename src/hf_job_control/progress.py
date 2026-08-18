"""Durable, application-neutral progress reporting."""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol, cast

from huggingface_hub import HfFileSystem

from hf_job_control.models import (
    MAX_SAFE_INTEGER,
    ArtifactRef,
    JsonObject,
    _optional_string,
    _require_fields,
    _require_int,
    _require_object,
    _require_string,
    format_datetime,
    parse_datetime,
    parse_json_object,
    sha256_bytes,
    stable_json_bytes,
    utc_now,
    validate_attempt_id,
    validate_job_id,
    validate_repo_id,
    validate_run_id,
)

PROGRESS_SCHEMA_VERSION = 1
MAX_TRACKS = 256
_PUBLICATION_LOCKS_GUARD = threading.Lock()
_PUBLICATION_LOCKS: dict[tuple[str, str], threading.RLock] = {}


class ProgressStatus(StrEnum):
    """Operational state for a run or progress track."""

    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_PROGRESS_STATUSES = {
    ProgressStatus.COMPLETED,
    ProgressStatus.FAILED,
    ProgressStatus.CANCELLED,
}


@dataclass(frozen=True, slots=True)
class ProgressInput:
    """Immutable input and producer-contract identity."""

    revision: str
    contract_sha256: str

    def __post_init__(self) -> None:
        if not self.revision or len(self.revision) > 200:
            raise ValueError("revision must contain 1 to 200 characters")
        if len(self.contract_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.contract_sha256
        ):
            raise ValueError("contract_sha256 must be 64 lowercase hex characters")

    def to_dict(self) -> JsonObject:
        return {"contract_sha256": self.contract_sha256, "revision": self.revision}

    @classmethod
    def from_dict(cls, value: object) -> ProgressInput:
        data = _require_object(value, "progress input")
        _require_fields(
            data,
            required={"revision", "contract_sha256"},
            allowed={"revision", "contract_sha256"},
        )
        return cls(
            revision=_require_string(data["revision"], "revision"),
            contract_sha256=_require_string(data["contract_sha256"], "contract_sha256"),
        )


@dataclass(frozen=True, slots=True)
class ProgressTrack:
    """One independently measurable workstream under a fixed plan."""

    key: str
    plan_id: str
    status: ProgressStatus
    label: str | None = None
    completed: int | None = None
    total: int | None = None
    unit: str | None = None
    source_updated_at: datetime | None = None

    def __post_init__(self) -> None:
        validate_run_id(self.key)
        validate_run_id(self.plan_id)
        if self.label is not None and (not self.label or len(self.label) > 200):
            raise ValueError("label must contain 1 to 200 characters")
        _validate_track_counts(self)
        if self.source_updated_at is not None and self.source_updated_at.tzinfo is None:
            raise ValueError("source_updated_at must be timezone-aware")

    def to_dict(self) -> JsonObject:
        result: JsonObject = {
            "key": self.key,
            "plan_id": self.plan_id,
            "status": self.status.value,
        }
        if self.label is not None:
            result["label"] = self.label
        if self.completed is not None:
            result["completed"] = self.completed
        if self.total is not None:
            result["total"] = self.total
        if self.unit is not None:
            result["unit"] = self.unit
        if self.source_updated_at is not None:
            result["source_updated_at"] = format_datetime(self.source_updated_at)
        return result

    @classmethod
    def from_dict(cls, value: object) -> ProgressTrack:
        data = _require_object(value, "progress track")
        allowed = {
            "key",
            "plan_id",
            "label",
            "status",
            "completed",
            "total",
            "unit",
            "source_updated_at",
        }
        _require_fields(data, required={"key", "plan_id", "status"}, allowed=allowed)
        completed = data.get("completed")
        total = data.get("total")
        source_updated_at = data.get("source_updated_at")
        return cls(
            key=_require_string(data["key"], "key"),
            plan_id=_require_string(data["plan_id"], "plan_id"),
            label=_optional_string(data, "label"),
            status=ProgressStatus(_require_string(data["status"], "status")),
            completed=None if completed is None else _require_int(completed, "completed"),
            total=None if total is None else _require_int(total, "total"),
            unit=_optional_string(data, "unit"),
            source_updated_at=(
                None
                if source_updated_at is None
                else parse_datetime(source_updated_at, "source_updated_at")
            ),
        )


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    """Durable progress state for one logical run."""

    run_id: str
    attempt_id: str
    sequence: int
    updated_at: datetime
    input: ProgressInput
    state: ProgressStatus
    tracks: tuple[ProgressTrack, ...]
    job_id: str | None = None
    previous: ArtifactRef | None = None
    schema_version: int = PROGRESS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROGRESS_SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {PROGRESS_SCHEMA_VERSION}")
        validate_run_id(self.run_id)
        validate_attempt_id(self.attempt_id)
        validate_job_id(self.job_id)
        if self.sequence < 1 or self.sequence > MAX_SAFE_INTEGER:
            raise ValueError("sequence must be a positive JavaScript-safe integer")
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")
        if not self.tracks or len(self.tracks) > MAX_TRACKS:
            raise ValueError(f"tracks must contain 1 to {MAX_TRACKS} items")
        keys = [track.key for track in self.tracks]
        if len(set(keys)) != len(keys):
            raise ValueError("track keys must be unique")
        if tuple(sorted(keys)) != tuple(keys):
            raise ValueError("tracks must be sorted by key")

    def to_dict(self) -> JsonObject:
        result: JsonObject = {
            "attempt_id": self.attempt_id,
            "input": self.input.to_dict(),
            "run_id": self.run_id,
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "state": self.state.value,
            "tracks": [track.to_dict() for track in self.tracks],
            "updated_at": format_datetime(self.updated_at),
        }
        if self.job_id is not None:
            result["job_id"] = self.job_id
        if self.previous is not None:
            result["previous"] = self.previous.to_dict()
        return result

    @classmethod
    def from_dict(cls, value: object) -> ProgressSnapshot:
        data = _require_object(value, "progress snapshot")
        allowed = {
            "schema_version",
            "run_id",
            "attempt_id",
            "job_id",
            "sequence",
            "updated_at",
            "input",
            "state",
            "tracks",
            "previous",
        }
        required = {
            "schema_version",
            "run_id",
            "attempt_id",
            "sequence",
            "updated_at",
            "input",
            "state",
            "tracks",
        }
        _require_fields(data, required=required, allowed=allowed)
        raw_tracks = data["tracks"]
        if not isinstance(raw_tracks, list):
            raise TypeError("tracks must be an array")
        previous = data.get("previous")
        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            run_id=_require_string(data["run_id"], "run_id"),
            attempt_id=_require_string(data["attempt_id"], "attempt_id"),
            job_id=_optional_string(data, "job_id"),
            sequence=_require_int(data["sequence"], "sequence", 1),
            updated_at=parse_datetime(data["updated_at"], "updated_at"),
            input=ProgressInput.from_dict(data["input"]),
            state=ProgressStatus(_require_string(data["state"], "state")),
            tracks=tuple(
                sorted(
                    (ProgressTrack.from_dict(track) for track in raw_tracks),
                    key=lambda track: track.key,
                )
            ),
            previous=None if previous is None else ArtifactRef.from_dict(previous),
        )


@dataclass(frozen=True, slots=True)
class ProgressPointer:
    """Mutable pointer to one verified immutable progress snapshot."""

    run_id: str
    sequence: int
    updated_at: datetime
    snapshot: ArtifactRef
    schema_version: int = PROGRESS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROGRESS_SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {PROGRESS_SCHEMA_VERSION}")
        validate_run_id(self.run_id)
        if self.sequence < 1 or self.sequence > MAX_SAFE_INTEGER:
            raise ValueError("sequence must be a positive JavaScript-safe integer")
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")

    def to_dict(self) -> JsonObject:
        return {
            "run_id": self.run_id,
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "snapshot": self.snapshot.to_dict(),
            "updated_at": format_datetime(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: object) -> ProgressPointer:
        data = _require_object(value, "progress pointer")
        required = {"schema_version", "run_id", "sequence", "updated_at", "snapshot"}
        _require_fields(data, required=required, allowed=required)
        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            run_id=_require_string(data["run_id"], "run_id"),
            sequence=_require_int(data["sequence"], "sequence", 1),
            updated_at=parse_datetime(data["updated_at"], "updated_at"),
            snapshot=ArtifactRef.from_dict(data["snapshot"]),
        )


@dataclass(frozen=True, slots=True)
class ProgressClaim:
    """Immutable claim for one logical sequence number."""

    run_id: str
    attempt_id: str
    sequence: int
    created_at: datetime
    snapshot: ArtifactRef
    schema_version: int = PROGRESS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROGRESS_SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {PROGRESS_SCHEMA_VERSION}")
        validate_run_id(self.run_id)
        validate_attempt_id(self.attempt_id)
        if self.sequence < 1 or self.sequence > MAX_SAFE_INTEGER:
            raise ValueError("sequence must be a positive JavaScript-safe integer")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

    def to_dict(self) -> JsonObject:
        return {
            "attempt_id": self.attempt_id,
            "created_at": format_datetime(self.created_at),
            "run_id": self.run_id,
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "snapshot": self.snapshot.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> ProgressClaim:
        data = _require_object(value, "progress claim")
        required = {
            "schema_version",
            "run_id",
            "attempt_id",
            "sequence",
            "created_at",
            "snapshot",
        }
        _require_fields(data, required=required, allowed=required)
        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            run_id=_require_string(data["run_id"], "run_id"),
            attempt_id=_require_string(data["attempt_id"], "attempt_id"),
            sequence=_require_int(data["sequence"], "sequence", 1),
            created_at=parse_datetime(data["created_at"], "created_at"),
            snapshot=ArtifactRef.from_dict(data["snapshot"]),
        )


@dataclass(frozen=True, slots=True)
class StoredProgress:
    """One verified snapshot and its content-addressed reference."""

    snapshot: ProgressSnapshot
    reference: ArtifactRef


class ProgressStore(Protocol):
    """Durable storage for progress snapshots and current pointers."""

    def load_latest(self, run_id: str) -> StoredProgress | None:
        """Load and verify the latest snapshot for a logical run."""

    def load_reference(self, reference: ArtifactRef) -> ProgressSnapshot:
        """Load and verify one immutable snapshot reference."""

    def publish(self, snapshot: ProgressSnapshot) -> StoredProgress:
        """Publish an ordered immutable snapshot and replace the current pointer."""


def progress_pointer_key(prefix: str, run_id: str) -> str:
    """Return the mutable pointer key for one logical run."""

    validate_run_id(run_id)
    root = _normalized_prefix(prefix)
    return _join_key(root, "operations", run_id, "progress", "current.json")


def progress_snapshot_key(prefix: str, run_id: str, digest: str) -> str:
    """Return the content-addressed key for one immutable snapshot."""

    validate_run_id(run_id)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("digest must be 64 lowercase hex characters")
    root = _normalized_prefix(prefix)
    return _join_key(
        root,
        "operations",
        run_id,
        "progress",
        "snapshots",
        f"sha256-{digest}",
        "progress.json",
    )


def progress_claim_prefix(prefix: str, run_id: str, sequence: int) -> str:
    """Return the immutable-claim directory for one sequence."""

    validate_run_id(run_id)
    if sequence < 1 or sequence > MAX_SAFE_INTEGER:
        raise ValueError("sequence must be a positive JavaScript-safe integer")
    root = _normalized_prefix(prefix)
    return _join_key(
        root,
        "operations",
        run_id,
        "progress",
        "claims",
        f"sequence-{sequence:016d}",
    )


def progress_claim_key(prefix: str, claim: ProgressClaim) -> str:
    """Return the immutable key for one attempt's sequence claim."""

    return _join_key(
        progress_claim_prefix(prefix, claim.run_id, claim.sequence),
        f"{claim.attempt_id}.json",
    )


class MemoryProgressStore:
    """In-memory progress store for tests and local adapters."""

    def __init__(self, bucket_id: str = "memory/progress", prefix: str = "") -> None:
        validate_repo_id(bucket_id, "bucket_id")
        self.bucket_id = bucket_id
        self.prefix = _normalized_prefix(prefix)
        self.objects: dict[str, bytes] = {}
        self.claims: dict[str, bytes] = {}
        self.pointers: dict[str, ProgressPointer] = {}
        self._publication_lock = _shared_publication_lock(bucket_id, self.prefix)

    def load_latest(self, run_id: str) -> StoredProgress | None:
        return _reconcile_latest(
            self.pointers.get(run_id),
            run_id,
            self.load_reference,
            self._load_claims,
            lambda pointer: self.pointers.__setitem__(run_id, pointer),
        )

    def load_reference(self, reference: ArtifactRef) -> ProgressSnapshot:
        if reference.bucket != self.bucket_id:
            raise ValueError("progress snapshot Bucket mismatch")
        raw = self.objects.get(reference.key)
        if raw is None:
            raise FileNotFoundError(reference.key)
        _verify_progress_bytes(raw, reference)
        return ProgressSnapshot.from_dict(parse_json_object(raw))

    def publish(self, snapshot: ProgressSnapshot) -> StoredProgress:
        with self._publication_lock:
            raw, reference, pointer = _prepare_publication(
                snapshot,
                self.load_latest(snapshot.run_id),
                self.bucket_id,
                self.prefix,
            )
            existing = self.objects.get(reference.key)
            if existing is not None and existing != raw:
                raise RuntimeError("immutable progress snapshot differs")
            self.objects[reference.key] = raw
            claim = _claim_for(snapshot, reference)
            claim_key = progress_claim_key(self.prefix, claim)
            claim_raw = stable_json_bytes(claim.to_dict())
            existing_claim = self.claims.get(claim_key)
            if existing_claim is not None and existing_claim != claim_raw:
                raise RuntimeError("immutable progress claim differs")
            self.claims[claim_key] = claim_raw
            _require_single_claim(self._load_claims(snapshot.run_id, snapshot.sequence), claim)
            self.pointers[snapshot.run_id] = pointer
            return StoredProgress(snapshot, reference)

    def _load_claims(self, run_id: str, sequence: int) -> list[ProgressClaim]:
        prefix = f"{progress_claim_prefix(self.prefix, run_id, sequence)}/"
        return sorted(
            (
                ProgressClaim.from_dict(parse_json_object(raw))
                for key, raw in self.claims.items()
                if key.startswith(prefix)
            ),
            key=lambda claim: claim.attempt_id,
        )


class LocalProgressStore:
    """Filesystem progress store with atomic local pointer replacement."""

    def __init__(self, root: Path, bucket_id: str = "local/progress", prefix: str = "") -> None:
        validate_repo_id(bucket_id, "bucket_id")
        self.root = root
        self.bucket_id = bucket_id
        self.prefix = _normalized_prefix(prefix)
        self._publication_lock = _shared_publication_lock(bucket_id, self.prefix)

    def load_latest(self, run_id: str) -> StoredProgress | None:
        pointer_path = self.root / progress_pointer_key(self.prefix, run_id)
        pointer = (
            None
            if not pointer_path.exists()
            else ProgressPointer.from_dict(parse_json_object(pointer_path.read_bytes()))
        )
        return _reconcile_latest(
            pointer,
            run_id,
            self.load_reference,
            self._load_claims,
            self._write_pointer,
        )

    def load_reference(self, reference: ArtifactRef) -> ProgressSnapshot:
        if reference.bucket != self.bucket_id:
            raise ValueError("progress snapshot Bucket mismatch")
        source = _safe_local_path(self.root, reference.key)
        raw = source.read_bytes()
        _verify_progress_bytes(raw, reference)
        return ProgressSnapshot.from_dict(parse_json_object(raw))

    def publish(self, snapshot: ProgressSnapshot) -> StoredProgress:
        with self._publication_lock:
            raw, reference, pointer = _prepare_publication(
                snapshot,
                self.load_latest(snapshot.run_id),
                self.bucket_id,
                self.prefix,
            )
            destination = _safe_local_path(self.root, reference.key)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if destination.read_bytes() != raw:
                    raise RuntimeError("immutable progress snapshot differs")
            else:
                destination.write_bytes(raw)
            _verify_progress_bytes(destination.read_bytes(), reference)
            claim = _claim_for(snapshot, reference)
            claim_path = self.root / progress_claim_key(self.prefix, claim)
            claim_raw = stable_json_bytes(claim.to_dict())
            if claim_path.exists():
                if claim_path.read_bytes() != claim_raw:
                    raise RuntimeError("immutable progress claim differs")
            else:
                claim_path.parent.mkdir(parents=True, exist_ok=True)
                claim_path.write_bytes(claim_raw)
            _require_single_claim(self._load_claims(snapshot.run_id, snapshot.sequence), claim)
            self._write_pointer(pointer)
            return StoredProgress(snapshot, reference)

    def _load_claims(self, run_id: str, sequence: int) -> list[ProgressClaim]:
        directory = self.root / progress_claim_prefix(self.prefix, run_id, sequence)
        if not directory.exists():
            return []
        return sorted(
            (
                ProgressClaim.from_dict(parse_json_object(path.read_bytes()))
                for path in directory.glob("*.json")
            ),
            key=lambda claim: claim.attempt_id,
        )

    def _write_pointer(self, pointer: ProgressPointer) -> None:
        pointer_path = self.root / progress_pointer_key(self.prefix, pointer.run_id)
        _write_atomic(pointer_path, stable_json_bytes(pointer.to_dict()))


class BucketFileSystem(Protocol):
    """Typed Bucket operations required by HubBucketProgressStore."""

    def exists(self, path: str) -> bool:
        """Return whether a Bucket object exists."""

    def open(self, path: str, mode: str) -> BinaryIO:
        """Open one Bucket object."""

    def glob(self, path: str) -> list[str]:
        """List Bucket objects matching one glob pattern."""


class HubBucketProgressStore:
    """Content-addressed progress storage in an existing Hugging Face Bucket."""

    def __init__(
        self,
        bucket_id: str,
        *,
        prefix: str = "",
        token: str | bool | None = None,
        filesystem: BucketFileSystem | None = None,
    ) -> None:
        validate_repo_id(bucket_id, "bucket_id")
        self.bucket_id = bucket_id
        self.prefix = _normalized_prefix(prefix)
        self.filesystem = (
            cast(BucketFileSystem, HfFileSystem(token=token)) if filesystem is None else filesystem
        )
        self._publication_lock = _shared_publication_lock(bucket_id, self.prefix)

    def load_latest(self, run_id: str) -> StoredProgress | None:
        path = self._url(progress_pointer_key(self.prefix, run_id))
        pointer = None
        if self.filesystem.exists(path):
            with self.filesystem.open(path, "rb") as source:
                pointer = ProgressPointer.from_dict(parse_json_object(source.read()))
        return _reconcile_latest(
            pointer,
            run_id,
            self.load_reference,
            self._load_claims,
            self._write_pointer,
        )

    def load_reference(self, reference: ArtifactRef) -> ProgressSnapshot:
        if reference.bucket != self.bucket_id:
            raise ValueError("progress snapshot Bucket mismatch")
        with self.filesystem.open(self._url(reference.key), "rb") as source:
            raw = source.read()
        _verify_progress_bytes(raw, reference)
        return ProgressSnapshot.from_dict(parse_json_object(raw))

    def publish(self, snapshot: ProgressSnapshot) -> StoredProgress:
        with self._publication_lock:
            raw, reference, pointer = _prepare_publication(
                snapshot,
                self.load_latest(snapshot.run_id),
                self.bucket_id,
                self.prefix,
            )
            destination = self._url(reference.key)
            if self.filesystem.exists(destination):
                with self.filesystem.open(destination, "rb") as source:
                    if source.read() != raw:
                        raise RuntimeError("immutable progress snapshot differs")
            else:
                with self.filesystem.open(destination, "wb") as target:
                    target.write(raw)
            self.load_reference(reference)
            claim = _claim_for(snapshot, reference)
            claim_key = progress_claim_key(self.prefix, claim)
            claim_raw = stable_json_bytes(claim.to_dict())
            claim_url = self._url(claim_key)
            if self.filesystem.exists(claim_url):
                with self.filesystem.open(claim_url, "rb") as source:
                    if source.read() != claim_raw:
                        raise RuntimeError("immutable progress claim differs")
            else:
                with self.filesystem.open(claim_url, "wb") as target:
                    target.write(claim_raw)
            _require_single_claim(self._load_claims(snapshot.run_id, snapshot.sequence), claim)
            self._write_pointer(pointer)
            return StoredProgress(snapshot, reference)

    def _load_claims(self, run_id: str, sequence: int) -> list[ProgressClaim]:
        pattern = f"{self._url(progress_claim_prefix(self.prefix, run_id, sequence))}/*.json"
        return sorted(
            (
                ProgressClaim.from_dict(parse_json_object(self._read(path)))
                for path in self.filesystem.glob(pattern)
            ),
            key=lambda claim: claim.attempt_id,
        )

    def _write_pointer(self, pointer: ProgressPointer) -> None:
        pointer_url = self._url(progress_pointer_key(self.prefix, pointer.run_id))
        pointer_raw = stable_json_bytes(pointer.to_dict())
        with self.filesystem.open(pointer_url, "wb") as target:
            target.write(pointer_raw)
        with self.filesystem.open(pointer_url, "rb") as source:
            if source.read() != pointer_raw:
                raise RuntimeError("uploaded progress pointer verification failed")

    def _read(self, path: str) -> bytes:
        with self.filesystem.open(path, "rb") as source:
            return source.read()

    def _url(self, key: str) -> str:
        return f"hf://buckets/{self.bucket_id}/{key}"


class ProgressReporter:
    """Validate, throttle, and publish durable progress for one logical run."""

    def __init__(
        self,
        *,
        run_id: str,
        attempt_id: str,
        input: ProgressInput,
        store: ProgressStore,
        job_id: str | None = None,
        flush_interval: timedelta = timedelta(seconds=30),
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        validate_run_id(run_id)
        validate_attempt_id(attempt_id)
        validate_job_id(job_id)
        if flush_interval.total_seconds() < 0:
            raise ValueError("flush_interval must be nonnegative")
        self.run_id = run_id
        self.attempt_id = attempt_id
        self.job_id = job_id
        self.input = input
        self.store = store
        self.flush_interval = flush_interval
        self.clock = clock
        self._lock = threading.RLock()
        self._latest = store.load_latest(run_id)
        previous = self._latest.snapshot if self._latest is not None else None
        self._sequence = 0 if previous is None else previous.sequence
        self._tracks: dict[str, ProgressTrack]
        if previous is not None and previous.input == input:
            self._tracks = {track.key: track for track in previous.tracks}
            self._state = previous.state
            self._dirty = previous.state not in TERMINAL_PROGRESS_STATUSES and (
                previous.attempt_id != attempt_id or previous.job_id != job_id
            )
        else:
            self._tracks = {}
            self._state = ProgressStatus.RUNNING
            self._dirty = True
        self._last_flush_at = None if previous is None else previous.updated_at

    @property
    def tracks(self) -> tuple[ProgressTrack, ...]:
        """Return the current validated tracks sorted by key."""

        with self._lock:
            return tuple(self._tracks[key] for key in sorted(self._tracks))

    def plan(self, tracks: tuple[ProgressTrack, ...] | list[ProgressTrack]) -> None:
        """Add or reconcile planned tracks without removing existing tracks."""

        with self._lock:
            if not tracks:
                raise ValueError("plan requires at least one track")
            incoming_keys = [track.key for track in tracks]
            if len(set(incoming_keys)) != len(incoming_keys):
                raise ValueError("planned track keys must be unique")
            candidate = dict(self._tracks)
            for track in tracks:
                current = candidate.get(track.key)
                if current is not None:
                    _validate_track_transition(current, track)
                candidate[track.key] = track
            if len(candidate) > MAX_TRACKS:
                raise ValueError(f"tracks must not exceed {MAX_TRACKS}")
            self._tracks = candidate
            self._dirty = True

    def update(self, track: ProgressTrack) -> None:
        """Replace one track after validating its monotonic transition."""

        with self._lock:
            current = self._tracks.get(track.key)
            if current is None:
                raise KeyError(f"unknown progress track: {track.key}")
            _validate_track_transition(current, track)
            if current != track:
                self._tracks[track.key] = track
                self._dirty = True

    def set_state(self, state: ProgressStatus) -> None:
        """Set run-level state without changing track state."""

        with self._lock:
            if self._state in TERMINAL_PROGRESS_STATUSES and state is not self._state:
                raise ValueError("terminal progress state cannot change")
            if state is not self._state:
                self._state = state
                self._dirty = True

    def heartbeat(self) -> StoredProgress | None:
        """Publish a fresh observation when the reporting interval elapsed."""

        with self._lock:
            self._dirty = True
            return self.flush()

    def flush(self, *, force: bool = False) -> StoredProgress | None:
        """Publish the current snapshot when dirty and due."""

        with self._lock:
            if not self._dirty:
                return None
            if not self._tracks:
                raise ValueError("at least one progress track is required before flush")
            now = self.clock()
            if now.tzinfo is None:
                raise ValueError("clock must return a timezone-aware datetime")
            if (
                not force
                and self._last_flush_at is not None
                and now - self._last_flush_at < self.flush_interval
            ):
                return None
            snapshot = ProgressSnapshot(
                run_id=self.run_id,
                attempt_id=self.attempt_id,
                job_id=self.job_id,
                sequence=self._sequence + 1,
                updated_at=now,
                input=self.input,
                state=self._state,
                tracks=tuple(self._tracks[key] for key in sorted(self._tracks)),
                previous=None if self._latest is None else self._latest.reference,
            )
            stored = self.store.publish(snapshot)
            self._latest = stored
            self._sequence = snapshot.sequence
            self._last_flush_at = now
            self._dirty = False
            return stored


def _shared_publication_lock(bucket_id: str, prefix: str) -> threading.RLock:
    key = (bucket_id, prefix)
    with _PUBLICATION_LOCKS_GUARD:
        lock = _PUBLICATION_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _PUBLICATION_LOCKS[key] = lock
        return lock


def _stored_from_pointer(
    pointer: ProgressPointer,
    run_id: str,
    load: Callable[[ArtifactRef], ProgressSnapshot],
) -> StoredProgress:
    if pointer.run_id != run_id:
        raise ValueError("progress pointer run_id mismatch")
    snapshot = load(pointer.snapshot)
    if snapshot.run_id != pointer.run_id:
        raise ValueError("progress pointer snapshot run_id mismatch")
    if snapshot.sequence != pointer.sequence:
        raise ValueError("progress pointer snapshot sequence mismatch")
    if snapshot.updated_at != pointer.updated_at:
        raise ValueError("progress pointer snapshot timestamp mismatch")
    return StoredProgress(snapshot, pointer.snapshot)


def _claim_for(snapshot: ProgressSnapshot, reference: ArtifactRef) -> ProgressClaim:
    return ProgressClaim(
        run_id=snapshot.run_id,
        attempt_id=snapshot.attempt_id,
        sequence=snapshot.sequence,
        created_at=snapshot.updated_at,
        snapshot=reference,
    )


def _validate_claim(claim: ProgressClaim, stored: StoredProgress) -> None:
    snapshot = stored.snapshot
    if claim.run_id != snapshot.run_id:
        raise ValueError("progress claim snapshot run_id mismatch")
    if claim.attempt_id != snapshot.attempt_id:
        raise ValueError("progress claim snapshot attempt_id mismatch")
    if claim.sequence != snapshot.sequence:
        raise ValueError("progress claim snapshot sequence mismatch")
    if claim.created_at != snapshot.updated_at:
        raise ValueError("progress claim snapshot timestamp mismatch")
    if claim.snapshot != stored.reference:
        raise ValueError("progress claim snapshot reference mismatch")


def _require_single_claim(
    claims: list[ProgressClaim],
    expected: ProgressClaim | None = None,
) -> ProgressClaim:
    if not claims:
        raise ValueError("progress sequence claim is missing")
    if len(claims) > 1:
        raise RuntimeError("competing progress sequence claims detected")
    claim = claims[0]
    if expected is not None and claim != expected:
        raise RuntimeError("progress sequence is claimed by another attempt")
    return claim


def _reconcile_latest(
    pointer: ProgressPointer | None,
    run_id: str,
    load: Callable[[ArtifactRef], ProgressSnapshot],
    load_claims: Callable[[str, int], list[ProgressClaim]],
    write_pointer: Callable[[ProgressPointer], None],
) -> StoredProgress | None:
    current = None if pointer is None else _stored_from_pointer(pointer, run_id, load)
    if current is not None:
        claim = _require_single_claim(load_claims(run_id, current.snapshot.sequence))
        _validate_claim(claim, current)
    while True:
        next_sequence = 1 if current is None else current.snapshot.sequence + 1
        claims = load_claims(run_id, next_sequence)
        if not claims:
            return current
        claim = _require_single_claim(claims)
        child = StoredProgress(load(claim.snapshot), claim.snapshot)
        _validate_claim(claim, child)
        _validate_publication(child.snapshot, current)
        pointer = ProgressPointer(
            run_id=child.snapshot.run_id,
            sequence=child.snapshot.sequence,
            updated_at=child.snapshot.updated_at,
            snapshot=child.reference,
        )
        write_pointer(pointer)
        current = child


def _prepare_publication(
    snapshot: ProgressSnapshot,
    latest: StoredProgress | None,
    bucket_id: str,
    prefix: str,
) -> tuple[bytes, ArtifactRef, ProgressPointer]:
    _validate_publication(snapshot, latest)
    raw = stable_json_bytes(snapshot.to_dict())
    digest = sha256_bytes(raw)
    reference = ArtifactRef(
        bucket=bucket_id,
        key=progress_snapshot_key(prefix, snapshot.run_id, digest),
        sha256=digest,
        bytes=len(raw),
    )
    pointer = ProgressPointer(
        run_id=snapshot.run_id,
        sequence=snapshot.sequence,
        updated_at=snapshot.updated_at,
        snapshot=reference,
    )
    return raw, reference, pointer


def _validate_track_counts(track: ProgressTrack) -> None:
    if track.completed is not None and not 0 <= track.completed <= MAX_SAFE_INTEGER:
        raise ValueError("completed must be a nonnegative JavaScript-safe integer")
    if track.total is not None and not 0 <= track.total <= MAX_SAFE_INTEGER:
        raise ValueError("total must be a nonnegative JavaScript-safe integer")
    if track.total is not None and track.completed is None:
        raise ValueError("completed is required when total is set")
    if track.completed is not None and track.unit is None:
        raise ValueError("unit is required when completed is set")
    if track.unit is not None and (not track.unit or len(track.unit) > 64):
        raise ValueError("unit must contain 1 to 64 characters")
    if track.completed is not None and track.total is not None and track.completed > track.total:
        raise ValueError("completed must not exceed total")
    if (
        track.status is ProgressStatus.COMPLETED
        and track.total is not None
        and track.completed != track.total
    ):
        raise ValueError("a completed track must reach its total")


def _validate_track_transition(previous: ProgressTrack, current: ProgressTrack) -> None:
    if previous.key != current.key:
        raise ValueError("progress track key cannot change")
    if previous.plan_id != current.plan_id:
        return
    if previous.unit != current.unit:
        raise ValueError("progress track unit cannot change within a plan")
    if previous.total != current.total:
        raise ValueError("progress track total cannot change within a plan")
    if (
        previous.completed is not None
        and current.completed is not None
        and current.completed < previous.completed
    ):
        raise ValueError("progress track completed count cannot move backwards")
    if previous.status in TERMINAL_PROGRESS_STATUSES and current != previous:
        raise ValueError("terminal progress track cannot change within a plan")


def _validate_publication(snapshot: ProgressSnapshot, latest: StoredProgress | None) -> None:
    if latest is None:
        if snapshot.sequence != 1:
            raise ValueError("first progress sequence must be 1")
        if snapshot.previous is not None:
            raise ValueError("first progress snapshot must not have a predecessor")
        return
    if snapshot.sequence != latest.snapshot.sequence + 1:
        raise ValueError("progress sequence must increase by exactly one")
    if snapshot.previous != latest.reference:
        raise ValueError("progress predecessor does not match current snapshot")


def _verify_progress_bytes(raw: bytes, reference: ArtifactRef) -> None:
    if len(raw) != reference.bytes:
        raise ValueError("progress snapshot byte count mismatch")
    if sha256_bytes(raw) != reference.sha256:
        raise ValueError("progress snapshot SHA-256 mismatch")


def _normalized_prefix(prefix: str) -> str:
    normalized = prefix.strip("/")
    if not normalized:
        return ""
    path = PurePosixPath(normalized)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("prefix must be a safe relative POSIX path")
    return normalized


def _join_key(*parts: str) -> str:
    return "/".join(part for part in parts if part)


def _safe_local_path(root: Path, key: str) -> Path:
    destination = (root / key).resolve()
    resolved_root = root.resolve()
    if destination != resolved_root and resolved_root not in destination.parents:
        raise ValueError("progress key escapes local store root")
    return destination


def _write_atomic(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(raw)
    temporary.replace(path)
