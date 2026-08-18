"""Validated domain models for job control."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, TypeAlias, cast

if TYPE_CHECKING:
    from hf_job_control.progress import ProgressSnapshot

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]

SCHEMA_VERSION = 1
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
REPO_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ATTEMPT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class Action(StrEnum):
    """Desired lifecycle action."""

    RUN = "run"
    PAUSE = "pause"
    STOP = "stop"
    ABORT = "abort"


class ResumeMode(StrEnum):
    """Strength of a job adapter's resume guarantee."""

    EXACT = "exact"
    BOUNDARY = "boundary"
    RESTART = "restart"
    UNSUPPORTED = "unsupported"


class RunState(StrEnum):
    """Observed state of a logical run."""

    CREATED = "created"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    COMPLETED = "completed"
    ABORTING = "aborting"
    ABORTED = "aborted"
    FAILED = "failed"


class ControlError(RuntimeError):
    """Raised when external control state is unsafe or invalid."""


def utc_now() -> datetime:
    """Return the current timezone-aware UTC time."""

    return datetime.now(UTC)


def format_datetime(value: datetime) -> str:
    """Format a timezone-aware datetime as RFC 3339 UTC."""

    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_datetime(value: object, field_name: str) -> datetime:
    """Parse an RFC 3339 timestamp."""

    text = _require_string(value, field_name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed.astimezone(UTC)


def stable_json_bytes(value: JsonObject) -> bytes:
    """Serialize an object in the canonical repository format."""

    return (
        json.dumps(value, allow_nan=False, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode()


def parse_json_object(raw: bytes) -> JsonObject:
    """Parse JSON while retaining an unknown external-input boundary."""

    parsed = cast(object, json.loads(raw))
    return _require_object(parsed, "document")


def sha256_bytes(value: bytes) -> str:
    """Return a lowercase SHA-256 digest."""

    return hashlib.sha256(value).hexdigest()


def validate_run_id(value: str) -> str:
    """Validate a logical run ID."""

    if not RUN_ID_RE.fullmatch(value):
        raise ValueError("run_id must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    return value


def validate_attempt_id(value: str) -> str:
    """Validate a physical attempt ID."""

    if not ATTEMPT_ID_RE.fullmatch(value):
        raise ValueError("attempt_id must be a safe path component")
    return value


def validate_job_id(value: str | None) -> str | None:
    """Validate an optional platform-assigned physical Job ID."""

    if value is not None and (not value.strip() or len(value) > 200):
        raise ValueError("job_id must be non-empty and at most 200 characters")
    return value


def validate_repo_id(value: str, field_name: str = "repo_id") -> str:
    """Validate a Hub namespace/name identifier."""

    if not REPO_ID_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a namespace/name identifier")
    return value


def _require_object(value: object, field_name: str) -> JsonObject:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{field_name} must be an object with string keys")
    return cast(JsonObject, value)


def _require_fields(value: JsonObject, *, required: set[str], allowed: set[str]) -> None:
    unknown = set(value) - allowed
    missing = required - set(value)
    if unknown or missing:
        raise ValueError(f"fields mismatch: missing={sorted(missing)} unknown={sorted(unknown)}")


def _require_string(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    return value


def _require_nonempty_string(value: object, field_name: str, maximum: int) -> str:
    text = _require_string(value, field_name)
    if not text.strip() or len(text) > maximum:
        raise ValueError(f"{field_name} must be non-empty and at most {maximum} characters")
    return text


def _require_int(value: object, field_name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field_name} must be an integer >= {minimum}")
    return value


def _optional_string(value: JsonObject, field_name: str) -> str | None:
    raw = value.get(field_name)
    return None if raw is None else _require_string(raw, field_name)


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """Immutable reference to a content-addressed Bucket object."""

    bucket: str
    key: str
    sha256: str
    bytes: int

    def __post_init__(self) -> None:
        validate_repo_id(self.bucket, "bucket")
        if not self.key or len(self.key) > 1024 or self.key.startswith(("/", "\\")):
            raise ValueError("key must be a relative POSIX path <= 1024 characters")
        if "\\" in self.key:
            raise ValueError("key must use POSIX separators")
        parts = PurePosixPath(self.key).parts
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("key contains an unsafe path component")
        if not SHA256_RE.fullmatch(self.sha256):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        if f"sha256-{self.sha256}" not in parts:
            raise ValueError("key must contain a sha256-<digest> segment")
        if self.bytes < 1:
            raise ValueError("bytes must be positive")

    def to_dict(self) -> JsonObject:
        return {"bucket": self.bucket, "bytes": self.bytes, "key": self.key, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, value: object) -> ArtifactRef:
        data = _require_object(value, "artifact")
        _require_fields(
            data,
            required={"bucket", "key", "sha256", "bytes"},
            allowed={"bucket", "key", "sha256", "bytes"},
        )
        return cls(
            bucket=_require_string(data["bucket"], "bucket"),
            key=_require_string(data["key"], "key"),
            sha256=_require_string(data["sha256"], "sha256"),
            bytes=_require_int(data["bytes"], "bytes", 1),
        )


@dataclass(frozen=True, slots=True)
class ControlDocument:
    """Versioned desired state for one logical run."""

    run_id: str
    generation: int
    action: Action
    reason: str | None = None
    resume_from: ArtifactRef | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        validate_run_id(self.run_id)
        if self.generation < 1:
            raise ValueError("generation must be >= 1")
        if self.reason is not None and (not self.reason.strip() or len(self.reason) > 2000):
            raise ValueError("reason must be non-empty and at most 2000 characters")
        if self.action is not Action.RUN and self.resume_from is not None:
            raise ValueError("resume_from is only valid with action run")

    def to_dict(self) -> JsonObject:
        result: JsonObject = {
            "action": self.action.value,
            "generation": self.generation,
            "run_id": self.run_id,
            "schema_version": self.schema_version,
        }
        if self.reason is not None:
            result["reason"] = self.reason
        if self.resume_from is not None:
            result["resume_from"] = self.resume_from.to_dict()
        return result

    @classmethod
    def from_dict(cls, value: object, *, expected_run_id: str | None = None) -> ControlDocument:
        data = _require_object(value, "control")
        allowed = {
            "schema_version",
            "run_id",
            "generation",
            "action",
            "reason",
            "resume_from",
        }
        _require_fields(
            data, required={"schema_version", "run_id", "generation", "action"}, allowed=allowed
        )
        run_id = _require_string(data["run_id"], "run_id")
        if expected_run_id is not None and run_id != expected_run_id:
            raise ValueError(f"run_id mismatch: {run_id!r} != {expected_run_id!r}")
        resume_raw = data.get("resume_from")
        try:
            action = Action(_require_string(data["action"], "action"))
        except ValueError as error:
            raise ValueError("action must be run, pause, stop, or abort") from error
        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            run_id=run_id,
            generation=_require_int(data["generation"], "generation", 1),
            action=action,
            reason=None
            if data.get("reason") is None
            else _require_nonempty_string(data["reason"], "reason", 2000),
            resume_from=None if resume_raw is None else ArtifactRef.from_dict(resume_raw),
        )


@dataclass(frozen=True, slots=True)
class ControlSnapshot:
    """A control document read from one exact dataset revision."""

    repo_id: str
    revision: str
    path: str
    sha256: str
    observed_at: datetime
    control: ControlDocument

    def __post_init__(self) -> None:
        validate_repo_id(self.repo_id)
        if not re.fullmatch(r"[0-9a-f]{40}", self.revision):
            raise ValueError("revision must be a 40-character Git commit")
        if not SHA256_RE.fullmatch(self.sha256):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class Boundary:
    """A durable unit-of-work boundary."""

    name: str
    sequence: int
    reached_at: datetime = field(default_factory=utc_now)
    metadata: JsonObject = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name or len(self.name) > 100:
            raise ValueError("boundary name must be non-empty and at most 100 characters")
        if self.sequence < 0:
            raise ValueError("boundary sequence must be >= 0")
        if self.reached_at.tzinfo is None:
            raise ValueError("reached_at must be timezone-aware")

    def to_dict(self) -> JsonObject:
        return {
            "metadata": self.metadata,
            "name": self.name,
            "reached_at": format_datetime(self.reached_at),
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, value: object) -> Boundary:
        data = _require_object(value, "boundary")
        _require_fields(
            data,
            required={"name", "sequence", "reached_at", "metadata"},
            allowed={"name", "sequence", "reached_at", "metadata"},
        )
        return cls(
            name=_require_nonempty_string(data["name"], "name", 100),
            sequence=_require_int(data["sequence"], "sequence"),
            reached_at=parse_datetime(data["reached_at"], "reached_at"),
            metadata=_require_object(data["metadata"], "metadata"),
        )


@dataclass(frozen=True, slots=True)
class AdapterSpec:
    """Stable identity and resume guarantee of a checkpoint adapter."""

    name: str
    version: int
    resume_mode: ResumeMode

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", self.name):
            raise ValueError("adapter name must be a lowercase identifier")
        if self.version < 1:
            raise ValueError("adapter version must be >= 1")

    def to_dict(self) -> JsonObject:
        return {"name": self.name, "resume_mode": self.resume_mode.value, "version": self.version}

    @classmethod
    def from_dict(cls, value: object) -> AdapterSpec:
        data = _require_object(value, "adapter")
        _require_fields(
            data,
            required={"name", "version", "resume_mode"},
            allowed={"name", "version", "resume_mode"},
        )
        return cls(
            name=_require_string(data["name"], "name"),
            version=_require_int(data["version"], "version", 1),
            resume_mode=ResumeMode(_require_string(data["resume_mode"], "resume_mode")),
        )


@dataclass(frozen=True, slots=True)
class CheckpointManifest:
    """Manifest stored inside a checkpoint bundle."""

    run_id: str
    attempt_id: str
    adapter: AdapterSpec
    boundary: Boundary
    payload_sha256: str
    payload_bytes: int
    created_at: datetime
    metadata: JsonObject = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)
        validate_attempt_id(self.attempt_id)
        if not SHA256_RE.fullmatch(self.payload_sha256):
            raise ValueError("payload_sha256 must be 64 lowercase hex characters")
        if self.payload_bytes < 0:
            raise ValueError("payload_bytes must be >= 0")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

    def to_dict(self) -> JsonObject:
        return {
            "adapter": self.adapter.to_dict(),
            "attempt_id": self.attempt_id,
            "boundary": self.boundary.to_dict(),
            "created_at": format_datetime(self.created_at),
            "metadata": self.metadata,
            "payload_bytes": self.payload_bytes,
            "payload_sha256": self.payload_sha256,
            "run_id": self.run_id,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, value: object) -> CheckpointManifest:
        data = _require_object(value, "checkpoint manifest")
        allowed = {
            "schema_version",
            "run_id",
            "attempt_id",
            "adapter",
            "boundary",
            "payload_sha256",
            "payload_bytes",
            "created_at",
            "metadata",
        }
        _require_fields(data, required=allowed, allowed=allowed)
        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            run_id=_require_string(data["run_id"], "run_id"),
            attempt_id=_require_string(data["attempt_id"], "attempt_id"),
            adapter=AdapterSpec.from_dict(data["adapter"]),
            boundary=Boundary.from_dict(data["boundary"]),
            payload_sha256=_require_string(data["payload_sha256"], "payload_sha256"),
            payload_bytes=_require_int(data["payload_bytes"], "payload_bytes"),
            created_at=parse_datetime(data["created_at"], "created_at"),
            metadata=_require_object(data["metadata"], "metadata"),
        )


@dataclass(frozen=True, slots=True)
class AppliedControlReceipt:
    """Immutable evidence that one attempt applied a control generation."""

    run_id: str
    attempt_id: str
    control_repo: str
    control_revision: str
    control_path: str
    control_sha256: str
    generation: int
    action: Action
    observed_at: datetime
    applied_at: datetime
    outcome: str
    job_id: str | None = None
    evidence: JsonObject = field(default_factory=dict)
    boundary: Boundary | None = None
    checkpoint: ArtifactRef | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        validate_run_id(self.run_id)
        validate_attempt_id(self.attempt_id)
        validate_job_id(self.job_id)
        validate_repo_id(self.control_repo, "control_repo")
        if not re.fullmatch(r"[0-9a-f]{40}", self.control_revision):
            raise ValueError("control_revision must be a 40-character Git commit")
        if self.control_path != f"controls/{self.run_id}.json":
            raise ValueError("control_path does not match run_id")
        if not SHA256_RE.fullmatch(self.control_sha256):
            raise ValueError("control_sha256 must be 64 lowercase hex characters")
        if self.generation < 1:
            raise ValueError("generation must be >= 1")
        if self.observed_at.tzinfo is None or self.applied_at.tzinfo is None:
            raise ValueError("receipt timestamps must be timezone-aware")
        if not self.outcome or len(self.outcome) > 200:
            raise ValueError("outcome must be non-empty and at most 200 characters")

    def to_dict(self) -> JsonObject:
        result: JsonObject = {
            "action": self.action.value,
            "applied_at": format_datetime(self.applied_at),
            "attempt_id": self.attempt_id,
            "control_path": self.control_path,
            "control_repo": self.control_repo,
            "control_revision": self.control_revision,
            "control_sha256": self.control_sha256,
            "generation": self.generation,
            "observed_at": format_datetime(self.observed_at),
            "outcome": self.outcome,
            "run_id": self.run_id,
            "schema_version": self.schema_version,
        }
        if self.job_id is not None:
            result["job_id"] = self.job_id
        if self.evidence:
            result["evidence"] = self.evidence
        if self.boundary is not None:
            result["boundary"] = self.boundary.to_dict()
        if self.checkpoint is not None:
            result["checkpoint"] = self.checkpoint.to_dict()
        return result


@dataclass(frozen=True, slots=True)
class RunStatus:
    """Latest observed state of one logical run."""

    run_id: str
    attempt_id: str
    state: RunState
    updated_at: datetime
    last_applied_generation: int
    last_action: Action
    job_id: str | None = None
    boundary: Boundary | None = None
    checkpoint: ArtifactRef | None = None
    metrics: JsonObject = field(default_factory=dict)
    progress: ProgressSnapshot | None = None
    message: str | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        validate_run_id(self.run_id)
        validate_attempt_id(self.attempt_id)
        validate_job_id(self.job_id)
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")
        if self.last_applied_generation < 0:
            raise ValueError("last_applied_generation must be >= 0")
        _validate_status_progress(self)
        if self.message is not None and len(self.message) > 2000:
            raise ValueError("message must be at most 2000 characters")

    def to_dict(self) -> JsonObject:
        result: JsonObject = {
            "attempt_id": self.attempt_id,
            "last_action": self.last_action.value,
            "last_applied_generation": self.last_applied_generation,
            "run_id": self.run_id,
            "schema_version": self.schema_version,
            "state": self.state.value,
            "updated_at": format_datetime(self.updated_at),
        }
        if self.job_id is not None:
            result["job_id"] = self.job_id
        if self.boundary is not None:
            result["boundary"] = self.boundary.to_dict()
        if self.checkpoint is not None:
            result["checkpoint"] = self.checkpoint.to_dict()
        if self.metrics:
            result["metrics"] = self.metrics
        if self.progress is not None:
            result["progress"] = self.progress.to_dict()
        if self.message is not None:
            result["message"] = self.message
        return result

    @classmethod
    def from_dict(cls, value: object) -> RunStatus:
        data = _require_object(value, "run status")
        allowed = {
            "schema_version",
            "run_id",
            "attempt_id",
            "state",
            "updated_at",
            "last_applied_generation",
            "last_action",
            "job_id",
            "boundary",
            "checkpoint",
            "metrics",
            "progress",
            "message",
        }
        required = {
            "schema_version",
            "run_id",
            "attempt_id",
            "state",
            "updated_at",
            "last_applied_generation",
            "last_action",
        }
        _require_fields(data, required=required, allowed=allowed)
        boundary = data.get("boundary")
        checkpoint = data.get("checkpoint")
        progress = data.get("progress")
        if progress is not None:
            from hf_job_control.progress import ProgressSnapshot

        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            run_id=_require_string(data["run_id"], "run_id"),
            attempt_id=_require_string(data["attempt_id"], "attempt_id"),
            state=RunState(_require_string(data["state"], "state")),
            updated_at=parse_datetime(data["updated_at"], "updated_at"),
            last_applied_generation=_require_int(
                data["last_applied_generation"], "last_applied_generation"
            ),
            last_action=Action(_require_string(data["last_action"], "last_action")),
            job_id=_optional_string(data, "job_id"),
            boundary=None if boundary is None else Boundary.from_dict(boundary),
            checkpoint=None if checkpoint is None else ArtifactRef.from_dict(checkpoint),
            metrics=(
                {} if data.get("metrics") is None else _require_object(data["metrics"], "metrics")
            ),
            progress=None if progress is None else ProgressSnapshot.from_dict(progress),
            message=_optional_string(data, "message"),
        )


def _validate_status_progress(status: RunStatus) -> None:
    progress = status.progress
    if progress is None:
        return
    if progress.run_id != status.run_id:
        raise ValueError("progress run_id must match status run_id")
    if progress.attempt_id != status.attempt_id:
        raise ValueError("progress attempt_id must match status attempt_id")
    if progress.job_id is not None and progress.job_id != status.job_id:
        raise ValueError("progress job_id must match status job_id")


@dataclass(frozen=True, slots=True)
class PublishedDocument:
    """A document written to one immutable dataset revision."""

    repo_id: str
    revision: str
    path: str
    sha256: str

    def __post_init__(self) -> None:
        validate_repo_id(self.repo_id)
        if not re.fullmatch(r"[0-9a-f]{40}", self.revision):
            raise ValueError("revision must be a 40-character Git commit")
        if not self.path:
            raise ValueError("path must be non-empty")
        if not SHA256_RE.fullmatch(self.sha256):
            raise ValueError("sha256 must be 64 lowercase hex characters")


@dataclass(frozen=True, slots=True)
class Decision:
    """Lifecycle decision returned at a safe boundary."""

    action: Action
    generation: int
    should_exit: bool
    exit_code: int
    target_state: RunState


@dataclass(frozen=True, slots=True)
class StartResult:
    """Result of validating and optionally restoring at startup."""

    resumed: bool
    generation: int
    checkpoint: ArtifactRef | None
    boundary: Boundary | None
    resume_evidence: JsonObject


@dataclass(frozen=True, slots=True)
class LaunchSpec:
    """Immutable, secret-free description of a Hugging Face Job launch."""

    image: str
    command: tuple[str, ...]
    flavor: str
    timeout: str
    environment: dict[str, str] = field(default_factory=dict)
    secret_names: tuple[str, ...] = ()
    labels: dict[str, str] = field(default_factory=dict)
    namespace: str | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        if not self.image or not self.command or not self.flavor or not self.timeout:
            raise ValueError("image, command, flavor, and timeout are required")
        if any(not item for item in self.command):
            raise ValueError("command items must be non-empty")
        if "RUN_ID" in self.environment or "ATTEMPT_ID" in self.environment:
            raise ValueError("RUN_ID and ATTEMPT_ID are assigned by the launcher")
        if any(not name for name in self.secret_names):
            raise ValueError("secret names must be non-empty")

    def to_dict(self) -> JsonObject:
        result: JsonObject = {
            "command": list(self.command),
            "environment": dict(self.environment),
            "flavor": self.flavor,
            "image": self.image,
            "labels": dict(self.labels),
            "schema_version": self.schema_version,
            "secret_names": list(self.secret_names),
            "timeout": self.timeout,
        }
        if self.namespace is not None:
            result["namespace"] = self.namespace
        return result

    @classmethod
    def from_dict(cls, value: object) -> LaunchSpec:
        data = _require_object(value, "launch specification")
        allowed = {
            "schema_version",
            "image",
            "command",
            "flavor",
            "timeout",
            "environment",
            "secret_names",
            "labels",
            "namespace",
        }
        required = {
            "schema_version",
            "image",
            "command",
            "flavor",
            "timeout",
            "environment",
            "secret_names",
            "labels",
        }
        _require_fields(data, required=required, allowed=allowed)
        command = data["command"]
        secret_names = data["secret_names"]
        if not isinstance(command, list) or any(not isinstance(item, str) for item in command):
            raise TypeError("command must be an array of strings")
        if not isinstance(secret_names, list) or any(
            not isinstance(item, str) for item in secret_names
        ):
            raise TypeError("secret_names must be an array of strings")
        environment = _string_map(data["environment"], "environment")
        labels = _string_map(data["labels"], "labels")
        return cls(
            schema_version=_require_int(data["schema_version"], "schema_version", 1),
            image=_require_nonempty_string(data["image"], "image", 500),
            command=tuple(cast(list[str], command)),
            flavor=_require_nonempty_string(data["flavor"], "flavor", 100),
            timeout=_require_nonempty_string(data["timeout"], "timeout", 100),
            environment=environment,
            secret_names=tuple(cast(list[str], secret_names)),
            labels=labels,
            namespace=_optional_string(data, "namespace"),
        )


def _string_map(value: object, field_name: str) -> dict[str, str]:
    data = _require_object(value, field_name)
    if any(not isinstance(item, str) for item in data.values()):
        raise TypeError(f"{field_name} values must be strings")
    return cast(dict[str, str], data)
