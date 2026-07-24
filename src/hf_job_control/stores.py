"""Storage interfaces and Hugging Face implementations."""

from __future__ import annotations

import hashlib
import shutil
import threading
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from huggingface_hub import CommitOperationAdd, HfApi, HfFileSystem, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from hf_job_control.models import (
    AppliedControlReceipt,
    ArtifactRef,
    ControlDocument,
    ControlSnapshot,
    LaunchSpec,
    PublishedDocument,
    RunStatus,
    parse_json_object,
    sha256_bytes,
    stable_json_bytes,
    utc_now,
    validate_repo_id,
    validate_run_id,
)

DEFAULT_REVISION = "main"


class ControlStore(Protocol):
    """Read and publish desired state."""

    def fetch(self, run_id: str) -> ControlSnapshot:
        """Read control from one exact store revision."""

    def publish(self, control: ControlDocument, *, expected_generation: int) -> ControlSnapshot:
        """Publish the next control generation with optimistic concurrency."""

    def register_launch_spec(self, run_id: str, spec: LaunchSpec) -> PublishedDocument:
        """Register or verify the immutable launch specification."""


class StatusStore(Protocol):
    """Read and publish observed run state and receipts."""

    def fetch_status(self, run_id: str) -> RunStatus | None:
        """Read the latest observed status."""

    def publish_status(self, status: RunStatus) -> PublishedDocument:
        """Publish the latest observed status."""

    def publish_receipt(self, receipt: AppliedControlReceipt) -> PublishedDocument:
        """Publish one immutable applied-control receipt."""


class BucketFileSystem(Protocol):
    """Typed portion of HfFileSystem used by the Bucket store."""

    def exists(self, path: str) -> bool:
        """Return whether a Bucket object exists."""

    def open(self, path: str, mode: str) -> BinaryIO:
        """Open a Bucket object."""


class ArtifactStore(Protocol):
    """Content-addressed checkpoint storage."""

    @property
    def bucket_id(self) -> str:
        """Return the stable Bucket identity."""

    def put_checkpoint(self, run_id: str, source: Path) -> ArtifactRef:
        """Upload one checkpoint without overwriting existing content."""

    def get_checkpoint(self, reference: ArtifactRef, destination: Path) -> None:
        """Download and verify one checkpoint."""


def control_path(run_id: str) -> str:
    """Return the control path for a logical run."""

    return f"controls/{validate_run_id(run_id)}.json"


def launch_spec_path(run_id: str) -> str:
    """Return the immutable launch specification path for a logical run."""

    return f"launch-specs/{validate_run_id(run_id)}.json"


def status_path(prefix: str, run_id: str) -> str:
    """Return the mutable status path for a logical run."""

    return f"{prefix.rstrip('/')}/{validate_run_id(run_id)}/status.json"


def receipt_path(prefix: str, receipt: AppliedControlReceipt) -> str:
    """Return the immutable path for one applied-control receipt."""

    return (
        f"{prefix.rstrip('/')}/{receipt.run_id}/attempts/{receipt.attempt_id}/receipts/"
        f"generation-{receipt.generation:08d}.json"
    )


class HubControlStore:
    """Desired-state store backed by a Hugging Face dataset repository."""

    def __init__(
        self,
        repo_id: str,
        *,
        revision: str = DEFAULT_REVISION,
        token: str | bool | None = None,
        api: HfApi | None = None,
    ) -> None:
        self.repo_id = validate_repo_id(repo_id)
        self.revision = revision
        self.token = token
        self.api = api or HfApi(token=token)

    def fetch(self, run_id: str) -> ControlSnapshot:
        _head, snapshot = self._fetch_optional(run_id)
        if snapshot is None:
            raise ValueError(f"missing control document for {run_id}")
        return snapshot

    def publish(self, control: ControlDocument, *, expected_generation: int) -> ControlSnapshot:
        head, current = self._fetch_optional(control.run_id)
        actual = 0 if current is None else current.control.generation
        if actual != expected_generation:
            raise RuntimeError(f"expected generation {expected_generation}, found {actual}")
        if control.generation != expected_generation + 1:
            raise ValueError(
                "control generation must be exactly one greater than expected_generation"
            )
        raw = stable_json_bytes(control.to_dict())
        commit = self.api.create_commit(
            repo_id=self.repo_id,
            repo_type="dataset",
            revision=self.revision,
            parent_commit=head,
            operations=[
                CommitOperationAdd(path_in_repo=control_path(control.run_id), path_or_fileobj=raw)
            ],
            commit_message=(
                f"control({control.run_id}): generation {control.generation} {control.action.value}"
            ),
            commit_description=control.reason,
        )
        return ControlSnapshot(
            repo_id=self.repo_id,
            revision=str(commit.oid),
            path=control_path(control.run_id),
            sha256=sha256_bytes(raw),
            observed_at=utc_now(),
            control=control,
        )

    def register_launch_spec(self, run_id: str, spec: LaunchSpec) -> PublishedDocument:
        path = launch_spec_path(run_id)
        raw = stable_json_bytes(spec.to_dict())
        for attempt in range(3):
            head = str(
                self.api.repo_info(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    revision=self.revision,
                ).sha
            )
            if self.api.file_exists(
                repo_id=self.repo_id,
                repo_type="dataset",
                filename=path,
                revision=head,
            ):
                existing = Path(
                    hf_hub_download(
                        repo_id=self.repo_id,
                        repo_type="dataset",
                        filename=path,
                        revision=head,
                        token=self.token,
                    )
                ).read_bytes()
                if existing != raw:
                    raise RuntimeError(f"immutable launch specification differs for run {run_id}")
                return PublishedDocument(
                    repo_id=self.repo_id,
                    revision=head,
                    path=path,
                    sha256=sha256_bytes(raw),
                )
            try:
                commit = self.api.create_commit(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    revision=self.revision,
                    parent_commit=head,
                    operations=[CommitOperationAdd(path_in_repo=path, path_or_fileobj=raw)],
                    commit_message=f"launch({run_id}): register immutable specification",
                )
            except RuntimeError:
                if attempt == 2:
                    raise
                continue
            return PublishedDocument(
                repo_id=self.repo_id,
                revision=str(commit.oid),
                path=path,
                sha256=sha256_bytes(raw),
            )
        raise AssertionError("unreachable")

    def _fetch_optional(self, run_id: str) -> tuple[str, ControlSnapshot | None]:
        path = control_path(run_id)
        head = str(
            self.api.repo_info(
                repo_id=self.repo_id,
                repo_type="dataset",
                revision=self.revision,
            ).sha
        )
        try:
            local = Path(
                hf_hub_download(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    filename=path,
                    revision=head,
                    token=self.token,
                )
            )
        except EntryNotFoundError:
            return head, None
        raw = local.read_bytes()
        control = ControlDocument.from_dict(parse_json_object(raw), expected_run_id=run_id)
        return head, ControlSnapshot(
            repo_id=self.repo_id,
            revision=head,
            path=path,
            sha256=sha256_bytes(raw),
            observed_at=utc_now(),
            control=control,
        )


class HubStatusStore:
    """Observed-state store backed by a Hugging Face dataset repository."""

    def __init__(
        self,
        repo_id: str,
        *,
        prefix: str = "runs",
        revision: str = DEFAULT_REVISION,
        token: str | bool | None = None,
        api: HfApi | None = None,
    ) -> None:
        self.repo_id = validate_repo_id(repo_id)
        self.prefix = prefix.strip("/")
        if not self.prefix:
            raise ValueError("prefix must be non-empty")
        self.revision = revision
        self.token = token
        self.api = api or HfApi(token=token)

    def fetch_status(self, run_id: str) -> RunStatus | None:
        path = status_path(self.prefix, run_id)
        head = str(
            self.api.repo_info(
                repo_id=self.repo_id, repo_type="dataset", revision=self.revision
            ).sha
        )
        try:
            local = Path(
                hf_hub_download(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    filename=path,
                    revision=head,
                    token=self.token,
                )
            )
        except EntryNotFoundError:
            return None
        return RunStatus.from_dict(parse_json_object(local.read_bytes()))

    def publish_status(self, status: RunStatus) -> PublishedDocument:
        return self._commit_document(
            path=status_path(self.prefix, status.run_id),
            raw=stable_json_bytes(status.to_dict()),
            message=f"status({status.run_id}): {status.state.value}",
            immutable=False,
        )

    def publish_receipt(self, receipt: AppliedControlReceipt) -> PublishedDocument:
        return self._commit_document(
            path=receipt_path(self.prefix, receipt),
            raw=stable_json_bytes(receipt.to_dict()),
            message=(
                f"receipt({receipt.run_id}): generation {receipt.generation} {receipt.action.value}"
            ),
            immutable=True,
        )

    def _commit_document(
        self,
        *,
        path: str,
        raw: bytes,
        message: str,
        immutable: bool,
    ) -> PublishedDocument:
        for attempt in range(3):
            head = str(
                self.api.repo_info(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    revision=self.revision,
                ).sha
            )
            if immutable and self.api.file_exists(
                repo_id=self.repo_id,
                repo_type="dataset",
                filename=path,
                revision=head,
            ):
                existing = Path(
                    hf_hub_download(
                        repo_id=self.repo_id,
                        repo_type="dataset",
                        filename=path,
                        revision=head,
                        token=self.token,
                    )
                ).read_bytes()
                if existing != raw:
                    raise RuntimeError(
                        f"immutable document already exists with different content: {path}"
                    )
                return PublishedDocument(
                    repo_id=self.repo_id,
                    revision=head,
                    path=path,
                    sha256=sha256_bytes(raw),
                )
            try:
                commit = self.api.create_commit(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    revision=self.revision,
                    parent_commit=head,
                    operations=[CommitOperationAdd(path_in_repo=path, path_or_fileobj=raw)],
                    commit_message=message,
                )
            except RuntimeError:
                if attempt == 2:
                    raise
                continue
            return PublishedDocument(
                repo_id=self.repo_id,
                revision=str(commit.oid),
                path=path,
                sha256=sha256_bytes(raw),
            )
        raise AssertionError("unreachable")


class HubBucketArtifactStore:
    """Content-addressed checkpoint storage in a Hugging Face Bucket."""

    def __init__(self, bucket_id: str, *, token: str | bool | None = None) -> None:
        self._bucket_id = validate_repo_id(bucket_id, "bucket_id")
        self.fs = cast(BucketFileSystem, HfFileSystem(token=token))

    @property
    def bucket_id(self) -> str:
        return self._bucket_id

    def put_checkpoint(self, run_id: str, source: Path) -> ArtifactRef:
        validate_run_id(run_id)
        digest, size = _hash_file(source)
        key = f"{run_id}/checkpoints/sha256-{digest}/checkpoint.hfjob"
        remote = self._remote_path(key)
        if self.fs.exists(remote):
            with self.fs.open(remote, "rb") as existing:
                existing_digest, existing_size = _hash_stream(existing)
            if (existing_digest, existing_size) != (digest, size):
                raise RuntimeError(f"content-addressed Bucket object differs: {key}")
        else:
            with source.open("rb") as local, self.fs.open(remote, "wb") as destination:
                shutil.copyfileobj(local, destination, length=1024 * 1024)
        return ArtifactRef(bucket=self.bucket_id, key=key, sha256=digest, bytes=size)

    def get_checkpoint(self, reference: ArtifactRef, destination: Path) -> None:
        if reference.bucket != self.bucket_id:
            raise ValueError(
                f"artifact bucket mismatch: {reference.bucket!r} != {self.bucket_id!r}"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        with (
            self.fs.open(self._remote_path(reference.key), "rb") as source,
            destination.open("wb") as local,
        ):
            digest, size = _copy_and_hash(source, local)
        _verify_digest(digest, size, reference)

    def _remote_path(self, key: str) -> str:
        return f"hf://buckets/{self.bucket_id}/{key}"


class LocalArtifactStore:
    """Filesystem artifact store for tests and local jobs."""

    def __init__(self, root: Path, *, bucket_id: str = "local/artifacts") -> None:
        self.root = root
        self._bucket_id = validate_repo_id(bucket_id, "bucket_id")

    @property
    def bucket_id(self) -> str:
        return self._bucket_id

    def put_checkpoint(self, run_id: str, source: Path) -> ArtifactRef:
        digest, size = _hash_file(source)
        key = f"{validate_run_id(run_id)}/checkpoints/sha256-{digest}/checkpoint.hfjob"
        destination = self.root / key
        if destination.exists() and _hash_file(destination) != (digest, size):
            raise RuntimeError(f"content-addressed object differs: {key}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            shutil.copyfile(source, destination)
        return ArtifactRef(bucket=self.bucket_id, key=key, sha256=digest, bytes=size)

    def get_checkpoint(self, reference: ArtifactRef, destination: Path) -> None:
        if reference.bucket != self.bucket_id:
            raise ValueError("artifact bucket mismatch")
        source = self.root / reference.key
        digest, size = _hash_file(source)
        _verify_digest(digest, size, reference)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


class MemoryControlStore:
    """Thread-safe control store for deterministic tests."""

    def __init__(self) -> None:
        self._controls: dict[str, ControlDocument] = {}
        self._launch_specs: dict[str, LaunchSpec] = {}
        self._counter = 0
        self._lock = threading.Lock()

    def fetch(self, run_id: str) -> ControlSnapshot:
        with self._lock:
            control = self._controls.get(run_id)
            if control is None:
                raise ValueError(f"missing control document for {run_id}")
            return self._snapshot(control)

    def publish(self, control: ControlDocument, *, expected_generation: int) -> ControlSnapshot:
        with self._lock:
            current = self._controls.get(control.run_id)
            actual = 0 if current is None else current.generation
            if actual != expected_generation:
                raise RuntimeError(f"expected generation {expected_generation}, found {actual}")
            if control.generation != expected_generation + 1:
                raise ValueError("control generation must advance by one")
            self._controls[control.run_id] = control
            self._counter += 1
            return self._snapshot(control)

    def register_launch_spec(self, run_id: str, spec: LaunchSpec) -> PublishedDocument:
        validate_run_id(run_id)
        with self._lock:
            existing = self._launch_specs.get(run_id)
            if existing is not None and existing != spec:
                raise RuntimeError(f"immutable launch specification differs for run {run_id}")
            self._launch_specs[run_id] = spec
            self._counter += 1
            raw = stable_json_bytes(spec.to_dict())
            return PublishedDocument(
                repo_id="memory/control",
                revision=f"{self._counter:040x}",
                path=launch_spec_path(run_id),
                sha256=sha256_bytes(raw),
            )

    def _snapshot(self, control: ControlDocument) -> ControlSnapshot:
        raw = stable_json_bytes(control.to_dict())
        return ControlSnapshot(
            repo_id="memory/control",
            revision=f"{self._counter:040x}",
            path=control_path(control.run_id),
            sha256=sha256_bytes(raw),
            observed_at=utc_now(),
            control=control,
        )


class MemoryStatusStore:
    """In-memory observed state and receipt sink for tests."""

    def __init__(self) -> None:
        self.statuses: dict[str, RunStatus] = {}
        self.receipts: list[AppliedControlReceipt] = []
        self._counter = 0

    def fetch_status(self, run_id: str) -> RunStatus | None:
        return self.statuses.get(run_id)

    def publish_status(self, status: RunStatus) -> PublishedDocument:
        self.statuses[status.run_id] = status
        return self._published(
            status_path("runs", status.run_id), stable_json_bytes(status.to_dict())
        )

    def publish_receipt(self, receipt: AppliedControlReceipt) -> PublishedDocument:
        matching = [
            item
            for item in self.receipts
            if item.run_id == receipt.run_id
            and item.attempt_id == receipt.attempt_id
            and item.generation == receipt.generation
        ]
        if matching and matching[0] != receipt:
            raise RuntimeError("immutable receipt differs")
        if not matching:
            self.receipts.append(receipt)
        return self._published(receipt_path("runs", receipt), stable_json_bytes(receipt.to_dict()))

    def _published(self, path: str, raw: bytes) -> PublishedDocument:
        self._counter += 1
        return PublishedDocument(
            repo_id="memory/status",
            revision=f"{self._counter:040x}",
            path=path,
            sha256=sha256_bytes(raw),
        )


def _hash_file(path: Path) -> tuple[str, int]:
    with path.open("rb") as source:
        return _hash_stream(source)


def _hash_stream(source: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _copy_and_hash(source: BinaryIO, destination: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(1024 * 1024):
        destination.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _verify_digest(digest: str, size: int, reference: ArtifactRef) -> None:
    if size != reference.bytes:
        raise ValueError("artifact byte count mismatch")
    if digest != reference.sha256:
        raise ValueError("artifact SHA-256 mismatch")
