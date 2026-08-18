from __future__ import annotations

import fnmatch
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hf_job_control.models import ArtifactRef, parse_json_object, stable_json_bytes
from hf_job_control.progress import (
    HubBucketProgressStore,
    LocalProgressStore,
    MemoryProgressStore,
    ProgressClaim,
    ProgressInput,
    ProgressPointer,
    ProgressReporter,
    ProgressSnapshot,
    ProgressStatus,
    ProgressTrack,
    progress_claim_key,
)

NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
INPUT = ProgressInput(revision="a" * 40, contract_sha256="b" * 64)


def track(
    completed: int,
    *,
    plan_id: str = "plan-1",
    total: int = 10,
    status: ProgressStatus = ProgressStatus.RUNNING,
) -> ProgressTrack:
    return ProgressTrack(
        key="items",
        plan_id=plan_id,
        label="Items",
        status=status,
        completed=completed,
        total=total,
        unit="items",
        source_updated_at=NOW,
    )


def test_cross_language_fixture_is_canonical() -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "progress-v1.json"
    raw = fixture.read_bytes()
    snapshot = ProgressSnapshot.from_dict(parse_json_object(raw))
    assert stable_json_bytes(snapshot.to_dict()) == raw


def test_progress_snapshot_round_trip() -> None:
    snapshot = ProgressSnapshot(
        run_id="run",
        attempt_id="attempt-1",
        job_id="job-1",
        sequence=1,
        updated_at=NOW,
        input=INPUT,
        state=ProgressStatus.RUNNING,
        tracks=(track(2),),
    )

    assert ProgressSnapshot.from_dict(snapshot.to_dict()) == snapshot


def test_progress_track_requires_consistent_counts() -> None:
    with pytest.raises(ValueError, match="unit is required"):
        ProgressTrack(
            key="items",
            plan_id="plan-1",
            status=ProgressStatus.RUNNING,
            completed=1,
        )
    with pytest.raises(ValueError, match="must not exceed"):
        track(11)
    with pytest.raises(ValueError, match="must reach"):
        track(9, status=ProgressStatus.COMPLETED)


def test_reporter_publishes_ordered_snapshots_and_throttles() -> None:
    store = MemoryProgressStore()
    moments = iter([NOW, NOW + timedelta(seconds=10), NOW + timedelta(seconds=31)])
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: next(moments),
    )
    reporter.plan([track(1)])
    first = reporter.flush(force=True)
    assert first is not None
    assert first.snapshot.sequence == 1

    reporter.update(track(2))
    assert reporter.flush() is None
    second = reporter.flush()
    assert second is not None
    assert second.snapshot.sequence == 2
    assert second.snapshot.previous == first.reference
    assert store.load_latest("run") == second


def test_reporter_rejects_regression_but_accepts_new_plan() -> None:
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=MemoryProgressStore(),
        clock=lambda: NOW,
    )
    reporter.plan([track(5)])
    with pytest.raises(ValueError, match="backwards"):
        reporter.update(track(4))
    with pytest.raises(ValueError, match="total cannot change"):
        reporter.update(track(5, total=11))
    added = ProgressTrack(
        key="later",
        plan_id="plan-1",
        status=ProgressStatus.PENDING,
    )
    with pytest.raises(ValueError, match="backwards"):
        reporter.plan([added, track(4)])
    assert [item.key for item in reporter.tracks] == ["items"]

    reporter.update(track(0, plan_id="plan-2", total=20))
    assert reporter.tracks == (track(0, plan_id="plan-2", total=20),)


def test_competing_reporters_cannot_overwrite_the_same_sequence() -> None:
    store = MemoryProgressStore()
    reporters = [
        ProgressReporter(
            run_id="run",
            attempt_id=f"attempt-{index}",
            input=INPUT,
            store=store,
            clock=lambda: NOW,
        )
        for index in (1, 2)
    ]
    for reporter in reporters:
        reporter.plan([track(1)])

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(reporter.flush, force=True) for reporter in reporters]
    results: list[object] = []
    for future in futures:
        try:
            results.append(future.result())
        except ValueError as error:
            results.append(error)

    assert sum(isinstance(result, ValueError) for result in results) == 1
    latest = store.load_latest("run")
    assert latest is not None
    assert latest.snapshot.sequence == 1


def test_reporter_restores_committed_progress_for_new_attempt() -> None:
    store = MemoryProgressStore()
    first = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    first.plan([track(6)])
    published = first.flush(force=True)
    assert published is not None

    second = ProgressReporter(
        run_id="run",
        attempt_id="attempt-2",
        job_id="job-2",
        input=INPUT,
        store=store,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    assert second.tracks == (track(6),)
    second.update(track(7))
    resumed = second.flush(force=True)
    assert resumed is not None
    assert resumed.snapshot.sequence == 2
    assert resumed.snapshot.attempt_id == "attempt-2"
    assert resumed.snapshot.previous == published.reference


def test_new_input_starts_new_tracks_but_keeps_sequence_chain() -> None:
    store = MemoryProgressStore()
    first = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    first.plan([track(10, status=ProgressStatus.COMPLETED)])
    published = first.flush(force=True)
    assert published is not None

    changed = ProgressReporter(
        run_id="run",
        attempt_id="attempt-2",
        input=ProgressInput(revision="c" * 40, contract_sha256="b" * 64),
        store=store,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    assert changed.tracks == ()
    changed.plan([track(0, plan_id="plan-2")])
    current = changed.flush(force=True)
    assert current is not None
    assert current.snapshot.sequence == 2
    assert current.snapshot.previous == published.reference


def test_terminal_state_is_preserved_after_restart() -> None:
    store = MemoryProgressStore()
    first = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    first.plan([track(10, status=ProgressStatus.COMPLETED)])
    first.set_state(ProgressStatus.COMPLETED)
    assert first.flush(force=True) is not None

    replacement = ProgressReporter(
        run_id="run",
        attempt_id="attempt-2",
        input=INPUT,
        store=store,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    assert replacement.flush(force=True) is None
    with pytest.raises(ValueError, match="terminal progress state"):
        replacement.set_state(ProgressStatus.RUNNING)


def test_terminal_track_and_run_cannot_reopen() -> None:
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=MemoryProgressStore(),
        clock=lambda: NOW,
    )
    completed = track(10, status=ProgressStatus.COMPLETED)
    reporter.plan([completed])
    with pytest.raises(ValueError, match="terminal progress track"):
        reporter.update(track(10, status=ProgressStatus.RUNNING))
    reporter.set_state(ProgressStatus.COMPLETED)
    with pytest.raises(ValueError, match="terminal progress state"):
        reporter.set_state(ProgressStatus.RUNNING)


def test_local_store_verifies_pointer_and_snapshot(tmp_path: Path) -> None:
    store = LocalProgressStore(tmp_path)
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    reporter.plan([track(3)])
    stored = reporter.flush(force=True)
    assert stored is not None
    assert store.load_reference(stored.reference) == stored.snapshot

    snapshot_path = tmp_path / stored.reference.key
    snapshot_path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="byte count mismatch"):
        store.load_latest("run")


def test_pointer_metadata_must_match_snapshot() -> None:
    store = MemoryProgressStore()
    reporter = ProgressReporter(
        run_id="other-run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    reporter.plan([track(1)])
    stored = reporter.flush(force=True)
    assert stored is not None
    store.pointers["run"] = ProgressPointer(
        run_id="run",
        sequence=stored.snapshot.sequence,
        updated_at=stored.snapshot.updated_at,
        snapshot=stored.reference,
    )

    with pytest.raises(ValueError, match="snapshot run_id mismatch"):
        store.load_latest("run")


def test_orphan_sequence_claim_restores_missing_pointer() -> None:
    store = MemoryProgressStore()
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    reporter.plan([track(1)])
    stored = reporter.flush(force=True)
    assert stored is not None
    del store.pointers["run"]

    assert store.load_latest("run") == stored
    assert store.pointers["run"].snapshot == stored.reference


def test_competing_sequence_claims_are_rejected() -> None:
    store = MemoryProgressStore()
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    reporter.plan([track(1)])
    stored = reporter.flush(force=True)
    assert stored is not None
    competing = ProgressClaim(
        run_id="run",
        attempt_id="attempt-2",
        sequence=1,
        created_at=NOW,
        snapshot=stored.reference,
    )
    store.claims[progress_claim_key("", competing)] = stable_json_bytes(competing.to_dict())

    with pytest.raises(RuntimeError, match="competing progress sequence claims"):
        store.load_latest("run")


def test_store_rejects_out_of_order_snapshot() -> None:
    store = MemoryProgressStore()
    snapshot = ProgressSnapshot(
        run_id="run",
        attempt_id="attempt-1",
        sequence=2,
        updated_at=NOW,
        input=INPUT,
        state=ProgressStatus.RUNNING,
        tracks=(track(1),),
    )
    with pytest.raises(ValueError, match="first progress sequence"):
        store.publish(snapshot)


class MemoryBucketFileSystem:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.files

    def glob(self, path: str) -> list[str]:
        return sorted(key for key in self.files if fnmatch.fnmatch(key, path))

    def open(self, path: str, mode: str) -> io.BytesIO:
        if mode == "rb":
            if path not in self.files:
                raise FileNotFoundError(path)
            return io.BytesIO(self.files[path])
        if mode != "wb":
            raise ValueError(mode)
        filesystem = self

        class Writer(io.BytesIO):
            def close(self) -> None:
                filesystem.files[path] = self.getvalue()
                super().close()

        return Writer()


def test_hub_bucket_store_round_trip() -> None:
    filesystem = MemoryBucketFileSystem()
    store = HubBucketProgressStore(
        "owner/bucket",
        prefix="project",
        filesystem=filesystem,
    )
    reporter = ProgressReporter(
        run_id="run",
        attempt_id="attempt-1",
        input=INPUT,
        store=store,
        clock=lambda: NOW,
    )
    reporter.plan([track(4)])
    stored = reporter.flush(force=True)
    assert stored is not None
    assert store.load_latest("run") == stored


def test_reference_bucket_must_match_store() -> None:
    store = MemoryProgressStore()
    reference = ArtifactRef(
        bucket="other/bucket",
        key=f"snapshots/sha256-{'a' * 64}/progress.json",
        sha256="a" * 64,
        bytes=1,
    )
    with pytest.raises(ValueError, match="Bucket mismatch"):
        store.load_reference(reference)
