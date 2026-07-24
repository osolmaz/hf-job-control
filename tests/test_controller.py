from __future__ import annotations

from pathlib import Path

import pytest

from hf_job_control.canary import CounterAdapter
from hf_job_control.controller import Controller, ControllerConfig
from hf_job_control.models import (
    Action,
    AdapterSpec,
    ArtifactRef,
    Boundary,
    ControlDocument,
    ControlSnapshot,
    JsonObject,
    LaunchSpec,
    PublishedDocument,
    ResumeMode,
    RunState,
)
from hf_job_control.stores import (
    LocalArtifactStore,
    MemoryControlStore,
    MemoryStatusStore,
)


def publish(
    store: MemoryControlStore,
    run_id: str,
    generation: int,
    action: Action,
    *,
    resume_from: ArtifactRef | None = None,
) -> None:
    store.publish(
        ControlDocument(
            run_id=run_id,
            generation=generation,
            action=action,
            resume_from=resume_from,
        ),
        expected_generation=generation - 1,
    )


def controller(
    *,
    attempt_id: str,
    controls: MemoryControlStore,
    statuses: MemoryStatusStore,
    artifacts: LocalArtifactStore,
) -> Controller:
    return Controller(
        ControllerConfig(
            run_id="run",
            attempt_id=attempt_id,
            control_attempts=1,
            retry_delay_seconds=0,
        ),
        control_store=controls,
        status_store=statuses,
        artifact_store=artifacts,
    )


def test_pause_resume_and_stop_preserve_state(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    publish(controls, "run", 1, Action.RUN)

    first_adapter = CounterAdapter()
    first = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    assert not first.start(first_adapter).resumed
    first_adapter.value = 7
    assert not first.boundary(
        boundary=Boundary(name="counter", sequence=7),
        adapter=first_adapter,
    ).should_exit

    publish(controls, "run", 2, Action.PAUSE)
    first_adapter.value = 8
    pause = first.boundary(
        boundary=Boundary(name="counter", sequence=8),
        adapter=first_adapter,
    )
    assert pause.action is Action.PAUSE
    assert pause.should_exit
    assert first.finish(pause).state is RunState.PAUSED
    paused = statuses.fetch_status("run")
    assert paused is not None
    assert paused.checkpoint is not None

    publish(controls, "run", 3, Action.RUN, resume_from=paused.checkpoint)
    second_adapter = CounterAdapter()
    second = controller(
        attempt_id="attempt-2",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    start = second.start(second_adapter)
    assert start.resumed
    assert second_adapter.value == 8
    assert start.boundary is not None
    assert start.boundary.sequence == 8

    publish(controls, "run", 4, Action.STOP)
    second_adapter.value += 1
    stop = second.boundary(
        boundary=Boundary(name="counter", sequence=9),
        adapter=second_adapter,
    )
    assert stop.action is Action.STOP
    assert stop.exit_code == 0
    assert second.finish(stop).state is RunState.COMPLETED

    assert [(item.generation, item.action) for item in statuses.receipts] == [
        (1, Action.RUN),
        (2, Action.PAUSE),
        (3, Action.RUN),
        (4, Action.STOP),
    ]


def test_abort_returns_error_exit_code(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    publish(controls, "run", 1, Action.RUN)
    worker = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    adapter = CounterAdapter()
    worker.start(adapter)
    publish(controls, "run", 2, Action.ABORT)

    decision = worker.boundary(
        boundary=Boundary(name="counter", sequence=1),
        adapter=adapter,
    )

    assert decision.exit_code == 1
    assert worker.finish(decision).state is RunState.ABORTED


def test_metric_sink_failure_does_not_break_control(tmp_path: Path) -> None:
    class BrokenMetrics:
        def publish(self, boundary: Boundary, metrics: JsonObject) -> None:
            del boundary, metrics
            raise OSError("wandb offline")

    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    publish(controls, "run", 1, Action.RUN)
    worker = Controller(
        ControllerConfig(run_id="run", attempt_id="attempt-1"),
        control_store=controls,
        status_store=statuses,
        artifact_store=LocalArtifactStore(tmp_path),
        metric_sink=BrokenMetrics(),
    )
    adapter = CounterAdapter()
    worker.start(adapter)

    decision = worker.boundary(
        boundary=Boundary(name="counter", sequence=1),
        adapter=adapter,
        metrics={"loss": 0.5},
    )

    assert not decision.should_exit
    status = statuses.fetch_status("run")
    assert status is not None
    assert status.metrics == {"loss": 0.5}
    assert status.message == "metric sink failed: wandb offline"


def test_repeated_run_generation_is_idempotent(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    publish(controls, "run", 1, Action.RUN)
    worker = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    adapter = CounterAdapter()
    worker.start(adapter)

    for sequence in (1, 2):
        decision = worker.boundary(
            boundary=Boundary(name="counter", sequence=sequence),
            adapter=adapter,
        )
        assert not decision.should_exit

    assert len(statuses.receipts) == 1


def test_start_rejects_replayed_generation(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    publish(controls, "run", 1, Action.RUN)
    first = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    first.start(CounterAdapter())
    replay = controller(
        attempt_id="attempt-2",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )

    with pytest.raises(RuntimeError, match="newer than observed status"):
        replay.start(CounterAdapter())


def test_start_rejects_non_run_control(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    publish(controls, "run", 1, Action.PAUSE)
    worker = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=MemoryStatusStore(),
        artifacts=LocalArtifactStore(tmp_path),
    )

    with pytest.raises(RuntimeError, match="publish run first"):
        worker.start(CounterAdapter())


def test_control_failure_pauses_after_checkpoint(tmp_path: Path) -> None:
    class FailingAfterStart:
        def __init__(self, wrapped: MemoryControlStore) -> None:
            self.wrapped = wrapped
            self.calls = 0

        def fetch(self, run_id: str) -> ControlSnapshot:
            self.calls += 1
            if self.calls > 1:
                raise OSError("offline")
            return self.wrapped.fetch(run_id)

        def publish(
            self,
            control: ControlDocument,
            *,
            expected_generation: int,
        ) -> ControlSnapshot:
            return self.wrapped.publish(control, expected_generation=expected_generation)

        def register_launch_spec(
            self,
            run_id: str,
            spec: LaunchSpec,
        ) -> PublishedDocument:
            return self.wrapped.register_launch_spec(run_id, spec)

    controls = MemoryControlStore()
    publish(controls, "run", 1, Action.RUN)
    statuses = MemoryStatusStore()
    worker = Controller(
        ControllerConfig(
            run_id="run",
            attempt_id="attempt-1",
            control_attempts=1,
            retry_delay_seconds=0,
        ),
        control_store=FailingAfterStart(controls),
        status_store=statuses,
        artifact_store=LocalArtifactStore(tmp_path),
    )
    adapter = CounterAdapter()
    worker.start(adapter)

    decision = worker.boundary(
        boundary=Boundary(name="counter", sequence=1),
        adapter=adapter,
    )

    assert decision.action is Action.PAUSE
    assert worker.finish(decision).state is RunState.PAUSED


def test_controller_config_rejects_invalid_retry_settings() -> None:
    with pytest.raises(ValueError, match="control_attempts"):
        ControllerConfig(run_id="run", attempt_id="attempt-1", control_attempts=0)
    with pytest.raises(ValueError, match="retry_delay"):
        ControllerConfig(run_id="run", attempt_id="attempt-1", retry_delay_seconds=-1)


def test_controller_config_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUN_ID", "run")
    monkeypatch.setenv("ATTEMPT_ID", "attempt-1")

    assert ControllerConfig.from_environment() == ControllerConfig(
        run_id="run",
        attempt_id="attempt-1",
    )


def test_start_rejects_resume_for_restart_adapter(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    publish(controls, "run", 1, Action.RUN)
    original = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    adapter = CounterAdapter(value=3)
    original.start(adapter)
    original.boundary(boundary=Boundary(name="counter", sequence=3), adapter=adapter)
    status = statuses.fetch_status("run")
    assert status is not None
    assert status.checkpoint is not None
    publish(controls, "run", 2, Action.PAUSE)
    publish(controls, "run", 3, Action.RUN, resume_from=status.checkpoint)

    class RestartAdapter(CounterAdapter):
        @property
        def spec(self) -> AdapterSpec:
            return AdapterSpec(name="counter", version=1, resume_mode=ResumeMode.RESTART)

    restarted = controller(
        attempt_id="attempt-2",
        controls=controls,
        statuses=statuses,
        artifacts=artifacts,
    )
    with pytest.raises(RuntimeError, match="does not support checkpoint resume"):
        restarted.start(RestartAdapter())


def test_unsupported_adapter_rejects_pause(tmp_path: Path) -> None:
    class UnsupportedAdapter(CounterAdapter):
        @property
        def spec(self) -> AdapterSpec:
            return AdapterSpec(name="counter", version=1, resume_mode=ResumeMode.UNSUPPORTED)

    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    publish(controls, "run", 1, Action.RUN)
    worker = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=statuses,
        artifacts=LocalArtifactStore(tmp_path),
    )
    adapter = UnsupportedAdapter()
    worker.start(adapter)
    publish(controls, "run", 2, Action.PAUSE)

    decision = worker.boundary(
        boundary=Boundary(name="counter", sequence=1),
        adapter=adapter,
    )

    assert decision.action is Action.PAUSE
    assert decision.exit_code == 1
    assert decision.target_state is RunState.FAILED
    assert statuses.receipts[-1].outcome == "rejected-unsupported"
    status = statuses.fetch_status("run")
    assert status is not None
    assert status.state is RunState.ABORTING
    assert "does not support" in (status.message or "")
    assert worker.finish(decision).state is RunState.FAILED


def test_boundary_requires_start(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    publish(controls, "run", 1, Action.RUN)
    worker = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=MemoryStatusStore(),
        artifacts=LocalArtifactStore(tmp_path),
    )

    with pytest.raises(RuntimeError, match="start"):
        worker.boundary(
            boundary=Boundary(name="counter", sequence=1),
            adapter=CounterAdapter(),
        )


def test_finish_rejects_continue_decision(tmp_path: Path) -> None:
    controls = MemoryControlStore()
    publish(controls, "run", 1, Action.RUN)
    worker = controller(
        attempt_id="attempt-1",
        controls=controls,
        statuses=MemoryStatusStore(),
        artifacts=LocalArtifactStore(tmp_path),
    )
    adapter = CounterAdapter()
    worker.start(adapter)
    decision = worker.boundary(
        boundary=Boundary(name="counter", sequence=1),
        adapter=adapter,
    )

    with pytest.raises(ValueError, match="continue decision"):
        worker.finish(decision)
