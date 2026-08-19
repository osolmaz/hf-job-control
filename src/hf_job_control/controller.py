"""Safe-boundary worker controller."""

from __future__ import annotations

import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from hf_job_control.checkpoint import CheckpointAdapter, create_bundle, restore_bundle
from hf_job_control.metrics import MetricSink, NullMetricSink
from hf_job_control.models import (
    Action,
    AppliedControlReceipt,
    ArtifactRef,
    Boundary,
    ControlError,
    ControlSnapshot,
    Decision,
    JsonObject,
    ResumeMode,
    RunState,
    RunStatus,
    StartResult,
    utc_now,
    validate_attempt_id,
    validate_job_id,
    validate_run_id,
    validate_sha256,
)
from hf_job_control.progress import ProgressSnapshot
from hf_job_control.stores import ArtifactStore, ControlStore, StatusStore


@dataclass(frozen=True, slots=True)
class ControllerConfig:
    """Identity and retry settings for one physical attempt."""

    run_id: str
    attempt_id: str
    plan_sha256: str
    job_id: str | None = None
    control_attempts: int = 3
    retry_delay_seconds: float = 2.0

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)
        validate_attempt_id(self.attempt_id)
        validate_sha256(self.plan_sha256, "plan_sha256")
        validate_job_id(self.job_id)
        if self.control_attempts < 1:
            raise ValueError("control_attempts must be >= 1")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be >= 0")

    @classmethod
    def from_environment(cls) -> ControllerConfig:
        """Build identity from the standard worker environment."""

        run_id = os.environ.get("RUN_ID")
        attempt_id = os.environ.get("ATTEMPT_ID")
        plan_sha256 = os.environ.get("PLAN_SHA256")
        if not run_id or not attempt_id or not plan_sha256:
            raise ValueError("RUN_ID, ATTEMPT_ID, and PLAN_SHA256 are required")
        return cls(
            run_id=run_id,
            attempt_id=attempt_id,
            plan_sha256=plan_sha256,
            job_id=os.environ.get("JOB_ID"),
        )


class Controller:
    """Coordinate control, checkpoints, receipts, and observed state."""

    def __init__(
        self,
        config: ControllerConfig,
        *,
        control_store: ControlStore,
        status_store: StatusStore,
        artifact_store: ArtifactStore,
        metric_sink: MetricSink | None = None,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.control_store = control_store
        self.status_store = status_store
        self.artifact_store = artifact_store
        self.metric_sink = metric_sink or NullMetricSink()
        self._clock = clock
        self._sleep = sleep
        previous = status_store.fetch_status(config.run_id)
        self._generation = 0 if previous is None else previous.last_applied_generation
        self._last_action = Action.RUN if previous is None else previous.last_action
        self._boundary = None if previous is None else previous.boundary
        self._checkpoint = None if previous is None else previous.checkpoint
        self._metrics = {} if previous is None else previous.metrics
        self._progress = None if previous is None else previous.progress
        self._started = False

    def start(self, adapter: CheckpointAdapter) -> StartResult:
        """Validate initial desired state and restore a requested checkpoint."""

        snapshot = self._fetch_control()
        control = snapshot.control
        if control.generation <= self._generation:
            raise ControlError(
                "start control generation must be newer than observed status "
                f"({control.generation} <= {self._generation})"
            )
        if control.action is not Action.RUN:
            raise ControlError(
                f"cannot start while desired action is {control.action.value}; publish run first"
            )
        resumed = False
        resume_evidence: JsonObject = {}
        boundary: Boundary | None = None
        if control.resume_from is not None:
            if adapter.spec.resume_mode.value in {"restart", "unsupported"}:
                raise ControlError(
                    f"adapter {adapter.spec.name} does not support checkpoint resume"
                )
            with tempfile.TemporaryDirectory(prefix="hf-job-control-start-") as temp_dir:
                bundle = Path(temp_dir) / "checkpoint.hfjob"
                self.artifact_store.get_checkpoint(control.resume_from, bundle)
                manifest, resume_evidence = restore_bundle(
                    bundle=bundle,
                    expected_run_id=self.config.run_id,
                    expected_plan_sha256=self.config.plan_sha256,
                    adapter=adapter,
                )
            resumed = True
            boundary = manifest.boundary
            self._boundary = boundary
            self._checkpoint = control.resume_from
        else:
            self._boundary = None
            self._checkpoint = None
        self._apply_snapshot(
            snapshot,
            boundary=boundary,
            checkpoint=self._checkpoint,
            outcome="resumed" if resumed else "started",
            evidence=resume_evidence,
        )
        self._publish_status(RunState.RUNNING)
        self._started = True
        return StartResult(
            resumed=resumed,
            generation=control.generation,
            checkpoint=control.resume_from,
            boundary=boundary,
            resume_evidence=resume_evidence,
        )

    def boundary(
        self,
        *,
        boundary: Boundary,
        adapter: CheckpointAdapter,
        metrics: JsonObject | None = None,
        progress: ProgressSnapshot | None = None,
    ) -> Decision:
        """Checkpoint one safe boundary, then apply the latest desired state."""

        if not self._started:
            raise RuntimeError("start() must be called before boundary()")
        self._update_progress(progress)
        current_metrics = {} if metrics is None else metrics
        metric_error: str | None = None
        try:
            self.metric_sink.publish(boundary, current_metrics)
        except Exception as error:  # Metric reporting must not break control.
            metric_error = f"metric sink failed: {error}"
        checkpoint = self._save_checkpoint(boundary, adapter)
        self._boundary = boundary
        self._checkpoint = checkpoint
        self._metrics = current_metrics
        self._publish_status(RunState.RUNNING, message=metric_error)
        try:
            snapshot = self._fetch_control()
        except (OSError, RuntimeError, ValueError) as error:
            decision = Decision(
                action=Action.PAUSE,
                generation=self._generation,
                should_exit=True,
                exit_code=0,
                target_state=RunState.PAUSED,
            )
            self._publish_status(RunState.PAUSING, message=f"control unavailable: {error}")
            return decision
        control = snapshot.control
        if control.generation < self._generation:
            self._publish_status(RunState.PAUSING, message="control generation moved backwards")
            return Decision(
                action=Action.PAUSE,
                generation=self._generation,
                should_exit=True,
                exit_code=0,
                target_state=RunState.PAUSED,
            )
        if control.generation == self._generation:
            if control.action is not self._last_action:
                self._publish_status(
                    RunState.PAUSING, message="control changed without a new generation"
                )
                return Decision(
                    action=Action.PAUSE,
                    generation=self._generation,
                    should_exit=True,
                    exit_code=0,
                    target_state=RunState.PAUSED,
                )
            return self._decision(control.action, control.generation)
        rejected_pause = (
            control.action is Action.PAUSE and adapter.spec.resume_mode is ResumeMode.UNSUPPORTED
        )
        decision = self._decision(
            control.action,
            control.generation,
            pause_supported=not rejected_pause,
        )
        self._apply_snapshot(
            snapshot,
            boundary=boundary,
            checkpoint=checkpoint,
            outcome="rejected-unsupported" if rejected_pause else "accepted",
        )
        if rejected_pause:
            self._publish_status(
                RunState.ABORTING,
                message=f"adapter {adapter.spec.name} does not support pause or resume",
            )
        else:
            self._publish_status(self._transition_state(control.action))
        return decision

    def finish(self, decision: Decision, *, message: str | None = None) -> RunStatus:
        """Record the final observed state after the adapter has finalized."""

        if not decision.should_exit:
            raise ValueError("cannot finish a continue decision")
        return self._publish_status(decision.target_state, message=message)

    def _save_checkpoint(self, boundary: Boundary, adapter: CheckpointAdapter) -> ArtifactRef:
        with tempfile.TemporaryDirectory(prefix="hf-job-control-boundary-") as temp_dir:
            bundle = Path(temp_dir) / "checkpoint.hfjob"
            create_bundle(
                destination=bundle,
                run_id=self.config.run_id,
                attempt_id=self.config.attempt_id,
                plan_sha256=self.config.plan_sha256,
                boundary=boundary,
                previous_checkpoint_sha256=(
                    None if self._checkpoint is None else self._checkpoint.sha256
                ),
                adapter=adapter,
            )
            return self.artifact_store.put_checkpoint(self.config.run_id, bundle)

    def _fetch_control(self) -> ControlSnapshot:
        last_error: Exception | None = None
        for attempt in range(self.config.control_attempts):
            try:
                return self.control_store.fetch(self.config.run_id)
            except (OSError, RuntimeError, ValueError) as error:
                last_error = error
                if attempt + 1 < self.config.control_attempts:
                    self._sleep(self.config.retry_delay_seconds)
        if last_error is None:
            raise RuntimeError("control fetch loop did not execute")
        raise ControlError(f"control fetch failed after retries: {last_error}") from last_error

    def _apply_snapshot(
        self,
        snapshot: ControlSnapshot,
        *,
        boundary: Boundary | None,
        checkpoint: ArtifactRef | None,
        outcome: str,
        evidence: JsonObject | None = None,
    ) -> None:
        control = snapshot.control
        receipt = AppliedControlReceipt(
            run_id=self.config.run_id,
            attempt_id=self.config.attempt_id,
            job_id=self.config.job_id,
            control_repo=snapshot.repo_id,
            control_revision=snapshot.revision,
            control_path=snapshot.path,
            control_sha256=snapshot.sha256,
            generation=control.generation,
            action=control.action,
            observed_at=snapshot.observed_at,
            applied_at=self._clock(),
            outcome=outcome,
            evidence={} if evidence is None else evidence,
            boundary=boundary,
            checkpoint=checkpoint,
        )
        self.status_store.publish_receipt(receipt)
        self._generation = control.generation
        self._last_action = control.action

    def _publish_status(self, state: RunState, *, message: str | None = None) -> RunStatus:
        status = RunStatus(
            run_id=self.config.run_id,
            attempt_id=self.config.attempt_id,
            job_id=self.config.job_id,
            state=state,
            updated_at=self._clock(),
            last_applied_generation=self._generation,
            last_action=self._last_action,
            boundary=self._boundary,
            checkpoint=self._checkpoint,
            metrics=self._metrics,
            progress=self._progress,
            message=message,
        )
        self.status_store.publish_status(status)
        return status

    def _update_progress(self, progress: ProgressSnapshot | None) -> None:
        if progress is None:
            return
        if progress.run_id != self.config.run_id:
            raise ValueError("progress run_id must match controller run_id")
        if progress.attempt_id != self.config.attempt_id:
            raise ValueError("progress attempt_id must match controller attempt_id")
        if progress.job_id is not None and progress.job_id != self.config.job_id:
            raise ValueError("progress job_id must match controller job_id")
        self._progress = progress

    @staticmethod
    def _decision(
        action: Action,
        generation: int,
        *,
        pause_supported: bool = True,
    ) -> Decision:
        if action is Action.RUN:
            return Decision(action, generation, False, 0, RunState.RUNNING)
        if action is Action.PAUSE:
            if not pause_supported:
                return Decision(action, generation, True, 1, RunState.FAILED)
            return Decision(action, generation, True, 0, RunState.PAUSED)
        if action is Action.STOP:
            return Decision(action, generation, True, 0, RunState.COMPLETED)
        return Decision(action, generation, True, 1, RunState.ABORTED)

    @staticmethod
    def _transition_state(action: Action) -> RunState:
        if action is Action.RUN:
            return RunState.RUNNING
        if action is Action.PAUSE:
            return RunState.PAUSING
        if action is Action.STOP:
            return RunState.STOPPING
        return RunState.ABORTING
