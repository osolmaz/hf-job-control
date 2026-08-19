from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from hf_job_control.controller import Controller, ControllerConfig
from hf_job_control.models import (
    Action,
    AdapterSpec,
    Boundary,
    CheckpointManifest,
    ControlDocument,
    JsonObject,
    ResumeMode,
)
from hf_job_control.stores import LocalArtifactStore, MemoryControlStore, MemoryStatusStore

PLAN_SHA256 = "a" * 64


@dataclass
class TrainingState:
    step: int = 0
    weight: int = 1000
    momentum: int = 0
    rng: int = 20260719

    def advance(self) -> None:
        self.rng = (1103515245 * self.rng + 12345) % (2**31)
        gradient = self.rng % 97 - 48
        self.momentum = 9 * self.momentum + gradient
        self.weight -= self.momentum
        self.step += 1


@dataclass
class TrainingAdapter:
    state: TrainingState

    @property
    def spec(self) -> AdapterSpec:
        return AdapterSpec(name="training-state", version=1, resume_mode=ResumeMode.EXACT)

    def save(self, destination: Path, boundary: Boundary) -> None:
        del boundary
        value = {
            "momentum": self.state.momentum,
            "rng": self.state.rng,
            "step": self.state.step,
            "weight": self.state.weight,
        }
        (destination / "state.json").write_text(json.dumps(value), encoding="utf-8")

    def restore(self, source: Path, manifest: CheckpointManifest) -> JsonObject:
        value = json.loads((source / "state.json").read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError("training checkpoint must be an object")
        self.state = TrainingState(
            step=int(value["step"]),
            weight=int(value["weight"]),
            momentum=int(value["momentum"]),
            rng=int(value["rng"]),
        )
        return {"step": manifest.boundary.sequence}


def make_controller(
    attempt_id: str,
    controls: MemoryControlStore,
    statuses: MemoryStatusStore,
    artifacts: LocalArtifactStore,
) -> Controller:
    return Controller(
        ControllerConfig(run_id="run", attempt_id=attempt_id, plan_sha256=PLAN_SHA256),
        control_store=controls,
        status_store=statuses,
        artifact_store=artifacts,
    )


def test_exact_resume_matches_uninterrupted_state(tmp_path: Path) -> None:
    reference = TrainingState()
    for _ in range(12):
        reference.advance()

    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path)
    controls.publish(
        ControlDocument(run_id="run", generation=1, action=Action.RUN),
        expected_generation=0,
    )
    first_adapter = TrainingAdapter(TrainingState())
    first = make_controller("attempt-1", controls, statuses, artifacts)
    first.start(first_adapter)
    for _ in range(6):
        first_adapter.state.advance()
    controls.publish(
        ControlDocument(run_id="run", generation=2, action=Action.PAUSE),
        expected_generation=1,
    )
    pause = first.boundary(
        boundary=Boundary(name="optimizer-step", sequence=6),
        adapter=first_adapter,
    )
    first.finish(pause)
    paused = statuses.fetch_status("run")
    assert paused is not None
    assert paused.checkpoint is not None

    controls.publish(
        ControlDocument(
            run_id="run",
            generation=3,
            action=Action.RUN,
            resume_from=paused.checkpoint,
        ),
        expected_generation=2,
    )
    second_adapter = TrainingAdapter(TrainingState())
    second = make_controller("attempt-2", controls, statuses, artifacts)
    second.start(second_adapter)
    for _ in range(6):
        second_adapter.state.advance()
    controls.publish(
        ControlDocument(run_id="run", generation=4, action=Action.STOP),
        expected_generation=3,
    )
    stop = second.boundary(
        boundary=Boundary(name="optimizer-step", sequence=12),
        adapter=second_adapter,
    )
    second.finish(stop)

    assert second_adapter.state == reference
