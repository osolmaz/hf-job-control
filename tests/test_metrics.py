from __future__ import annotations

from hf_job_control.metrics import NullMetricSink, WandbMetricSink
from hf_job_control.models import Boundary


class FakeRun:
    def __init__(self) -> None:
        self.data: dict[str, object] | None = None
        self.step: int | None = None

    def log(self, data: dict[str, object], *, step: int) -> None:
        self.data = data
        self.step = step


def test_wandb_sink_adds_boundary_and_step() -> None:
    run = FakeRun()
    sink = WandbMetricSink(run)

    sink.publish(Boundary(name="half-epoch", sequence=3), {"exact": 0.8})

    assert run.data == {"control/boundary": "half-epoch", "exact": 0.8}
    assert run.step == 3


def test_null_sink_accepts_metrics() -> None:
    NullMetricSink().publish(Boundary(name="batch", sequence=1), {"rows": 32})
