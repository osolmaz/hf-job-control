"""Optional metric sinks kept outside the control path."""

from __future__ import annotations

from typing import Protocol

from hf_job_control.models import Boundary, JsonObject


class MetricSink(Protocol):
    """Receive metrics without owning lifecycle decisions."""

    def publish(self, boundary: Boundary, metrics: JsonObject) -> None:
        """Publish metrics for one safe boundary."""


class NullMetricSink:
    """Default sink that performs no external metric IO."""

    def publish(self, boundary: Boundary, metrics: JsonObject) -> None:
        del boundary, metrics


class WandbRun(Protocol):
    """Small part of the W&B run API used by the sink."""

    def log(self, data: dict[str, object], *, step: int) -> None:
        """Log one metric record."""


class WandbMetricSink:
    """Send boundary metrics to an initialized W&B run."""

    def __init__(self, run: WandbRun) -> None:
        self.run = run

    def publish(self, boundary: Boundary, metrics: JsonObject) -> None:
        data: dict[str, object] = dict(metrics)
        data["control/boundary"] = boundary.name
        self.run.log(data, step=boundary.sequence)
