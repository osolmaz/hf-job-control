"""Small worker used to prove live control transitions."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

from hf_job_control.controller import Controller, ControllerConfig
from hf_job_control.models import AdapterSpec, Boundary, CheckpointManifest, JsonObject, ResumeMode
from hf_job_control.stores import HubBucketArtifactStore, HubControlStore, HubStatusStore


@dataclass(slots=True)
class CounterAdapter:
    """Boundary-resumable integer counter for canary jobs."""

    value: int = 0

    @property
    def spec(self) -> AdapterSpec:
        return AdapterSpec(name="counter", version=1, resume_mode=ResumeMode.EXACT)

    def save(self, destination: Path, boundary: Boundary) -> JsonObject:
        destination.write_text(json.dumps({"value": self.value}) + "\n", encoding="utf-8")
        return {"value": self.value, "sequence": boundary.sequence}

    def restore(self, source: Path, manifest: CheckpointManifest) -> JsonObject:
        value = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or not isinstance(value.get("value"), int):
            raise TypeError("counter checkpoint is invalid")
        self.value = value["value"]
        return {"restored_value": self.value, "sequence": manifest.boundary.sequence}


def run_worker(
    *,
    control_repo: str,
    status_repo: str,
    artifact_bucket: str,
    status_prefix: str,
    interval_seconds: float,
    max_boundaries: int,
) -> int:
    """Run a controllable counter until a command or safety ceiling ends it."""

    adapter = CounterAdapter()
    controller = Controller(
        ControllerConfig.from_environment(),
        control_store=HubControlStore(control_repo),
        status_store=HubStatusStore(status_repo, prefix=status_prefix),
        artifact_store=HubBucketArtifactStore(artifact_bucket),
    )
    start = controller.start(adapter)
    print(
        json.dumps(
            {
                "event": "started",
                "generation": start.generation,
                "resumed": start.resumed,
                "value": adapter.value,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    for _ in range(max_boundaries):
        time.sleep(interval_seconds)
        adapter.value += 1
        boundary = Boundary(name="counter", sequence=adapter.value)
        decision = controller.boundary(
            boundary=boundary,
            adapter=adapter,
            metrics={"value": adapter.value},
        )
        print(
            json.dumps(
                {
                    "action": decision.action.value,
                    "event": "boundary",
                    "generation": decision.generation,
                    "value": adapter.value,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if decision.should_exit:
            controller.finish(decision, message=f"counter ended at {adapter.value}")
            return decision.exit_code
    raise RuntimeError("canary reached its safety boundary limit without a control action")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-repo", required=True)
    parser.add_argument("--status-repo", required=True)
    parser.add_argument("--artifact-bucket", required=True)
    parser.add_argument("--status-prefix", default="canary-runs")
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-boundaries", type=int, default=120)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not os.environ.get("RUN_ID") or not os.environ.get("ATTEMPT_ID"):
        raise ValueError("RUN_ID and ATTEMPT_ID are required")
    return run_worker(
        control_repo=args.control_repo,
        status_repo=args.status_repo,
        artifact_bucket=args.artifact_bucket,
        status_prefix=args.status_prefix,
        interval_seconds=args.interval_seconds,
        max_boundaries=args.max_boundaries,
    )


if __name__ == "__main__":
    raise SystemExit(main())
