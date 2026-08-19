"""Hugging Face Job launcher for immutable launch specifications."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

from huggingface_hub import HfApi

from hf_job_control.models import Action, LaunchSpec, validate_attempt_id, validate_run_id
from hf_job_control.stores import ControlStore


@dataclass(frozen=True, slots=True)
class LaunchedJob:
    """Identity returned after one physical Job launch."""

    run_id: str
    attempt_id: str
    job_id: str
    url: str


class HubJobLauncher:
    """Launch physical HF Jobs under a logical control run."""

    def __init__(
        self,
        control_store: ControlStore,
        *,
        api: HfApi | None = None,
    ) -> None:
        self.control_store = control_store
        self.api = api or HfApi()

    def launch(
        self,
        run_id: str,
        spec: LaunchSpec,
        *,
        attempt_id: str | None = None,
        secret_values: dict[str, str] | None = None,
    ) -> LaunchedJob:
        """Launch one attempt after verifying desired state is run."""

        validate_run_id(run_id)
        control = self.control_store.fetch(run_id).control
        if control.action is not Action.RUN:
            raise ValueError(f"cannot launch while desired action is {control.action.value}")
        chosen_attempt = attempt_id or f"attempt-{uuid.uuid4().hex}"
        validate_attempt_id(chosen_attempt)
        values = secret_values or {}
        secrets: dict[str, str] = {}
        for name in spec.secret_names:
            value = values.get(name, os.environ.get(name))
            if value is None:
                raise ValueError(f"missing secret value for {name}")
            secrets[name] = value
        launch_spec = self.control_store.register_launch_spec(run_id, spec)
        environment = {
            **spec.environment,
            "ATTEMPT_ID": chosen_attempt,
            "PLAN_SHA256": launch_spec.sha256,
            "RUN_ID": run_id,
        }
        labels = {**spec.labels, "attempt_id": chosen_attempt, "run_id": run_id}
        info = self.api.run_job(
            image=spec.image,
            command=list(spec.command),
            env=environment,
            secrets=secrets,
            flavor=spec.flavor,
            timeout=spec.timeout,
            name=run_id,
            labels=labels,
            namespace=spec.namespace,
        )
        job_id = info.id
        url = info.url
        return LaunchedJob(
            run_id=run_id,
            attempt_id=chosen_attempt,
            job_id=job_id,
            url=url,
        )
