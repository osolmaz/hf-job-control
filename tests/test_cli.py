from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

import hf_job_control.cli as cli
from hf_job_control.canary import CounterAdapter
from hf_job_control.checkpoint import create_bundle
from hf_job_control.launch import LaunchedJob
from hf_job_control.models import (
    Action,
    Boundary,
    ControlDocument,
    LaunchSpec,
    RunState,
    RunStatus,
    utc_now,
)
from hf_job_control.stores import LocalArtifactStore, MemoryControlStore, MemoryStatusStore


def install_stores(
    monkeypatch: pytest.MonkeyPatch,
    controls: MemoryControlStore,
    statuses: MemoryStatusStore,
    artifacts: LocalArtifactStore,
) -> None:
    monkeypatch.setattr(cli, "HubControlStore", lambda _repo: controls)
    monkeypatch.setattr(
        cli,
        "HubStatusStore",
        lambda _repo, prefix="runs": statuses,
    )
    monkeypatch.setattr(cli, "HubBucketArtifactStore", lambda _bucket: artifacts)


def test_cli_create_show_and_pause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    install_stores(monkeypatch, controls, statuses, LocalArtifactStore(tmp_path))

    assert cli.main(["create", "--control-repo", "owner/control", "--run-id", "run"]) == 0
    created = json.loads(capsys.readouterr().out)
    assert created["control"]["action"] == "run"

    assert cli.main(["pause", "--control-repo", "owner/control", "run"]) == 0
    paused = json.loads(capsys.readouterr().out)
    assert paused["control"]["generation"] == 2
    assert paused["control"]["action"] == "pause"

    assert cli.main(["show", "--control-repo", "owner/control", "run"]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["control"]["action"] == "pause"


def test_cli_resume_and_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    artifacts = LocalArtifactStore(tmp_path / "artifacts")
    install_stores(monkeypatch, controls, statuses, artifacts)
    controls.publish(
        ControlDocument(run_id="run", generation=1, action=Action.PAUSE),
        expected_generation=0,
    )
    bundle = tmp_path / "checkpoint.hfjob"
    create_bundle(
        destination=bundle,
        run_id="run",
        attempt_id="attempt-1",
        boundary=Boundary(name="counter", sequence=5),
        adapter=CounterAdapter(value=5),
    )
    checkpoint = artifacts.put_checkpoint("run", bundle)
    statuses.publish_status(
        RunStatus(
            run_id="run",
            attempt_id="attempt-1",
            state=RunState.PAUSED,
            updated_at=utc_now(),
            last_applied_generation=1,
            last_action=Action.PAUSE,
            checkpoint=checkpoint,
        )
    )

    resume_args = cli.parse_args(
        [
            "resume",
            "--control-repo",
            "owner/control",
            "--status-repo",
            "owner/status",
            "--artifact-bucket",
            "local/artifacts",
            "run",
        ]
    )
    resumed = cli.dispatch(resume_args)
    assert resumed["control"] == controls.fetch("run").control.to_dict()
    assert controls.fetch("run").control.resume_from == checkpoint

    verify_args = cli.parse_args(
        [
            "verify",
            "--status-repo",
            "owner/status",
            "--artifact-bucket",
            "local/artifacts",
            "run",
        ]
    )
    verified = cli.dispatch(verify_args)
    assert verified["verified"] is True


def test_cli_watch_returns_terminal_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    install_stores(monkeypatch, controls, statuses, LocalArtifactStore(tmp_path))
    statuses.publish_status(
        RunStatus(
            run_id="run",
            attempt_id="attempt-1",
            state=RunState.COMPLETED,
            updated_at=utc_now(),
            last_applied_generation=2,
            last_action=Action.STOP,
        )
    )

    result = cli.dispatch(
        cli.parse_args(["watch", "--status-repo", "owner/status", "--timeout", "1", "run"])
    )

    assert result == {"final_state": "completed", "run_id": "run"}


def test_cli_launch_reads_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = MemoryControlStore()
    controls.publish(
        ControlDocument(run_id="run", generation=1, action=Action.RUN),
        expected_generation=0,
    )
    statuses = MemoryStatusStore()
    install_stores(monkeypatch, controls, statuses, LocalArtifactStore(tmp_path))
    launch_spec = tmp_path / "launch.json"
    launch_spec.write_text(
        json.dumps(
            LaunchSpec(
                image="python",
                command=("true",),
                flavor="cpu-basic",
                timeout="10m",
            ).to_dict()
        ),
        encoding="utf-8",
    )

    class FakeLauncher:
        def __init__(self, _store: object) -> None:
            pass

        def launch(
            self,
            run_id: str,
            _spec: LaunchSpec,
            *,
            attempt_id: str | None,
        ) -> LaunchedJob:
            return LaunchedJob(
                run_id=run_id,
                attempt_id=attempt_id or "generated",
                job_id="job-1",
                url="https://example.test/job-1",
            )

    monkeypatch.setattr(cli, "HubJobLauncher", FakeLauncher)
    result = cli.dispatch(
        cli.parse_args(
            [
                "launch",
                "--control-repo",
                "owner/control",
                "run",
                str(launch_spec),
                "--attempt-id",
                "attempt-1",
            ]
        )
    )
    assert result["job_id"] == "job-1"


def test_cli_canary_builds_cpu_launch_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = MemoryControlStore()
    controls.publish(
        ControlDocument(run_id="run", generation=1, action=Action.RUN),
        expected_generation=0,
    )
    install_stores(
        monkeypatch,
        controls,
        MemoryStatusStore(),
        LocalArtifactStore(tmp_path),
    )
    captured: list[LaunchSpec] = []

    class CapturingLauncher:
        def __init__(self, _store: object) -> None:
            pass

        def launch(
            self,
            run_id: str,
            spec: LaunchSpec,
            *,
            attempt_id: str | None,
        ) -> LaunchedJob:
            captured.append(spec)
            return LaunchedJob(
                run_id=run_id,
                attempt_id=attempt_id or "generated",
                job_id="job-1",
                url="https://example.test/job-1",
            )

    monkeypatch.setattr(cli, "HubJobLauncher", CapturingLauncher)
    result = cli.dispatch(
        cli.parse_args(
            [
                "canary",
                "--control-repo",
                "owner/control",
                "--status-repo",
                "owner/status",
                "--artifact-bucket",
                "owner/bucket",
                "--package-ref",
                "hf-job-control @ git+https://example.test/repo@sha",
                "run",
            ]
        )
    )

    assert result["job_id"] == "job-1"
    assert captured[0].flavor == "cpu-basic"
    assert "hf_job_control.canary" in captured[0].command
    assert captured[0].secret_names == ("HF_TOKEN",)


def test_cli_generates_petname(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("hf_job_control.cli.shutil.which", lambda _name: "/usr/bin/npx")
    monkeypatch.setattr(
        "hf_job_control.cli.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout="202607241430-calm-otter\n"),
    )

    assert cli._generate_run_id() == "202607241430-calm-otter"


def test_cli_resume_requires_paused_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controls = MemoryControlStore()
    statuses = MemoryStatusStore()
    install_stores(monkeypatch, controls, statuses, LocalArtifactStore(tmp_path))
    controls.publish(
        ControlDocument(run_id="run", generation=1, action=Action.RUN),
        expected_generation=0,
    )
    args = cli.parse_args(
        [
            "resume",
            "--control-repo",
            "owner/control",
            "--status-repo",
            "owner/status",
            "run",
        ]
    )
    with pytest.raises(ValueError, match="no observed status"):
        cli.dispatch(args)

    statuses.publish_status(
        RunStatus(
            run_id="run",
            attempt_id="attempt-1",
            state=RunState.RUNNING,
            updated_at=utc_now(),
            last_applied_generation=1,
            last_action=Action.RUN,
        )
    )
    with pytest.raises(ValueError, match="must be paused"):
        cli.dispatch(args)

    statuses.publish_status(
        RunStatus(
            run_id="run",
            attempt_id="attempt-1",
            state=RunState.PAUSED,
            updated_at=utc_now(),
            last_applied_generation=1,
            last_action=Action.PAUSE,
        )
    )
    with pytest.raises(ValueError, match="no checkpoint"):
        cli.dispatch(args)


def test_cli_environment_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HF_JOB_CONTROL_REPO", "owner/control")
    monkeypatch.setenv("HF_JOB_STATUS_REPO", "owner/status")
    monkeypatch.setenv("HF_JOB_ARTIFACT_BUCKET", "owner/bucket")
    args = Namespace(control_repo=None, status_repo=None, artifact_bucket=None)

    assert cli._control_repo(args) == "owner/control"
    assert cli._status_repo(args) == "owner/status"
    assert cli._artifact_bucket(args) == "owner/bucket"


def test_cli_reports_missing_configuration(capsys: pytest.CaptureFixture[str]) -> None:
    args = Namespace(control_repo=None)
    with pytest.raises(ValueError, match="HF_JOB_CONTROL_REPO"):
        cli._control_repo(args)

    assert cli.main(["show", "run"]) == 2
    assert "HF_JOB_CONTROL_REPO" in capsys.readouterr().err
