"""Operator CLI for cooperative Hugging Face Job control."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from hf_job_control.checkpoint import read_manifest
from hf_job_control.launch import HubJobLauncher, LaunchedJob
from hf_job_control.models import (
    Action,
    ArtifactRef,
    CheckpointManifest,
    ControlDocument,
    JsonObject,
    LaunchSpec,
    ResumeMode,
    RunState,
    stable_json_bytes,
)
from hf_job_control.stores import HubBucketArtifactStore, HubControlStore, HubStatusStore

TERMINAL_STATES = {RunState.PAUSED, RunState.COMPLETED, RunState.ABORTED, RunState.FAILED}


def _control_repo(args: argparse.Namespace) -> str:
    value = args.control_repo or os.environ.get("HF_JOB_CONTROL_REPO")
    if not value:
        raise ValueError("--control-repo or HF_JOB_CONTROL_REPO is required")
    return value


def _status_repo(args: argparse.Namespace) -> str:
    value = args.status_repo or os.environ.get("HF_JOB_STATUS_REPO")
    if not value:
        raise ValueError("--status-repo or HF_JOB_STATUS_REPO is required")
    return value


def _artifact_bucket(args: argparse.Namespace) -> str:
    value = args.artifact_bucket or os.environ.get("HF_JOB_ARTIFACT_BUCKET")
    if not value:
        raise ValueError("--artifact-bucket or HF_JOB_ARTIFACT_BUCKET is required")
    return value


def _generate_run_id() -> str:
    npx = shutil.which("npx")
    if npx is None:
        raise RuntimeError("npx is required to generate a run ID")
    result = subprocess.run(  # noqa: S603 -- executable is resolved from PATH
        [npx, "--yes", "@osolmaz/petname"],
        check=True,
        capture_output=True,
        text=True,
    )
    run_id = result.stdout.strip()
    if not run_id:
        raise RuntimeError("petname returned an empty run ID")
    return run_id


def _publish(
    args: argparse.Namespace,
    *,
    action: Action,
    resume_from: ArtifactRef | None = None,
) -> JsonObject:
    store = HubControlStore(_control_repo(args))
    expected = args.expected_generation
    if expected is None:
        try:
            expected = store.fetch(args.run_id).control.generation
        except ValueError:
            expected = 0
    control = ControlDocument(
        run_id=args.run_id,
        generation=expected + 1,
        action=action,
        reason=args.reason,
        resume_from=resume_from,
    )
    snapshot = store.publish(control, expected_generation=expected)
    return {
        "control": control.to_dict(),
        "path": snapshot.path,
        "repo": snapshot.repo_id,
        "revision": snapshot.revision,
        "sha256": snapshot.sha256,
    }


def command_create(args: argparse.Namespace) -> JsonObject:
    args.run_id = args.run_id or _generate_run_id()
    args.expected_generation = 0
    return _publish(args, action=Action.RUN)


def command_show(args: argparse.Namespace) -> JsonObject:
    snapshot = HubControlStore(_control_repo(args)).fetch(args.run_id)
    result: JsonObject = {
        "control": snapshot.control.to_dict(),
        "path": snapshot.path,
        "repo": snapshot.repo_id,
        "revision": snapshot.revision,
        "sha256": snapshot.sha256,
    }
    if args.status_repo or os.environ.get("HF_JOB_STATUS_REPO"):
        status = HubStatusStore(_status_repo(args), prefix=args.status_prefix).fetch_status(
            args.run_id
        )
        result["status"] = None if status is None else status.to_dict()
    return result


def command_action(args: argparse.Namespace) -> JsonObject:
    return _publish(args, action=Action(args.command))


def command_resume(args: argparse.Namespace) -> JsonObject:
    status = HubStatusStore(_status_repo(args), prefix=args.status_prefix).fetch_status(args.run_id)
    if status is None:
        raise ValueError(f"no observed status exists for {args.run_id}")
    if status.state is not RunState.PAUSED:
        raise ValueError(f"run must be paused before resume; found {status.state.value}")
    if status.checkpoint is None:
        raise ValueError("paused run has no checkpoint")
    manifest = _verify_checkpoint(args, status.checkpoint)
    if manifest.adapter.resume_mode is ResumeMode.UNSUPPORTED:
        raise ValueError("checkpoint adapter does not support resume")
    resume_from = None if manifest.adapter.resume_mode is ResumeMode.RESTART else status.checkpoint
    return _publish(args, action=Action.RUN, resume_from=resume_from)


def _launch_result(job: LaunchedJob) -> JsonObject:
    return {
        "attempt_id": job.attempt_id,
        "job_id": job.job_id,
        "run_id": job.run_id,
        "url": job.url,
    }


def command_launch(args: argparse.Namespace) -> JsonObject:
    raw = args.launch_spec.read_bytes()
    spec = LaunchSpec.from_dict(json.loads(raw))
    job = HubJobLauncher(HubControlStore(_control_repo(args))).launch(
        args.run_id,
        spec,
        attempt_id=args.attempt_id,
    )
    return _launch_result(job)


def command_canary(args: argparse.Namespace) -> JsonObject:
    spec = LaunchSpec(
        image="ghcr.io/astral-sh/uv:python3.13-bookworm",
        command=(
            "uv",
            "run",
            "--with",
            args.package_ref,
            "python",
            "-m",
            "hf_job_control.canary",
            "--control-repo",
            _control_repo(args),
            "--status-repo",
            _status_repo(args),
            "--artifact-bucket",
            _artifact_bucket(args),
            "--status-prefix",
            args.status_prefix,
            "--interval-seconds",
            str(args.interval),
            "--max-boundaries",
            str(args.max_boundaries),
        ),
        flavor="cpu-basic",
        timeout=args.job_timeout,
        secret_names=("HF_TOKEN",),
        labels={"kind": "hf-job-control-canary"},
    )
    job = HubJobLauncher(HubControlStore(_control_repo(args))).launch(
        args.run_id,
        spec,
        attempt_id=args.attempt_id,
    )
    return _launch_result(job)


def _verify_checkpoint(
    args: argparse.Namespace,
    checkpoint: ArtifactRef,
) -> CheckpointManifest:
    store = HubBucketArtifactStore(_artifact_bucket(args))
    with tempfile.TemporaryDirectory(prefix="hf-job-control-verify-") as temp_dir:
        destination = Path(temp_dir) / "checkpoint.hfjob"
        store.get_checkpoint(checkpoint, destination)
        manifest = read_manifest(destination)
    if manifest.run_id != args.run_id:
        raise ValueError("checkpoint manifest run_id does not match requested run")
    return manifest


def command_verify(args: argparse.Namespace) -> JsonObject:
    status = HubStatusStore(_status_repo(args), prefix=args.status_prefix).fetch_status(args.run_id)
    if status is None or status.checkpoint is None:
        raise ValueError("run has no checkpoint to verify")
    manifest = _verify_checkpoint(args, status.checkpoint)
    return {
        "checkpoint": status.checkpoint.to_dict(),
        "manifest": manifest.to_dict(),
        "verified": True,
    }


def command_watch(args: argparse.Namespace) -> JsonObject:
    status_store = HubStatusStore(_status_repo(args), prefix=args.status_prefix)
    deadline = time.monotonic() + args.timeout
    previous: bytes | None = None
    while True:
        status = status_store.fetch_status(args.run_id)
        if status is not None:
            raw = stable_json_bytes(status.to_dict())
            if raw != previous:
                print(raw.decode(), end="", flush=True)
                previous = raw
            if status.state in TERMINAL_STATES:
                return {"final_state": status.state.value, "run_id": args.run_id}
        if time.monotonic() >= deadline:
            raise TimeoutError(f"watch timed out after {args.timeout} seconds")
        time.sleep(args.interval)


def _add_control_repo(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--control-repo")


def _add_status_repo(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--status-repo")
    parser.add_argument("--status-prefix", default="runs")


def _add_mutation(parser: argparse.ArgumentParser) -> None:
    _add_control_repo(parser)
    parser.add_argument("run_id")
    parser.add_argument("--expected-generation", type=int)
    parser.add_argument("--reason")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create", help="Create a logical run in desired state run")
    _add_control_repo(create)
    create.add_argument("--run-id")
    create.add_argument("--reason")

    show = subparsers.add_parser("show", help="Show exact desired and observed state")
    _add_control_repo(show)
    _add_status_repo(show)
    show.add_argument("run_id")

    for action in (Action.PAUSE, Action.STOP, Action.ABORT):
        action_parser = subparsers.add_parser(action.value, help=f"Request {action.value}")
        _add_mutation(action_parser)

    resume = subparsers.add_parser("resume", help="Publish run with the paused checkpoint")
    _add_mutation(resume)
    _add_status_repo(resume)
    resume.add_argument("--artifact-bucket")

    launch = subparsers.add_parser("launch", help="Launch one physical Job attempt")
    _add_control_repo(launch)
    launch.add_argument("run_id")
    launch.add_argument("launch_spec", type=Path)
    launch.add_argument("--attempt-id")

    canary = subparsers.add_parser("canary", help="Launch the remote CPU control canary")
    _add_control_repo(canary)
    _add_status_repo(canary)
    canary.add_argument("run_id")
    canary.add_argument("--artifact-bucket")
    canary.add_argument("--package-ref", required=True)
    canary.add_argument("--attempt-id")
    canary.add_argument("--interval", type=float, default=5.0)
    canary.add_argument("--max-boundaries", type=int, default=120)
    canary.add_argument("--job-timeout", default="15m")

    verify = subparsers.add_parser("verify", help="Verify the latest checkpoint")
    _add_status_repo(verify)
    verify.add_argument("run_id")
    verify.add_argument("--artifact-bucket")

    watch = subparsers.add_parser("watch", help="Watch observed state until terminal")
    _add_status_repo(watch)
    watch.add_argument("run_id")
    watch.add_argument("--interval", type=float, default=10.0)
    watch.add_argument("--timeout", type=float, default=3600.0)
    return parser.parse_args(argv)


def dispatch(args: argparse.Namespace) -> JsonObject:
    handlers: dict[str, Callable[[argparse.Namespace], JsonObject]] = {
        "abort": command_action,
        "canary": command_canary,
        "create": command_create,
        "launch": command_launch,
        "pause": command_action,
        "resume": command_resume,
        "show": command_show,
        "stop": command_action,
        "verify": command_verify,
        "watch": command_watch,
    }
    try:
        handler = handlers[args.command]
    except KeyError as error:
        raise AssertionError(f"unhandled command {args.command}") from error
    return handler(args)


def main(argv: list[str] | None = None) -> int:
    try:
        result = dispatch(parse_args(argv))
    except (OSError, RuntimeError, TimeoutError, TypeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
