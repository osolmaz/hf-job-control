from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import unquote

from hf_job_control.models import LaunchSpec

ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = ROOT / "skills" / "hf-job-control"
SKILL_PATH = SKILL_ROOT / "SKILL.md"
FRONTMATTER_RE = re.compile(r"\A---\n(?P<body>.*?)\n---\n", re.DOTALL)
LINK_RE = re.compile(r"\[[^]]+\]\(([^)]+)\)")
CLI_COMMANDS = {
    "abort",
    "canary",
    "create",
    "launch",
    "pause",
    "resume",
    "show",
    "stop",
    "verify",
    "watch",
}
REQUIRED_REFERENCES = {
    "operations-runbook.md",
    "operator-workflows.md",
    "protocol-and-storage.md",
    "verification-checklists.md",
    "worker-integration.md",
}
REQUIRED_SCHEMAS = {
    "applied-control-v1.schema.json",
    "checkpoint-manifest-v1.schema.json",
    "control-v1.schema.json",
    "launch-spec-v1.schema.json",
    "run-status-v1.schema.json",
}


def _frontmatter_fields(text: str) -> dict[str, str]:
    match = FRONTMATTER_RE.match(text)
    if match is None:
        raise AssertionError("SKILL.md must begin with YAML frontmatter")
    fields: dict[str, str] = {}
    for line in match.group("body").splitlines():
        if line.startswith(" ") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        fields[key] = value.strip().strip('"')
    return fields


def _markdown_files() -> list[Path]:
    return sorted(SKILL_ROOT.rglob("*.md"))


def test_bundled_skill_has_valid_agent_skills_frontmatter() -> None:
    text = SKILL_PATH.read_text(encoding="utf-8")
    fields = _frontmatter_fields(text)

    assert fields["name"] == SKILL_ROOT.name
    assert re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", fields["name"])
    assert 1 <= len(fields["name"]) <= 64
    assert 1 <= len(fields["description"]) <= 1024
    assert len(fields["compatibility"]) <= 500
    assert fields["license"] == "MIT"


def test_bundled_skill_has_all_progressive_disclosure_references() -> None:
    references = SKILL_ROOT / "references"
    assert {path.name for path in references.glob("*.md")} == REQUIRED_REFERENCES
    assert (SKILL_ROOT / "assets" / "launch-spec.example.json").is_file()
    assert (SKILL_ROOT / "agents" / "openai.yaml").is_file()


def test_bundled_schema_copies_match_package_contracts() -> None:
    bundled = SKILL_ROOT / "references" / "schemas"
    assert {path.name for path in bundled.glob("*.json")} == REQUIRED_SCHEMAS
    for name in REQUIRED_SCHEMAS:
        assert (bundled / name).read_bytes() == (ROOT / "schemas" / name).read_bytes()


def test_bundled_skill_local_links_resolve() -> None:
    failures: list[str] = []
    for markdown in _markdown_files():
        text = markdown.read_text(encoding="utf-8")
        for raw_target in LINK_RE.findall(text):
            target = unquote(raw_target.split("#", 1)[0])
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            resolved = (markdown.parent / target).resolve()
            if not resolved.exists():
                failures.append(f"{markdown.relative_to(ROOT)} -> {raw_target}")
    assert not failures, "broken local skill links:\n" + "\n".join(failures)


def test_bundled_skill_documents_current_cli_surface() -> None:
    corpus = "\n".join(path.read_text(encoding="utf-8") for path in _markdown_files())
    missing = sorted(
        command for command in CLI_COMMANDS if f"hf-job-control {command}" not in corpus
    )
    assert not missing
    assert "job_control.py publish" not in corpus


def test_bundled_launch_spec_parses_with_domain_model() -> None:
    path = SKILL_ROOT / "assets" / "launch-spec.example.json"
    spec = LaunchSpec.from_dict(json.loads(path.read_text(encoding="utf-8")))

    assert spec.timeout
    assert spec.secret_names == ("HF_TOKEN",)
    assert "RUN_ID" not in spec.environment
    assert "ATTEMPT_ID" not in spec.environment


def test_pi_manifest_exposes_only_the_public_hf_job_control_skill() -> None:
    manifest = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))

    assert "pi-package" in manifest["keywords"]
    assert manifest["pi"]["skills"] == ["./skills/hf-job-control"]
