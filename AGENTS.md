# Agent instructions

## Required checks

Run these commands before finishing a change:

```bash
uv sync --all-groups
npm ci
npm run check
npm audit
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest --cov --cov-report=term-missing
uv run pip-audit
uvx slophammer-py==0.4.0 dry .
uvx slophammer-py==0.4.0 check .
uv run python scripts/check-mutation.py --min-kill-rate 70
npx -y @simpledoc/simpledoc check
```

Keep `slophammer.yml` and `.github/workflows/ci.yml` aligned with the
[Slophammer Python entrypoint](https://github.com/osolmaz/slophammer/blob/main/docs/AGENT_ENTRYPOINT.md).

## TypeScript

- Keep the TypeScript package strict and aligned with the Python progress models.
- Add shared fixtures when a wire-format change affects both languages.
- Avoid `any`, unchecked casts, and unvalidated external input.

## Python

- Require Python 3.11 or newer.
- Keep mypy strict and annotate public functions and meaningful helpers.
- Validate every value read from the Hub before creating domain objects.
- Keep network IO, clocks, filesystem access, and process state at module boundaries.
- Avoid `Any` outside narrow third-party API boundaries.

## Bundled agent skill

- `skills/hf-job-control/` is the canonical HF Job Control agent skill.
- Keep its copied v1 schemas byte-identical to the root `schemas/` files.
- Keep its command documentation aligned with the released CLI and domain models.
- After changing the canonical skill, mirror the complete directory to
  `osolmaz/tools/agents/skills/hf-job-control/` and run
  `agents/sync-skills.py hf-job-control` from the tools repository.
- Do not restore the superseded standalone `job_control.py` publisher. The skill
  must use the package CLI.

## Control behavior

- Keep scientific stopping policy outside this package.
- Write a receipt before applying a lifecycle action.
- Never overwrite a content-addressed artifact.
- Treat logical run IDs and physical Hugging Face Job IDs as separate identities.
- Add nearby tests for every behavior change.

**Attention agent!** Before creating ANY documentation, use the `simpledoc` skill in `internal/skills/simpledoc/SKILL.md`.
