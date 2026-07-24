# Agent instructions

## Required checks

Run these commands before finishing a change:

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest --cov --cov-report=term-missing
uv run pip-audit
uv run slophammer-py dry .
uv run slophammer-py check .
uv run python scripts/check-mutation.py --min-kill-rate 70
npx -y @simpledoc/simpledoc check
```

Keep `slophammer.yml` and `.github/workflows/ci.yml` aligned with the
[Slophammer Python entrypoint](https://github.com/osolmaz/slophammer/blob/main/docs/AGENT_ENTRYPOINT.md).

## Python

- Require Python 3.11 or newer.
- Keep mypy strict and annotate public functions and meaningful helpers.
- Validate every value read from the Hub before creating domain objects.
- Keep network IO, clocks, filesystem access, and process state at module boundaries.
- Avoid `Any` outside narrow third-party API boundaries.

## Control behavior

- Keep scientific stopping policy outside this package.
- Write a receipt before applying a lifecycle action.
- Never overwrite a content-addressed artifact.
- Treat logical run IDs and physical Hugging Face Job IDs as separate identities.
- Add nearby tests for every behavior change.

**Attention agent!** Before creating ANY documentation, use the `simpledoc` skill in `skills/simpledoc/SKILL.md`.
