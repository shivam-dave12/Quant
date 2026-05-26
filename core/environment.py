"""Runtime environment bootstrap with explicit, secret-safe `.env` handling.

The container never embeds `.env` in its image. Credentials can enter runtime by:
1. Podman/Docker environment injection (`--env-file` / secrets), which takes precedence.
2. An explicit read-only mounted file passed as `BOT_ENV_FILE`.
3. A local project `.env` during direct host execution.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


@dataclass(frozen=True)
class EnvironmentLoadState:
    source: str
    file_loaded: bool
    explicit_file_requested: bool


_STATE = EnvironmentLoadState(source="not_initialised", file_loaded=False, explicit_file_requested=False)


def _candidate_paths() -> tuple[Path, ...]:
    explicit = os.getenv("BOT_ENV_FILE", "").strip()
    if explicit:
        return (Path(explicit).expanduser(),)
    project_root = Path(__file__).resolve().parents[1]
    cwd_path = Path.cwd() / ".env"
    project_path = project_root / ".env"
    return (cwd_path,) if cwd_path == project_path else (cwd_path, project_path)


def load_runtime_environment() -> EnvironmentLoadState:
    """Load credentials before configuration objects are constructed.

    Injected process variables retain priority over `.env` entries by using
    `override=False`; this prevents a mounted development file from replacing
    secrets intentionally injected by the container runtime.
    """
    global _STATE
    explicit = bool(os.getenv("BOT_ENV_FILE", "").strip())
    candidates = _candidate_paths()
    for path in candidates:
        if path.is_file():
            loaded = bool(load_dotenv(dotenv_path=path, override=False))
            _STATE = EnvironmentLoadState(
                source=f"dotenv_file:{path}",
                file_loaded=loaded,
                explicit_file_requested=explicit,
            )
            return _STATE
    if explicit:
        _STATE = EnvironmentLoadState(
            source=f"dotenv_file_missing:{candidates[0]}",
            file_loaded=False,
            explicit_file_requested=True,
        )
    else:
        _STATE = EnvironmentLoadState(
            source="process_environment_only_no_dotenv_file",
            file_loaded=False,
            explicit_file_requested=False,
        )
    return _STATE


def environment_state() -> EnvironmentLoadState:
    return _STATE


def environment_diagnostics(required_keys: Iterable[str]) -> dict[str, object]:
    """Return presence diagnostics only; never expose secret values."""
    return {
        "source": _STATE.source,
        "file_loaded": _STATE.file_loaded,
        "explicit_file_requested": _STATE.explicit_file_requested,
        "present": {key: bool(os.getenv(key, "")) for key in required_keys},
    }
