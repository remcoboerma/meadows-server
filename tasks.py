from __future__ import annotations

from invoke import Context, task

try:
    from edwh import tasks as edwh_tasks
    from edwh import task as edwh_task
except ImportError:  # pragma: no cover
    edwh_tasks = None
    edwh_task = task


def _check_env(key: str, default: str = "", comment: str = "") -> str:
    if edwh_tasks is not None:
        return edwh_tasks.check_env(key, default=default, comment=comment)
    import os

    val = os.environ.get(key, default)
    print(f"[check_env fallback] {key}={val!r}  # {comment}")
    return val


@task
def setup(c: Context) -> None:
    """Configure environment for meadows-server."""
    if hasattr(c, "sudo"):
        c.sudo("chmod +x captain-hooks/*.sh")

    _check_env("MEADOWS_HOST", default="0.0.0.0", comment="Host to bind the ASGI server to")
    _check_env("MEADOWS_PORT", default="8080", comment="Port to bind the ASGI server to")
    _check_env(
        "MEADOWS_JWT_SECRET",
        default="./shared_keys/jwt.key",
        comment="Path to the JWT secret key shared with meadows-client / meadows-bot",
    )
    _check_env(
        "MEADOWS_MESSAGES_DIR",
        default="./messages",
        comment="Directory where JSONL message stores live (one file per group)",
    )
    _check_env(
        "MEADOWS_CORS_ORIGINS",
        default="*",
        comment="CORS origins allowed for the Socket.IO server",
    )
    _check_env("MEADOWS_DEBUG", default="0", comment="Debug logging (0 or 1)")
    _check_env("PROJECT", default="meadows", comment="Traefik project prefix")
    _check_env("HOSTINGDOMAIN", default="localhost", comment="Traefik hosting domain")


@task
def test(c: Context) -> None:
    c.run("uv run pytest -q")


@task
def lint(c: Context) -> None:
    c.run("uv run ruff check src tests")


@task
def fmt(c: Context) -> None:
    c.run("uv run ruff format src tests")
    c.run("uv run ruff check --fix src tests")


__all__ = ["setup", "test", "lint", "fmt"]
