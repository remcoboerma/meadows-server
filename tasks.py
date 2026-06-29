from __future__ import annotations

import pathlib

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


def _parse_expiry(expiry_str: str) -> float:
    """Parse an expiry string like '30d', '1w', '3h', '1y' into seconds.

    BUSINESS RULE (monolith tasks.py:297-310): matches the monolith's
    _parse_expiry so CLI-generated and server-generated JWTs use the same
    format. Also re-exported from meadows.server.namespace for the
    JWT-invite handler.
    """
    multipliers = {"d": 86400, "h": 3600, "w": 604800, "m": 2592000, "y": 31536000}
    if expiry_str and expiry_str[-1] in multipliers:
        try:
            return float(expiry_str[:-1]) * multipliers[expiry_str[-1]]
        except ValueError:
            pass
    try:
        return float(expiry_str)
    except ValueError:
        return 30 * 86400  # default 30 days


def _load_jwt_secret() -> bytes:
    """Load the JWT secret from the configured path.

    BUSINESS RULE (MEADOWS §4 line 116): JWT-secret via a gedeeld volume
    (/shared_keys/jwt.key). The server is the authority that holds the
    secret; the task layer reads the same path to mint tokens offline.
    """
    import os

    jwt_secret_path = os.environ.get(
        "MEADOWS_JWT_SECRET", "./shared_keys/jwt.key"
    )
    path = pathlib.Path(jwt_secret_path)
    if not path.exists():
        print(f"Error: JWT secret key not found at {path}")
        print("Run 'inv setup' first to configure the path, or create the key manually.")
        raise SystemExit(1)
    return path.read_bytes()


def _validate_permissions(permissions: list[str]) -> list[str]:
    """Filter out invalid permissions, warning on each.

    BUSINESS RULE (MEADOWS §3.2 line 60): the system contracts what it
    itself must understand. Permissions are protocol (the server acts on
    them), so only AVAILABLE_PERMISSIONS from meadows.protocol are valid.
    """
    from meadows.protocol import AVAILABLE_PERMISSIONS

    valid = []
    for perm in permissions:
        if perm in AVAILABLE_PERMISSIONS:
            valid.append(perm)
        else:
            print(f"Warning: Unknown permission '{perm}' - ignoring.")
            print(f"Available: {list(AVAILABLE_PERMISSIONS.keys())}")
    return valid


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

    # Ensure the JWT secret key exists (generate if missing).
    import os
    import secrets as pysecrets

    jwt_secret_path = os.environ.get("MEADOWS_JWT_SECRET", "./shared_keys/jwt.key")
    path = pathlib.Path(jwt_secret_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(pysecrets.token_hex(32).encode())
        print(f"Generated new JWT secret key: {path}")


@task
def user_jwt(
    c: Context,
    name: str = "testuser",
    permissions: str = "",
    expiry: str = "30d",
) -> None:
    """Generate a JWT token for MEADOWS user authentication.

    BUSINESS RULE (monolith tasks.py:353-396): permissions are intentionally
    NOT exposed in the public API of the server. Only the CLI/task layer
    constructs permissions, preventing clients from granting themselves
    arbitrary permissions via API calls. JWTs are signed with the server's
    secret key, so clients cannot forge tokens.

    BUSINESS RULE (MEADOWS §3.2 line 64): the JWT claim structure (sub
    prefix 'user-', role, permissions, exp) is contracted in protocol.

    Usage:
        inv user-jwt --name=alice
        inv user-jwt --name=alice --permissions=user-invite,bot-invite,mention-all
        inv user-jwt --name=alice --permissions=mention-all --expiry=7d
    """
    from meadows.protocol import JWTRole, build_claims
    import jwt as pyjwt

    secret = _load_jwt_secret()
    perm_list = (
        [p.strip() for p in permissions.split(",") if p.strip()] if permissions else []
    )
    valid_perms = _validate_permissions(perm_list)

    claims = build_claims(
        name=name,
        role=JWTRole.USER,
        permissions=valid_perms,
        expires_in_seconds=_parse_expiry(expiry),
    )
    token = pyjwt.encode(
        claims.model_dump(exclude_none=True),
        secret,
        algorithm="HS256",
    )
    print(token)


@task
def bot_jwt(
    c: Context,
    name: str = "mybot",
    permissions: str = "",
    expiry: str = "1y",
) -> None:
    """Generate a JWT token for bot authentication.

    BUSINESS RULE (monolith tasks.py:399-442): bots have a separate
    'role: bot' field to distinguish them from users. Bot tokens use
    'bot-' prefix in the 'sub' claim. Bot names are validated against
    the JWT's bot_name claim during bot registration to prevent
    impersonation.

    External bots need a pre-crafted JWT with a server-assigned bot_name.
    Internal bots self-select their name using their own secret.

    Usage:
        inv bot-jwt --name=echo
        inv bot-jwt --name=externalbot --permissions=bot-invite
        inv bot-jwt --name=externalbot --expiry=30d
    """
    from meadows.protocol import JWTRole, build_claims
    import jwt as pyjwt

    secret = _load_jwt_secret()
    perm_list = (
        [p.strip() for p in permissions.split(",") if p.strip()] if permissions else []
    )
    valid_perms = _validate_permissions(perm_list)

    claims = build_claims(
        name=name,
        role=JWTRole.BOT,
        permissions=valid_perms,
        expires_in_seconds=_parse_expiry(expiry),
    )
    token = pyjwt.encode(
        claims.model_dump(exclude_none=True),
        secret,
        algorithm="HS256",
    )
    print(token)


@task
def permissions_list(c: Context) -> None:
    """List all available JWT permissions.

    BUSINESS RULE (MEADOWS §3.2 line 60): the system contracts what it
    itself must understand. This task makes the permission catalog
    discoverable without reading source code.
    """
    from meadows.protocol import AVAILABLE_PERMISSIONS

    print("Available JWT permissions:")
    for perm, desc in AVAILABLE_PERMISSIONS.items():
        print(f"  {perm:20s} - {desc}")


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


__all__ = ["setup", "test", "lint", "fmt", "user_jwt", "bot_jwt", "permissions_list"]
