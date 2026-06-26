"""MEADOWS server — the coordination hub.

The server-as-object: an object-oriented Hub (no module globals), a single
chokepoint emit that validates frames against meadows.protocol, JWT auth,
and JSONL persistence. See MEADOWS-migration-intent.md section 3.4.

This package depends on meadows.protocol only — never on meadows.client or
meadows.bot (those are different leaves of the namespace).
"""

import importlib

from meadows.server.__about__ import __version__
from meadows.server.hub import Hub

_LAZY = {"MeadowServer", "create_app", "app"}


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    # Lazy: importing meadows.server should not construct the ASGI app. Only
    # an explicit request for MeadowServer / create_app / app does (and only
    # then, so `uvicorn meadows.server.app:app` and `python -m meadows.server`
    # still build it).
    if name in _LAZY:
        module = importlib.import_module("meadows.server.app")
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | _LAZY)


__all__ = ["Hub", "MeadowServer", "__version__", "create_app"]
