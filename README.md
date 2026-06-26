# meadows-server

> MEADOWS coordination hub: the server-as-object. Socket.IO AsyncServer with an
> ASGI wrapper, an object-oriented Hub (no module globals), and a single
> chokepoint emit that validates frames against `meadows.protocol` before they
> hit the wire. See `MEADOWS-migration-intent.md` section 3.4 (client edge).

## What this package contains

- `hub.py` — `Hub`: the server-as-object. State lives on the instance, not in
  module globals. Explicit `start()` / `stop()` lifecycle.
- `chokepoint.py` — `validate_frame()` / `emit_frame()`: the single client edge.
  Every client-bound frame passes through here and is validated against the
  protocol before it reaches the wire.
- `auth.py` — `verify_token()` + `AuthASGIApp`: JWT verification via pyjwt,
  validated against `meadows.protocol.jwt.JWTClaims`.
- `namespace.py` — `ChatNamespace`: the Socket.IO `/chat` namespace handler.
- `persistence.py` — `JSONLPersistence`: append-only JSONL message store.
- `groups.py` — `GroupState`: in-memory group membership (history is on disk).
- `app.py` — `MeadowServer` / `create_app()`: the ASGI entrypoint composing
  Hub + auth middleware. Exposes `app` for `uvicorn meadows.server.app:app`.

## Architecture invariants (hard)

1. **Hub is an object with explicit lifecycle** — no module-level `sio`,
   `user_sessions`, `bot_registry`, etc. Someone can instantiate `Hub()`,
   wrap it, run it in another process.
2. **Single chokepoint emit** — one path through which all client-bound frames
   pass, validating against `meadows.protocol` first.
3. **Protocol is the only sibling dependency** — imports from
   `meadows.protocol` only, never from `meadows.client` or `meadows.bot`.
4. **PEP 420 namespace** — `src/meadows/server/__init__.py` is fine; there is
   no `src/meadows/__init__.py` anywhere.

## Install

```bash
uv pip install -e .
```

## Run

```bash
uv run python -m meadows.server
# or
uv run uvicorn meadows.server.app:app --host 0.0.0.0 --port 8080
```

## Test

```bash
uv run pytest -q
```
