# meadows-server

> MEADOWS coordination hub: the server-as-object. Socket.IO AsyncServer with an
> ASGI wrapper, an object-oriented Hub (no module globals), and a single
> chokepoint emit that validates frames against `meadows.protocol` before they
> hit the wire.

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

## Configuration

| Env var | Default | Description |
|---|---|---|
| `MEADOWS_JWT_SECRET` | `./shared_keys/jwt.key` | JWT secret. If the value is an existing file path, its bytes are used; otherwise the literal string. Must be 32+ bytes for HS256. |
| `MEADOWS_MESSAGES_DIR` | `./messages` | Directory for JSONL message files. Created on startup if missing. Each `<group_id>.jsonl` file is a group. |
| `MEADOWS_CORS_ORIGINS` | `*` | CORS allowed origins for Socket.IO. |

## Architecture

```
MeadowServer (ASGI entrypoint)
  -> AuthASGIApp (JWT middleware — gates /chat* and /r/* HTTP routes)
    -> socketio.ASGIApp (Engine.IO/Socket.IO transport)
      -> ChatNamespace (/chat namespace — all event handlers)
        -> Hub (state: sessions, bots, groups, patterns, persistence)
```

### Middleware stack

1. **MeadowServer** (`app.py`) — intercepts `POST /r/{group_id}` for the webhook; everything else passes through.
2. **AuthASGIApp** (`auth.py`) — HTTP-level JWT gate. `/socket.io` always passes through (Engine.IO transport). Paths starting with `/chat` require a valid `Authorization: Bearer <jwt>` header.
3. **socketio.ASGIApp** — standard Socket.IO transport.
4. **ChatNamespace** (`namespace.py`) — the `/chat` namespace handler. All application logic lives here.

### The Hub (state container)

All mutable state lives on the `Hub` instance — never in module globals:

| Attribute | Type | Purpose |
|---|---|---|
| `sio` | `socketio.AsyncServer` | The Socket.IO server |
| `user_sessions` | `dict[str, dict]` | Connected sessions (keyed by sid) |
| `bot_registry` | `dict[str, dict]` | Registered bots (keyed by bot_name) |
| `groups` | `dict[str, GroupState]` | Active groups (keyed by group_id) |
| `pattern_registry` | `dict[str, list]` | Registered regex patterns (keyed by scope) |
| `persistence` | `JSONLPersistence` | Append-only JSONL message store |
| `ntfy_prefs` | `NtfyPrefsStore` | Per-user ntfy notification preferences |

### The chokepoint

Every client-bound frame passes through `hub.emit_frame()`, which runs
`validate_frame()` from `meadows.protocol` before the data hits the wire.
Invalid frames raise `ValueError` and are never emitted. This is the single
enforcement point for the protocol contract.

## JWT authentication

All clients authenticate by emitting an `authenticate` event with a JWT token
on the `/chat` Socket.IO namespace. The server verifies the token and extracts
identity from the claims.

### Token structure

Tokens are HS256 JWTs validated against `meadows.protocol.jwt.JWTClaims`:

```json
{
  "sub": "user-alice",
  "role": "user",
  "exp": 1735689600,
  "iat": 1735686000,
  "username": "alice",
  "permissions": ["mention-all"]
}
```

| Claim | Required | Description |
|---|---|---|
| `sub` | yes | Stable identity. Must be prefixed: `user-<name>` or `bot-<name>`. |
| `role` | yes | `"user"` or `"bot"` |
| `exp` | yes | Expiry timestamp |
| `iat` | auto | Issued-at (auto-set by `build_claims`) |
| `username` | for users | Display name |
| `bot_name` | for bots | Bot display name (required when role=bot) |
| `permissions` | no | List of permission strings |

### Minting tokens

Use `build_claims()` from `meadows.protocol`:

```python
from meadows.protocol import build_claims, JWTRole
import jwt as pyjwt

claims = build_claims(name="alice", role=JWTRole.USER, permissions=["mention-all"])
token = pyjwt.encode(claims.model_dump(exclude_none=True), secret, algorithm="HS256")
```

Bots can also mint tokens for new users/bots via the `request_user_jwt` /
`request_bot_jwt` Socket.IO events (requires `user-invite` / `bot-invite`
permission).

### Permissions

| Permission | Description |
|---|---|
| `user-invite` | Mint JWTs for new users |
| `bot-invite` | Mint JWTs for new bots |
| `mention-all` | Use `@everyone` / `@all` |
| `presence-read` | Read online status per group |

## Socket.IO API

All application events are on the **`/chat`** namespace. The client connects,
emits `authenticate` with a JWT, then interacts via events.

### Connection lifecycle

| Event | Direction | Description |
|---|---|---|
| `connect` | client -> server | Establishes WebSocket connection |
| `authenticate` | client -> server | JWT handshake. Server responds with `authenticated` (user) or `bot_authenticated` (bot), then sends `group_list`, `bot_list`, `my_permissions`, and auto-joins `general`. |
| `disconnect` | client -> server | Server cleans up session and leaves all rooms. |

### Message types

Messages carry a `type` field that identifies their origin:

| Type | Description |
|---|---|
| `user` | Sent by a human user via Socket.IO |
| `bot` | Sent by a bot via `bot_response` event |
| `webhook` | Sent via the HTTP webhook endpoint |
| `reaction` | Emoji reaction on a message |
| `form_submission` | Interactive form submission (future) |
| `system` | System-generated message (future) |

### Chat

| Event | Direction | Auth | Description |
|---|---|---|---|
| `message` | client -> server | yes | Send a message. Server broadcasts `message` to the group room, persists to JSONL, routes `@bot` mentions, and evaluates regex patterns. |
| `typing` | client -> server | yes | Typing indicator. Server broadcasts `user_typing` to the group (rate-limited to once per second). |
| `remove_message` | client -> server | yes | Mark a message as removed (strikethrough). Server broadcasts `message_removed`. |
| `fetch_messages` | client -> server | yes | Fetch specific messages by ID. Server responds with `fetch_messages_result`. |
| `bot_response` | client -> server | bot only | Bot sends a response. Server broadcasts `message` with `type: "bot"`. |

### Groups

| Event | Direction | Auth | Description |
|---|---|---|---|
| `create_group` | client -> server | yes | Create a group. `group_id` must match `^[a-z0-9_-]{1,32}$`. All connected bots auto-join. |
| `list_groups` | client -> server | yes | Returns `group_list` with all groups. |
| `join_group` | client -> server | yes | Join a group. Server sends `joined_group` with display history and broadcasts `members_updated`. |
| `leave_group` | client -> server | yes | Leave a group. Server broadcasts `members_updated` and `user_left`. |
| `delete_group` | client -> server | yes | Delete a group (cannot delete `general`). Archives the JSONL file. |

### Reactions

| Event | Direction | Auth | Description |
|---|---|---|---|
| `add_reaction` | client -> server | yes | Toggle a reaction (emoji) on a message. If the same reaction exists, it's removed (toggle). |
| `remove_reaction` | client -> server | yes | Explicitly remove a reaction. |

### Patterns (bot feature)

| Event | Direction | Auth | Description |
|---|---|---|---|
| `register_pattern` | client -> server | bot only | Register a regex pattern. Server evaluates all patterns on every incoming message and emits `pattern_matched` to the registering bot. Max 50 patterns per scope (room or global), 512 chars. |
| `unregister_pattern` | client -> server | bot only | Remove a pattern by name. |

### Bot registration

| Event | Direction | Auth | Description |
|---|---|---|---|
| `register_bot` | client -> server | bot only | Register bot metadata (description, commands, context_limit). Identity comes from JWT, not payload. |
| `bot_list_bots` | client -> server | yes | Returns `bot_list` with all registered bots. |

### Rate limiting (bot messages)

| Limit | Value | Scope |
|---|---|---|
| Max messages per window | 30 | per bot (sliding 60s window) |
| Cooldown on violation | 60 seconds | per bot |
| Max patterns per scope | 50 | per scope-key (room or global) |
| Max pattern length | 512 chars | — |

When a bot exceeds 30 messages in 60 seconds, the server emits
`rate_limited` to the bot and skips broadcasting the message. The bot
enters a 60-second cooldown during which all `bot_response` events are
rejected. Rate limit state is cleared when the bot disconnects.

### Message envelope

All messages (user, bot, webhook, reaction) share the same envelope from
`meadows.protocol`:

```json
{
  "id": "01923a4f5e6c-3a2f4b8c0d1e",
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "type": "user",
  "user_id": "user-alice",
  "username": "alice",
  "group_id": "general",
  "content": "Hello world",
  "timestamp": "2026-07-07T12:00:00.000000",
  "is_everyone": false,
  "removed": false
}
```

| Field | Type | Description |
|---|---|---|
| `id` | string | Sortable message ID (`<13-digit-ms>-<12-hex>`) |
| `uuid` | string | UUID4 |
| `type` | string | `user`, `bot`, `webhook`, `reaction`, `form_submission`, `system` |
| `user_id` | string | Stable identity from JWT `sub` (e.g. `user-alice`, `bot-echo`) |
| `username` | string | Display name (users only) |
| `bot_name` | string | Bot display name (bots only) |
| `group_id` | string | Group this message belongs to |
| `content` | string | Message body (markdown) |
| `timestamp` | string | ISO 8601 timestamp |
| `is_everyone` | bool | `true` if `@everyone`/`@all` was used with `mention-all` permission |
| `removed` | bool | `true` if message was soft-deleted |
| `quoted_message` | object | Reply context (if replying to another message) |
| `emoji` | string | Emoji (reaction messages only) |
| `target_message_id` | string | Target message (reaction messages only) |
| `original_command` | string | Original `@bot` command (bot responses only) |

### JWT invite

| Event | Direction | Auth | Description |
|---|---|---|---|
| `request_user_jwt` | client -> server | `user-invite` | Mint a JWT for a new user. Responds with `user_jwt_generated`. |
| `request_bot_jwt` | client -> server | `bot-invite` | Mint a JWT for a new bot. Responds with `bot_jwt_generated`. |

### ntfy preferences

| Event | Direction | Auth | Description |
|---|---|---|---|
| `get_ntfy_prefs` | client -> server | yes | Returns `ntfy_prefs` with the user's notification settings. |
| `save_ntfy_prefs` | client -> server | yes | Save notification settings. |

### Server-to-client events (emitted by server)

| Event | Trigger |
|---|---|
| `authenticated` | User auth success (includes groups, bots, permissions) |
| `bot_authenticated` | Bot auth success |
| `auth_error` | Auth failure |
| `message` | Message broadcast (user, bot, or webhook) |
| `message_removed` | Message marked as removed |
| `user_typing` | Typing indicator |
| `joined_group` | Group joined (includes display history) |
| `left_group` | Group left |
| `group_list` | Full list of groups |
| `group_created` | New group created |
| `group_deleted` | Group deleted |
| `members_updated` | Group membership changed |
| `user_joined` | User joined a group |
| `user_left` | User left a group |
| `bot_list` | List of registered bots |
| `bot_registered` | Bot registration confirmed |
| `bot_command` | @bot mention routed to a bot |
| `bot_jwt_generated` | Bot JWT minted |
| `user_jwt_generated` | User JWT minted |
| `my_permissions` | User's permission list |
| `reaction_added` | New reaction on a message |
| `reaction_toggled` | Reaction toggled off |
| `reaction_removed_event` | Reaction explicitly removed |
| `pattern_registered` | Pattern registration confirmed |
| `pattern_unregistered` | Pattern removed |
| `pattern_matched` | Regex pattern matched a message |
| `bot_unregistered` | Bot disconnected (broadcast to other bots) |
| `bot_not_found` | @mention targets a non-existent bot |
| `ntfy_prefs` | ntfy preferences returned |
| `ntfy_prefs_saved` | ntfy preferences saved |
| `error` | Generic error |

## Webhook API

In addition to Socket.IO, the server exposes an HTTP endpoint for injecting
messages without a persistent WebSocket connection.

### `POST /r/{group_id}`

Send a message to a group over HTTP. The message goes through the same
pipeline as Socket.IO messages: broadcast, persistence, @bot routing, and
regex pattern evaluation.

**Request:**

```
POST /r/general
Authorization: Bearer <jwt>
Content-Type: application/json

{"content": "Build passed"}
```

**Auth:** Any valid JWT (user or bot). No specific permission required.

**Body:**

| Field | Type | Required | Description |
|---|---|---|---|
| `content` | string | yes | Message content (markdown, max 100k chars) |

**Response:**

```json
{"status": "ok", "message_id": "01923a4f5e6c-3a2f4b8c0d1e"}
```

**Error responses:**

| Status | Body | Condition |
|---|---|---|
| 401 | `{"error": "missing bearer token"}` | No Authorization header |
| 401 | `{"error": "invalid token"}` | Bad or expired JWT |
| 404 | `{"error": "group not found"}` | Unknown group_id |
| 400 | `{"error": "invalid JSON body"}` | Malformed request body |
| 400 | `{"error": "content is required"}` | Empty or missing content |
| 400 | `{"error": "content too large"}` | Content exceeds 100k chars |

**Behaviour:**

- Messages are typed as `webhook` (distinct from `user` / `bot`)
- `@botname command` in content triggers bot routing
- `@everyone` / `@all` sets `is_everyone=true` if the JWT has `mention-all`
- Registered regex patterns are evaluated against the message content
- Sender identity is derived from the JWT (not the request body) to prevent spoofing

**cURL example:**

```bash
curl -X POST http://localhost:8080/r/general \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -d '{"content":"Deploy #42 complete"}'
```

**Python example:**

```python
import httpx

resp = httpx.post(
    "http://localhost:8080/r/general",
    headers={"Authorization": f"Bearer {jwt_token}"},
    json={"content": "Build passed"},
)
print(resp.json())  # {"status": "ok", "message_id": "..."}
```

## Groups model

Groups are the routing substrate: messages, reactions, mentions, and patterns
all scope to a group.

- **Discovery:** On startup, `hub.start()` scans `MEADOWS_MESSAGES_DIR` for
  `*.jsonl` files. Each file is a group. `general` is always seeded.
- **Creation:** `create_group` event. Validates `group_id` against
  `^[a-z0-9_-]{1,32}$`. All connected bots auto-join.
- **Deletion:** `delete_group` event (cannot delete `general`). Renames the
  JSONL file to `.jsonl.deleted` (audit trail).
- **Membership:** Tracked in-memory on `GroupState.members` (keyed by
  `user_id` from JWT `sub`, not by Socket.IO sid).

## Persistence

Messages are stored as append-only JSONL files — one file per group at
`<messages_dir>/<group_id>.jsonl`. Each line is a JSON-serialized `Message`
envelope from `meadows.protocol`.

- **Append:** New messages are written to the end of the file.
- **Removal:** `mark_removed()` rewrites the file with `removed: true` on the
  target message. Data is never deleted.
- **Display history:** All messages (including removed) for the chat UI.
- **Thread context:** Last N non-removed messages for bot context (default 30).

## Message pipeline

When a message arrives (via Socket.IO `message` event or HTTP webhook), the
server runs `_dispatch_message()`:

1. **Broadcast** — emit `message` to the group room via the chokepoint
2. **Persist** — append to the group's JSONL file
3. **@bot routing** — parse `@botname command args` from content, emit
   `bot_command` to the named bot with thread context
4. **Pattern evaluation** — run all registered regex patterns against the
   content, emit `pattern_matched` to registering bots

## Package contents

| File | Purpose |
|---|---|
| `hub.py` | `Hub`: the server-as-object. State container with explicit `start()`/`stop()` lifecycle. |
| `chokepoint.py` | `validate_frame()` / `emit_frame()`: the single client edge. Validates every frame against `meadows.protocol`. |
| `auth.py` | `verify_token()` + `AuthASGIApp`: JWT verification and HTTP middleware. |
| `namespace.py` | `ChatNamespace`: the Socket.IO `/chat` namespace handler. All event handlers. |
| `persistence.py` | `JSONLPersistence`: append-only JSONL message store. |
| `groups.py` | `GroupState`: in-memory group membership. |
| `ntfy_prefs.py` | `NtfyPrefsStore`: per-user notification preferences. |
| `app.py` | `MeadowServer` / `create_app()`: ASGI entrypoint composing Hub + auth + webhook routing. |

## Architecture invariants

1. **Hub is an object** — no module-level state. Someone can instantiate
   `Hub()`, wrap it, run it in another process.
2. **Single chokepoint emit** — one path through which all client-bound
   frames pass, validating against `meadows.protocol` first.
3. **Protocol is the only sibling dependency** — imports from
   `meadows.protocol` only, never from `meadows.client` or `meadows.bot`.
4. **PEP 420 namespace** — `src/meadows/server/__init__.py` is fine; there is
   no `src/meadows/__init__.py` anywhere.

## Test

```bash
uv run pytest -q
```
