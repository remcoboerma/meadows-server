"""``python -m meadows.server`` entrypoint."""

from __future__ import annotations

import os

import uvicorn

from meadows.server.app import create_app

app = create_app()


def main() -> None:
    host = os.environ.get("MEADOWS_HOST", "0.0.0.0")
    port = int(os.environ.get("MEADOWS_PORT", "8080"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
