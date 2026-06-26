FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml .
COPY src/ ./src/
COPY README.md .
RUN uv pip install --system --no-cache .
EXPOSE 8080
CMD ["python", "-m", "meadows.server"]
