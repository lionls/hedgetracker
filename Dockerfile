# Application image for the hedgetracker flows: runs the CLI, the flows and the
# Prefect worker. Python is pinned to the 3.13 line that uv.lock was resolved
# against; dependencies are installed from the lockfile, so the image and a local
# `uv sync` have identical packages.
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

# No uv cache (it would only bloat the image) and no copied bytecode.
ENV UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH=/app/.venv/bin:$PATH

WORKDIR /app

# Installed in a single layer so the image stays one copy of the virtualenv.
# README.md is required by the project metadata; the dev group is included so
# this image doubles as the `pytest` harness.
COPY pyproject.toml uv.lock README.md prefect.yaml ./
COPY src ./src
RUN uv sync --frozen

# Mounted by docker-compose: the Parquet lake and the edgartools HTTP cache.
VOLUME ["/data"]

CMD ["hedgetracker", "--help"]
