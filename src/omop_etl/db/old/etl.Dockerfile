ARG PYTHON_IMAGE=python:3.13.5-slim

FROM ghcr.io/astral-sh/uv:latest AS uv

FROM ${PYTHON_IMAGE}
WORKDIR /app

# put UV in a guaranteed existing dir
COPY --from=uv /uv /uvx /usr/local/bin/

# install runtime deps only
ENV UV_NO_DEV=1

# cache-friendly: deps metadata
COPY pyproject.toml uv.lock README.md /app/
RUN uv sync --locked --no-install-project

# then sources
COPY src/ /app/src/
RUN uv sync --locked --no-editable


ENV PATH="/app/.venv/bin:${PATH}"
CMD ["etl", "load"]
