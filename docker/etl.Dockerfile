# Multi-stage: the deps layer (pyproject.toml/uv.lock) is cached separately
# from the src/ copy, so Docker's content layer cache skips re-installing
# dependencies when only src/ changed
ARG PYTHON_IMAGE=python:3.13.5-slim

FROM ghcr.io/astral-sh/uv:latest AS uv

FROM ${PYTHON_IMAGE}
WORKDIR /app

# put uv in a guaranteed-existing dir
COPY --from=uv /uv /uvx /usr/local/bin/

# install runtime deps only
ENV UV_NO_DEV=1

# cache-friendly: deps metadata first
COPY pyproject.toml uv.lock README.md /app/
RUN uv sync --locked --no-install-project

# then sources
COPY src/ /app/src/
RUN uv sync --locked --no-editable

ENV PATH="/app/.venv/bin:${PATH}"
CMD ["etl", "load"]