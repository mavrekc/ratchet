FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY . .

RUN uv sync --frozen --no-dev

CMD ["python", "-c", "import ratchet; print(ratchet.__version__)"]
