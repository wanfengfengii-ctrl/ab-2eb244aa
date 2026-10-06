# Shared image for gateway, simulator and the one-shot verifier.
# Pure Python standard library: no third-party packages, fully offline build.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY common ./common
COPY gateway ./gateway
COPY simulator ./simulator
COPY tests ./tests
COPY verify ./verify

# Default service; docker compose overrides the command per service.
CMD ["python", "-m", "gateway.app"]
