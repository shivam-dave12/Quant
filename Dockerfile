# ─────────────────────────────────────────────────────────────────────────────
# BTC HFT Live Learning Bot — Production Dockerfile
# Build:
#   podman build --no-cache -t btc-live-hft:v5.8 .
#
# Run:
#   podman run -d \
#     --name btc-live-hft \
#     --replace \
#     --restart unless-stopped \
#     --env-file ~/quant/Quant/.env \
#     -v /data/btc-hft/artifacts/live:/app/artifacts/live:Z \
#     btc-live-hft:v5.8
#
# IMPORTANT:
# - Do NOT copy .env into the image.
# - Secrets must be passed only using --env-file.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/app \
    PYTHONPATH=/app

WORKDIR /app

# Create non-root runtime user BEFORE using --chown.
RUN groupadd --gid 10001 botuser \
    && useradd --uid 10001 --gid 10001 --home-dir /app --shell /usr/sbin/nologin --system botuser \
    && mkdir -p /app/artifacts/live /app/artifacts/live/models /app/.cache \
    && chown -R botuser:botuser /app

# Install Python dependencies first for better Docker layer caching.
COPY requirements.txt ./requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# Copy application code.
# .env must be excluded using .dockerignore.
COPY --chown=botuser:botuser . .

# Ensure writable artifact paths after source copy.
RUN mkdir -p /app/artifacts/live /app/artifacts/live/models \
    && chown -R botuser:botuser /app

USER botuser

# Default command. Adjust only if your CLI entrypoint name differs.
CMD ["btc-live-hft"]