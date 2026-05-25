# ─────────────────────────────────────────────────────────────────────────────
# Unified Quant Bot — Production Dockerfile (AWS/EC2/ECS safe)
# Build: docker build -t quant-bot:v90 .
# Run:   docker run --env-file .env --restart unless-stopped quant-bot:v90
#
# Notes:
# - .env should contain only keys/secrets/tokens. Runtime policy lives in config.py.
# - Playwright Chromium is installed at image-build time, not during trading startup.
# - Browser cache and HOME are app-local and writable by botuser.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/.ms-playwright \
    HOME=/app/.runtime-home \
    ICICI_PLAYWRIGHT_AUTO_INSTALL=false

WORKDIR /app

# Python packages from builder.
COPY --from=builder /install /usr/local

# Install Chromium + Linux dependencies at image build time.
# This avoids live-start crashes like: "Executable doesn't exist" / EACCES under /home/botuser.
RUN python -m playwright install --with-deps chromium

# Non-root runtime user. Home/cache path is inside /app and will be writable.
RUN useradd --home-dir /app/.runtime-home --create-home --shell /usr/sbin/nologin botuser

# Copy project tree. .dockerignore must exclude .env, logs, __pycache__, .git, etc.
COPY . .

# Runtime-writable dirs for logs, sessions, ICICI debug screenshots, Playwright cache, and home.
RUN mkdir -p /app/data /app/data/icici_debug /app/logs /app/.runtime-home /app/.ms-playwright \
    && chown -R botuser:botuser /app

USER botuser

# Quick import/browser sanity check during build finalization.
RUN python - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    path = p.chromium.executable_path
    assert path, 'Chromium executable path not resolved'
print('Playwright Chromium ready')
PY

# Start Telegram controller; /start launches the strategy.
CMD ["python", "telegram/controller.py"]
