# ─────────────────────────────────────────────────────────────────────────────
# Institutional Multi-Desk Bot — Telegram-controlled certified runtime
#
# Build:
#   podman build --no-cache -t localhost/quant:latest .
# Preflight only (recommended: Podman injects host .env at runtime):
#   podman run --rm --env-file "$(pwd)/.env" localhost/quant:latest --preflight-status
# Controller runtime:
#   podman run -d --name quant --restart unless-stopped --env-file "$(pwd)/.env" \
#     -p 8088:8088 localhost/quant:latest
# Alternate application-read file mode (never bake .env into the image):
#   podman run -d --name quant --restart unless-stopped \
#     -v "$(pwd)/.env:/run/secrets/quant.env:ro,Z" \
#     -e BOT_ENV_FILE=/run/secrets/quant.env -p 8088:8088 localhost/quant:latest
# ─────────────────────────────────────────────────────────────────────────────

FROM docker.io/library/python:3.11-slim AS certification

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    LIGHTGBM_NUM_THREADS=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash ca-certificates libgomp1 \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
RUN python -m pip install --no-cache-dir -r requirements-dev.txt

COPY . .

RUN sed -i 's/\r$//' /app/scripts/certify_release.sh \
 && chmod 0755 /app/scripts/certify_release.sh \
 && /usr/bin/env bash /app/scripts/certify_release.sh \
 && find /app -type d \( -name __pycache__ -o -name .pytest_cache \) -prune -exec rm -rf '{}' +

FROM docker.io/library/python:3.11-slim AS runtime

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/app/.runtime-home \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    LIGHTGBM_NUM_THREADS=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libgomp1 \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt \
 && useradd --home-dir /app/.runtime-home --create-home --shell /usr/sbin/nologin botuser

COPY --from=certification /app /app

RUN mkdir -p /app/data/state /app/data/research /app/data/models /app/logs /app/.runtime-home \
 && chown -R botuser:botuser /app

USER botuser
EXPOSE 8088

# The container is a persistent Telegram controller. No market runtime exists
# until an authorised chat sends /start shadow, /start paper or /start live.
ENTRYPOINT ["python", "-m", "telegram.controller"]
CMD []
