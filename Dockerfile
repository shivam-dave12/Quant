# syntax=docker/dockerfile:1.6
# ─────────────────────────────────────────────────────────────────────────────
# Delta BTCUSD live-learning HFT bot
# Build: docker build -t btc-live-hft:v5.1 .
# Run:   docker run --rm --env-file .env -v $(pwd)/artifacts/live:/app/artifacts/live btc-live-hft:v5.1
#
# Only secrets belong in .env:
#   DELTA_API_KEY
#   DELTA_SECRET_KEY
# Every non-secret runtime/strategy/risk setting lives in btchft/config.py.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS wheel_builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Build tooling is isolated to this stage and is not shipped in the runtime image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml requirements.txt README.md ./
COPY btchft ./btchft

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip wheel --wheel-dir /wheels .


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/app \
    APP_HOME=/app

WORKDIR /app

# Minimal runtime user. Do not run trading processes as root.
RUN groupadd --system botuser \
    && useradd --system --create-home --home-dir /app --gid botuser botuser \
    && mkdir -p /app/artifacts/live /app/artifacts/models /app/logs \
    && chown -R botuser:botuser /app

COPY --from=wheel_builder /wheels /wheels
RUN python -m pip install --no-index --find-links=/wheels btc-hft-live-learning \
    && rm -rf /wheels

# Ship only non-secret static artefacts. Runtime journals/fills/models are mounted
# through /app/artifacts/live and should not be baked into the image.
COPY --chown=botuser:botuser README.md .env
COPY --chown=botuser:botuser artifacts/bootstrap_tradeflow_model.joblib artifacts/bootstrap_tradeflow_manifest.json artifacts/tradeflow_inspection.json ./artifacts/

USER botuser

VOLUME ["/app/artifacts/live"]

# This verifies code/config importability. It is not a profitability or exchange-connectivity check.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "from btchft.config import Settings; Settings().validate()"

ENTRYPOINT ["btc-live-hft"]
CMD ["run"]
