FROM python:3.11-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/app/.runtime-home

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-groww-live.txt .

RUN python -m pip install --no-cache-dir -r requirements-groww-live.txt \
    && python - <<'PY'
from importlib.util import find_spec
assert find_spec("growwapi") is not None, "growwapi is absent from runtime image"
assert find_spec("pyotp") is not None, "pyotp is absent from runtime image"
print("Groww official SDK and TOTP helper verified")
PY

RUN useradd --home-dir /app/.runtime-home --create-home --shell /usr/sbin/nologin botuser

COPY . .

RUN mkdir -p /app/data /app/logs /app/.runtime-home \
    && chown -R botuser:botuser /app

USER botuser

RUN python - <<'PY'
from importlib.util import find_spec
assert find_spec("growwapi") is not None
assert find_spec("pyotp") is not None
print("Runtime dependencies ready for Groww live trading")
PY

STOPSIGNAL SIGTERM

CMD ["python", "telegram/controller.py"]
