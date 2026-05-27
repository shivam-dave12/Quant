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

# GrowwFeed(groww) in the official SDK writes package-local cache/state files at
# runtime:
#   - growwapi/instruments.csv
#   - growwapi/common/a.creds
# Grant the non-root process write access only to those SDK-owned runtime files;
# do not make the Python package/code directory writable.
RUN python - <<'PY'
from pathlib import Path
from pwd import getpwnam
import os
import growwapi

user = getpwnam("botuser")
sdk_root = Path(growwapi.__file__).resolve().parent
runtime_files = [
    sdk_root / "instruments.csv",
    sdk_root / "common" / "a.creds",
]
for cache in runtime_files:
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.touch(exist_ok=True)
    os.chown(cache, user.pw_uid, user.pw_gid)
    os.chmod(cache, 0o600)
    print(f"Groww SDK runtime cache prepared for non-root runtime: {cache}")
PY

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
