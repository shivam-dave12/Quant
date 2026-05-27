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

# GrowwFeed(groww) in the official SDK writes package-local runtime state at
# startup. Live logs proved this is not a single fixed filename: after
# instruments.csv it created growwapi/common/a.creds, then growwapi/common/b.creds.
# Institutional boundary:
#   - keep the full site-packages tree read-only;
#   - keep Groww package code read-only;
#   - allow botuser write access only to the SDK-owned runtime-state directory
#     that the official feed client itself uses, plus the SDK instruments cache.
# This avoids filename whack-a-mole without granting broad package write access.
RUN python - <<'EOF'
from pathlib import Path
from pwd import getpwnam
import os
import growwapi

user = getpwnam("botuser")
sdk_root = Path(growwapi.__file__).resolve().parent
common_state_dir = sdk_root / "common"
instruments_cache = sdk_root / "instruments.csv"

common_state_dir.mkdir(parents=True, exist_ok=True)
instruments_cache.touch(exist_ok=True)

os.chown(common_state_dir, user.pw_uid, user.pw_gid)
os.chmod(common_state_dir, 0o700)
os.chown(instruments_cache, user.pw_uid, user.pw_gid)
os.chmod(instruments_cache, 0o600)

print(f"Groww SDK runtime state dir prepared for non-root runtime: {common_state_dir}")
print(f"Groww SDK instruments cache prepared for non-root runtime: {instruments_cache}")
EOF

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
