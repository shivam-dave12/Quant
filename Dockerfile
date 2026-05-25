# ─────────────────────────────────────────────────────────────────────────────
# Unified Quant Bot — Production Dockerfile (AWS/EC2/ECS safe)
# ICICI Breeze websocket enabled: dependency installed and SDK config collision fixed.
#
# Build: docker build --no-cache -t quant-bot:v90 .
# Run:   docker run -d --name quant --env-file .env --restart unless-stopped quant-bot:v90
#
# Notes:
# - .env should contain only keys/secrets/tokens. Runtime policy lives in config.py.
# - Playwright Chromium is installed at image-build time, not during trading startup.
# - Browser cache and HOME are app-local and writable by botuser.
# - ICICI live websocket remains fail-closed at runtime if Breeze cannot connect/authenticate.
# - breeze-connect==1.0.69 contains a bare "import config" which collides with
#   this application's /app/config.py. This image strictly patches that SDK defect.
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

ARG BREEZE_CONNECT_VERSION=1.0.69

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# The Docker build consumes requirements.txt only, so the mandatory ICICI SDK
# is installed here in the actual dependency transaction used by the image.
RUN python -m pip install --prefix=/install \
        -r requirements.txt \
        "breeze-connect==${BREEZE_CONNECT_VERSION}"

# Vendor patch for official breeze-connect 1.0.69:
# its bare `import config` resolves to this bot's /app/config.py after COPY . .,
# instead of breeze_connect/config.py. Fail the build if SDK source changes,
# rather than applying an unsafe or unverified patch.
RUN python - <<'PY'
from pathlib import Path

modules = list(Path("/install").glob("lib/python*/site-packages/breeze_connect/breeze_connect.py"))
assert len(modules) == 1, f"Expected exactly one Breeze SDK module; found {modules}"

sdk = modules[0]
source = sdk.read_text(encoding="utf-8")
old = "\nimport config\n"
new = "\nfrom . import config  # Docker vendor fix: isolate Breeze SDK config\n"

assert source.count(old) == 1, (
    "Expected Breeze SDK 1.0.69 bare `import config` once; "
    "SDK source has changed and requires review before live deployment."
)

sdk.write_text(source.replace(old, new, 1), encoding="utf-8")

patched = sdk.read_text(encoding="utf-8")
assert new in patched
assert old not in patched
print(f"Patched Breeze SDK application-config collision: {sdk}")
PY


# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/.ms-playwright \
    HOME=/app/.runtime-home \
    ICICI_PLAYWRIGHT_AUTO_INSTALL=false

WORKDIR /app

# Python packages from builder, including patched breeze-connect.
COPY --from=builder /install /usr/local

# Validate installation and vendor patch without importing BreezeConnect.
# Importing Breeze during image build is deliberately avoided because the SDK
# can access ICICI's remote SecurityMaster endpoint at module import time.
RUN python - <<'PY'
from importlib.util import find_spec
from pathlib import Path
import sysconfig

assert find_spec("breeze_connect") is not None, "breeze-connect is absent from runtime image"

sdk = Path(sysconfig.get_paths()["purelib"]) / "breeze_connect" / "breeze_connect.py"
assert sdk.exists(), f"Breeze module file not found: {sdk}"

source = sdk.read_text(encoding="utf-8")
assert "\nfrom . import config  # Docker vendor fix: isolate Breeze SDK config\n" in source
assert "\nimport config\n" not in source
print("ICICI Breeze SDK installed and config-collision fix verified")
PY

# Install Chromium + Linux dependencies at image-build time.
RUN python -m playwright install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Non-root runtime identity.
RUN useradd --home-dir /app/.runtime-home --create-home --shell /usr/sbin/nologin botuser

# Copy project tree. Keep .env, logs, caches and secrets out through .dockerignore.
COPY . .

# Runtime-writable paths for session/token state, logs, debug screenshots and Playwright.
RUN mkdir -p \
        /app/data \
        /app/data/icici_debug \
        /app/logs \
        /app/.runtime-home \
        /app/.ms-playwright \
    && chown -R botuser:botuser /app

USER botuser

# Build-time validation under the same OS user that will run the bot.
# Do not import Breeze here; runtime /start performs live broker validation.
RUN python - <<'PY'
from importlib.util import find_spec
from pathlib import Path
import sysconfig
from playwright.sync_api import sync_playwright

assert find_spec("breeze_connect") is not None, "Breeze SDK not visible to botuser"

sdk = Path(sysconfig.get_paths()["purelib"]) / "breeze_connect" / "breeze_connect.py"
source = sdk.read_text(encoding="utf-8")
assert "\nfrom . import config  # Docker vendor fix: isolate Breeze SDK config\n" in source

with sync_playwright() as p:
    executable = p.chromium.executable_path
    assert executable, "Chromium executable path not resolved"

print("Runtime dependencies ready for botuser: patched Breeze SDK + Playwright Chromium")
PY

# Telegram controller starts the process; /start launches strategy desks.
CMD ["python", "telegram/controller.py"]
