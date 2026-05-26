FROM python:3.11-slim AS certification
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt
COPY . .
RUN ./scripts/certify_release.sh

FROM python:3.11-slim AS runtime
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/app/.runtime-home
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt \
 && useradd --home-dir /app/.runtime-home --create-home --shell /usr/sbin/nologin botuser
COPY --from=certification /app /app
RUN mkdir -p /app/data/state /app/data/research /app/logs /app/.runtime-home && chown -R botuser:botuser /app
USER botuser
CMD ["python", "main.py", "--status"]
