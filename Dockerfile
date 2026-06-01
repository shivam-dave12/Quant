FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN groupadd -r botuser \
    && useradd -r -g botuser -d /app -s /usr/sbin/nologin botuser

COPY requirements.txt ./

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# This copies the project. If .env exists in this directory and is not ignored,
# it will also be copied.
COPY --chown=botuser:botuser . .

RUN mkdir -p /app/data /app/models /app/logs \
    && chown -R botuser:botuser /app

USER botuser

CMD ["python", "-m", "bot.cli", "live-loop"]