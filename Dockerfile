FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Create runtime user before COPY --chown
RUN groupadd -r botuser \
    && useradd -r -g botuser -d /app -s /usr/sbin/nologin botuser

COPY requirements.txt ./

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copies code + .env if .env exists inside the build context
COPY --chown=botuser:botuser . .

RUN mkdir -p /app/data /app/models /app/logs \
    && chown -R botuser:botuser /app

USER botuser

CMD ["python", "-m", "bot.cli", "live-loop"]