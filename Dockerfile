FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy full project. .dockerignore excludes local secrets such as .env.
COPY . .

RUN mkdir -p /app/data /app/models /app/logs

CMD ["python", "-m", "bot.cli", "live-loop"]
