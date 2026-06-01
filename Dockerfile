FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy project files first.
COPY . .

# Copy local environment file into the image when present in the build context.
# The glob keeps docker build working even before you create .env, because .env.example exists.
COPY .env* ./

CMD ["python", "-m", "bot.cli", "metrics"]
