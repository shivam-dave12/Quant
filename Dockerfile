# ============================================================================
# Stage 1: Builder
# ============================================================================
FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    cargo \
    rustc \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip setuptools wheel

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# ============================================================================
# Stage 2: Runtime
# ============================================================================
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/root/.local/bin:$PATH

# Install runtime dependencies only (much smaller)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    libffi8 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local

# Copy application files
COPY *.py ./
COPY .env .env

# Create logs directory
RUN mkdir -p /app/logs

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Run the bot
CMD ["python", "-u", "telegram_bot_controller.py"]