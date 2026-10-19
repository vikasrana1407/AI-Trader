FROM python:3.10-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY fastapi_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the fastapi_app package
COPY fastapi_app/ ./fastapi_app/

# Persistent storage directories
RUN mkdir -p /app/data /app/logs

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')"

# DRY_RUN=true by default — set false in .env for live trading
ENV DRY_RUN=true
ENV LOG_LEVEL=INFO
ENV TELEGRAM_ALERTS_ENABLED=true

CMD ["uvicorn", "fastapi_app.run:app", "--host", "0.0.0.0", "--port", "8000"]
