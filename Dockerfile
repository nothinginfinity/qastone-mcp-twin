# QA.Stone MCP Twin Server
# Railway-deployable container

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY *.py .

# Environment defaults (overridden by Railway)
ENV SERVER_INSTANCE=a
ENV SERVER_PORT=8301
ENV SERVER_VERSION=1.0.0

# Expose port (Railway will override)
EXPOSE 8301

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get(f'http://localhost:{__import__('os').getenv(\"SERVER_PORT\", 8301)}/health').raise_for_status()"

# Run server
CMD ["python", "qastone_server.py"]
