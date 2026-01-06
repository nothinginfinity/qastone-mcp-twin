# QA.Stone MCP Twin Server
# Railway-deployable container

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY *.py .
COPY static/ ./static/

# Environment defaults (overridden by Railway)
ENV SERVER_INSTANCE=a
ENV SERVER_VERSION=1.0.0

# Railway provides PORT dynamically, no need to EXPOSE or HEALTHCHECK
# Railway handles healthcheck via /health endpoint

# Run server
CMD ["python", "qastone_server.py"]
