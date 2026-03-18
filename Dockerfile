FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy project metadata first for dependency layer caching
COPY pyproject.toml ./

# Copy application code
COPY bau/ ./bau/

# Install the package and all dependencies
RUN pip install --no-cache-dir .

# Create data directory for SQLite DB
RUN mkdir -p /app/data

# Default: run as long-running service (60-minute interval)
# Override with: docker compose run bau-assistant bau run
ENTRYPOINT ["bau"]
CMD ["serve", "--interval", "60"]
