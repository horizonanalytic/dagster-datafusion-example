FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install awscli-local for LocalStack (optional, for debugging)
RUN pip install awscli-local

# Copy application code
COPY dagster_pipeline/ /opt/dagster/app/dagster_pipeline/
COPY workspace.yaml /opt/dagster/app/

# Set Dagster home
ENV DAGSTER_HOME=/opt/dagster/app/dagster_home

# Create Dagster home directory
RUN mkdir -p $DAGSTER_HOME

# Expose port for webserver
EXPOSE 3000

# Default command (overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
