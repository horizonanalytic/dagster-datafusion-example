FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Set working directory
WORKDIR /opt/dagster/app

# Copy project files
COPY pyproject.toml .
COPY dagster_pipeline/ /opt/dagster/app/dagster_pipeline/
COPY workspace.yaml /opt/dagster/app/

# Install dependencies with uv
RUN uv pip install --system -e .

# Set Dagster home
ENV DAGSTER_HOME=/opt/dagster/app/dagster_home

# Create Dagster home directory
RUN mkdir -p $DAGSTER_HOME

# Expose port for webserver
EXPOSE 3000

# Default command (overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
