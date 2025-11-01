# Get python 3.11 image
FROM ghcr.io/astral-sh/uv:debian-slim

# Set /app to be the working directory
WORKDIR /app
ENV UV_LINK_MODE=copy

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python libraries required for running the application
COPY .python-version uv.lock pyproject.toml ./ 
RUN uv sync --frozen

# Expose port
EXPOSE 3000

# Run the application
ENTRYPOINT ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "src.main"]