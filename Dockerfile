# Base image: Python 3.11.0 on Debian Buster (slim version)
# Slim version reduces image size while maintaining essential functionality
FROM python:3.11.0-slim-buster

# Set environment variables:
# - PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files
# - PYTHONUNBUFFERED: Ensures Python output is sent straight to terminal
# - PATH: Adds Poetry's bin directory to PATH
ENV PYTHONDONTWRITEBYTECODE 1 \
    PYTHONUNBUFFERED 1 \
    PATH "/root/.local/bin:$PATH"

# Install system dependencies and Poetry:
# 1. Update package lists
# 2. Install curl for downloading Poetry
# 3. Clean up apt cache to reduce image size
# 4. Install Poetry 2.0.0 using the official installer
RUN apt-get update \
    && apt-get install curl -y \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://install.python-poetry.org | python - --version 2.0.0

# Set working directory for the application
WORKDIR /usr/app

# Copy dependency files first (for better layer caching):
# - pyproject.toml: Project metadata and dependencies
# - poetry.lock: Locked dependency versions
# - README.md: Project documentation
COPY pyproject.toml poetry.lock README.md ./

# Install Python dependencies:
# 1. Upgrade pip to latest version
# 2. Install the project in development mode
RUN pip install --upgrade pip && pip install .

# Copy application code into the container
COPY response_by_variance ./response_by_variance

# Expose port 5000 for potential web service
EXPOSE 5000

# Set the entry point to run the main application
ENTRYPOINT ["python", "entry.py"]

