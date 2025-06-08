# Makefile for Immune Health Response Analysis
# Provides convenient commands for development, testing, and deployment

# Install project dependencies using Poetry
# Dependencies are specified in pyproject.toml
install: pyproject.toml
	poetry install

# Run unit tests using pytest
# Requires dependencies to be installed first
pytest: install
	poetry run pytest

# Run the application using Docker Compose
# Builds and starts the container with live code updates
run:
	docker compose up --build

# Build and push Docker image to registry
# Tags the image with the specified repository name
docker-build:
	docker build -t ludflu/i3h-response-and-variance .
	docker push ludflu/i3h-response-and-variance

# Run the application locally with test data
# Sets up input and output directories for testing
test:
	INPUT_DIR=data_testing/input \
	OUTPUT_DIR=data_testing/output \
	poetry run python entry.py
