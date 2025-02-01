install:
	poetry install

test: install
	poetry run pytest

run:
	docker compose up --build

docker-build:
	docker build -t i3h-response-and-variance .
	