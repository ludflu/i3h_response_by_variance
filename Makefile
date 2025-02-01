install:
	poetry install

test: install
	poetry run pytest

run:
	docker compose up --build