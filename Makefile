install:
	poetry install

test: install
	poetry run pytest

run:
	docker compose up --build

docker-build:
	docker build -t ludflu/i3h-response-and-variance .
	docker push ludflu/i3h-response-and-variance

test:
	INPUT_DIR=data_testing/input \
	OUTPUT_DIR=data_testing/output \
	poetry run python entry.py
