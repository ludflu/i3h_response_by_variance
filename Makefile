install:
	poetry install

test: install
	poetry run pytest

run:
	docker compose up --build

docker-build:
	docker build -t ludflu/i3h-response-and-variance .
	docker push ludflu/i3h-response-and-variance
