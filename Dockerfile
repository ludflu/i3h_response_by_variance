FROM python:3.11.0-slim-buster

ENV PYTHONDONTWRITEBYTECODE 1 \
    PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get install curl -y \
    && curl -sSL https://install.python-poetry.org | python - --version 2.0.0

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /usr/app

COPY pyproject.toml poetry.lock README.md ./

COPY response_by_variance ./response_by_variance

RUN pip install --upgrade pip && pip install .



EXPOSE 5000

#ENTRYPOINT ["poetry", "run", "python", "entry.py"]
ENTRYPOINT ["python", "entry.py"]

