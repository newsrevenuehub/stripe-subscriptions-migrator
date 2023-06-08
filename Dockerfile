FROM python:3.11.4

WORKDIR /app
COPY pyproject.toml poetry.lock /app/

ENV POETRY_VERSION=1.1.8
ENV PATH=/root/.poetry/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python \
   && poetry config virtualenvs.create false

COPY scripts/install-tools.sh /
RUN /install-tools.sh

COPY pyproject.toml poetry.lock /app/

RUN poetry install
