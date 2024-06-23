FROM debian:bookworm-slim as build-environment

RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes python3-venv gcc libpython3-dev && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip

COPY requirements.txt /requirements.txt
RUN  /venv/bin/pip install --disable-pip-version-check -r /requirements.txt
COPY requirements-cr.txt /requirements-cr.txt
RUN  /venv/bin/pip install --disable-pip-version-check -r /requirements-cr.txt

RUN useradd --create-home tarchia
USER tarchia

FROM gcr.io/distroless/python3-debian12
EXPOSE 8080

ENV PYTHONBUFFERED=1

COPY . /app
COPY --from=build-environment /venv /venv
WORKDIR /app/tarchia
ENV PYTHONPATH /app/tarchia

ENTRYPOINT [ "/venv/bin/python3", "main.py" ]