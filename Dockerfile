from python:3.9.13-alpine3.15

RUN apk update \
    && apk add linux-headers gcc musl-dev \
    && pip install --no-cache --upgrade pip setuptools

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY *.py /app/

WORKDIR /app