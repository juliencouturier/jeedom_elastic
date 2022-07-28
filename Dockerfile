from python:3.9.13-alpine3.15

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY *.py /app/

WORKDIR /app