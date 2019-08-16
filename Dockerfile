FROM alpine:3.10

RUN apk add --no-cache gcc git libffi-dev musl-dev openssl-dev python3-dev
RUN pip3 install --upgrade pip # as of alpine:3.10, /usr/bin/pip won't exist until this command runs

COPY README.md requirements.txt setup.cfg setup.py /usr/src/file_catalog/
COPY file_catalog /usr/src/file_catalog/file_catalog
RUN pip install --no-cache-dir /usr/src/file_catalog

RUN addgroup -S app && adduser -S -g app app
USER app

CMD ["python3", "-m", "file_catalog"]
