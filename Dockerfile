FROM alpine:3.5

COPY . /usr/src/file_catalog

RUN apk add --no-cache python py-pip && \
    pip install --no-cache-dir "/usr/src/file_catalog" && \
    addgroup -S app && adduser -S -g app app

USER app

CMD ["python", "-m", "file_catalog"]
