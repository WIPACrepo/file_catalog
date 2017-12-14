FROM alpine:3.5

COPY . /usr/src/file_catalog

RUN apk add --no-cache python py-pip && \
    pip install --no-cache-dir "/usr/src/file_catalog"

CMD ["python","-m","file_catalog","--config","/mnt/server.cfg"]
