FROM python:3.8

COPY README.md requirements.txt setup.cfg setup.py /usr/src/file_catalog/
COPY file_catalog /usr/src/file_catalog/file_catalog
RUN pip install --no-cache-dir /usr/src/file_catalog

RUN useradd -m -U app
USER app

WORKDIR /usr/src/file_catalog
CMD ["python3", "-m", "file_catalog"]
