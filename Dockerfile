FROM python:3.8

COPY README.md setup.cfg setup.py /usr/src/file_catalog/
COPY file_catalog /usr/src/file_catalog/file_catalog
RUN pip install /usr/src/file_catalog

RUN useradd -m -U app
USER app

ENV PYTHONPATH=/usr/src/file_catalog
WORKDIR /usr/src/file_catalog
CMD ["python3", "-m", "file_catalog"]
