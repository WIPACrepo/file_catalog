FROM python:3.10

COPY README.md setup.cfg setup.py /usr/src/file_catalog/
COPY file_catalog /usr/src/file_catalog/file_catalog
RUN pip install /usr/src/file_catalog

RUN useradd -m -U app
USER app

CMD ["python3", "-m", "file_catalog"]
