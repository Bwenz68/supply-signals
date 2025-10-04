FROM python:3.11-slim

WORKDIR /app
COPY services/data_ingest/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY services/data_ingest/main.py /app/main.py

CMD ["python", "-u", "main.py"]
