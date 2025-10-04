FROM python:3.11-slim

WORKDIR /app
COPY services/normalize_enrich/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY services/normalize_enrich/main.py /app/main.py

CMD ["python", "-u", "main.py"]
