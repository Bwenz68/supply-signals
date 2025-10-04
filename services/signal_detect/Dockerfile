FROM python:3.11-slim

WORKDIR /app
COPY services/signal_detect/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY common /app/common
COPY services/signal_detect/main.py /app/main.py

CMD ["python", "-u", "main.py"]
