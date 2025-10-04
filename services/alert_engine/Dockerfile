FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ../../common /app/common
COPY main.py /app/main.py

CMD ["python", "-u", "main.py"]
