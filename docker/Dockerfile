FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Log storage volume mount point
RUN mkdir -p /var/kafka/logs

EXPOSE 9092

CMD ["python", "-m", "broker.server"]
