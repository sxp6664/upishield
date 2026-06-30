# One image for every Python service; the compose file picks the command.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY producer/ ./producer/
COPY fraud_consumer/ ./fraud_consumer/
COPY alert_consumer/ ./alert_consumer/
COPY api/ ./api/
COPY dashboard/ ./dashboard/

# default command is overridden per-service in docker-compose.yml
CMD ["python", "-c", "print('set a command in compose')"]
