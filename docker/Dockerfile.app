# Reproducible dev/test image. Python 3.11 is used because PySpark 3.5 does not
# support the newest Python releases. Java (for Spark) is added in Phase 4.
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src:/app
WORKDIR /app
COPY requirements.txt requirements-api.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY . .
