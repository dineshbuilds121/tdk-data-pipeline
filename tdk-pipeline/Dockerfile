FROM python:3.11-slim

WORKDIR /app

# Copy shared module
COPY shared/ /app/shared/

# Copy unified pipeline service with ingest/export logic
COPY tdk-pipeline/ /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')" || exit 1

CMD ["python", "app.py"]
