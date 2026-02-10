FROM python:3.11-slim

WORKDIR /app

# Copy shared module
COPY shared/ /app/shared/

# Copy service code
COPY data-ingestion/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY data-ingestion/ /app/

EXPOSE 5001

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5001/health')" || exit 1

CMD ["python", "app.py"]
