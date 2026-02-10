FROM python:3.11-slim

WORKDIR /app

# Copy shared module
COPY shared/ /app/shared/

# Copy service code
COPY data-export/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY data-export/ /app/

EXPOSE 5002

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5002/health')" || exit 1

CMD ["python", "app.py"]
