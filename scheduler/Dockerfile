FROM python:3.11-slim

WORKDIR /app

COPY scheduler/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY scheduler/ /app/

CMD ["python", "scheduler.py"]
