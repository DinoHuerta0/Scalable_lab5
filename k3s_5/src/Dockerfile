# Una sola imagen para las dos funciones de Knative; cambia solo el comando:
#   ingest     gunicorn ... ingest:app    (k8s/ingest.yaml)
#   processor  gunicorn ... handler:app   (k8s/processor.yaml)
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
CMD ["gunicorn", "--bind", ":8080", "--workers", "1", "--threads", "32", "ingest:app"]
