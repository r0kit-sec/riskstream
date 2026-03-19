FROM python:3.11-slim

WORKDIR /app

COPY riskstream/services/ingestion/urlhaus/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY riskstream/shared /app/riskstream/shared
COPY riskstream/services/ingestion/urlhaus/src /app/riskstream/services/ingestion/urlhaus/src

ENV PORT=8083
ENV ENVIRONMENT=production
ENV PYTHONPATH=/app

EXPOSE 8083

CMD ["python", "riskstream/services/ingestion/urlhaus/src/main.py"]
