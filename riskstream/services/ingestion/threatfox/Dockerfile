FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

ENV PORT=8081
ENV ENVIRONMENT=production

EXPOSE 8081

CMD ["python", "main.py"]
