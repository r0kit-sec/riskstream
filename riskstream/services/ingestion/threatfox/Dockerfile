FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install debugpy for remote debugging
RUN pip install --no-cache-dir debugpy

COPY src/ .

ENV PORT=8081
ENV ENVIRONMENT=production

EXPOSE 8081
EXPOSE 5678

CMD ["python", "main.py"]
