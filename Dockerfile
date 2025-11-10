FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r req.txt

# Expose Prometheus metrics port
EXPOSE 8001

CMD ["python", "main.py"]
