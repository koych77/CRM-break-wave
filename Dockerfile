FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create assets directory if not exists
RUN mkdir -p /app/app/webapp/assets/icons

EXPOSE 8080

CMD ["python", "main.py"]
