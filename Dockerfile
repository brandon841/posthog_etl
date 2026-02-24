# PostHog ETL Dockerfile
# Builds containerized ETL with Flask HTTP wrapper for Cloud Run
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port for Cloud Run (Flask default)
EXPOSE 8080

# Run Flask app (app.py handles /run endpoint and calls main.py)
CMD ["python", "app.py"]