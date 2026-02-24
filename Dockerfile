# PostHog ETL Dockerfile
# Builds containerized ETL with Flask HTTP wrapper for Cloud Run
FROM python:3.11-slim

# Ensure Python output is sent straight to terminal without buffering
ENV PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# Copy project files (including requirements.txt)
COPY . .

# Install system deps required by some Python packages (e.g., pyarrow),
# install all Python dependencies from requirements.txt, then remove build tools.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && apt-get remove -y build-essential gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

EXPOSE ${PORT}

# Start the Flask app with gunicorn (app.py provides /run endpoint)
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app", "--workers", "1", "--threads", "4"]