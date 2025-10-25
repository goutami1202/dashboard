# Use a slim official Python image
FROM python:3.11-slim

# Install system deps for pandas/plotly (small essentials)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /home/app

# Copy requirements and install (cached layer)
COPY requirements.txt /home/app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /home/app

# Create runtime folders (and ensure writable permissions)
RUN mkdir -p /home/app/uploads /home/app/outputs && \
    chmod -R 755 /home/app

ENV PYTHONUNBUFFERED=1
ENV PORT=5000

EXPOSE 5000

# Use Gunicorn to serve app in production (1 worker for cloud platforms)
CMD ["sh", "-c", "gunicorn -w 1 -b 0.0.0.0:${PORT:-5000} web_app:app --timeout 300 --preload"]