# Use Python 3.11 base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for C++ compilation and graph processing
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    libboost-all-dev \
    libsqlite3-dev \
    libeigen3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Create directories
RUN mkdir -p /data
RUN mkdir -p /app/build_graph

COPY build_graph/* /app/build_graph/

# Copy build_graph files (C++ source, etc.)
# COPY ../../build_graph/ /app/build_graph/

# Data files will be mounted as volumes in Kubernetes
# This keeps the Docker image small and allows dynamic data updates
# Volume mount: /data -> host data directory

# Expose port
EXPOSE 8005

# Run the application
CMD ["python", "main.py"]