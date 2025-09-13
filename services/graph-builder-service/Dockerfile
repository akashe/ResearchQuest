# Multi-stage build for Graph Builder Service
FROM ubuntu:22.04 AS builder

# Install system dependencies for C++ compilation
RUN apt-get update && apt-get install -y \
    g++ \
    libboost-all-dev \
    libsqlite3-dev \
    libeigen3-dev \
    rapidjson-dev \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Production stage
FROM ubuntu:22.04

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    g++ \
    libboost-system1.74.0 \
    libboost-graph1.74.0 \
    libsqlite3-0 \
    libeigen3-dev \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.10/dist-packages /usr/local/lib/python3.10/dist-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Set working directory
WORKDIR /app

# Copy application code
COPY main.py .

# Copy build_graph directory for C++ source
COPY ./build_graph ./build_graph

# Create shared data directory and set permissions
RUN mkdir -p /shared-data && \
    chown -R appuser:appuser /app /shared-data

# Switch to non-root user
USER appuser

# Health check with extended timeout for long-running processes
HEALTHCHECK --interval=60s --timeout=30s --start-period=120s --retries=5 \
    CMD curl -f http://localhost:8006/health || exit 1

# Expose port
EXPOSE 8006

# Run the application
CMD ["python3", "main.py"]