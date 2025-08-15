# Base image with Java 17 (for Spark compatibility)
FROM openjdk:17-slim

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Python and common utilities
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    ca-certificates \
    gnupg \
    apt-transport-https \
    bash \
    git \
    make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Make python3 default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1

# Set Java environment variables
ENV JAVA_HOME=/usr/local/openjdk-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create workspace directory
WORKDIR /workspace

# Default command
CMD ["bash"]
