# .devcontainer/Dockerfile
FROM mcr.microsoft.com/devcontainers/python:0-3.11

# Install postgres client + build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the non-root user provided by the base image
USER vscode
