FROM mcr.microsoft.com/devcontainers/python:3.11

# Java (required by Spark) + Postgres client + build tools
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-17-jdk postgresql-client build-essential curl git vim \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# Pre-install Python deps for faster devcontainer start
COPY ../requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspaces/the-data-xi
CMD [ "bash" ]
