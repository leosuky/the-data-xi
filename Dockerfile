FROM python:3.11-slim

# Java for Spark
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl ca-certificates \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /workspace
CMD [ "bash" ]
