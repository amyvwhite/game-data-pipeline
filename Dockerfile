# Image for the pipeline: Python + Java (Spark needs a JVM) + the code.
FROM python:3.12-slim-bookworm

# PySpark 4 needs Java 17 or newer. procps gives Spark's scripts the `ps` command.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so Docker can cache this layer between code changes.
# Build with --build-arg INSTALL_DEV=true to also get pytest/ruff (used by CI/Jenkins).
ARG INSTALL_DEV=false
COPY requirements.txt requirements-dev.txt ./
RUN if [ "$INSTALL_DEV" = "true" ]; then pip install --no-cache-dir -r requirements-dev.txt; \
    else pip install --no-cache-dir -r requirements.txt; fi

COPY . .

# Run as a non-root user.
RUN useradd --create-home pipeline && mkdir -p data dashboard/data && chown -R pipeline /app
USER pipeline

CMD ["python", "-m", "pipeline.run"]
