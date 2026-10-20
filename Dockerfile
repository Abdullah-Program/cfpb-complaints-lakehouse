FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless make procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV LAKE_ROOT=/app/data \
    DBT_PROFILES_DIR=/app/dbt_cfpb \
    PYTHON=python

CMD ["make", "demo"]
