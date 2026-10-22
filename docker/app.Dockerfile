FROM python:3.11-slim-bookworm

# Install OpenJDK 17, procps, and networking tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    netcat-traditional \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Install Python requirements
RUN pip install --no-cache-dir \
    "pyspark==3.5.3" \
    "confluent-kafka==2.4.0" \
    "happybase==1.2.0" \
    "duckdb==1.1.3" \
    "pyarrow==17.0.0" \
    "pandas==2.2.3" \
    "numpy==1.26.4" \
    "pyyaml==6.0.2" \
    "jsonschema==4.23.0" \
    "pytest==8.3.3" \
    "streamlit==1.39.0" \
    "plotly==5.24.1" \
    "streamlit-autorefresh==1.0.1" \
    "scikit-learn>=1.5.0"

# Copy repository
COPY . /app

# Install project package in editable mode
RUN pip install --no-cache-dir -e .

CMD ["sleep", "infinity"]
