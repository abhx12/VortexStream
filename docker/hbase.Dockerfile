FROM openjdk:11-jre-slim-bullseye

ARG HBASE_VERSION=2.5.8
ENV HBASE_VERSION=${HBASE_VERSION}
ENV HBASE_HOME=/opt/hbase
ENV PATH="${HBASE_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Download HBase standalone from Apache archive
RUN curl -fsSL "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz" | tar -xz -C /opt && \
    mv "/opt/hbase-${HBASE_VERSION}" "${HBASE_HOME}"

WORKDIR /opt/hbase

EXPOSE 9090 16010

CMD ["sh", "-c", "hbase master start & sleep 5 && hbase thrift start -p 9090"]
