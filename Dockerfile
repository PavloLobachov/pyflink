FROM --platform=linux/amd64 flink:1.19.1-scala_2.12-java8

# ref: https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker

# install python3 and pip3
RUN apt-get update -y && \
    apt-get install -y unzip && \
    apt-get install -y python3 python3-pip python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
COPY requirements.txt .
RUN python -m pip install --upgrade pip; \
    pip3 install --upgrade google-api-python-client; \
    pip3 install -r requirements.txt  --no-cache-dir;

# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.19.1/flink-sql-avro-confluent-registry-1.19.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.19.1/flink-json-1.19.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/1.19.1/flink-csv-1.19.1.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/1.2.0-1.19/flink-connector-mongodb-1.2.0-1.19.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar; \
    wget -P /opt/flink/lib/ https://github.com/knaufk/flink-faker/releases/download/v0.5.2/flink-faker-0.5.2.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.1/mongodb-driver-sync-5.1.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.1/mongodb-driver-core-5.1.1.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/mongodb/bson/5.1.1/bson-5.1.1.jar;

RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml;

WORKDIR /opt/flink
