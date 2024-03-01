FROM confluentinc/ksqldb-server:0.28.2

USER root

# Download confluent-hub client
ADD https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz /tmp/confluent-hub-client.tar.gz

# Install confluent-hub client
RUN mkdir /confluent-hub-client && \
    tar -xzf /tmp/confluent-hub-client.tar.gz -C /confluent-hub-client && \
    rm -f /tmp/confluent-hub-client.tar.gz
ENV CONFLUENT_HOME=/confluent-hub-client
ENV PATH=$CONFLUENT_HOME/bin:$PATH

# Install connectors
RUN mkdir -p /usr/share/kafka/plugins
RUN confluent-hub install --component-dir /usr/share/kafka/plugins --worker-configs /etc/ksqldb/ksql-server.properties --no-prompt confluentinc/kafka-connect-s3:latest
RUN confluent-hub install --component-dir /usr/share/kafka/plugins --worker-configs /etc/ksqldb/ksql-server.properties --no-prompt confluentinc/kafka-connect-jdbc:latest

# Install AWS Redshift JDBC driver
ADD https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.26/redshift-jdbc42-2.1.0.26.jar /tmp/redshift-jdbc42-2.1.0.26.jar
RUN mv /tmp/redshift-jdbc42-2.1.0.26.jar /usr/share/kafka/plugins/confluentinc-kafka-connect-jdbc/lib/

RUN chown -R appuser /usr/share/kafka/plugins

# Run as appuser
USER appuser
CMD ["/usr/bin/docker/run"]
