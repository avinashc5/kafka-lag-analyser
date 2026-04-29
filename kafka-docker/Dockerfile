FROM eclipse-temurin:17

WORKDIR /kafka

COPY kafka/ .

COPY kafka-docker/start.sh .
RUN chmod +x start.sh

# Download the Prometheus JMX Exporter Java agent.
# The agent attaches to the Kafka JVM and serves metrics on port 9101.
RUN apt-get update -qq && apt-get install -y -qq curl && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL \
    "https://repo1.maven.org/central/repos/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar" \
    -o /kafka/jmx_prometheus_javaagent.jar

# Copy the exporter rules config built for our taxonomy metrics.
COPY scraper/jmx_exporter.yml /kafka/jmx_exporter.yml

CMD [ "./start.sh" ]
