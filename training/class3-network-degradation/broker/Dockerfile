FROM eclipse-temurin:17
WORKDIR /kafka
COPY kafka/ .

RUN apt-get update -qq && apt-get install -y -qq curl iproute2 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar" -o /kafka/jmx_prometheus_javaagent.jar

COPY scraper/jmx_exporter.yml /kafka/jmx_exporter.yml
# Scripts and properties will be mounted via docker-compose volumes to avoid fragile paths