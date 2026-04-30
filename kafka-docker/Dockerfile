FROM eclipse-temurin:17

WORKDIR /kafka

COPY kafka/ .

COPY kafka-docker/start.sh .
RUN chmod +x start.sh

# Copy the exporter rules config built for our taxonomy metrics.
COPY scraper/jmx_exporter.yml /kafka/jmx_exporter.yml

CMD [ "./start.sh" ]
