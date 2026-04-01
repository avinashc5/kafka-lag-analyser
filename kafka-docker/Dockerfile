FROM eclipse-temurin:17

WORKDIR /kafka

COPY kafka/ .

COPY kafka-docker/start.sh .
RUN chmod +x start.sh

CMD [ "./start.sh" ]