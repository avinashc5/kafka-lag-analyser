FROM eclipse-temurin:17-jdk-jammy

# Set working directory
WORKDIR /app

# Copy source code
COPY analyser/MetricsCollector.java .

# Copy Kafka client jars (VERY IMPORTANT)
# Copy Kafka client jar built from the local Kafka source tree
COPY kafka/clients/build/libs/kafka-clients-*.jar ./libs/

# Add minimal logging dependencies required by kafka-clients at runtime
ADD https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar ./libs/
ADD https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.17/slf4j-simple-2.0.17.jar ./libs/

# Compile
RUN javac -cp "libs/*" MetricsCollector.java

# Run the analyser (include Kafka client + logging jars on classpath)
CMD ["java", "-cp", ".:libs/*", "MetricsCollector"]