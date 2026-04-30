#! /bin/bash
CONFIG_FILE=$1
LOG_DIR=$(grep "log.dirs" $CONFIG_FILE | cut -d'=' -f2)
echo "Using log directory: $LOG_DIR"

if [ ! -f "$LOG_DIR/meta.properties" ]; then
    echo "Formatting Kafka Storage for $CONFIG_FILE..."
    bin/kafka-storage.sh format -t "N6MwZ_QjT6-hC0Yt6rXzZQ" -c $CONFIG_FILE
fi

export KAFKA_OPTS="$KAFKA_OPTS -javaagent:/kafka/jmx_prometheus_javaagent.jar=9101:/kafka/jmx_exporter.yml"
bin/kafka-server-start.sh $CONFIG_FILE