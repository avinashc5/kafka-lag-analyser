#! /bin/bash

LOG_DIR=$(grep "log.dirs" config/server.properties | cut -d'=' -f2)
echo "Using log directory: $LOG_DIR"

if [ ! -f "$LOG_DIR/meta.properties" ]; then
    echo "Formatting Kafka Storage..."
    KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
    bin/kafka-storage.sh format \
        -t $KAFKA_CLUSTER_ID \
        -c config/server.properties
fi

bin/kafka-server-start.sh config/server.properties