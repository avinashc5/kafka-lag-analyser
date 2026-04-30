# ── Scraper constants ─────────────────────────────────────────────────────────

POLL_INTERVAL_SECONDS = 10  # seconds between each metric collection round
BATCH_SIZE = 6  # number of scrapes to buffer before writing to DB
# (6 × 10 s = flush every ~1 minute)

# ── Connection endpoints ───────────────────────────────────────────────────────

# HTTP endpoint exposed by the Prometheus JMX Exporter Java agent on the broker
JMX_EXPORTER_URL = "http://kafka:9101/metrics"

# Kafka bootstrap address – used only for the Admin API (broker-side queries)
KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]

# ── Storage ────────────────────────────────────────────────────────────────────

DB_PATH = "data/metrics.db"  # SQLite file; relative to project root

# ── JMX metric filter ─────────────────────────────────────────────────────────
# Only metrics whose Prometheus name starts with one of these prefixes are kept.
# All comparisons are done in lowercase.  Set to ["kafka_"] to keep everything.
JMX_TARGET_PREFIXES = [
    # Throughput (bytes / messages in & out)
    "kafka_server_brokertopicmetrics_bytesinpersec",
    "kafka_server_brokertopicmetrics_bytesoutpersec",
    "kafka_server_brokertopicmetrics_messagesinpersec",
    "kafka_server_brokertopicmetrics_replicationbytesoutpersec",
    # Request latency components
    "kafka_network_requestmetrics_throttletimems",  # Class 2: Broker Saturation
    "kafka_network_requestmetrics_totaltimems",  # Class 3: Network Degradation
    "kafka_network_requestmetrics_localtimems",  # Class 2: disk I/O
    "kafka_network_requestmetrics_remotetimems",  # Class 3: inter-broker wait
    "kafka_network_requestmetrics_responsesendtimems",  # Class 3: broker→client link
    "kafka_network_requestmetrics_requestqueuetimems",  # Class 2: queue backlog
    # "kafka_network_requestmetrics_responsequeuetimems",
    # Request rates (JoinGroup / SyncGroup signal rebalancing)
    "kafka_network_requestmetrics_requestspersec",  # Class 1: Rebalance Loops
    # Broker thread utilisation
    "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent",  # Class 2
    "kafka_network_socketserver_networkprocessoravgidlepercent",  # Class 2
    # Replication health
    "kafka_server_replicamanager_underreplicatedpartitions",  # Class 3
    # "kafka_server_replicamanager_underminisrpartitioncount",
    "kafka_server_replicafetchermanager_maxlag",  # Class 3: inter-broker lag
    # Controller
    "kafka_controller_kafkacontroller_offlinepartitionscount",
    # "kafka_controller_kafkacontroller_activecontrollercount",
    # Consumer group coordinator (rebalance state)          # Class 1
    "kafka_coordinator_group_groupmetadatamanager_numgroups",
    "kafka_coordinator_group_groupmetadatamanager_numgroupspreparingrebalance",
    "kafka_coordinator_group_groupmetadatamanager_numgroupscompletingrebalance",
    "kafka_coordinator_group_groupmetadatamanager_numgroupsstable",
    # "kafka_coordinator_group_groupmetadatamanager_numgroupsdead",
    # Per-partition log positions (Class 4: Skewed Partition Load)
    "kafka_log_log_logendoffset",
    "kafka_log_log_logstartoffset",
    "kafka_log_log_size",
]
