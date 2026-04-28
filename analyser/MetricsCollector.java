import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class MetricsCollector {

    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
    private static final String GROUP_ID = System.getenv().getOrDefault("GROUP_ID", "group1");
    private static final String TOPIC_FILTER = System.getenv().getOrDefault("TOPIC", "test-topic");

    private static final long POLL_INTERVAL_MS = Long.parseLong(System.getenv().getOrDefault("POLL_INTERVAL_MS", "5000"));
    private static final long CONNECT_RETRY_MS = Long.parseLong(System.getenv().getOrDefault("CONNECT_RETRY_MS", "3000"));
    private static final long METRICS_WINDOW_MS = Long.parseLong(System.getenv().getOrDefault("METRICS_WINDOW_MS", "30000"));

    private final Deque<WindowSample> windowSamples = new ArrayDeque<>();

    private Map<TopicPartition, Long> previousCommittedOffsets = new HashMap<>();
    private String previousGroupSignature = null;
    private double inferredCommitEventsTotal = 0.0;
    private double inferredRebalanceEventsTotal = 0.0;

    public static void main(String[] args) throws Exception {
        MetricsCollector collector = new MetricsCollector();
        collector.run();
    }

    public void run() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        while (true) {
            try (AdminClient admin = AdminClient.create(adminProps)) {
                System.out.println("Connected to Kafka Admin API");
                System.out.println("Using bootstrap servers: " + BOOTSTRAP_SERVERS);
                System.out.println("Tracking group: " + GROUP_ID + " | topic filter: " + TOPIC_FILTER);
                System.out.println("Polling every " + POLL_INTERVAL_MS + " ms...");

                while (true) {
                    try {
                        printLagComponentsSnapshot(admin, GROUP_ID, TOPIC_FILTER);
                    } catch (Exception e) {
                        System.err.println("Read failed: " + e.getMessage());
                        break;
                    }
                    Thread.sleep(POLL_INTERVAL_MS);
                }
            } catch (Exception e) {
                System.err.println("Admin client failed (will retry in " + CONNECT_RETRY_MS + " ms): " + e.getMessage());
                Thread.sleep(CONNECT_RETRY_MS);
            }
        }
    }

    private void printLagComponentsSnapshot(AdminClient admin,
                                            String groupId,
                                            String topicFilter) throws Exception {

        Map<TopicPartition, Long> committedOffsets = getCommittedOffsets(admin, groupId, topicFilter);
        Map<TopicPartition, Long> logEndOffsets = getLogEndOffsets(admin, topicFilter, committedOffsets.keySet());
        String groupSignature = getGroupSignature(admin, groupId);

        updateInferredCounters(committedOffsets, groupSignature);

        System.out.println("\n============================================");
        System.out.println("Consumer Lag Inputs @ " + Instant.now());
        System.out.println("============================================");
        System.out.println("group.id=" + groupId + ", topic=" + topicFilter);

        if (committedOffsets.isEmpty()) {
            System.out.println("No committed offsets found yet for this group/topic.");
        }

        Set<TopicPartition> allPartitions = new HashSet<>();
        allPartitions.addAll(committedOffsets.keySet());
        allPartitions.addAll(logEndOffsets.keySet());

        long totalLag = 0L;
        long committedTotal = 0L;
        long logEndTotal = 0L;
        long partitionsWithLag = 0L;
        long maxLag = -1L;
        TopicPartition maxLagPartition = null;

        for (TopicPartition tp : allPartitions) {
            Long committed = committedOffsets.get(tp);
            Long logEnd = logEndOffsets.get(tp);
            Long lag = (committed != null && logEnd != null) ? (logEnd - committed) : null;

            if (committed != null) {
                committedTotal += committed;
            }
            if (logEnd != null) {
                logEndTotal += logEnd;
            }

            if (lag != null) {
                totalLag += lag;
                partitionsWithLag++;
                if (lag > maxLag) {
                    maxLag = lag;
                    maxLagPartition = tp;
                }
            }

            System.out.printf(
                    "%s | committedOffset=%s | logEndOffset=%s | lag=%s%n",
                    tp,
                    committed == null ? "N/A" : committed,
                    logEnd == null ? "N/A" : logEnd,
                    lag == null ? "N/A" : lag
            );
        }

        System.out.println("TotalLag=" + totalLag);
        printRequestedMetrics(totalLag, partitionsWithLag, maxLag, maxLagPartition, committedTotal, logEndTotal);
    }

    private void printRequestedMetrics(long totalLag,
                                       long partitionsWithLag,
                                       long maxLag,
                                       TopicPartition maxLagPartition,
                                       long committedTotal,
                                       long logEndTotal) {
        long now = System.currentTimeMillis();

        addWindowSample(now, committedTotal, logEndTotal, inferredCommitEventsTotal, inferredRebalanceEventsTotal);

        WindowSample oldest = getOldestInWindow(now);

        Double consumerThroughput = null; // metric 4 (windowed)
        Double producerIngressRate = null; // metric 5 (windowed)
        Double commitRateWindow = null; // metric 6 (windowed, inferred)
        Double rebalanceCountWindow = null; // metric 7 (windowed, inferred)

        if (oldest != null && now > oldest.timestampMs) {
            double dtSeconds = (now - oldest.timestampMs) / 1000.0;
            consumerThroughput = Math.max(0.0, (committedTotal - oldest.committedTotal) / dtSeconds);
            producerIngressRate = Math.max(0.0, (logEndTotal - oldest.logEndTotal) / dtSeconds);
            commitRateWindow = Math.max(0.0, (inferredCommitEventsTotal - oldest.commitEventsTotal) / dtSeconds);
            rebalanceCountWindow = Math.max(0.0, inferredRebalanceEventsTotal - oldest.rebalanceEventsTotal);
        }

        System.out.println("\n---- Requested Lag Analysis Metrics ----");
        System.out.println("Window: " + (METRICS_WINDOW_MS / 1000) + "s");

        System.out.println("4) Consumer throughput (records/sec, committed-offset delta over window): "
                + (consumerThroughput == null ? "warming up..." : String.format("%.2f", consumerThroughput)));

        System.out.println("5) Producer ingress rate (records/sec, log-end-offset delta over window): "
                + (producerIngressRate == null ? "warming up..." : String.format("%.2f", producerIngressRate)));

        System.out.println("6) Commit rate (inferred commits/sec over window): " + formatMetric(commitRateWindow));
        System.out.println("7) Rebalance count (inferred over window): " + formatMetric(rebalanceCountWindow));

        if (partitionsWithLag > 0) {
            double avgLag = (double) totalLag / partitionsWithLag;
            double skewRatio = avgLag > 0 ? (maxLag / avgLag) : 0.0;
            System.out.println("9) Partition skew:");
            System.out.println("   Avg lag=" + String.format("%.2f", avgLag)
                    + ", Max lag=" + maxLag
                    + " on " + (maxLagPartition == null ? "N/A" : maxLagPartition)
                    + ", Max/Avg=" + String.format("%.2f", skewRatio));
        } else {
            System.out.println("9) Partition skew: N/A (no lag values yet)");
        }
    }

    private void updateInferredCounters(Map<TopicPartition, Long> currentCommittedOffsets,
                                        String currentGroupSignature) {
        if (!previousCommittedOffsets.isEmpty()) {
            for (Map.Entry<TopicPartition, Long> entry : currentCommittedOffsets.entrySet()) {
                Long prev = previousCommittedOffsets.get(entry.getKey());
                Long cur = entry.getValue();
                if (prev != null && cur != null && cur > prev) {
                    inferredCommitEventsTotal += 1.0;
                }
            }
        }

        if (previousGroupSignature != null && currentGroupSignature != null
                && !previousGroupSignature.equals(currentGroupSignature)) {
            inferredRebalanceEventsTotal += 1.0;
        }

        previousCommittedOffsets = new HashMap<>(currentCommittedOffsets);
        previousGroupSignature = currentGroupSignature;
    }

    private void addWindowSample(long now,
                                 long committedTotal,
                                 long logEndTotal,
                                 Double commitEventsTotal,
                                 Double rebalanceEventsTotal) {
        windowSamples.addLast(new WindowSample(now, committedTotal, logEndTotal, commitEventsTotal, rebalanceEventsTotal));

        long cutoff = now - METRICS_WINDOW_MS;
        while (windowSamples.size() > 2) {
            WindowSample second = getSecond();
            if (second != null && second.timestampMs < cutoff) {
                windowSamples.removeFirst();
            } else {
                break;
            }
        }
    }

    private WindowSample getOldestInWindow(long now) {
        if (windowSamples.isEmpty()) {
            return null;
        }

        long cutoff = now - METRICS_WINDOW_MS;
        WindowSample first = windowSamples.peekFirst();
        if (first == null) {
            return null;
        }

        if (first.timestampMs >= cutoff) {
            return first;
        }

        WindowSample second = getSecond();
        return second != null ? second : first;
    }

    private WindowSample getSecond() {
        if (windowSamples.size() < 2) {
            return null;
        }

        WindowSample first = windowSamples.removeFirst();
        WindowSample second = windowSamples.peekFirst();
        windowSamples.addFirst(first);
        return second;
    }

    private String formatMetric(Double value) {
        return value == null ? "N/A" : String.format("%.2f", value);
    }

    private static class WindowSample {
        private final long timestampMs;
        private final long committedTotal;
        private final long logEndTotal;
        private final Double commitEventsTotal;
        private final Double rebalanceEventsTotal;

        private WindowSample(long timestampMs,
                             long committedTotal,
                             long logEndTotal,
                             Double commitEventsTotal,
                             Double rebalanceEventsTotal) {
            this.timestampMs = timestampMs;
            this.committedTotal = committedTotal;
            this.logEndTotal = logEndTotal;
            this.commitEventsTotal = commitEventsTotal;
            this.rebalanceEventsTotal = rebalanceEventsTotal;
        }
    }

    private Map<TopicPartition, Long> getCommittedOffsets(AdminClient admin,
                                                           String groupId,
                                                           String topicFilter) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = admin
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();

        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            if (!topicFilter.isEmpty() && !tp.topic().equals(topicFilter)) {
                continue;
            }
            result.put(tp, entry.getValue().offset());
        }
        return result;
    }

    private Map<TopicPartition, Long> getLogEndOffsets(AdminClient admin,
                                                        String topicFilter,
                                                        Set<TopicPartition> committedPartitions) throws Exception {
        Set<TopicPartition> targetPartitions = new HashSet<>(committedPartitions);

        if (!topicFilter.isEmpty()) {
            Map<String, TopicDescription> descriptions = admin.describeTopics(List.of(topicFilter)).allTopicNames().get();
            TopicDescription topicDescription = descriptions.get(topicFilter);
            if (topicDescription != null) {
                topicDescription.partitions().forEach(info ->
                        targetPartitions.add(new TopicPartition(topicFilter, info.partition())));
            }
        }

        if (targetPartitions.isEmpty()) {
            return new HashMap<>();
        }

        Map<TopicPartition, OffsetSpec> req = new HashMap<>();
        for (TopicPartition tp : targetPartitions) {
            req.put(tp, OffsetSpec.latest());
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = admin.listOffsets(req).all().get();
        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : latest.entrySet()) {
            result.put(e.getKey(), e.getValue().offset());
        }
        return result;
    }

    private String getGroupSignature(AdminClient admin, String groupId) {
        try {
            Map<String, ConsumerGroupDescription> groups = admin.describeConsumerGroups(List.of(groupId)).all().get();
            ConsumerGroupDescription d = groups.get(groupId);
            if (d == null) {
                return null;
            }

            Set<String> parts = new TreeSet<>();
            d.members().forEach(member -> {
                parts.add("member=" + member.consumerId());
                member.assignment().topicPartitions().forEach(tp -> parts.add("tp=" + tp));
            });
            return String.join("|", parts);
        } catch (Exception e) {
            return null;
        }
    }
}