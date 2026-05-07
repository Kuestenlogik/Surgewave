using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Template-based tuning advisor that matches metrics patterns to configuration recommendations.
/// </summary>
public sealed class TuningAdvisor : ITuningAdvisor
{
    /// <inheritdoc />
    public List<TuningRecommendation> GetRecommendations(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies)
    {
        var recommendations = new List<TuningRecommendation>();
        var anomalyTypes = new HashSet<string>(anomalies.Select(a => a.Type));

        // 1. High produce latency → increase batch.size
        if (snapshot.ProduceLatencyP99 > 100)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase producer batch size",
                Description = $"Produce P99 latency is {snapshot.ProduceLatencyP99:F1}ms. " +
                              "Larger batches amortize the per-message overhead and reduce round trips.",
                ConfigKey = "batch.size",
                SuggestedValue = "65536",
                CurrentValue = "16384",
                Impact = "High",
                Category = "Producer"
            });
        }

        // 2. High produce latency → increase linger.ms
        if (snapshot.ProduceLatencyP50 > 50)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase producer linger time",
                Description = $"Produce P50 latency is {snapshot.ProduceLatencyP50:F1}ms. " +
                              "A higher linger.ms allows the producer to accumulate more messages per batch.",
                ConfigKey = "linger.ms",
                SuggestedValue = "10",
                CurrentValue = "0",
                Impact = "Medium",
                Category = "Producer"
            });
        }

        // 3. Low throughput → increase partitions
        if (anomalyTypes.Contains("ThroughputDrop") || (snapshot.MessagesProducedTotal > 0 && snapshot.PartitionCount < 4))
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase topic partition count",
                Description = $"Current partition count is {snapshot.PartitionCount}. " +
                              "More partitions enable higher parallelism for both producers and consumers.",
                ConfigKey = "num.partitions",
                SuggestedValue = Math.Max(snapshot.PartitionCount * 2, 6).ToString(),
                CurrentValue = snapshot.PartitionCount.ToString(),
                Impact = "High",
                Category = "Broker"
            });
        }

        // 4. Low throughput → increase batch.size for producers
        if (anomalyTypes.Contains("ThroughputDrop"))
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase producer batch size for throughput",
                Description = "Throughput has dropped. Larger batch sizes reduce per-message overhead " +
                              "and increase the number of messages sent per network round trip.",
                ConfigKey = "batch.size",
                SuggestedValue = "131072",
                CurrentValue = "16384",
                Impact = "High",
                Category = "Producer"
            });
        }

        // 5. High consumer lag → add consumer instances
        if (anomalyTypes.Contains("ConsumerLagGrowing") || snapshot.MaxConsumerLag > 50_000)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Scale out consumer group",
                Description = $"Max consumer lag is {snapshot.MaxConsumerLag:N0}. " +
                              "Adding more consumer instances (up to partition count) will increase consumption throughput.",
                Impact = "High",
                Category = "Consumer"
            });
        }

        // 6. High consumer lag → increase fetch.max.bytes
        if (snapshot.MaxConsumerLag > 10_000)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase consumer fetch size",
                Description = $"Consumer lag is {snapshot.MaxConsumerLag:N0}. " +
                              "A larger fetch.max.bytes allows consumers to retrieve more data per request.",
                ConfigKey = "fetch.max.bytes",
                SuggestedValue = "104857600",
                CurrentValue = "52428800",
                Impact = "Medium",
                Category = "Consumer"
            });
        }

        // 7. High error rate → increase retries
        if (anomalyTypes.Contains("ErrorRateHigh"))
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase producer retries",
                Description = $"Error count is {snapshot.ErrorsTotal:N0}. " +
                              "Increasing retries with exponential backoff can recover from transient failures.",
                ConfigKey = "retries",
                SuggestedValue = "10",
                CurrentValue = "3",
                Impact = "Medium",
                Category = "Producer"
            });
        }

        // 8. High error rate → check broker health
        if (anomalyTypes.Contains("ErrorRateHigh") && snapshot.ProduceErrorsTotal > 0)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Investigate broker health",
                Description = $"Produce errors: {snapshot.ProduceErrorsTotal:N0}. " +
                              "Check broker logs, disk I/O, and network connectivity. Broker-side issues often manifest as producer errors.",
                Impact = "High",
                Category = "Broker"
            });
        }

        // 9. High memory pressure (large log size relative to partitions)
        if (snapshot.TotalLogSizeBytes > 10L * 1024 * 1024 * 1024)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Enable compression",
                Description = $"Total log size is {snapshot.TotalLogSizeBytes / (1024.0 * 1024 * 1024):F1} GB. " +
                              "Enabling compression (lz4 or zstd) reduces storage and network I/O.",
                ConfigKey = "compression.type",
                SuggestedValue = "lz4",
                CurrentValue = "none",
                Impact = "High",
                Category = "Broker"
            });
        }

        // 10. High memory → reduce buffer.memory
        if (snapshot.TotalLogSizeBytes > 50L * 1024 * 1024 * 1024)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Reduce producer buffer memory",
                Description = "With large log volumes, reducing producer buffer.memory prevents OOM in memory-constrained environments.",
                ConfigKey = "buffer.memory",
                SuggestedValue = "16777216",
                CurrentValue = "33554432",
                Impact = "Medium",
                Category = "Producer"
            });
        }

        // 11. Connection saturation → increase max.connections
        if (anomalyTypes.Contains("ConnectionSaturation"))
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Increase max connections",
                Description = $"Active connections ({snapshot.ActiveConnections:N0}) are near capacity. " +
                              "Increase the broker's max.connections or scale horizontally.",
                ConfigKey = "max.connections",
                SuggestedValue = "20000",
                CurrentValue = "10000",
                Impact = "High",
                Category = "Broker"
            });
        }

        // 12. High throttling → adjust quotas
        if (snapshot.ThrottledRequestsTotal > 100)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Review client quotas",
                Description = $"Throttled requests: {snapshot.ThrottledRequestsTotal:N0}. " +
                              "Review and increase client produce/fetch byte-rate quotas if legitimate traffic is being throttled.",
                ConfigKey = "quota.producer.default",
                Impact = "Medium",
                Category = "Broker"
            });
        }

        // 13. Latency spike → tune replica fetch settings
        if (anomalyTypes.Contains("LatencySpike") && snapshot.ProduceLatencyP99 > 200)
        {
            recommendations.Add(new TuningRecommendation
            {
                Title = "Tune replica fetch settings",
                Description = "High tail latency may be caused by slow replica fetches. " +
                              "Increasing replica.fetch.max.bytes and replica.fetch.wait.max.ms can help.",
                ConfigKey = "replica.fetch.max.bytes",
                SuggestedValue = "10485760",
                CurrentValue = "1048576",
                Impact = "Medium",
                Category = "Broker"
            });
        }

        // 14. Many topics with few partitions → rebalance
        if (snapshot.TopicCount > 0 && snapshot.PartitionCount > 0)
        {
            var avgPartitions = (double)snapshot.PartitionCount / snapshot.TopicCount;
            if (avgPartitions < 2)
            {
                recommendations.Add(new TuningRecommendation
                {
                    Title = "Increase default partition count",
                    Description = $"Average partitions per topic is {avgPartitions:F1}. " +
                                  "Single-partition topics limit parallelism. Consider a higher default.",
                    ConfigKey = "num.partitions",
                    SuggestedValue = "3",
                    CurrentValue = "1",
                    Impact = "Medium",
                    Category = "Broker"
                });
            }
        }

        // 15. Active transactions with high abort rate
        if (snapshot.TransactionsTotal > 10 && snapshot.TransactionAbortsTotal > 0)
        {
            var abortRate = (double)snapshot.TransactionAbortsTotal / snapshot.TransactionsTotal;
            if (abortRate > 0.1)
            {
                recommendations.Add(new TuningRecommendation
                {
                    Title = "Investigate transaction abort rate",
                    Description = $"Transaction abort rate is {abortRate:P1} ({snapshot.TransactionAbortsTotal:N0} aborts / {snapshot.TransactionsTotal:N0} total). " +
                                  "High abort rates waste resources. Check for producer timeouts or conflicting transactions.",
                    ConfigKey = "transaction.timeout.ms",
                    SuggestedValue = "60000",
                    CurrentValue = "30000",
                    Impact = "Medium",
                    Category = "Producer"
                });
            }
        }

        return recommendations;
    }
}
