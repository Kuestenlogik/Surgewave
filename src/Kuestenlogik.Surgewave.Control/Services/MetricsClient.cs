using System.Globalization;
using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client that fetches metrics from the Surgewave broker's Prometheus endpoint.
/// </summary>
public sealed partial class MetricsClient : IMetricsClient
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<MetricsClient> _logger;

    public MetricsClient(HttpClient httpClient, ILogger<MetricsClient> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public async Task<MetricsSnapshot> GetMetricsAsync(CancellationToken cancellationToken = default)
    {
        // The BaseAddress is configured in Program.cs
        var metricsUrl = "metrics";

        try
        {
            var response = await _httpClient.GetStringAsync(metricsUrl, cancellationToken);
            return ParsePrometheusMetrics(response);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to fetch metrics from {Url}", metricsUrl);
            return new MetricsSnapshot();
        }
    }

    private static MetricsSnapshot ParsePrometheusMetrics(string content)
    {
        var metrics = new Dictionary<string, double>();
        var histogramBuckets = new Dictionary<string, List<(double le, double count)>>();
        var topicMetrics = new Dictionary<string, TopicMetrics>();

        foreach (var line in content.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.StartsWith('#'))
                continue;

            var match = MetricLineRegex().Match(line);
            if (!match.Success)
                continue;

            var name = match.Groups["name"].Value;
            var labels = match.Groups["labels"].Value;
            var valueStr = match.Groups["value"].Value;

            if (!double.TryParse(valueStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
                continue;

            // Extract topic label if present
            var topicMatch = TopicLabelRegex().Match(labels);
            var topic = topicMatch.Success ? topicMatch.Groups[1].Value : null;

            // Handle per-topic metrics
            if (topic != null && IsTopicMetric(name))
            {
                if (!topicMetrics.TryGetValue(topic, out var tm))
                {
                    tm = new TopicMetrics { Topic = topic };
                    topicMetrics[topic] = tm;
                }

                topicMetrics[topic] = name switch
                {
                    "surgewave_messages_produced_total" => tm with { MessagesProducedTotal = tm.MessagesProducedTotal + value },
                    "surgewave_messages_fetched_total" => tm with { MessagesFetchedTotal = tm.MessagesFetchedTotal + value },
                    "surgewave_bytes_produced_total" => tm with { BytesProducedTotal = tm.BytesProducedTotal + value },
                    "surgewave_bytes_fetched_total" => tm with { BytesFetchedTotal = tm.BytesFetchedTotal + value },
                    _ => tm
                };
            }

            // Handle histogram buckets
            if (name.EndsWith("_bucket", StringComparison.Ordinal))
            {
                var baseName = name[..^7]; // Remove "_bucket"
                var leMatch = LeBucketRegex().Match(labels);
                if (leMatch.Success && double.TryParse(leMatch.Groups[1].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var le))
                {
                    if (!histogramBuckets.TryGetValue(baseName, out var buckets))
                    {
                        buckets = [];
                        histogramBuckets[baseName] = buckets;
                    }
                    buckets.Add((le, value));
                }
            }
            else
            {
                // Use first occurrence or sum if multiple (for aggregated metrics)
                var key = name;
                metrics[key] = metrics.TryGetValue(key, out var existing) ? existing + value : value;
            }
        }

        static bool IsTopicMetric(string name) => name is
            "surgewave_messages_produced_total" or
            "surgewave_messages_fetched_total" or
            "surgewave_bytes_produced_total" or
            "surgewave_bytes_fetched_total";

        // Calculate percentiles from histograms
        var produceP50 = CalculatePercentile(histogramBuckets, "surgewave_produce_latency_ms", 0.50);
        var produceP90 = CalculatePercentile(histogramBuckets, "surgewave_produce_latency_ms", 0.90);
        var produceP99 = CalculatePercentile(histogramBuckets, "surgewave_produce_latency_ms", 0.99);
        var fetchP50 = CalculatePercentile(histogramBuckets, "surgewave_fetch_latency_ms", 0.50);
        var fetchP90 = CalculatePercentile(histogramBuckets, "surgewave_fetch_latency_ms", 0.90);
        var fetchP99 = CalculatePercentile(histogramBuckets, "surgewave_fetch_latency_ms", 0.99);

        return new MetricsSnapshot
        {
            Timestamp = DateTime.UtcNow,

            // Throughput
            MessagesProducedTotal = GetMetric(metrics, "surgewave_messages_produced_total"),
            MessagesFetchedTotal = GetMetric(metrics, "surgewave_messages_fetched_total"),
            BytesProducedTotal = GetMetric(metrics, "surgewave_bytes_produced_total"),
            BytesFetchedTotal = GetMetric(metrics, "surgewave_bytes_fetched_total"),

            // Latency
            ProduceLatencyP50 = produceP50,
            ProduceLatencyP90 = produceP90,
            ProduceLatencyP99 = produceP99,
            FetchLatencyP50 = fetchP50,
            FetchLatencyP90 = fetchP90,
            FetchLatencyP99 = fetchP99,

            // Connections
            ActiveConnections = (int)GetMetric(metrics, "surgewave_connections_active"),
            ConnectionsTotal = (long)GetMetric(metrics, "surgewave_connections_total"),

            // Topics & Partitions
            TopicCount = (int)GetMetric(metrics, "surgewave_topics"),
            PartitionCount = (int)GetMetric(metrics, "surgewave_partitions"),
            TotalLogSizeBytes = (long)GetMetric(metrics, "surgewave_log_size_bytes"),

            // Consumer Groups
            ActiveConsumerGroups = (int)GetMetric(metrics, "surgewave_consumer_groups_active"),
            MaxConsumerLag = (long)GetMetric(metrics, "surgewave_consumer_lag_max"),

            // Transactions
            ActiveTransactions = (int)GetMetric(metrics, "surgewave_transactions_active"),
            TransactionsTotal = (long)GetMetric(metrics, "surgewave_transactions_total"),
            TransactionCommitsTotal = (long)GetMetric(metrics, "surgewave_transaction_commits_total"),
            TransactionAbortsTotal = (long)GetMetric(metrics, "surgewave_transaction_aborts_total"),

            // Errors
            ErrorsTotal = (long)GetMetric(metrics, "surgewave_errors_total"),
            ProduceErrorsTotal = (long)GetMetric(metrics, "surgewave_produce_errors_total"),
            ThrottledRequestsTotal = (long)GetMetric(metrics, "surgewave_throttled_requests_total"),

            // Requests
            RequestsTotal = (long)GetMetric(metrics, "surgewave_requests_total"),

            // Per-topic metrics
            TopicMetrics = topicMetrics.Values.ToList()
        };
    }

    private static double GetMetric(Dictionary<string, double> metrics, string name)
    {
        return metrics.TryGetValue(name, out var value) ? value : 0;
    }

    private static double CalculatePercentile(
        Dictionary<string, List<(double le, double count)>> histogramBuckets,
        string metricName,
        double percentile)
    {
        if (!histogramBuckets.TryGetValue(metricName, out var buckets) || buckets.Count == 0)
            return 0;

        // Sort by le value
        var sorted = buckets.OrderBy(b => b.le).ToList();
        var total = sorted[^1].count;
        if (total == 0)
            return 0;

        var targetCount = total * percentile;

        // Find the bucket where the percentile falls
        for (var i = 0; i < sorted.Count; i++)
        {
            if (sorted[i].count >= targetCount)
            {
                if (i == 0)
                    return sorted[i].le / 2; // Estimate: half of first bucket

                // Linear interpolation between buckets
                var prevLe = i > 0 ? sorted[i - 1].le : 0;
                var prevCount = i > 0 ? sorted[i - 1].count : 0;
                var currLe = sorted[i].le;
                var currCount = sorted[i].count;

                if (currCount == prevCount)
                    return prevLe;

                var fraction = (targetCount - prevCount) / (currCount - prevCount);
                return prevLe + (currLe - prevLe) * fraction;
            }
        }

        return sorted[^1].le;
    }

    [GeneratedRegex(@"^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(\{(?<labels>[^}]*)\})?\s+(?<value>[^\s]+)")]
    private static partial Regex MetricLineRegex();

    [GeneratedRegex(@"le=""([^""]+)""")]
    private static partial Regex LeBucketRegex();

    [GeneratedRegex(@"topic=""([^""]+)""")]
    private static partial Regex TopicLabelRegex();
}
