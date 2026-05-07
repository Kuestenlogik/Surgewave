using Kuestenlogik.Surgewave.Core.Dlq;

namespace Kuestenlogik.Surgewave.Connect.Dlq;

/// <summary>
/// DLQ configuration keys for Kafka Connect (Kafka-compatible naming).
/// </summary>
public static class ConnectDlqConfigKeys
{
    public const string Enabled = "errors.deadletterqueue.topic.enable";
    public const string TopicSuffix = "errors.deadletterqueue.topic.suffix";
    public const string MaxRetries = "errors.retry.max";
    public const string RetryBackoff = "errors.retry.backoff.ms";
    public const string IncludeStackTrace = "errors.deadletterqueue.context.headers.enable";
    public const string PartitionCount = "errors.deadletterqueue.topic.partitions";
    public const string RetentionMs = "errors.deadletterqueue.topic.retention.ms";
}

/// <summary>
/// Factory for creating DLQ configuration from connector properties.
/// </summary>
public static class ConnectDlqConfigFactory
{
    /// <summary>
    /// Create a DlqConfig from connector configuration properties.
    /// </summary>
    public static DlqConfig FromConnectorConfig(IDictionary<string, string> config, ConnectWorkerConfig? workerConfig = null)
    {
        var dlqConfig = new DlqConfig();

        // Use worker defaults if available
        if (workerConfig != null)
        {
            dlqConfig.Enabled = workerConfig.EnableDlq;
            dlqConfig.TopicSuffix = workerConfig.DlqTopicSuffix;
            dlqConfig.MaxRetries = workerConfig.DlqMaxRetries;
            dlqConfig.RetryBackoffMs = workerConfig.DlqRetryBackoffMs;
        }

        // Override with connector-specific settings
        if (config.TryGetValue(ConnectDlqConfigKeys.Enabled, out var enabledStr))
        {
            dlqConfig.Enabled = bool.TryParse(enabledStr, out var enabled) && enabled;
        }

        if (config.TryGetValue(ConnectDlqConfigKeys.TopicSuffix, out var suffix))
        {
            dlqConfig.TopicSuffix = suffix;
        }

        if (config.TryGetValue(ConnectDlqConfigKeys.MaxRetries, out var retriesStr) &&
            int.TryParse(retriesStr, out var retries))
        {
            dlqConfig.MaxRetries = retries;
        }

        if (config.TryGetValue(ConnectDlqConfigKeys.RetryBackoff, out var backoffStr) &&
            int.TryParse(backoffStr, out var backoff))
        {
            dlqConfig.RetryBackoffMs = backoff;
        }

        if (config.TryGetValue(ConnectDlqConfigKeys.IncludeStackTrace, out var stackTraceStr))
        {
            dlqConfig.IncludeStackTrace = bool.TryParse(stackTraceStr, out var includeStack) && includeStack;
        }

        if (config.TryGetValue(ConnectDlqConfigKeys.PartitionCount, out var partitionsStr) &&
            int.TryParse(partitionsStr, out var partitions))
        {
            dlqConfig.DlqPartitionCount = partitions;
        }

        if (config.TryGetValue(ConnectDlqConfigKeys.RetentionMs, out var retentionStr) &&
            long.TryParse(retentionStr, out var retention))
        {
            dlqConfig.RetentionMs = retention;
        }

        return dlqConfig;
    }
}
