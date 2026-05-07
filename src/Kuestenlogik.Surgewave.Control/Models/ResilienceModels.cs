namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Dead letter queue topic information.
/// </summary>
public sealed class DlqTopicInfo
{
    public string TopicName { get; set; } = "";
    public string? SourceTopic { get; set; }
    public int PartitionCount { get; set; }
    public long TotalMessages { get; set; }
    public DateTimeOffset? OldestMessage { get; set; }
    public DateTimeOffset? NewestMessage { get; set; }
    public Dictionary<string, int> ErrorGroups { get; set; } = [];
}

/// <summary>
/// A DLQ message with error metadata.
/// </summary>
public sealed class DlqMessageInfo
{
    public long Offset { get; set; }
    public int Partition { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public string? Key { get; set; }
    public string? Value { get; set; }
    public string? OriginalTopic { get; set; }
    public int? OriginalPartition { get; set; }
    public long? OriginalOffset { get; set; }
    public string? ErrorReason { get; set; }
    public string? ErrorClass { get; set; }
    public int RetryCount { get; set; }
    public IReadOnlyDictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
}

/// <summary>
/// Connector health metrics snapshot.
/// </summary>
public sealed class ConnectorHealthInfo
{
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public string State { get; set; } = "";
    public int TotalTasks { get; set; }
    public int RunningTasks { get; set; }
    public int FailedTasks { get; set; }
    public int PausedTasks { get; set; }
    public double RecordsPerSecond { get; set; }
    public long TotalRecords { get; set; }
    public long TotalErrors { get; set; }
    public double ErrorRate { get; set; }
    public TimeSpan Uptime { get; set; }
    public DateTimeOffset LastRestartedAt { get; set; }
    public List<ConnectorTaskHealth> Tasks { get; set; } = [];
    public List<ConnectorLogEntry> RecentLogs { get; set; } = [];
}

/// <summary>
/// Individual connector task health.
/// </summary>
public sealed class ConnectorTaskHealth
{
    public int TaskId { get; set; }
    public string State { get; set; } = "";
    public string WorkerId { get; set; } = "";
    public long RecordsProcessed { get; set; }
    public long ErrorCount { get; set; }
    public DateTimeOffset? LastErrorAt { get; set; }
    public string? LastErrorMessage { get; set; }
}

/// <summary>
/// Connector log entry.
/// </summary>
public sealed class ConnectorLogEntry
{
    public DateTimeOffset Timestamp { get; set; }
    public string Level { get; set; } = "INFO";
    public string Message { get; set; } = "";
    public string? TaskId { get; set; }
    public string? Exception { get; set; }
}

/// <summary>
/// Auto-restart configuration for a connector.
/// </summary>
public sealed class ConnectorAutoRestartConfig
{
    public bool Enabled { get; set; }
    public int MaxRetries { get; set; } = 3;
    public int BackoffSeconds { get; set; } = 60;
    public bool RestartOnlyFailed { get; set; } = true;
}
