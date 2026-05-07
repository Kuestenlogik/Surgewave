using Kuestenlogik.Surgewave.Core.Dlq;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Dlq;

/// <summary>
/// Handles DLQ routing for Kafka Connect tasks.
/// </summary>
public sealed class ConnectDlqHandler
{
    private readonly DlqConfig _config;
    private readonly IDlqRouter _router;
    private readonly string _connectorName;
    private readonly int _taskId;
    private readonly string? _connectorClass;
    private readonly ILogger? _logger;

    public ConnectDlqHandler(
        DlqConfig config,
        IDlqRouter router,
        string connectorName,
        int taskId,
        string? connectorClass = null,
        ILogger? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _connectorName = connectorName ?? throw new ArgumentNullException(nameof(connectorName));
        _taskId = taskId;
        _connectorClass = connectorClass;
        _logger = logger;
    }

    /// <summary>
    /// The DLQ configuration.
    /// </summary>
    public DlqConfig Config => _config;

    /// <summary>
    /// Handle a failed sink record by routing to DLQ.
    /// </summary>
    public async Task<bool> HandleSinkRecordErrorAsync(
        SinkRecord record,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        if (!_config.Enabled)
        {
            return false;
        }

        var dlqRecord = new DlqRecord
        {
            OriginalTopic = record.Topic,
            OriginalPartition = record.Partition,
            OriginalOffset = record.Offset,
            OriginalKey = record.Key,
            OriginalValue = record.Value,
            OriginalTimestamp = record.Timestamp,
            OriginalHeaders = record.Headers,
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            ExceptionMessage = exception.Message,
            StackTrace = _config.IncludeStackTrace ? exception.StackTrace : null,
            SourceName = _connectorName,
            SourceType = "connect-sink",
            TaskId = _taskId.ToString(),
            AttemptCount = attemptCount,
            FailedAt = DateTimeOffset.UtcNow,
            AdditionalContext = BuildAdditionalContext()
        };

        return await _router.RouteAsync(dlqRecord, cancellationToken);
    }

    /// <summary>
    /// Handle a failed source record by routing to DLQ.
    /// </summary>
    public async Task<bool> HandleSourceRecordErrorAsync(
        SourceRecord record,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        if (!_config.Enabled)
        {
            return false;
        }

        var dlqRecord = new DlqRecord
        {
            OriginalTopic = record.Topic,
            OriginalPartition = record.Partition ?? 0,
            OriginalOffset = 0, // Source records don't have offsets yet
            OriginalKey = record.Key,
            OriginalValue = record.Value,
            OriginalTimestamp = record.Timestamp ?? DateTimeOffset.UtcNow,
            OriginalHeaders = null, // Source records typically don't have headers
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            ExceptionMessage = exception.Message,
            StackTrace = _config.IncludeStackTrace ? exception.StackTrace : null,
            SourceName = _connectorName,
            SourceType = "connect-source",
            TaskId = _taskId.ToString(),
            AttemptCount = attemptCount,
            FailedAt = DateTimeOffset.UtcNow,
            AdditionalContext = BuildAdditionalContext()
        };

        return await _router.RouteAsync(dlqRecord, cancellationToken);
    }

    private Dictionary<string, string>? BuildAdditionalContext()
    {
        if (_connectorClass == null)
        {
            return null;
        }

        return new Dictionary<string, string>
        {
            ["connector.class"] = _connectorClass
        };
    }
}
