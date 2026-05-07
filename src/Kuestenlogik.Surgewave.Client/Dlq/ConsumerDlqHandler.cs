using Kuestenlogik.Surgewave.Core.Dlq;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Client.Dlq;

/// <summary>
/// Wraps consumer message handlers with retry and DLQ support.
/// </summary>
public sealed class ConsumerDlqHandler<TKey, TValue>
{
    private readonly ConsumerDlqConfig _config;
    private readonly IDlqRouter _router;
    private readonly string _consumerGroupId;
    private readonly string? _clientId;
    private readonly ILogger? _logger;
    private readonly Func<TKey?, byte[]?> _keySerializer;
    private readonly Func<TValue, byte[]> _valueSerializer;

    public ConsumerDlqHandler(
        ConsumerDlqConfig config,
        IDlqRouter router,
        string consumerGroupId,
        string? clientId = null,
        Func<TKey?, byte[]?>? keySerializer = null,
        Func<TValue, byte[]>? valueSerializer = null,
        ILogger? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _router = router ?? throw new ArgumentNullException(nameof(router));
        _consumerGroupId = consumerGroupId ?? throw new ArgumentNullException(nameof(consumerGroupId));
        _clientId = clientId;
        _keySerializer = keySerializer ?? DefaultKeySerializer;
        _valueSerializer = valueSerializer ?? DefaultValueSerializer;
        _logger = logger;
    }

    /// <summary>
    /// The DLQ configuration.
    /// </summary>
    public ConsumerDlqConfig Config => _config;

    /// <summary>
    /// Execute a handler with retry and DLQ support.
    /// </summary>
    /// <returns>True if handler succeeded, false if routed to DLQ.</returns>
    public async Task<bool> ExecuteWithDlqAsync(
        Consumer.ConsumeResult<TKey, TValue> message,
        Func<Consumer.ConsumeResult<TKey, TValue>, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        if (!_config.EnableDlq)
        {
            await handler(message, cancellationToken);
            return true;
        }

        var attemptCount = 0;
        Exception? lastException = null;

        while (attemptCount <= _config.MaxRetries)
        {
            attemptCount++;
            try
            {
                await handler(message, cancellationToken);
                return true; // Success
            }
            catch (OperationCanceledException)
            {
                throw; // Don't retry cancellation
            }
            catch (Exception ex) when (attemptCount <= _config.MaxRetries)
            {
                lastException = ex;
                _logger?.LogWarning(ex,
                    "Consumer handler failed attempt {Attempt}/{Max} for {Topic}:{Partition}:{Offset}",
                    attemptCount, _config.MaxRetries,
                    message.Topic, message.Partition, message.Offset);

                if (attemptCount <= _config.MaxRetries)
                {
                    await Task.Delay(_config.RetryBackoffMs, cancellationToken);
                }
            }
        }

        // Route to DLQ
        if (lastException != null)
        {
            await RouteToDlqAsync(message, lastException, attemptCount, cancellationToken);
        }

        return false;
    }

    private async Task RouteToDlqAsync(
        Consumer.ConsumeResult<TKey, TValue> message,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        var dlqRecord = new DlqRecord
        {
            OriginalTopic = message.Topic,
            OriginalPartition = message.Partition,
            OriginalOffset = message.Offset,
            OriginalKey = _keySerializer(message.Key),
            OriginalValue = _valueSerializer(message.Value),
            OriginalTimestamp = message.Timestamp,
            OriginalHeaders = null, // ConsumeResult doesn't expose headers currently
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            ExceptionMessage = exception.Message,
            StackTrace = _config.IncludeStackTrace ? exception.StackTrace : null,
            SourceName = _consumerGroupId,
            SourceType = "consumer",
            TaskId = _clientId,
            AttemptCount = attemptCount,
            FailedAt = DateTimeOffset.UtcNow
        };

        await _router.RouteAsync(dlqRecord, cancellationToken);
    }

    private static byte[]? DefaultKeySerializer(TKey? key)
    {
        if (key == null) return null;
        if (key is byte[] bytes) return bytes;
        if (key is string str) return System.Text.Encoding.UTF8.GetBytes(str);
        return System.Text.Encoding.UTF8.GetBytes(key.ToString() ?? "");
    }

    private static byte[] DefaultValueSerializer(TValue value)
    {
        if (value is byte[] bytes) return bytes;
        if (value is string str) return System.Text.Encoding.UTF8.GetBytes(str);
        return System.Text.Encoding.UTF8.GetBytes(value?.ToString() ?? "");
    }
}
