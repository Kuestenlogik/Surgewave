using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Deserialization exception handler that logs and continues processing.
/// </summary>
public sealed class LogAndContinueDeserializationHandler : IDeserializationExceptionHandler
{
    private readonly ILogger _logger;
    private readonly StreamsMetrics? _metrics;

    public LogAndContinueDeserializationHandler()
        : this(NullLogger.Instance, null)
    {
    }

    public LogAndContinueDeserializationHandler(ILogger logger, StreamsMetrics? metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    public DeserializationHandlerResponse Handle(
        string topic,
        int partition,
        long offset,
        byte[]? rawKey,
        byte[]? rawValue,
        Exception exception)
    {
        _logger.LogWarning(
            exception,
            "Skipping record due to deserialization error at {Topic}-{Partition}@{Offset}",
            topic,
            partition,
            offset);

        _metrics?.RecordDeserializationError();

        return DeserializationHandlerResponse.Continue;
    }
}
