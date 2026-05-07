using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Processing exception handler that logs and skips the failed record.
/// </summary>
public sealed class LogAndSkipProcessingHandler : IProcessingExceptionHandler
{
    private readonly ILogger _logger;
    private readonly StreamsMetrics? _metrics;

    public LogAndSkipProcessingHandler()
        : this(NullLogger.Instance, null)
    {
    }

    public LogAndSkipProcessingHandler(ILogger logger, StreamsMetrics? metrics)
    {
        _logger = logger;
        _metrics = metrics;
    }

    public ProcessingHandlerResponse Handle(
        string topic,
        int partition,
        long offset,
        object? key,
        object? value,
        Exception exception)
    {
        _logger.LogWarning(
            exception,
            "Skipping record due to processing error at {Topic}-{Partition}@{Offset}",
            topic,
            partition,
            offset);

        _metrics?.RecordProcessingError();

        return ProcessingHandlerResponse.Skip;
    }
}
