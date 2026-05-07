using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Default processing exception handler that logs and fails.
/// </summary>
public sealed class LogAndFailProcessingHandler : IProcessingExceptionHandler
{
    private readonly ILogger _logger;

    public LogAndFailProcessingHandler()
        : this(NullLogger.Instance)
    {
    }

    public LogAndFailProcessingHandler(ILogger logger)
    {
        _logger = logger;
    }

    public ProcessingHandlerResponse Handle(
        string topic,
        int partition,
        long offset,
        object? key,
        object? value,
        Exception exception)
    {
        _logger.LogError(
            exception,
            "Processing failed for record at {Topic}-{Partition}@{Offset}",
            topic,
            partition,
            offset);

        return ProcessingHandlerResponse.Fail;
    }
}
