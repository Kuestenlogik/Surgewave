using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Default deserialization exception handler that logs and fails.
/// </summary>
public sealed class LogAndFailDeserializationHandler : IDeserializationExceptionHandler
{
    private readonly ILogger _logger;

    public LogAndFailDeserializationHandler()
        : this(NullLogger.Instance)
    {
    }

    public LogAndFailDeserializationHandler(ILogger logger)
    {
        _logger = logger;
    }

    public DeserializationHandlerResponse Handle(
        string topic,
        int partition,
        long offset,
        byte[]? rawKey,
        byte[]? rawValue,
        Exception exception)
    {
        _logger.LogError(
            exception,
            "Deserialization failed for record at {Topic}-{Partition}@{Offset}",
            topic,
            partition,
            offset);

        return DeserializationHandlerResponse.Fail;
    }
}
