using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Default production exception handler that logs and fails.
/// </summary>
public sealed class LogAndFailProductionHandler : IProductionExceptionHandler
{
    private readonly ILogger _logger;

    public LogAndFailProductionHandler()
        : this(NullLogger.Instance)
    {
    }

    public LogAndFailProductionHandler(ILogger logger)
    {
        _logger = logger;
    }

    public ProductionHandlerResponse Handle(
        string topic,
        byte[]? key,
        byte[]? value,
        Exception exception)
    {
        _logger.LogError(
            exception,
            "Production failed for topic {Topic}",
            topic);

        return ProductionHandlerResponse.Fail;
    }
}
