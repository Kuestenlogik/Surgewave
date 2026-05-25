namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Exception thrown when production fails and the handler returns Fail.
/// </summary>
public sealed class ProductionException : Exception
{
    public string Topic { get; } = string.Empty;

    public ProductionException()
    {
    }

    public ProductionException(string message)
        : base(message)
    {
    }

    public ProductionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public ProductionException(string topic, string message, Exception innerException)
        : base(message, innerException)
    {
        Topic = topic;
    }
}
