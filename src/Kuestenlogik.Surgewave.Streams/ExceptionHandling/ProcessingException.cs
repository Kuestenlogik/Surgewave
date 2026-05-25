namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Exception thrown when processing fails and the handler returns Fail.
/// </summary>
public sealed class ProcessingException : Exception
{
    public string Topic { get; } = string.Empty;
    public int Partition { get; }
    public long Offset { get; }

    public ProcessingException()
    {
    }

    public ProcessingException(string message)
        : base(message)
    {
    }

    public ProcessingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public ProcessingException(string topic, int partition, long offset, string message, Exception innerException)
        : base(message, innerException)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}
