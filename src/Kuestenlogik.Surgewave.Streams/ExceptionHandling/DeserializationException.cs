namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Exception thrown when deserialization fails and the handler returns Fail.
/// </summary>
public sealed class DeserializationException : Exception
{
    public string Topic { get; } = string.Empty;
    public int Partition { get; }
    public long Offset { get; }

    public DeserializationException()
    {
    }

    public DeserializationException(string message)
        : base(message)
    {
    }

    public DeserializationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public DeserializationException(string topic, int partition, long offset, string message, Exception innerException)
        : base(message, innerException)
    {
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }
}
