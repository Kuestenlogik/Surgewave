using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Core.Exceptions;

/// <summary>
/// Exception thrown when data corruption is detected and recovery mode is set to FailFast.
/// </summary>
public class DataCorruptionException : Exception
{
    /// <summary>
    /// Creates a new DataCorruptionException with default message.
    /// </summary>
    public DataCorruptionException()
        : base("Data corruption detected")
    {
    }

    /// <summary>
    /// Creates a new DataCorruptionException with specified message.
    /// </summary>
    public DataCorruptionException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Creates a new DataCorruptionException with message and inner exception.
    /// </summary>
    public DataCorruptionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// The topic containing the corrupted data.
    /// </summary>
    public string? Topic { get; }

    /// <summary>
    /// The partition containing the corrupted data.
    /// </summary>
    public int? Partition { get; }

    /// <summary>
    /// The base offset of the corrupted batch.
    /// </summary>
    public long? BaseOffset { get; }

    /// <summary>
    /// The CRC value stored in the batch header.
    /// </summary>
    public uint? ExpectedCrc { get; }

    /// <summary>
    /// The CRC value computed from the batch data.
    /// </summary>
    public uint? ActualCrc { get; }

    /// <summary>
    /// Creates a new DataCorruptionException.
    /// </summary>
    public DataCorruptionException(string topic, int partition, long baseOffset, uint expectedCrc, uint actualCrc)
        : base($"Data corruption detected in {topic}-{partition} at offset {baseOffset}: expected CRC 0x{expectedCrc:X8}, actual 0x{actualCrc:X8}")
    {
        Topic = topic;
        Partition = partition;
        BaseOffset = baseOffset;
        ExpectedCrc = expectedCrc;
        ActualCrc = actualCrc;
    }

    /// <summary>
    /// Creates a new DataCorruptionException from corruption info.
    /// </summary>
    public DataCorruptionException(CorruptedBatchInfo info)
        : this(info.Topic, info.Partition, info.BaseOffset, info.ExpectedCrc, info.ActualCrc)
    {
    }

    /// <summary>
    /// Creates a new DataCorruptionException with an inner exception.
    /// </summary>
    public DataCorruptionException(string topic, int partition, long baseOffset, uint expectedCrc, uint actualCrc, Exception innerException)
        : base($"Data corruption detected in {topic}-{partition} at offset {baseOffset}: expected CRC 0x{expectedCrc:X8}, actual 0x{actualCrc:X8}", innerException)
    {
        Topic = topic;
        Partition = partition;
        BaseOffset = baseOffset;
        ExpectedCrc = expectedCrc;
        ActualCrc = actualCrc;
    }
}
