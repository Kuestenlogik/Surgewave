namespace Kuestenlogik.Surgewave.Client.Producer;

/// <summary>
/// Producer record to be sent
/// </summary>
public sealed record ProducerRecord
{
    public required string Topic { get; init; }
    public int? Partition { get; init; }
    public byte[]? Key { get; init; }
    public required byte[] Value { get; init; }
    public Dictionary<string, byte[]>? Headers { get; init; }
    public long? Timestamp { get; init; }
}
