namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Consumer record
/// </summary>
public sealed record ConsumerRecord
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required long Timestamp { get; init; }
    public byte[]? Key { get; init; }
    public required byte[] Value { get; init; }
    public Dictionary<string, byte[]>? Headers { get; init; }
}
