namespace Kuestenlogik.Surgewave.Core.Models;

/// <summary>
/// Represents a message in the log with metadata
/// </summary>
public readonly record struct Message
{
    public required long Offset { get; init; }
    public required long Timestamp { get; init; }
    public required ReadOnlyMemory<byte> Key { get; init; }
    public required ReadOnlyMemory<byte> Value { get; init; }
    public required ReadOnlyMemory<byte> Headers { get; init; }

    public int TotalSize =>
        sizeof(long) + // Offset
        sizeof(long) + // Timestamp
        sizeof(int) + Key.Length + // Key length + data
        sizeof(int) + Value.Length + // Value length + data
        sizeof(int) + Headers.Length; // Headers length + data
}
