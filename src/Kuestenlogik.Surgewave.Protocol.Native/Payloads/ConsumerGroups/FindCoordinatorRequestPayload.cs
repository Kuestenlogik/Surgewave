using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for FindCoordinator request.
/// Shared between client (write) and broker (read) to ensure consistency.
/// </summary>
public readonly record struct FindCoordinatorRequestPayload
{
    public string Key { get; init; }
    public byte KeyType { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static FindCoordinatorRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new FindCoordinatorRequestPayload
        {
            Key = reader.ReadString() ?? "",
            KeyType = reader.ReadUInt8()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Key);
        writer.WriteUInt8(KeyType);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Key);
        writer.WriteUInt8(KeyType);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Key ?? "") +
        1; // KeyType
}
