using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;

/// <summary>
/// Wire format for ListConnectors response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct ListConnectorsPayload
{
    public IReadOnlyList<string> Connectors { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ListConnectorsPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var connectors = new List<string>(count);
        for (int i = 0; i < count; i++)
        {
            connectors.Add(reader.ReadString() ?? string.Empty);
        }

        return new ListConnectorsPayload
        {
            Connectors = connectors
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Connectors?.Count ?? 0);
        if (Connectors != null)
        {
            foreach (var connector in Connectors)
            {
                writer.WriteString(connector);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Connectors?.Count ?? 0);
        if (Connectors != null)
        {
            foreach (var connector in Connectors)
            {
                writer.WriteString(connector);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size = 4; // Count
        if (Connectors != null)
        {
            foreach (var connector in Connectors)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(connector ?? "");
            }
        }
        return size;
    }
}
