using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;

/// <summary>
/// Wire format for GetConnectorConfig response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct ConnectorConfigPayload
{
    public IReadOnlyDictionary<string, string> Config { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ConnectorConfigPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var config = new Dictionary<string, string>(count);
        for (int i = 0; i < count; i++)
        {
            var key = reader.ReadString() ?? string.Empty;
            var value = reader.ReadString() ?? string.Empty;
            config[key] = value;
        }

        return new ConnectorConfigPayload
        {
            Config = config
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Config?.Count ?? 0);
        if (Config != null)
        {
            foreach (var (key, value) in Config)
            {
                writer.WriteString(key);
                writer.WriteString(value);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Config?.Count ?? 0);
        if (Config != null)
        {
            foreach (var (key, value) in Config)
            {
                writer.WriteString(key);
                writer.WriteString(value);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size = 4; // Count
        if (Config != null)
        {
            foreach (var (key, value) in Config)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(key ?? "");
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(value ?? "");
            }
        }
        return size;
    }
}
