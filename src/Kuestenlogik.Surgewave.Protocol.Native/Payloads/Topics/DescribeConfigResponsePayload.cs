using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for DescribeConfig response containing topic name and configs.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct DescribeConfigResponsePayload
{
    public string TopicName { get; init; }
    public TopicConfigPayload[] Configs { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static DescribeConfigResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var topicName = reader.ReadString() ?? string.Empty;
        var configCount = reader.ReadInt16();
        var configs = new TopicConfigPayload[configCount];

        for (int i = 0; i < configCount; i++)
        {
            configs[i] = TopicConfigPayload.Read(ref reader);
        }

        return new DescribeConfigResponsePayload
        {
            TopicName = topicName,
            Configs = configs
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt16((short)(Configs?.Length ?? 0));

        if (Configs != null)
        {
            foreach (var config in Configs)
            {
                config.Write(ref writer);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt16((short)(Configs?.Length ?? 0));

        if (Configs != null)
        {
            foreach (var config in Configs)
            {
                config.WriteTo(writer);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size = 2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? "") + // TopicName
                   2;                                                              // config count

        if (Configs != null)
        {
            foreach (var config in Configs)
            {
                size += config.EstimateSize();
            }
        }

        return size;
    }
}
