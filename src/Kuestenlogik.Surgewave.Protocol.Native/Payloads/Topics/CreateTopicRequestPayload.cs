using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for CreateTopic request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct CreateTopicRequestPayload
{
    public string Name { get; init; }
    public int Partitions { get; init; }
    public short ReplicationFactor { get; init; }
    public TopicConfigPayload[]? Configs { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static CreateTopicRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var name = reader.ReadString() ?? string.Empty;
        var partitions = reader.ReadInt32();
        var replicationFactor = reader.ReadInt16();

        TopicConfigPayload[]? configs = null;
        if (reader.Remaining > 0)
        {
            var configCount = reader.ReadInt16();
            if (configCount > 0)
            {
                configs = new TopicConfigPayload[configCount];
                for (int i = 0; i < configCount; i++)
                {
                    configs[i] = TopicConfigPayload.Read(ref reader);
                }
            }
        }

        return new CreateTopicRequestPayload
        {
            Name = name,
            Partitions = partitions,
            ReplicationFactor = replicationFactor,
            Configs = configs
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Name);
        writer.WriteInt32(Partitions);
        writer.WriteInt16(ReplicationFactor);
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
        writer.WriteString(Name);
        writer.WriteInt32(Partitions);
        writer.WriteInt16(ReplicationFactor);
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
        int size = 2 + System.Text.Encoding.UTF8.GetByteCount(Name ?? "") + // Name (length prefix + bytes)
                   4 +                                                       // Partitions
                   2 +                                                       // ReplicationFactor
                   2;                                                        // Config count

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
