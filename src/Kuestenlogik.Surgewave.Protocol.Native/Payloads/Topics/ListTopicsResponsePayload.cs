using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for ListTopics response containing multiple topic entries.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct ListTopicsResponsePayload
{
    public TopicInfoPayload[] Topics { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ListTopicsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt16();
        var topics = new TopicInfoPayload[count];

        for (int i = 0; i < count; i++)
        {
            topics[i] = TopicInfoPayload.Read(ref reader);
        }

        return new ListTopicsResponsePayload
        {
            Topics = topics
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt16((short)(Topics?.Length ?? 0));

        if (Topics != null)
        {
            foreach (var topic in Topics)
            {
                topic.Write(ref writer);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt16((short)(Topics?.Length ?? 0));

        if (Topics != null)
        {
            foreach (var topic in Topics)
            {
                topic.WriteTo(writer);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size = 2; // count

        if (Topics != null)
        {
            foreach (var topic in Topics)
            {
                size += topic.EstimateSize();
            }
        }

        return size;
    }
}
