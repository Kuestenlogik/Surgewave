namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Streaming;

/// <summary>
/// Wire format for Subscribe response.
/// Confirms the subscription and echoes back the assigned partitions.
/// </summary>
public readonly record struct SubscribeResponsePayload
{
    /// <summary>
    /// Subscription identifier echoed from the request.
    /// </summary>
    public string SubscriptionId { get; init; }

    /// <summary>
    /// Partitions that the broker will push messages from.
    /// </summary>
    public int[] Partitions { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static SubscribeResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var subscriptionId = reader.ReadString() ?? "";

        var partitionCount = reader.ReadInt32();
        var partitions = new int[partitionCount];
        for (var i = 0; i < partitionCount; i++)
            partitions[i] = reader.ReadInt32();

        return new SubscribeResponsePayload
        {
            SubscriptionId = subscriptionId,
            Partitions = partitions
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteInt32(Partitions.Length);
        foreach (var p in Partitions)
            writer.WriteInt32(p);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(SubscriptionId);
        writer.WriteInt32(Partitions.Length);
        foreach (var p in Partitions)
            writer.WriteInt32(p);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(SubscriptionId ?? "") +
        4 + // Partition count
        Partitions.Length * 4;
}
