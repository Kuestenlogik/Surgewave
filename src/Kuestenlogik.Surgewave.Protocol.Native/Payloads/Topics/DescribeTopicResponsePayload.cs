namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for DescribeTopic response containing detailed topic and partition info.
/// </summary>
public readonly record struct DescribeTopicResponsePayload
{
    public string TopicName { get; init; }
    public int PartitionCount { get; init; }
    public short ReplicationFactor { get; init; }
    public bool IsInternal { get; init; }
    public PartitionMetadataPayload[] Partitions { get; init; }

    public static DescribeTopicResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var topicName = reader.ReadString() ?? string.Empty;
        var partitionCount = reader.ReadInt32();
        var replicationFactor = reader.ReadInt16();
        var isInternal = reader.ReadUInt8() != 0;

        var partitions = new PartitionMetadataPayload[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            partitions[i] = PartitionMetadataPayload.Read(ref reader);
        }

        return new DescribeTopicResponsePayload
        {
            TopicName = topicName,
            PartitionCount = partitionCount,
            ReplicationFactor = replicationFactor,
            IsInternal = isInternal,
            Partitions = partitions
        };
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TopicName);
        writer.WriteInt32(PartitionCount);
        writer.WriteInt16(ReplicationFactor);
        writer.WriteUInt8((byte)(IsInternal ? 1 : 0));

        var partitions = Partitions ?? [];
        foreach (var partition in partitions)
        {
            partition.WriteTo(writer);
        }
    }

    public int EstimateSize()
    {
        // topicName(2+n) + partitionCount(4) + replicationFactor(2) + isInternal(1) + partitions
        int size = 2 + System.Text.Encoding.UTF8.GetByteCount(TopicName ?? "") + 4 + 2 + 1;

        if (Partitions != null)
        {
            foreach (var p in Partitions)
                size += p.EstimateSize();
        }

        return size;
    }
}
