namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

/// <summary>
/// Wire format for partition metadata including leader, replicas, and ISR.
/// </summary>
public readonly record struct PartitionMetadataPayload
{
    public int PartitionId { get; init; }
    public int Leader { get; init; }
    public int LeaderEpoch { get; init; }
    public int[] Replicas { get; init; }
    public int[] Isr { get; init; }
    public long HighWatermark { get; init; }
    public long LogStartOffset { get; init; }

    public static PartitionMetadataPayload Read(ref SurgewavePayloadReader reader)
    {
        var partitionId = reader.ReadInt32();
        var leader = reader.ReadInt32();
        var leaderEpoch = reader.ReadInt32();

        var replicaCount = reader.ReadInt16();
        var replicas = new int[replicaCount];
        for (int i = 0; i < replicaCount; i++)
            replicas[i] = reader.ReadInt32();

        var isrCount = reader.ReadInt16();
        var isr = new int[isrCount];
        for (int i = 0; i < isrCount; i++)
            isr[i] = reader.ReadInt32();

        var highWatermark = reader.ReadInt64();
        var logStartOffset = reader.ReadInt64();

        return new PartitionMetadataPayload
        {
            PartitionId = partitionId,
            Leader = leader,
            LeaderEpoch = leaderEpoch,
            Replicas = replicas,
            Isr = isr,
            HighWatermark = highWatermark,
            LogStartOffset = logStartOffset
        };
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(PartitionId);
        writer.WriteInt32(Leader);
        writer.WriteInt32(LeaderEpoch);

        var replicas = Replicas ?? [];
        writer.WriteInt16((short)replicas.Length);
        foreach (var r in replicas)
            writer.WriteInt32(r);

        var isr = Isr ?? [];
        writer.WriteInt16((short)isr.Length);
        foreach (var i in isr)
            writer.WriteInt32(i);

        writer.WriteInt64(HighWatermark);
        writer.WriteInt64(LogStartOffset);
    }

    public int EstimateSize()
    {
        // partitionId(4) + leader(4) + leaderEpoch(4) + replicaCount(2) + replicas(4*n) + isrCount(2) + isr(4*n) + hwm(8) + lso(8)
        return 4 + 4 + 4 + 2 + ((Replicas?.Length ?? 0) * 4) + 2 + ((Isr?.Length ?? 0) * 4) + 8 + 8;
    }
}
