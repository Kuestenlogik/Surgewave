namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

/// <summary>
/// Wire format for partition reassignment request.
/// </summary>
public readonly record struct PartitionReassignmentRequestPayload
{
    public string Topic { get; init; }
    public int Partition { get; init; }
    public IReadOnlyList<int> Replicas { get; init; }

    public static PartitionReassignmentRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var topic = reader.ReadString() ?? string.Empty;
        var partition = reader.ReadInt32();
        var replicaCount = reader.ReadInt32();
        var replicas = new int[replicaCount];

        for (int i = 0; i < replicaCount; i++)
        {
            replicas[i] = reader.ReadInt32();
        }

        return new PartitionReassignmentRequestPayload
        {
            Topic = topic,
            Partition = partition,
            Replicas = replicas
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt32(Replicas.Count);
        foreach (var replica in Replicas)
        {
            writer.WriteInt32(replica);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt32(Replicas.Count);
        foreach (var replica in Replicas)
        {
            writer.WriteInt32(replica);
        }
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") + // Topic
        4 +                                                        // Partition
        4 +                                                        // Replicas count
        Replicas.Count * 4;                                        // Replicas
}

/// <summary>
/// Wire format for AlterPartitionReassignments request.
/// </summary>
public readonly record struct AlterReassignmentsRequestPayload
{
    public IReadOnlyList<PartitionReassignmentRequestPayload> Reassignments { get; init; }

    public static AlterReassignmentsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var reassignments = new PartitionReassignmentRequestPayload[count];

        for (int i = 0; i < count; i++)
        {
            reassignments[i] = PartitionReassignmentRequestPayload.Read(ref reader);
        }

        return new AlterReassignmentsRequestPayload { Reassignments = reassignments };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Reassignments.Count);
        foreach (var r in Reassignments)
        {
            r.Write(ref writer);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Reassignments.Count);
        foreach (var r in Reassignments)
        {
            r.WriteTo(writer);
        }
    }

    public int EstimateSize() =>
        4 + Reassignments.Sum(r => r.EstimateSize()); // Count + all reassignments
}

/// <summary>
/// Wire format for AlterPartitionReassignments response.
/// </summary>
public readonly record struct AlterReassignmentsResponsePayload
{
    public bool Success { get; init; }
    public int PartitionCount { get; init; }

    public static AlterReassignmentsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new AlterReassignmentsResponsePayload
        {
            Success = reader.ReadUInt8() != 0,
            PartitionCount = reader.ReadInt32()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt8(Success ? (byte)1 : (byte)0);
        writer.WriteInt32(PartitionCount);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt8(Success ? (byte)1 : (byte)0);
        writer.WriteInt32(PartitionCount);
    }

    public int EstimateSize() => 1 + 4; // Success + PartitionCount
}

/// <summary>
/// Reassignment status code.
/// </summary>
public enum ReassignmentStatus : byte
{
    Pending = 0,
    Adding = 1,
    Syncing = 2,
    Completing = 3,
    Completed = 4,
    Failed = 5,
    Cancelled = 6
}

/// <summary>
/// Wire format for partition reassignment status.
/// </summary>
public readonly record struct PartitionReassignmentStatusPayload
{
    public string Topic { get; init; }
    public int Partition { get; init; }
    public ReassignmentStatus Status { get; init; }
    public int ProgressPercent { get; init; }
    public IReadOnlyList<int> OriginalReplicas { get; init; }
    public IReadOnlyList<int> TargetReplicas { get; init; }

    public static PartitionReassignmentStatusPayload Read(ref SurgewavePayloadReader reader)
    {
        var topic = reader.ReadString() ?? string.Empty;
        var partition = reader.ReadInt32();
        var status = (ReassignmentStatus)reader.ReadUInt8();
        var progressPercent = reader.ReadInt32();

        var originalCount = reader.ReadInt32();
        var originalReplicas = new int[originalCount];
        for (int i = 0; i < originalCount; i++)
        {
            originalReplicas[i] = reader.ReadInt32();
        }

        var targetCount = reader.ReadInt32();
        var targetReplicas = new int[targetCount];
        for (int i = 0; i < targetCount; i++)
        {
            targetReplicas[i] = reader.ReadInt32();
        }

        return new PartitionReassignmentStatusPayload
        {
            Topic = topic,
            Partition = partition,
            Status = status,
            ProgressPercent = progressPercent,
            OriginalReplicas = originalReplicas,
            TargetReplicas = targetReplicas
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteUInt8((byte)Status);
        writer.WriteInt32(ProgressPercent);

        writer.WriteInt32(OriginalReplicas.Count);
        foreach (var r in OriginalReplicas)
        {
            writer.WriteInt32(r);
        }

        writer.WriteInt32(TargetReplicas.Count);
        foreach (var r in TargetReplicas)
        {
            writer.WriteInt32(r);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteUInt8((byte)Status);
        writer.WriteInt32(ProgressPercent);

        writer.WriteInt32(OriginalReplicas.Count);
        foreach (var r in OriginalReplicas)
        {
            writer.WriteInt32(r);
        }

        writer.WriteInt32(TargetReplicas.Count);
        foreach (var r in TargetReplicas)
        {
            writer.WriteInt32(r);
        }
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") + // Topic
        4 +                                                        // Partition
        1 +                                                        // Status
        4 +                                                        // ProgressPercent
        4 + OriginalReplicas.Count * 4 +                          // OriginalReplicas
        4 + TargetReplicas.Count * 4;                             // TargetReplicas
}

/// <summary>
/// Wire format for ListPartitionReassignments response.
/// </summary>
public readonly record struct ListReassignmentsPayload
{
    public IReadOnlyList<PartitionReassignmentStatusPayload> Reassignments { get; init; }

    public static ListReassignmentsPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var reassignments = new PartitionReassignmentStatusPayload[count];

        for (int i = 0; i < count; i++)
        {
            reassignments[i] = PartitionReassignmentStatusPayload.Read(ref reader);
        }

        return new ListReassignmentsPayload { Reassignments = reassignments };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Reassignments.Count);
        foreach (var r in Reassignments)
        {
            r.Write(ref writer);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Reassignments.Count);
        foreach (var r in Reassignments)
        {
            r.WriteTo(writer);
        }
    }

    public int EstimateSize() =>
        4 + Reassignments.Sum(r => r.EstimateSize()); // Count + all status entries
}
