namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

/// <summary>
/// Wire format for VerifyLogIntegrity request.
/// </summary>
public readonly record struct VerifyLogIntegrityRequestPayload
{
    /// <summary>
    /// Topic to verify. Empty string = all topics.
    /// </summary>
    public string Topic { get; init; }

    /// <summary>
    /// Partition to verify. -1 = all partitions.
    /// </summary>
    public int Partition { get; init; }

    /// <summary>
    /// Maximum number of corrupted batches to find before stopping. 0 = no limit.
    /// </summary>
    public int MaxCorruptedBatches { get; init; }

    /// <summary>
    /// Include details for each corrupted batch.
    /// </summary>
    public bool IncludeDetails { get; init; }

    public static VerifyLogIntegrityRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new VerifyLogIntegrityRequestPayload
        {
            Topic = reader.ReadString() ?? string.Empty,
            Partition = reader.ReadInt32(),
            MaxCorruptedBatches = reader.ReadInt32(),
            IncludeDetails = reader.ReadUInt8() != 0
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt32(MaxCorruptedBatches);
        writer.WriteUInt8(IncludeDetails ? (byte)1 : (byte)0);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt32(MaxCorruptedBatches);
        writer.WriteUInt8(IncludeDetails ? (byte)1 : (byte)0);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") + // Topic
        4 + // Partition
        4 + // MaxCorruptedBatches
        1;  // IncludeDetails
}

/// <summary>
/// Wire format for corrupted batch details in verification result.
/// </summary>
public readonly record struct CorruptedBatchDetailPayload
{
    public string Topic { get; init; }
    public int Partition { get; init; }
    public long BaseOffset { get; init; }
    public uint ExpectedCrc { get; init; }
    public uint ActualCrc { get; init; }
    public int BatchLength { get; init; }

    public static CorruptedBatchDetailPayload Read(ref SurgewavePayloadReader reader)
    {
        return new CorruptedBatchDetailPayload
        {
            Topic = reader.ReadString() ?? string.Empty,
            Partition = reader.ReadInt32(),
            BaseOffset = reader.ReadInt64(),
            ExpectedCrc = reader.ReadUInt32(),
            ActualCrc = reader.ReadUInt32(),
            BatchLength = reader.ReadInt32()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt64(BaseOffset);
        writer.WriteUInt32(ExpectedCrc);
        writer.WriteUInt32(ActualCrc);
        writer.WriteInt32(BatchLength);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
        writer.WriteInt64(BaseOffset);
        writer.WriteUInt32(ExpectedCrc);
        writer.WriteUInt32(ActualCrc);
        writer.WriteInt32(BatchLength);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") + // Topic
        4 +  // Partition
        8 +  // BaseOffset
        4 +  // ExpectedCrc
        4 +  // ActualCrc
        4;   // BatchLength
}

/// <summary>
/// Wire format for VerifyLogIntegrity response.
/// </summary>
public readonly record struct VerifyLogIntegrityResponsePayload
{
    public int BatchesChecked { get; init; }
    public int CorruptedBatches { get; init; }
    public long BytesChecked { get; init; }
    public long CorruptedBytes { get; init; }
    public int PartitionsChecked { get; init; }
    public long DurationMs { get; init; }
    public IReadOnlyList<string> TopicsVerified { get; init; }
    public IReadOnlyList<CorruptedBatchDetailPayload> CorruptedBatchDetails { get; init; }

    public static VerifyLogIntegrityResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var batchesChecked = reader.ReadInt32();
        var corruptedBatches = reader.ReadInt32();
        var bytesChecked = reader.ReadInt64();
        var corruptedBytes = reader.ReadInt64();
        var partitionsChecked = reader.ReadInt32();
        var durationMs = reader.ReadInt64();

        var topicCount = reader.ReadInt32();
        var topics = new string[topicCount];
        for (int i = 0; i < topicCount; i++)
        {
            topics[i] = reader.ReadString() ?? string.Empty;
        }

        var detailCount = reader.ReadInt32();
        var details = new CorruptedBatchDetailPayload[detailCount];
        for (int i = 0; i < detailCount; i++)
        {
            details[i] = CorruptedBatchDetailPayload.Read(ref reader);
        }

        return new VerifyLogIntegrityResponsePayload
        {
            BatchesChecked = batchesChecked,
            CorruptedBatches = corruptedBatches,
            BytesChecked = bytesChecked,
            CorruptedBytes = corruptedBytes,
            PartitionsChecked = partitionsChecked,
            DurationMs = durationMs,
            TopicsVerified = topics,
            CorruptedBatchDetails = details
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(BatchesChecked);
        writer.WriteInt32(CorruptedBatches);
        writer.WriteInt64(BytesChecked);
        writer.WriteInt64(CorruptedBytes);
        writer.WriteInt32(PartitionsChecked);
        writer.WriteInt64(DurationMs);

        writer.WriteInt32(TopicsVerified.Count);
        foreach (var topic in TopicsVerified)
        {
            writer.WriteString(topic);
        }

        writer.WriteInt32(CorruptedBatchDetails.Count);
        foreach (var detail in CorruptedBatchDetails)
        {
            detail.Write(ref writer);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(BatchesChecked);
        writer.WriteInt32(CorruptedBatches);
        writer.WriteInt64(BytesChecked);
        writer.WriteInt64(CorruptedBytes);
        writer.WriteInt32(PartitionsChecked);
        writer.WriteInt64(DurationMs);

        writer.WriteInt32(TopicsVerified.Count);
        foreach (var topic in TopicsVerified)
        {
            writer.WriteString(topic);
        }

        writer.WriteInt32(CorruptedBatchDetails.Count);
        foreach (var detail in CorruptedBatchDetails)
        {
            detail.WriteTo(writer);
        }
    }

    public int EstimateSize() =>
        4 + // BatchesChecked
        4 + // CorruptedBatches
        8 + // BytesChecked
        8 + // CorruptedBytes
        4 + // PartitionsChecked
        8 + // DurationMs
        4 + TopicsVerified.Sum(t => 2 + System.Text.Encoding.UTF8.GetByteCount(t ?? "")) + // TopicsVerified
        4 + CorruptedBatchDetails.Sum(d => d.EstimateSize()); // CorruptedBatchDetails
}
