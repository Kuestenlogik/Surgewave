namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Wire format for AddOffsetsToTxn request.
/// Used to add consumer group offsets to a transaction before committing them.
/// </summary>
public readonly record struct AddOffsetsToTxnRequestPayload
{
    public string TransactionalId { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public string GroupId { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static AddOffsetsToTxnRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new AddOffsetsToTxnRequestPayload
        {
            TransactionalId = reader.ReadString() ?? string.Empty,
            ProducerId = reader.ReadInt64(),
            ProducerEpoch = reader.ReadInt16(),
            GroupId = reader.ReadString() ?? string.Empty
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteString(GroupId);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteString(GroupId);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        return 2 + System.Text.Encoding.UTF8.GetByteCount(TransactionalId ?? "") +
               8 +  // ProducerId
               2 +  // ProducerEpoch
               2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "");
    }
}

/// <summary>
/// Wire format for AddOffsetsToTxn response.
/// </summary>
public readonly record struct AddOffsetsToTxnResponsePayload
{
    public ushort ErrorCode { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static AddOffsetsToTxnResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new AddOffsetsToTxnResponsePayload
        {
            ErrorCode = reader.ReadUInt16()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() => 2;
}
