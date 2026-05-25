using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Wire format for EndTxn request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct EndTxnRequestPayload
{
    public string TransactionalId { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public bool Committed { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static EndTxnRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new EndTxnRequestPayload
        {
            TransactionalId = reader.ReadString() ?? string.Empty,
            ProducerId = reader.ReadInt64(),
            ProducerEpoch = reader.ReadInt16(),
            Committed = reader.ReadUInt8() != 0
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteUInt8(Committed ? (byte)1 : (byte)0);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteUInt8(Committed ? (byte)1 : (byte)0);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TransactionalId ?? "") + // TransactionalId
        8 +                                           // ProducerId
        2 +                                           // ProducerEpoch
        1;                                            // Committed
}

/// <summary>
/// Wire format for EndTxn response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct EndTxnResponsePayload
{
    public ushort ErrorCode { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static EndTxnResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new EndTxnResponsePayload
        {
            ErrorCode = reader.ReadUInt16()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() => 2; // ErrorCode
}
