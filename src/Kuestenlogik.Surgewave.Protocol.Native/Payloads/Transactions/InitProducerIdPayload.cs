using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Wire format for InitProducerId request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct InitProducerIdRequestPayload
{
    public string? TransactionalId { get; init; }
    public int TransactionTimeoutMs { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static InitProducerIdRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new InitProducerIdRequestPayload
        {
            TransactionalId = reader.ReadString(),
            TransactionTimeoutMs = reader.ReadInt32(),
            ProducerId = reader.ReadInt64(),
            ProducerEpoch = reader.ReadInt16()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt32(TransactionTimeoutMs);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteInt32(TransactionTimeoutMs);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 + (TransactionalId != null ? System.Text.Encoding.UTF8.GetByteCount(TransactionalId) : 0) + // TransactionalId (length prefix + bytes)
        4 +                                           // TransactionTimeoutMs
        8 +                                           // ProducerId
        2;                                            // ProducerEpoch
}

/// <summary>
/// Wire format for InitProducerId response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct InitProducerIdResponsePayload
{
    public ushort ErrorCode { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static InitProducerIdResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        return new InitProducerIdResponsePayload
        {
            ErrorCode = reader.ReadUInt16(),
            ProducerId = reader.ReadInt64(),
            ProducerEpoch = reader.ReadInt16()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        2 +  // ErrorCode
        8 +  // ProducerId
        2;   // ProducerEpoch
}
