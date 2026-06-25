namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Wire format for CrossTopicTxnBegin request.
/// </summary>
public readonly record struct CrossTopicTxnBeginRequestPayload
{
    public string? ProducerId { get; init; }
    public int TimeoutSeconds { get; init; }

    public static CrossTopicTxnBeginRequestPayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            ProducerId = reader.ReadString(),
            TimeoutSeconds = reader.ReadInt32()
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(ProducerId);
        writer.WriteInt32(TimeoutSeconds);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(ProducerId);
        writer.WriteInt32(TimeoutSeconds);
    }

    public int EstimateSize() =>
        2 + (ProducerId != null ? System.Text.Encoding.UTF8.GetByteCount(ProducerId) : 0) +
        4;
}

/// <summary>
/// Wire format for CrossTopicTxnBegin response.
/// </summary>
public readonly record struct CrossTopicTxnBeginResponsePayload
{
    public ushort ErrorCode { get; init; }
    public string TransactionId { get; init; }

    public static CrossTopicTxnBeginResponsePayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            ErrorCode = reader.ReadUInt16(),
            TransactionId = reader.ReadString() ?? string.Empty
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(TransactionId);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(TransactionId);
    }

    public int EstimateSize() =>
        2 + 2 + System.Text.Encoding.UTF8.GetByteCount(TransactionId ?? "");
}

/// <summary>
/// Wire format for CrossTopicTxnAddWrite request.
/// </summary>
public readonly record struct CrossTopicTxnAddWriteRequestPayload
{
    public string TransactionId { get; init; }
    public string Topic { get; init; }
    public int Partition { get; init; }
    public byte[]? Key { get; init; }
    public byte[] Value { get; init; }

    public static CrossTopicTxnAddWriteRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var transactionId = reader.ReadString() ?? string.Empty;
        var topic = reader.ReadString() ?? string.Empty;
        var partition = reader.ReadInt32();

        var hasKey = reader.ReadUInt8() != 0;
        byte[]? key = null;
        if (hasKey)
        {
            var keyLen = reader.ReadInt32();
            key = reader.ReadRaw(keyLen).ToArray();
        }

        var valueLen = reader.ReadInt32();
        var value = reader.ReadRaw(valueLen).ToArray();

        return new CrossTopicTxnAddWriteRequestPayload
        {
            TransactionId = transactionId,
            Topic = topic,
            Partition = partition,
            Key = key,
            Value = value
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionId);
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);

        if (Key != null)
        {
            writer.WriteUInt8(1);
            writer.WriteInt32(Key.Length);
            // WriteRaw, not WriteBytes — SurgewavePayloadWriter.WriteBytes
            // emits an int32 length prefix of its own, which would double
            // up with the explicit one above and break the Read side
            // (Read expects [len][raw]). The WriteTo(IPayloadWriter) path
            // below uses WriteBytes against BigEndianWriter, whose
            // WriteBytes contract is "raw bytes, no prefix" — so the two
            // produce the same wire bytes by design.
            writer.WriteRaw(Key);
        }
        else
        {
            writer.WriteUInt8(0);
        }

        writer.WriteInt32(Value.Length);
        writer.WriteRaw(Value);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionId);
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);

        if (Key != null)
        {
            writer.WriteUInt8(1);
            writer.WriteInt32(Key.Length);
            writer.WriteBytes(Key);
        }
        else
        {
            writer.WriteUInt8(0);
        }

        writer.WriteInt32(Value.Length);
        writer.WriteBytes(Value);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TransactionId ?? "") +
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") +
        4 + 1 + (Key != null ? 4 + Key.Length : 0) + 4 + (Value?.Length ?? 0);
}

/// <summary>
/// Wire format for CrossTopicTxnAddWrite response.
/// </summary>
public readonly record struct CrossTopicTxnAddWriteResponsePayload
{
    public ushort ErrorCode { get; init; }
    public int PendingWriteCount { get; init; }

    public static CrossTopicTxnAddWriteResponsePayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            ErrorCode = reader.ReadUInt16(),
            PendingWriteCount = reader.ReadInt32()
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(PendingWriteCount);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(PendingWriteCount);
    }

    public int EstimateSize() => 2 + 4;
}

/// <summary>
/// Wire format for CrossTopicTxnCommit request.
/// </summary>
public readonly record struct CrossTopicTxnCommitRequestPayload
{
    public string TransactionId { get; init; }

    public static CrossTopicTxnCommitRequestPayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            TransactionId = reader.ReadString() ?? string.Empty
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionId);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionId);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TransactionId ?? "");
}

/// <summary>
/// Wire format for CrossTopicTxnCommit response.
/// </summary>
public readonly record struct CrossTopicTxnCommitResponsePayload
{
    public ushort ErrorCode { get; init; }
    public int TopicsWritten { get; init; }
    public int MessagesWritten { get; init; }
    public long DurationMs { get; init; }
    public string? Error { get; init; }

    public static CrossTopicTxnCommitResponsePayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            ErrorCode = reader.ReadUInt16(),
            TopicsWritten = reader.ReadInt32(),
            MessagesWritten = reader.ReadInt32(),
            DurationMs = reader.ReadInt64(),
            Error = reader.ReadString()
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(TopicsWritten);
        writer.WriteInt32(MessagesWritten);
        writer.WriteInt64(DurationMs);
        writer.WriteString(Error);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(TopicsWritten);
        writer.WriteInt32(MessagesWritten);
        writer.WriteInt64(DurationMs);
        writer.WriteString(Error);
    }

    public int EstimateSize() =>
        2 + 4 + 4 + 8 + 2 + (Error != null ? System.Text.Encoding.UTF8.GetByteCount(Error) : 0);
}

/// <summary>
/// Wire format for CrossTopicTxnAbort request.
/// </summary>
public readonly record struct CrossTopicTxnAbortRequestPayload
{
    public string TransactionId { get; init; }

    public static CrossTopicTxnAbortRequestPayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            TransactionId = reader.ReadString() ?? string.Empty
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionId);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionId);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(TransactionId ?? "");
}

/// <summary>
/// Wire format for CrossTopicTxnAbort response.
/// </summary>
public readonly record struct CrossTopicTxnAbortResponsePayload
{
    public ushort ErrorCode { get; init; }

    public static CrossTopicTxnAbortResponsePayload Read(ref SurgewavePayloadReader reader)
        => new()
        {
            ErrorCode = reader.ReadUInt16()
        };

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
    }

    public int EstimateSize() => 2;
}
