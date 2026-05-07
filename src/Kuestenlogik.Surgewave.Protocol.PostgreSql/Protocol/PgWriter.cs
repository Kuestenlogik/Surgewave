using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql.Protocol;

/// <summary>
/// Writes PostgreSQL wire protocol v3 messages to a network stream.
/// All multi-byte integers are big-endian per the PG protocol specification.
/// Uses text format (format code 0) for all column values.
/// </summary>
internal sealed class PgWriter
{
    private readonly Stream _stream;

    public PgWriter(Stream stream)
    {
        _stream = stream;
    }

    /// <summary>
    /// Writes AuthenticationOk: 'R' + length(8) + int32(0).
    /// </summary>
    public async Task WriteAuthenticationOkAsync(CancellationToken ct)
    {
        var buf = new byte[9];
        buf[0] = PgBackendMessage.Authentication;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 8); // length = 4 (self) + 4 (status)
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(5), 0); // AuthenticationOk
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes AuthenticationCleartextPassword: 'R' + length(8) + int32(3).
    /// </summary>
    public async Task WriteAuthenticationCleartextPasswordAsync(CancellationToken ct)
    {
        var buf = new byte[9];
        buf[0] = PgBackendMessage.Authentication;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 8);
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(5), 3); // AuthenticationCleartextPassword
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a ParameterStatus message: 'S' + length + name\0 + value\0.
    /// </summary>
    public async Task WriteParameterStatusAsync(string name, string value, CancellationToken ct)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var valueBytes = Encoding.UTF8.GetBytes(value);
        var payloadLen = nameBytes.Length + 1 + valueBytes.Length + 1;
        var totalLen = 1 + 4 + payloadLen;
        var buf = new byte[totalLen];

        buf[0] = PgBackendMessage.ParameterStatus;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4 + payloadLen);
        nameBytes.CopyTo(buf.AsSpan(5));
        buf[5 + nameBytes.Length] = 0;
        valueBytes.CopyTo(buf.AsSpan(5 + nameBytes.Length + 1));
        buf[totalLen - 1] = 0;

        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes BackendKeyData: 'K' + length(12) + int32(processId) + int32(secretKey).
    /// </summary>
    public async Task WriteBackendKeyDataAsync(int processId, int secretKey, CancellationToken ct)
    {
        var buf = new byte[13];
        buf[0] = PgBackendMessage.BackendKeyData;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 12); // length = 4 + 4 + 4
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(5), processId);
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(9), secretKey);
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes ReadyForQuery: 'Z' + length(5) + byte(status).
    /// Status: 'I' = idle, 'T' = in transaction, 'E' = error.
    /// </summary>
    public async Task WriteReadyForQueryAsync(byte status, CancellationToken ct)
    {
        var buf = new byte[6];
        buf[0] = PgBackendMessage.ReadyForQuery;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 5); // length = 4 + 1
        buf[5] = status;
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a RowDescription message with column metadata.
    /// Each column: name\0 + tableOid(4) + columnIndex(2) + typeOid(4) + typeLen(2) + typeMod(4) + formatCode(2).
    /// </summary>
    public async Task WriteRowDescriptionAsync(IReadOnlyList<PgColumnDescriptor> columns, CancellationToken ct)
    {
        // Calculate total size
        var payloadLen = 2; // int16 field count
        foreach (var col in columns)
        {
            payloadLen += Encoding.UTF8.GetByteCount(col.Name) + 1 // name + null
                          + 4 + 2 + 4 + 2 + 4 + 2; // tableOid + colIndex + typeOid + typeLen + typeMod + format
        }

        var totalLen = 1 + 4 + payloadLen;
        var buf = new byte[totalLen];
        var offset = 0;

        buf[offset++] = PgBackendMessage.RowDescription;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), 4 + payloadLen);
        offset += 4;
        BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(offset), (short)columns.Count);
        offset += 2;

        foreach (var col in columns)
        {
            var nameBytes = Encoding.UTF8.GetBytes(col.Name);
            nameBytes.CopyTo(buf.AsSpan(offset));
            offset += nameBytes.Length;
            buf[offset++] = 0;

            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), 0); // tableOid
            offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(offset), 0); // column index
            offset += 2;
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), col.TypeOid);
            offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(offset), PgTypeOids.TypeLength(col.TypeOid));
            offset += 2;
            BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), -1); // typeMod
            offset += 4;
            BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(offset), 0); // text format
            offset += 2;
        }

        await _stream.WriteAsync(buf.AsMemory(0, offset), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a DataRow message. Each value is either NULL (length=-1) or text-encoded UTF-8.
    /// </summary>
    public async Task WriteDataRowAsync(IReadOnlyList<string?> values, CancellationToken ct)
    {
        // Calculate payload size: int16(numCols) + per-column [int32(len) + bytes]
        var payloadLen = 2;
        var encodedValues = new byte[values.Count][];
        for (var i = 0; i < values.Count; i++)
        {
            if (values[i] is null)
            {
                encodedValues[i] = [];
                payloadLen += 4; // -1 for null
            }
            else
            {
                encodedValues[i] = Encoding.UTF8.GetBytes(values[i]!);
                payloadLen += 4 + encodedValues[i].Length;
            }
        }

        var totalLen = 1 + 4 + payloadLen;
        var buf = new byte[totalLen];
        var offset = 0;

        buf[offset++] = PgBackendMessage.DataRow;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), 4 + payloadLen);
        offset += 4;
        BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(offset), (short)values.Count);
        offset += 2;

        for (var i = 0; i < values.Count; i++)
        {
            if (values[i] is null)
            {
                BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), -1);
                offset += 4;
            }
            else
            {
                BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), encodedValues[i].Length);
                offset += 4;
                encodedValues[i].CopyTo(buf.AsSpan(offset));
                offset += encodedValues[i].Length;
            }
        }

        await _stream.WriteAsync(buf.AsMemory(0, offset), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a CommandComplete message: 'C' + length + tag\0.
    /// Example tags: "SELECT 5", "SET", "BEGIN".
    /// </summary>
    public async Task WriteCommandCompleteAsync(string tag, CancellationToken ct)
    {
        var tagBytes = Encoding.UTF8.GetBytes(tag);
        var payloadLen = tagBytes.Length + 1;
        var buf = new byte[1 + 4 + payloadLen];

        buf[0] = PgBackendMessage.CommandComplete;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4 + payloadLen);
        tagBytes.CopyTo(buf.AsSpan(5));
        buf[^1] = 0;

        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes an ErrorResponse message with severity, SQLSTATE code, and message.
    /// Format: 'E' + length + ['S' severity\0] + ['V' severity\0] + ['C' code\0] + ['M' message\0] + \0.
    /// </summary>
    public async Task WriteErrorResponseAsync(string severity, string code, string message, CancellationToken ct)
    {
        var sevBytes = Encoding.UTF8.GetBytes(severity);
        var codeBytes = Encoding.UTF8.GetBytes(code);
        var msgBytes = Encoding.UTF8.GetBytes(message);

        // Fields: S(severity), V(severity), C(code), M(message), terminator
        var payloadLen = (1 + sevBytes.Length + 1) * 2 // S + V fields
                         + (1 + codeBytes.Length + 1)   // C field
                         + (1 + msgBytes.Length + 1)    // M field
                         + 1;                           // terminator

        var buf = new byte[1 + 4 + payloadLen];
        var offset = 0;

        buf[offset++] = PgBackendMessage.ErrorResponse;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(offset), 4 + payloadLen);
        offset += 4;

        // Severity (localized)
        buf[offset++] = (byte)'S';
        sevBytes.CopyTo(buf.AsSpan(offset));
        offset += sevBytes.Length;
        buf[offset++] = 0;

        // Severity (non-localized, always English)
        buf[offset++] = (byte)'V';
        sevBytes.CopyTo(buf.AsSpan(offset));
        offset += sevBytes.Length;
        buf[offset++] = 0;

        // SQLSTATE code
        buf[offset++] = (byte)'C';
        codeBytes.CopyTo(buf.AsSpan(offset));
        offset += codeBytes.Length;
        buf[offset++] = 0;

        // Message
        buf[offset++] = (byte)'M';
        msgBytes.CopyTo(buf.AsSpan(offset));
        offset += msgBytes.Length;
        buf[offset++] = 0;

        // Terminator
        buf[offset++] = 0;

        await _stream.WriteAsync(buf.AsMemory(0, offset), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes EmptyQueryResponse: 'I' + length(4).
    /// </summary>
    public async Task WriteEmptyQueryResponseAsync(CancellationToken ct)
    {
        var buf = new byte[5];
        buf[0] = PgBackendMessage.EmptyQueryResponse;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4);
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes ParseComplete: '1' + length(4).
    /// </summary>
    public async Task WriteParseCompleteAsync(CancellationToken ct)
    {
        var buf = new byte[5];
        buf[0] = PgBackendMessage.ParseComplete;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4);
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes BindComplete: '2' + length(4).
    /// </summary>
    public async Task WriteBindCompleteAsync(CancellationToken ct)
    {
        var buf = new byte[5];
        buf[0] = PgBackendMessage.BindComplete;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4);
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes CloseComplete: '3' + length(4).
    /// </summary>
    public async Task WriteCloseCompleteAsync(CancellationToken ct)
    {
        var buf = new byte[5];
        buf[0] = PgBackendMessage.CloseComplete;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4);
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes NoData: 'n' + length(4).
    /// </summary>
    public async Task WriteNoDataAsync(CancellationToken ct)
    {
        var buf = new byte[5];
        buf[0] = PgBackendMessage.NoData;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 4);
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes ParameterDescription with zero parameters: 't' + length(6) + int16(0).
    /// </summary>
    public async Task WriteParameterDescriptionAsync(CancellationToken ct)
    {
        var buf = new byte[7];
        buf[0] = PgBackendMessage.ParameterDescription;
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), 6); // 4 + 2
        BinaryPrimitives.WriteInt16BigEndian(buf.AsSpan(5), 0); // 0 params
        await _stream.WriteAsync(buf, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Flushes the underlying stream.
    /// </summary>
    public Task FlushAsync(CancellationToken ct)
        => _stream.FlushAsync(ct);
}

/// <summary>
/// Describes a column in a RowDescription message.
/// </summary>
internal sealed record PgColumnDescriptor(string Name, int TypeOid);
