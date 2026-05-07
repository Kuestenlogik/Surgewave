using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql.Protocol;

/// <summary>
/// Reads PostgreSQL wire protocol v3 messages from a network stream.
/// All multi-byte integers are big-endian per the PG protocol specification.
/// </summary>
internal sealed class PgReader
{
    private readonly Stream _stream;

    public PgReader(Stream stream)
    {
        _stream = stream;
    }

    /// <summary>
    /// Reads the startup message (no type byte). The first 4 bytes are length,
    /// then 4 bytes protocol version, then key=value pairs terminated by \0.
    /// Returns the protocol version and a dictionary of startup parameters.
    /// </summary>
    public async Task<(int ProtocolVersion, Dictionary<string, string> Parameters)?> ReadStartupMessageAsync(
        CancellationToken ct)
    {
        var lengthBuf = new byte[4];
        if (!await ReadExactAsync(lengthBuf, ct).ConfigureAwait(false))
            return null;

        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBuf);
        if (length < 8 || length > 10_000)
            return null;

        var payload = new byte[length - 4];
        if (!await ReadExactAsync(payload, ct).ConfigureAwait(false))
            return null;

        var protocolVersion = BinaryPrimitives.ReadInt32BigEndian(payload);

        // SSL request: version = 80877103
        // Cancel request: version = 80877102
        // Normal startup: version = 196608 (3.0)
        var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (protocolVersion == 196608) // 3.0
        {
            var offset = 4;
            while (offset < payload.Length)
            {
                var key = ReadCString(payload, ref offset);
                if (string.IsNullOrEmpty(key))
                    break;
                var value = ReadCString(payload, ref offset);
                parameters[key] = value;
            }
        }

        return (protocolVersion, parameters);
    }

    /// <summary>
    /// Reads a standard PG message: [1-byte type][4-byte length][payload].
    /// Returns (type, payload) or null on disconnect.
    /// </summary>
    public async Task<(byte Type, byte[] Payload)?> ReadMessageAsync(CancellationToken ct)
    {
        var header = new byte[5];
        if (!await ReadExactAsync(header, ct).ConfigureAwait(false))
            return null;

        var type = header[0];
        var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1)) - 4;

        if (length < 0 || length > 10_000_000)
            return null;

        var payload = length > 0 ? new byte[length] : [];
        if (length > 0 && !await ReadExactAsync(payload, ct).ConfigureAwait(false))
            return null;

        return (type, payload);
    }

    /// <summary>
    /// Reads a password string from a Password message payload.
    /// The payload is a null-terminated string.
    /// </summary>
    public static string ReadPasswordFromPayload(byte[] payload)
    {
        var end = Array.IndexOf(payload, (byte)0);
        return end >= 0
            ? Encoding.UTF8.GetString(payload, 0, end)
            : Encoding.UTF8.GetString(payload);
    }

    /// <summary>
    /// Reads a query string from a Query message payload.
    /// The payload is a null-terminated string.
    /// </summary>
    public static string ReadQueryFromPayload(byte[] payload)
    {
        var end = Array.IndexOf(payload, (byte)0);
        return end >= 0
            ? Encoding.UTF8.GetString(payload, 0, end)
            : Encoding.UTF8.GetString(payload);
    }

    /// <summary>
    /// Parses a Parse message payload: [name\0][query\0][int16 numParams][int32 paramOids...].
    /// </summary>
    public static (string Name, string Query, int[] ParamOids) ReadParsePayload(byte[] payload)
    {
        var offset = 0;
        var name = ReadCString(payload, ref offset);
        var query = ReadCString(payload, ref offset);

        var numParams = 0;
        if (offset + 2 <= payload.Length)
        {
            numParams = BinaryPrimitives.ReadInt16BigEndian(payload.AsSpan(offset));
            offset += 2;
        }

        var paramOids = new int[numParams];
        for (var i = 0; i < numParams && offset + 4 <= payload.Length; i++)
        {
            paramOids[i] = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(offset));
            offset += 4;
        }

        return (name, query, paramOids);
    }

    /// <summary>
    /// Parses a Bind message payload:
    /// [portal\0][statement\0][int16 numFormatCodes][int16...][int16 numParams][int32 len][bytes]...[int16 numResultFormats][int16...].
    /// </summary>
    public static (string Portal, string Statement) ReadBindPayload(byte[] payload)
    {
        var offset = 0;
        var portal = ReadCString(payload, ref offset);
        var statement = ReadCString(payload, ref offset);
        return (portal, statement);
    }

    /// <summary>
    /// Parses a Describe message payload: [byte type ('S' or 'P')][name\0].
    /// </summary>
    public static (byte DescribeType, string Name) ReadDescribePayload(byte[] payload)
    {
        if (payload.Length == 0)
            return (0, "");
        var descType = payload[0];
        var offset = 1;
        var name = ReadCString(payload, ref offset);
        return (descType, name);
    }

    /// <summary>
    /// Parses an Execute message payload: [portal\0][int32 maxRows].
    /// </summary>
    public static (string Portal, int MaxRows) ReadExecutePayload(byte[] payload)
    {
        var offset = 0;
        var portal = ReadCString(payload, ref offset);
        var maxRows = 0;
        if (offset + 4 <= payload.Length)
            maxRows = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(offset));
        return (portal, maxRows);
    }

    private static string ReadCString(byte[] buf, ref int offset)
    {
        var start = offset;
        while (offset < buf.Length && buf[offset] != 0)
            offset++;
        var str = Encoding.UTF8.GetString(buf, start, offset - start);
        if (offset < buf.Length)
            offset++; // skip null terminator
        return str;
    }

    private async Task<bool> ReadExactAsync(byte[] buffer, CancellationToken ct)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await _stream.ReadAsync(
                buffer.AsMemory(totalRead, buffer.Length - totalRead), ct).ConfigureAwait(false);
            if (read == 0)
                return false;
            totalRead += read;
        }
        return true;
    }
}
