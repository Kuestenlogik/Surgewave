using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Client.RequestReply;

/// <summary>
/// Binary envelope format for request-reply messages.
/// Encodes correlation metadata alongside the payload using a compact binary format
/// that avoids the overhead of JSON serialization on the hot path.
///
/// Wire format (request):
///   [1 byte: version] [1 byte: flags] [2 bytes: correlationId length] [correlationId UTF-8]
///   [2 bytes: replyTopic length] [replyTopic UTF-8] [remaining: payload]
///
/// Wire format (reply):
///   [1 byte: version] [1 byte: flags (bit 0 = isError)] [2 bytes: correlationId length] [correlationId UTF-8]
///   [2 bytes: errorMessage length, 0 if none] [errorMessage UTF-8 if present] [remaining: payload]
/// </summary>
internal static class RequestReplyEnvelope
{
    private const byte CurrentVersion = 1;
    private const byte ErrorFlag = 0x01;

    /// <summary>
    /// Wraps a request payload with correlation metadata into a single byte array.
    /// </summary>
    internal static byte[] WrapRequest(string correlationId, string replyTopic, byte[] payload)
    {
        var correlationBytes = Encoding.UTF8.GetByteCount(correlationId);
        var replyTopicBytes = Encoding.UTF8.GetByteCount(replyTopic);
        var totalSize = 1 + 1 + 2 + correlationBytes + 2 + replyTopicBytes + payload.Length;

        var buffer = new byte[totalSize];
        var offset = 0;

        buffer[offset++] = CurrentVersion;
        buffer[offset++] = 0; // flags

        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset), (ushort)correlationBytes);
        offset += 2;
        Encoding.UTF8.GetBytes(correlationId.AsSpan(), buffer.AsSpan(offset, correlationBytes));
        offset += correlationBytes;

        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset), (ushort)replyTopicBytes);
        offset += 2;
        Encoding.UTF8.GetBytes(replyTopic.AsSpan(), buffer.AsSpan(offset, replyTopicBytes));
        offset += replyTopicBytes;

        payload.CopyTo(buffer.AsSpan(offset));

        return buffer;
    }

    /// <summary>
    /// Unwraps a request envelope, extracting correlation metadata and the original payload.
    /// </summary>
    internal static (string CorrelationId, string ReplyTopic, byte[] Payload) UnwrapRequest(ReadOnlySpan<byte> data)
    {
        var offset = 0;

        var version = data[offset++];
        if (version != CurrentVersion)
            throw new InvalidOperationException($"Unsupported request-reply envelope version: {version}");

        _ = data[offset++]; // flags (reserved)

        var correlationLen = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset));
        offset += 2;
        var correlationId = Encoding.UTF8.GetString(data.Slice(offset, correlationLen));
        offset += correlationLen;

        var replyTopicLen = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset));
        offset += 2;
        var replyTopic = Encoding.UTF8.GetString(data.Slice(offset, replyTopicLen));
        offset += replyTopicLen;

        var payload = data.Slice(offset).ToArray();

        return (correlationId, replyTopic, payload);
    }

    /// <summary>
    /// Wraps a reply payload with correlation metadata.
    /// </summary>
    internal static byte[] WrapReply(string correlationId, byte[] payload, bool isError, string? errorMessage)
    {
        var correlationBytes = Encoding.UTF8.GetByteCount(correlationId);
        var errorMsgBytes = isError && errorMessage != null ? Encoding.UTF8.GetByteCount(errorMessage) : 0;
        var totalSize = 1 + 1 + 2 + correlationBytes + 2 + errorMsgBytes + payload.Length;

        var buffer = new byte[totalSize];
        var offset = 0;

        buffer[offset++] = CurrentVersion;
        buffer[offset++] = isError ? ErrorFlag : (byte)0;

        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset), (ushort)correlationBytes);
        offset += 2;
        Encoding.UTF8.GetBytes(correlationId.AsSpan(), buffer.AsSpan(offset, correlationBytes));
        offset += correlationBytes;

        BinaryPrimitives.WriteUInt16BigEndian(buffer.AsSpan(offset), (ushort)errorMsgBytes);
        offset += 2;
        if (errorMsgBytes > 0)
        {
            Encoding.UTF8.GetBytes(errorMessage!.AsSpan(), buffer.AsSpan(offset, errorMsgBytes));
            offset += errorMsgBytes;
        }

        payload.CopyTo(buffer.AsSpan(offset));

        return buffer;
    }

    /// <summary>
    /// Unwraps a reply envelope, extracting correlation metadata and the response payload.
    /// </summary>
    internal static (string CorrelationId, byte[] Payload, bool IsError, string? ErrorMessage) UnwrapReply(ReadOnlySpan<byte> data)
    {
        var offset = 0;

        var version = data[offset++];
        if (version != CurrentVersion)
            throw new InvalidOperationException($"Unsupported request-reply envelope version: {version}");

        var flags = data[offset++];
        var isError = (flags & ErrorFlag) != 0;

        var correlationLen = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset));
        offset += 2;
        var correlationId = Encoding.UTF8.GetString(data.Slice(offset, correlationLen));
        offset += correlationLen;

        var errorMsgLen = BinaryPrimitives.ReadUInt16BigEndian(data.Slice(offset));
        offset += 2;
        string? errorMessage = null;
        if (errorMsgLen > 0)
        {
            errorMessage = Encoding.UTF8.GetString(data.Slice(offset, errorMsgLen));
            offset += errorMsgLen;
        }

        var payload = data.Slice(offset).ToArray();

        return (correlationId, payload, isError, errorMessage);
    }
}
