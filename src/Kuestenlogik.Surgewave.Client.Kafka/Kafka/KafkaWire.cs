using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Client.Kafka;

/// <summary>
/// One Kafka request and its response over an open connection: the size-prefixed request out,
/// the size-prefixed response in. The response is checked against the request's correlation
/// id and handed back without it, which is what the protocol types' <c>ReadFrom</c> methods
/// take (#236).
/// </summary>
internal static class KafkaWire
{
    public static async Task<byte[]> RoundTripAsync(
        Stream stream, KafkaRequest request, string apiName, CancellationToken cancellationToken)
    {
        using (var writer = new KafkaProtocolWriter())
        {
            request.WriteTo(writer);
            var payload = writer.WrittenSpan;
            var frame = ArrayPool<byte>.Shared.Rent(4 + payload.Length);
            try
            {
                BinaryPrimitives.WriteInt32BigEndian(frame, payload.Length);
                payload.CopyTo(frame.AsSpan(4));
                await stream.WriteAsync(frame.AsMemory(0, 4 + payload.Length), cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(frame);
            }
        }

        var sizeBytes = new byte[4];
        await stream.ReadExactlyAsync(sizeBytes, cancellationToken).ConfigureAwait(false);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBytes);
        if (size < 4)
        {
            throw new BrokerResponseException($"{apiName} response too short ({size} bytes)", apiName);
        }

        var response = new byte[size];
        await stream.ReadExactlyAsync(response, cancellationToken).ConfigureAwait(false);
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(response);
        if (correlationId != request.CorrelationId)
        {
            throw new BrokerResponseException(
                $"{apiName} response carries correlation id {correlationId}, expected {request.CorrelationId}", apiName);
        }
        return response[4..];
    }

    /// <summary>
    /// The native-wire header block (int32 count + int32-prefixed key/value pairs, big-endian)
    /// that <c>RecordBatchSerializer.SerializeMessages</c> writes into a record batch as Kafka
    /// headers. No headers is an empty block.
    /// </summary>
    public static byte[] EncodeHeaders(IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return [];
        }

        var size = 4;
        foreach (var (key, value) in headers)
        {
            size += 8 + Encoding.UTF8.GetByteCount(key) + value.Length;
        }

        var block = new byte[size];
        BinaryPrimitives.WriteInt32BigEndian(block, headers.Count);
        var pos = 4;
        foreach (var (key, value) in headers)
        {
            var keyLength = Encoding.UTF8.GetBytes(key, block.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), keyLength);
            pos += 4 + keyLength;
            BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), value.Length);
            value.CopyTo(block.AsSpan(pos + 4));
            pos += 4 + value.Length;
        }
        return block;
    }

    /// <summary>The inverse of <see cref="EncodeHeaders"/>; an empty block is no headers.</summary>
    public static Dictionary<string, byte[]>? DecodeHeaders(ReadOnlySpan<byte> block)
    {
        if (block.Length < 4)
        {
            return null;
        }

        var count = BinaryPrimitives.ReadInt32BigEndian(block);
        if (count <= 0)
        {
            return null;
        }

        var headers = new Dictionary<string, byte[]>(count, StringComparer.Ordinal);
        var pos = 4;
        for (var i = 0; i < count; i++)
        {
            var keyLength = BinaryPrimitives.ReadInt32BigEndian(block[pos..]);
            pos += 4;
            var key = Encoding.UTF8.GetString(block.Slice(pos, keyLength));
            pos += keyLength;
            var valueLength = BinaryPrimitives.ReadInt32BigEndian(block[pos..]);
            pos += 4;
            headers[key] = block.Slice(pos, valueLength).ToArray();
            pos += valueLength;
        }
        return headers;
    }
}
