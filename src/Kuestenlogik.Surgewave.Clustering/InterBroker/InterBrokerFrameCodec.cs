using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker;

/// <summary>
/// #60 Inc4 — the native SRWV inter-broker frame codec. A frame is length-prefixed and carries an
/// opcode plus an opcode-specific payload:
/// <code>[int32 size][uint16 opcode][payload bytes]</code>
/// where <c>size</c> counts everything after itself (opcode + payload), all big-endian. Requests and
/// responses share the shape; a response echoes the request opcode on success or carries
/// <see cref="SurgewaveOpCode.Error"/> for a frame-level failure. Native frames only travel between
/// native-capable peers (opcodes in the 0x15xx/0x16xx band), so the format need not match the Kafka
/// wire — it only has to round-trip with itself and with the send side (Inc5–7 clients).
/// </summary>
public static class InterBrokerFrameCodec
{
    /// <summary>Maximum accepted frame body size (matches the replication server's fetch cap).</summary>
    public const int MaxFrameSize = 100 * 1024 * 1024;

    private const int OpcodeSize = 2;

    /// <summary>Serialize a payload to its raw bytes (no frame header) via the ref-struct writer.</summary>
    public static byte[] EncodePayload<TPayload>(TPayload payload) where TPayload : ISerializablePayload<TPayload>
    {
        var buffer = new byte[payload.EstimateSize()];
        var writer = new SurgewavePayloadWriter(buffer);
        payload.Write(ref writer);
        // EstimateSize is an upper bound (strings estimate 3 bytes/char); trim to what was written.
        return writer.Position == buffer.Length ? buffer : buffer[..writer.Position];
    }

    /// <summary>Build a complete frame <c>[size][opcode][payload]</c> from an opcode and raw payload bytes.</summary>
    public static byte[] EncodeFrame(SurgewaveOpCode opcode, ReadOnlySpan<byte> payload)
    {
        var frame = new byte[4 + OpcodeSize + payload.Length];
        BinaryPrimitives.WriteInt32BigEndian(frame, OpcodeSize + payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(4), (ushort)opcode);
        payload.CopyTo(frame.AsSpan(4 + OpcodeSize));
        return frame;
    }

    /// <summary>Build a complete frame from an opcode and a serializable payload.</summary>
    public static byte[] EncodeFrame<TPayload>(SurgewaveOpCode opcode, TPayload payload)
        where TPayload : ISerializablePayload<TPayload>
        => EncodeFrame(opcode, EncodePayload(payload));

    /// <summary>
    /// Read one frame from <paramref name="stream"/>. Returns <c>null</c> on a clean end-of-stream
    /// (no bytes / truncated header) so the caller can stop looping on a closed connection instead of
    /// spinning. Throws on I/O errors.
    /// </summary>
    public static async ValueTask<InterBrokerRequestFrame?> ReadFrameAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        var got = await stream.ReadAtLeastAsync(sizeBuffer, 4, throwOnEndOfStream: false, ct).ConfigureAwait(false);
        if (got < 4)
            return null; // clean EOF (0) or truncated header

        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
        if (size < OpcodeSize || size > MaxFrameSize)
            return null; // need at least an opcode; oversized frames are refused

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct).ConfigureAwait(false);

        var opcode = (SurgewaveOpCode)BinaryPrimitives.ReadUInt16BigEndian(body);
        return new InterBrokerRequestFrame(opcode, body.AsMemory(OpcodeSize));
    }

    /// <summary>Read the opcode from an already-read frame body (<c>[opcode][payload]</c>).</summary>
    public static SurgewaveOpCode PeekOpcode(ReadOnlySpan<byte> body)
        => (SurgewaveOpCode)BinaryPrimitives.ReadUInt16BigEndian(body);
}

/// <summary>A decoded native inter-broker request frame: the opcode plus the raw payload bytes.</summary>
public readonly record struct InterBrokerRequestFrame(SurgewaveOpCode Opcode, ReadOnlyMemory<byte> Payload);
