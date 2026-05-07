using System.Buffers.Binary;

namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Reads AMQP 0.9.1 frames from a <see cref="Stream"/>.
/// </summary>
/// <remarks>
/// Wire format per frame:
/// <code>
///   type     : uint8
///   channel  : uint16 (big-endian)
///   size     : uint32 (big-endian)  — byte count of payload
///   payload  : size bytes
///   frame-end: uint8 = 0xCE
/// </code>
/// </remarks>
internal sealed class AmqpFrameReader
{
    private readonly Stream _stream;
    private readonly int _maxFrameSize;
    private readonly byte[] _header = new byte[7]; // type(1)+channel(2)+size(4)

    public AmqpFrameReader(Stream stream, int maxFrameSize)
    {
        _stream = stream;
        _maxFrameSize = maxFrameSize;
    }

    /// <summary>
    /// Reads the next frame from the stream, or returns <c>null</c> on end of stream.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The parsed frame, or <c>null</c> if the connection was closed cleanly.</returns>
    /// <exception cref="InvalidDataException">Thrown when the frame-end byte is not 0xCE.</exception>
    public async ValueTask<AmqpFrame?> ReadFrameAsync(CancellationToken ct = default)
    {
        // Read 7-byte header
        int read = await ReadExactAsync(_header, 0, 7, ct).ConfigureAwait(false);
        if (read == 0)
            return null; // clean close

        var type = _header[0];
        var channel = BinaryPrimitives.ReadUInt16BigEndian(_header.AsSpan(1, 2));
        var size = (int)BinaryPrimitives.ReadUInt32BigEndian(_header.AsSpan(3, 4));

        if (size > _maxFrameSize)
            throw new InvalidDataException(
                $"AMQP frame size {size} exceeds configured maximum {_maxFrameSize}.");

        var payload = new byte[size];
        if (size > 0)
            await ReadExactAsync(payload, 0, size, ct).ConfigureAwait(false);

        // Read and validate frame-end
        var frameEnd = new byte[1];
        await ReadExactAsync(frameEnd, 0, 1, ct).ConfigureAwait(false);
        if (frameEnd[0] != AmqpFrameType.FrameEnd)
            throw new InvalidDataException(
                $"Expected AMQP frame-end 0xCE but got 0x{frameEnd[0]:X2}.");

        return new AmqpFrame(type, channel, payload);
    }

    private async ValueTask<int> ReadExactAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = await _stream.ReadAsync(buffer.AsMemory(offset + totalRead, count - totalRead), ct)
                .ConfigureAwait(false);
            if (read == 0)
                return totalRead; // EOF
            totalRead += read;
        }
        return totalRead;
    }
}
