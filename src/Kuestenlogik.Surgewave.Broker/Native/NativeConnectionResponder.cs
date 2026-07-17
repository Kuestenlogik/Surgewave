using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Per-connection response writer for the native protocol. Serializes response frames onto the
/// connection's PipeWriter; the write lock serializes the request-processing loop against
/// push-streaming background tasks.
/// Created once per connection — this is what lets the request context drop its per-request
/// closure delegates (#83).
/// </summary>
internal sealed class NativeConnectionResponder
{
    private readonly PipeWriter _pipeWriter;
    private readonly SemaphoreSlim _writeLock;

    /// <summary>Set once the handshake has negotiated compression; false before that.</summary>
    public bool ClientSupportsCompression { get; set; }

    public NativeConnectionResponder(PipeWriter pipeWriter, SemaphoreSlim writeLock)
    {
        _pipeWriter = pipeWriter;
        _writeLock = writeLock;
    }

    public async Task SendResponseAsync(uint requestId, SurgewaveOpCode opCode, SurgewaveErrorCode errorCode,
        ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var flags = SurgewaveProtocolFlags.None;
        ReadOnlyMemory<byte> actualPayload = payload;
        byte[]? compressionBuffer = null;

        // Compress large responses if client supports it — straight into a pooled buffer that the
        // finally below returns once the bytes were copied into the PipeWriter. On rejection
        // nothing is rented and the original payload goes out unchanged.
        if (ClientSupportsCompression && payload.Length >= NativeCompressionCodec.MinCompressionSize &&
            NativeCompressionCodec.TryCompressWithHeader(payload.Span, out compressionBuffer, out var frameLength))
        {
            actualPayload = compressionBuffer.AsMemory(0, frameLength);
            flags |= SurgewaveProtocolFlags.Compressed;
        }

        try
        {
            var header = new SurgewaveResponseHeader
            {
                Flags = flags,
                RequestId = requestId,
                OpCode = opCode,
                ErrorCode = errorCode,
                PayloadLength = actualPayload.Length
            };

            // Serialize writes: both the processor loop and push-streaming tasks share this PipeWriter.
            await _writeLock.WaitAsync(cancellationToken);
            try
            {
                // Write header + payload directly into PipeWriter's managed buffer (zero-allocation)
                var totalLength = SurgewaveResponseHeader.Size + actualPayload.Length;
                var span = _pipeWriter.GetSpan(totalLength);
                header.WriteTo(span);
                if (actualPayload.Length > 0)
                {
                    actualPayload.Span.CopyTo(span.Slice(SurgewaveResponseHeader.Size));
                }
                _pipeWriter.Advance(totalLength);

                // Flush to push data to the socket
                await _pipeWriter.FlushAsync(cancellationToken);
            }
            finally
            {
                _writeLock.Release();
            }
        }
        finally
        {
            // The pipe holds its own copy now; actualPayload must not be touched past this point.
            if (compressionBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(compressionBuffer);
            }
        }
    }

    /// <summary>
    /// Sends an error frame. Note the wire opcode is always <see cref="SurgewaveOpCode.Error"/>,
    /// regardless of the request's opcode.
    /// </summary>
    public Task SendErrorAsync(uint requestId, SurgewaveOpCode opCode, SurgewaveErrorCode errorCode,
        string message, CancellationToken cancellationToken)
    {
        var messageBytes = System.Text.Encoding.UTF8.GetBytes(message);
        var payload = new byte[2 + messageBytes.Length];
        BinaryPrimitives.WriteInt16BigEndian(payload.AsSpan(0, 2), (short)messageBytes.Length);
        messageBytes.CopyTo(payload.AsSpan(2));

        return SendResponseAsync(requestId, SurgewaveOpCode.Error, errorCode, payload, cancellationToken);
    }
}
