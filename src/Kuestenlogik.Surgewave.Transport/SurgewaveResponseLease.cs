using System.Buffers;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// A response whose payload may live in a pooled buffer that the caller gives back (#80).
///
/// <para><b>Why this exists.</b> <see cref="ISurgewaveTransport.SendRequestAsync"/> hands the
/// payload out as a bare <see cref="ReadOnlyMemory{T}"/>, which cannot express "this buffer is on
/// loan" — so the transport had to read every uncompressed response into a fresh array. On a fetch
/// that is one allocation the size of the fetched data, per fetch, and it is the client's largest
/// source of garbage. This carrier pairs the payload with the means to return it.</para>
///
/// <para><b>Contract.</b> <see cref="Payload"/> is valid until <see cref="Dispose"/>. Decode or
/// copy it inside the <c>using</c> scope; never let the memory escape. A returned buffer is handed
/// to the next reader, so touching it afterwards reads another response's bytes. The native
/// consume path already copies each message out while decoding, which is why it can borrow.</para>
///
/// <para>A lease over memory the caller already owns (the compatibility path, and any transport
/// that has not opted in) releases nothing — disposing it is a no-op, so callers need not care
/// which kind they hold.</para>
/// </summary>
public readonly struct SurgewaveResponseLease : IDisposable
{
    private readonly byte[]? _pooledBuffer;

    /// <summary>The response header.</summary>
    public SurgewaveResponseHeader Header { get; }

    /// <summary>The response payload. Only valid until <see cref="Dispose"/>.</summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>
    /// Wraps a payload the caller already owns — a decompressed result, or a plain array from a
    /// transport that does not pool. Disposing is a no-op.
    /// </summary>
    public SurgewaveResponseLease(SurgewaveResponseHeader header, ReadOnlyMemory<byte> payload)
    {
        Header = header;
        Payload = payload;
        _pooledBuffer = null;
    }

    /// <summary>
    /// Wraps the first <paramref name="length"/> bytes of a buffer rented from
    /// <see cref="ArrayPool{T}.Shared"/>. Disposing returns it.
    /// </summary>
    public SurgewaveResponseLease(SurgewaveResponseHeader header, byte[] pooledBuffer, int length)
    {
        Header = header;
        Payload = pooledBuffer.AsMemory(0, length);
        _pooledBuffer = pooledBuffer;
    }

    /// <summary>Returns the pooled buffer, if any. <see cref="Payload"/> is invalid afterwards.</summary>
    public void Dispose()
    {
        if (_pooledBuffer is not null)
            ArrayPool<byte>.Shared.Return(_pooledBuffer);
    }
}
