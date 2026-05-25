namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// Default buffer pool implementation using ArrayPool.
/// </summary>
public sealed class DefaultSurgewaveBufferPool : ISurgewaveBufferPool
{
    public static readonly DefaultSurgewaveBufferPool Shared = new();

    public ISurgewaveBuffer Empty => EmptySurgewaveBuffer.Instance;

    public ISurgewaveWritableBuffer Rent(int minimumLength)
    {
        return new PooledSurgewaveBuffer(minimumLength);
    }

    public ISurgewaveBuffer RentAndCopy(ReadOnlySpan<byte> source)
    {
        if (source.IsEmpty)
            return EmptySurgewaveBuffer.Instance;

        var buffer = new PooledSurgewaveBuffer(source.Length);
        source.CopyTo(((ISurgewaveWritableBuffer)buffer).Span);
        return buffer;
    }

    public ISurgewaveBuffer Wrap(ReadOnlyMemory<byte> memory)
    {
        if (memory.IsEmpty)
            return EmptySurgewaveBuffer.Instance;

        return new WrappedSurgewaveBuffer(memory);
    }
}
