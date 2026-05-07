using System.Buffers;

namespace Kuestenlogik.Surgewave.Storage.Engine;

/// <summary>
/// Empty buffer singleton.
/// </summary>
public sealed class EmptySurgewaveBuffer : ISurgewaveBuffer
{
    public static readonly EmptySurgewaveBuffer Instance = new();

    private EmptySurgewaveBuffer() { }

    public int Length => 0;
    public ReadOnlySpan<byte> Span => ReadOnlySpan<byte>.Empty;
    public ReadOnlyMemory<byte> Memory => ReadOnlyMemory<byte>.Empty;

    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        memory = ReadOnlyMemory<byte>.Empty;
        return true;
    }

    public MemoryHandle Pin() => default;
    public void CopyTo(Span<byte> destination) { }
    public byte[] ToArray() => [];
    public ISurgewaveBuffer Slice(int start, int length)
    {
        if (start != 0 || length != 0)
            throw new ArgumentOutOfRangeException(nameof(start), "Cannot slice empty buffer");
        return this;
    }

    public void Dispose() { } // No-op for singleton
}
