using System.Buffers;
using System.IO.MemoryMappedFiles;
using Kuestenlogik.Surgewave.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.FileSystem;

/// <summary>
/// A Surgewave buffer backed by a memory-mapped file region.
/// Provides true zero-copy access to file data.
/// Each buffer instance holds its own reference to the pointer via AcquirePointer/ReleasePointer.
/// </summary>
public sealed unsafe class FileMmapBuffer : ISurgewaveBuffer
{
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly byte* _pointer;
    private readonly int _offset;
    private readonly int _length;
    private readonly bool _ownsAccessor;
    private bool _disposed;

    public int Length => _length;

    public ReadOnlySpan<byte> Span
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return new ReadOnlySpan<byte>(_pointer + _offset, _length);
        }
    }

    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            // Memory-mapped regions don't have managed backing
            // We need to copy for Memory<T> compatibility
            ObjectDisposedException.ThrowIf(_disposed, this);
            return ToArray();
        }
    }

    /// <summary>
    /// Create a buffer from a memory-mapped view accessor.
    /// </summary>
    public FileMmapBuffer(MemoryMappedViewAccessor accessor, int offset, int length, bool ownsAccessor = true)
    {
        _accessor = accessor;
        _offset = offset;
        _length = length;
        _ownsAccessor = ownsAccessor;

        // Get the raw pointer - AcquirePointer is reference counted
        byte* ptr = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        _pointer = ptr;
    }

    /// <summary>
    /// Create a slice (shares the same accessor but acquires its own pointer reference).
    /// Uses a struct marker to differentiate from the public constructor.
    /// </summary>
    private FileMmapBuffer(MemoryMappedViewAccessor accessor, int offset, int length, SliceMarker _)
    {
        _accessor = accessor;
        _offset = offset;
        _length = length;
        _ownsAccessor = false; // Slice doesn't own the accessor

        // Acquire our own pointer reference (reference counted)
        // This ensures the pointer stays valid even if the original buffer is disposed
        byte* ptr = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        _pointer = ptr;
    }

    // Marker struct to differentiate slice constructor
    private readonly struct SliceMarker { }

    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        // Memory-mapped regions can't be directly converted to Memory<T>
        // because they don't have managed backing
        memory = default;
        return false;
    }

    public MemoryHandle Pin()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        // Already pinned via memory mapping
        return new MemoryHandle(_pointer + _offset);
    }

    public void CopyTo(Span<byte> destination)
    {
        Span.CopyTo(destination);
    }

    public byte[] ToArray()
    {
        var result = new byte[_length];
        Span.CopyTo(result);
        return result;
    }

    public ISurgewaveBuffer Slice(int start, int length)
    {
        if (start < 0 || length < 0 || start + length > _length)
            throw new ArgumentOutOfRangeException(nameof(start), "Slice parameters out of range");

        ObjectDisposedException.ThrowIf(_disposed, this);

        // Create slice with its own pointer reference
        return new FileMmapBuffer(_accessor, _offset + start, length, default(SliceMarker));
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        // Always release our pointer reference (reference counted)
        _accessor.SafeMemoryMappedViewHandle.ReleasePointer();

        // Only dispose the accessor if we own it
        if (_ownsAccessor)
        {
            _accessor.Dispose();
        }
    }
}

/// <summary>
/// Manager for memory-mapped file access.
/// </summary>
public sealed class FileMmapManager : IDisposable
{
    private readonly string _filePath;
    private MemoryMappedFile? _mmf;
    private long _mappedLength;
    private readonly object _lock = new();
    private bool _disposed;

    public FileMmapManager(string filePath)
    {
        _filePath = filePath;
    }

    /// <summary>
    /// Get a zero-copy buffer for the specified region.
    /// </summary>
    public FileMmapBuffer GetBuffer(long offset, int length)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_lock)
        {
            EnsureMapped(offset + length);

            // Create a new view for this specific region
            var view = _mmf!.CreateViewAccessor(offset, length, MemoryMappedFileAccess.Read);
            return new FileMmapBuffer(view, 0, length, ownsAccessor: true);
        }
    }

    /// <summary>
    /// Get a buffer wrapping the entire file (or specified region).
    /// Each call creates a new accessor to avoid lifetime issues with cached accessors.
    /// </summary>
    public FileMmapBuffer GetWholeFileBuffer(long offset, int length)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        lock (_lock)
        {
            EnsureMapped(offset + length);

            // Create a new accessor for each buffer to avoid lifetime issues
            // (cached accessor could be disposed by EnsureMapped while buffers still use it)
            var view = _mmf!.CreateViewAccessor(offset, length, MemoryMappedFileAccess.Read);
            return new FileMmapBuffer(view, 0, length, ownsAccessor: true);
        }
    }

    private void EnsureMapped(long requiredLength)
    {
        if (_mmf != null && _mappedLength >= requiredLength)
            return;

        _mmf?.Dispose();

        var fileInfo = new FileInfo(_filePath);
        if (!fileInfo.Exists || fileInfo.Length == 0)
        {
            throw new InvalidOperationException($"File does not exist or is empty: {_filePath}");
        }

        _mappedLength = Math.Max(fileInfo.Length, requiredLength);

        // Open with FileShare.ReadWrite to allow concurrent access from FileStream
        using var fs = new FileStream(
            _filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        _mmf = MemoryMappedFile.CreateFromFile(
            fs,
            null,
            0,
            MemoryMappedFileAccess.Read,
            HandleInheritability.None,
            leaveOpen: false);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        _mmf?.Dispose();
    }
}
