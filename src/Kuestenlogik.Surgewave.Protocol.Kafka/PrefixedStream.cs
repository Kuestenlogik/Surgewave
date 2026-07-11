using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// A stream wrapper that prepends a prefix buffer to the underlying stream.
/// Used for protocol detection where we need to peek bytes and then pass them
/// back to the original stream reader.
/// </summary>
/// <remarks>
/// This wrapper always leaves the inner stream open on disposal to avoid
/// LSP violations where disposing the wrapper unexpectedly closes the
/// underlying stream that callers may continue using.
/// </remarks>
internal sealed class PrefixedStream : Stream
{
    // CA2213 is suppressed because this stream wrapper intentionally does NOT
    // own or dispose the inner stream - the caller retains ownership.
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed",
        Justification = "Non-owning wrapper - inner stream lifecycle managed by caller")]
    private readonly Stream _innerStream;
    private readonly byte[] _prefix;
    private int _prefixPosition;
    private bool _prefixConsumed;

    public PrefixedStream(Stream innerStream, byte[] prefix)
    {
        ArgumentNullException.ThrowIfNull(innerStream);
        ArgumentNullException.ThrowIfNull(prefix);

        _innerStream = innerStream;
        _prefix = prefix;
        _prefixPosition = 0;
        _prefixConsumed = false;
    }

    /// <summary>
    /// Gets the underlying stream.
    /// </summary>
    public Stream InnerStream => _innerStream;

    public override bool CanRead => _innerStream.CanRead;
    public override bool CanSeek => false;
    public override bool CanWrite => _innerStream.CanWrite;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (!_prefixConsumed)
        {
            var prefixRemaining = _prefix.Length - _prefixPosition;
            if (prefixRemaining > 0)
            {
                var bytesToCopy = Math.Min(count, prefixRemaining);
                Array.Copy(_prefix, _prefixPosition, buffer, offset, bytesToCopy);
                _prefixPosition += bytesToCopy;

                if (_prefixPosition >= _prefix.Length)
                {
                    _prefixConsumed = true;
                }

                // If we filled the buffer from prefix, we're done
                if (bytesToCopy == count)
                {
                    return bytesToCopy;
                }

                // Otherwise, continue reading from inner stream
                var innerBytesRead = _innerStream.Read(buffer, offset + bytesToCopy, count - bytesToCopy);
                return bytesToCopy + innerBytesRead;
            }
        }

        return _innerStream.Read(buffer, offset, count);
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (!_prefixConsumed)
        {
            var prefixRemaining = _prefix.Length - _prefixPosition;
            if (prefixRemaining > 0)
            {
                var bytesToCopy = Math.Min(count, prefixRemaining);
                Array.Copy(_prefix, _prefixPosition, buffer, offset, bytesToCopy);
                _prefixPosition += bytesToCopy;

                if (_prefixPosition >= _prefix.Length)
                {
                    _prefixConsumed = true;
                }

                if (bytesToCopy == count)
                {
                    return bytesToCopy;
                }

                var innerBytesRead = await _innerStream.ReadAsync(buffer.AsMemory(offset + bytesToCopy, count - bytesToCopy), cancellationToken);
                return bytesToCopy + innerBytesRead;
            }
        }

        return await _innerStream.ReadAsync(buffer.AsMemory(offset, count), cancellationToken);
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (!_prefixConsumed)
        {
            var prefixRemaining = _prefix.Length - _prefixPosition;
            if (prefixRemaining > 0)
            {
                var bytesToCopy = Math.Min(buffer.Length, prefixRemaining);
                _prefix.AsSpan(_prefixPosition, bytesToCopy).CopyTo(buffer.Span);
                _prefixPosition += bytesToCopy;

                if (_prefixPosition >= _prefix.Length)
                {
                    _prefixConsumed = true;
                }

                if (bytesToCopy == buffer.Length)
                {
                    return bytesToCopy;
                }

                var innerBytesRead = await _innerStream.ReadAsync(buffer[bytesToCopy..], cancellationToken);
                return bytesToCopy + innerBytesRead;
            }
        }

        return await _innerStream.ReadAsync(buffer, cancellationToken);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _innerStream.Write(buffer, offset, count);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return _innerStream.WriteAsync(buffer, offset, count, cancellationToken);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return _innerStream.WriteAsync(buffer, cancellationToken);
    }

    public override void Flush()
    {
        _innerStream.Flush();
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        return _innerStream.FlushAsync(cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        // Intentionally do NOT dispose the inner stream.
        // This wrapper is non-owning to follow the principle of least surprise -
        // disposing a wrapper should not affect the wrapped resource that the
        // caller may continue using after the wrapper's lifetime.
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        // Intentionally do NOT dispose the inner stream.
        // This wrapper is non-owning to follow the principle of least surprise.
        return base.DisposeAsync();
    }
}
