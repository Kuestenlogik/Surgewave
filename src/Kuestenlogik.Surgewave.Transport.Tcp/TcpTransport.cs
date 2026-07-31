using System.Buffers;
using System.Collections.Concurrent;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport.Tcp;

/// <summary>
/// TCP/IP transport implementation for Surgewave native protocol.
/// Supports pipelined and synchronous request modes.
/// </summary>
public sealed class TcpTransport : ISurgewaveTransport
{
    private readonly TransportOptions _options;
    private TcpClient? _client;
    private NetworkStream? _stream;
    private uint _requestIdCounter;
    private readonly byte[] _responseHeaderBuffer = new byte[SurgewaveResponseHeader.Size];
    private readonly byte[] _requestHeaderBuffer = new byte[SurgewaveNativeProtocol.HeaderSize];
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    // Pipelining support
    private readonly ConcurrentDictionary<uint, PendingResponse> _pendingRequests = new();
    private readonly ConcurrentBag<PendingResponse> _pendingRequestPool = new();
    private int _pendingRequestPoolSize;
    private const int MaxPendingRequestPoolSize = 100;
    private Task? _readerTask;
    private CancellationTokenSource? _readerCts;

    // Server-push handler support (streaming subscriptions)
    private readonly ConcurrentDictionary<SurgewaveOpCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task>> _pushHandlers = new();
    private readonly SemaphoreSlim _pushConcurrencyLimit = new(16, 16); // Limit concurrent push handlers

    public SurgewaveTransportType TransportType => SurgewaveTransportType.Tcp;
    public bool IsConnected => _client?.Connected == true;
    public bool ServerSupportsCompression { get; private set; }

    public TcpTransport(TransportOptions options)
    {
        _options = options;
    }

    /// <summary>
    /// Register a handler for unsolicited server-push messages identified by op-code.
    /// Push messages arrive with RequestId == 0 and are dispatched here instead of
    /// completing a pending request.
    /// </summary>
    public void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler)
    {
        _pushHandlers[opCode] = handler;
    }

    /// <summary>
    /// Remove a previously registered push handler.
    /// </summary>
    public void UnregisterPushHandler(SurgewaveOpCode opCode)
    {
        _pushHandlers.TryRemove(opCode, out _);
    }

    private PendingResponse RentPendingRequest()
    {
        if (_pendingRequestPool.TryTake(out var request))
        {
            Interlocked.Decrement(ref _pendingRequestPoolSize);
            request.Reset();
            return request;
        }
        return new PendingResponse();
    }

    private void ReturnPendingRequest(PendingResponse request)
    {
        if (Interlocked.Increment(ref _pendingRequestPoolSize) <= MaxPendingRequestPoolSize)
        {
            _pendingRequestPool.Add(request);
        }
        else
        {
            Interlocked.Decrement(ref _pendingRequestPoolSize);
        }
    }

    public async ValueTask ConnectAsync(CancellationToken cancellationToken = default)
    {
        _client = new TcpClient
        {
            NoDelay = true,
            SendBufferSize = _options.SendBufferSize,
            ReceiveBufferSize = _options.ReceiveBufferSize
        };

        await _client.ConnectAsync(_options.Host, _options.Port, cancellationToken);
        _stream = _client.GetStream();

        // Send magic bytes + version for handshake
        var handshakeBuffer = new byte[5];
        SurgewaveNativeProtocol.Magic.CopyTo(handshakeBuffer);
        handshakeBuffer[4] = SurgewaveNativeProtocol.Version;
        await _stream.WriteAsync(handshakeBuffer, cancellationToken);

        // Read handshake response
        await _stream.ReadExactlyAsync(_responseHeaderBuffer, cancellationToken);
        var header = SurgewaveResponseHeader.ReadFrom(_responseHeaderBuffer);

        if (header.ErrorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"Handshake failed: {header.ErrorCode}");
        }

        // Read handshake payload (version + capabilities)
        var payload = new byte[header.PayloadLength];
        await _stream.ReadExactlyAsync(payload, cancellationToken);

        // Parse capabilities: [version:1][compression:1][streaming:1][reserved:2]
        if (payload.Length >= 2)
        {
            ServerSupportsCompression = payload[1] != 0;
        }

        // Start background reader task for pipelined mode
        if (_options.EnablePipelining)
        {
            _readerCts = new CancellationTokenSource();
            _readerTask = Task.Run(() => ReaderLoopAsync(_readerCts.Token));
        }
    }

    public async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress = true,
        CancellationToken cancellationToken = default)
    {
        if (_options.EnablePipelining)
        {
            return await SendRequestPipelinedAsync(opCode, payload, compress, cancellationToken);
        }
        return await SendRequestSynchronousAsync(opCode, payload, compress, cancellationToken);
    }

    private async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestPipelinedAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress,
        CancellationToken cancellationToken)
    {
        var requestId = Interlocked.Increment(ref _requestIdCounter);
        var pending = RentPendingRequest();

        _pendingRequests[requestId] = pending;

        try
        {
            var flags = SurgewaveProtocolFlags.None;
            ReadOnlyMemory<byte> actualPayload = payload;
            byte[]? compressionBuffer = null;

            if (compress && _options.EnableCompression && ServerSupportsCompression &&
                payload.Length >= NativeCompressionCodec.MinCompressionSize &&
                NativeCompressionCodec.TryCompressWithHeader(payload.Span, out compressionBuffer, out var frameLength))
            {
                actualPayload = compressionBuffer.AsMemory(0, frameLength);
                flags |= SurgewaveProtocolFlags.Compressed;
            }

            try
            {
                await _sendLock.WaitAsync(cancellationToken);
                try
                {
                    var header = new SurgewaveRequestHeader
                    {
                        Flags = flags,
                        RequestId = requestId,
                        OpCode = opCode,
                        PayloadLength = actualPayload.Length
                    };
                    await NativeRequestFrameWriter.WriteAsync(_stream!, header, actualPayload, _requestHeaderBuffer, cancellationToken);
                }
                finally
                {
                    _sendLock.Release();
                }
            }
            finally
            {
                // Return before awaiting the response: otherwise every in-flight request would
                // hold a pool array for a full round-trip. actualPayload is dead from here on.
                if (compressionBuffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(compressionBuffer);
                }
            }

            return await AwaitResponseAsync(requestId, pending, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Only the party that removes the entry owns the completion; if the reader
            // already took it, it will complete the (now unobserved) response itself.
            _pendingRequests.TryRemove(requestId, out _);
            throw;
        }
    }

    /// <summary>
    /// Awaits the pooled response source, honouring cancellation. The pending-request map is the
    /// arbiter: whoever removes the entry may complete it. A cancelled request is never recycled —
    /// the reader may still hold a reference, and reusing it would deliver a response to an
    /// unrelated caller (#80).
    /// </summary>
    private async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> AwaitResponseAsync(
        uint requestId, PendingResponse pending, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            var result = await pending.ValueTask.ConfigureAwait(false);
            ReturnPendingRequest(pending);
            return result;
        }

        await using var registration = cancellationToken.Register(static state =>
        {
            var (transport, id, source, token) =
                ((TcpTransport, uint, PendingResponse, CancellationToken))state!;
            if (transport._pendingRequests.TryRemove(id, out _))
                source.TrySetCanceled(token);
        }, (this, requestId, pending, cancellationToken)).ConfigureAwait(false);

        var response = await pending.ValueTask.ConfigureAwait(false);
        ReturnPendingRequest(pending);
        return response;
    }

    private async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestSynchronousAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress,
        CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken);
        try
        {
            var requestId = Interlocked.Increment(ref _requestIdCounter);

            var flags = SurgewaveProtocolFlags.None;
            ReadOnlyMemory<byte> actualPayload = payload;
            byte[]? compressionBuffer = null;

            if (compress && _options.EnableCompression && ServerSupportsCompression &&
                payload.Length >= NativeCompressionCodec.MinCompressionSize &&
                NativeCompressionCodec.TryCompressWithHeader(payload.Span, out compressionBuffer, out var frameLength))
            {
                actualPayload = compressionBuffer.AsMemory(0, frameLength);
                flags |= SurgewaveProtocolFlags.Compressed;
            }

            try
            {
                var header = new SurgewaveRequestHeader
                {
                    Flags = flags,
                    RequestId = requestId,
                    OpCode = opCode,
                    PayloadLength = actualPayload.Length
                };
                await NativeRequestFrameWriter.WriteAsync(_stream!, header, actualPayload, _requestHeaderBuffer, cancellationToken);
            }
            finally
            {
                // The request is on the wire; the rent is not needed for the response read.
                if (compressionBuffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(compressionBuffer);
                }
            }

            await _stream!.ReadExactlyAsync(_responseHeaderBuffer, cancellationToken);
            var responseHeader = SurgewaveResponseHeader.ReadFrom(_responseHeaderBuffer);

            if (responseHeader.RequestId != requestId)
            {
                throw new InvalidOperationException(
                    $"Request ID mismatch: expected {requestId}, got {responseHeader.RequestId}");
            }

            return (responseHeader, await ReadPayloadAsync(_stream, responseHeader, cancellationToken).ConfigureAwait(false));
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Reads one response payload off the wire.
    /// <para>
    /// A compressed frame is staged in a pooled buffer and returned immediately after
    /// decompression, which allocates the result array anyway — that read costs no garbage
    /// beyond the decompressed payload (#80). An uncompressed frame is read straight into the
    /// array that escapes to the caller as <see cref="ReadOnlyMemory{T}"/>; pooling it would
    /// need a buffer-return protocol on <see cref="ISurgewaveTransport"/>, which the interface
    /// does not have.
    /// </para>
    /// </summary>
    private static async ValueTask<ReadOnlyMemory<byte>> ReadPayloadAsync(
        Stream stream, SurgewaveResponseHeader header, CancellationToken cancellationToken)
    {
        var length = header.PayloadLength;
        if (length <= 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        if ((header.Flags & SurgewaveProtocolFlags.Compressed) != 0)
        {
            var staging = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                await stream.ReadExactlyAsync(staging.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                return NativeCompressionCodec.DecompressWithHeader(staging.AsSpan(0, length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(staging);
            }
        }

        var payload = new byte[length];
        await stream.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);
        return payload;
    }

    private async Task ReaderLoopAsync(CancellationToken cancellationToken)
    {
        var headerBuffer = new byte[SurgewaveResponseHeader.Size];

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await _stream!.ReadExactlyAsync(headerBuffer, cancellationToken);
                var responseHeader = SurgewaveResponseHeader.ReadFrom(headerBuffer);

                var finalPayload = await ReadPayloadAsync(_stream, responseHeader, cancellationToken).ConfigureAwait(false);

                if (_pendingRequests.TryRemove(responseHeader.RequestId, out var pending))
                {
                    // Recycling happens on the awaiting side, once the result is consumed:
                    // returning it here would hand the instance out again while the waiter
                    // still holds its ValueTask token.
                    pending.TrySetResult((responseHeader, finalPayload));
                }
                else if (responseHeader.RequestId == 0 &&
                         _pushHandlers.TryGetValue(responseHeader.OpCode, out var pushHandler))
                {
                    // Server-push message: route to handler with bounded concurrency
                    _ = Task.Run(async () =>
                    {
                        await _pushConcurrencyLimit.WaitAsync(cancellationToken).ConfigureAwait(false);
                        try { await pushHandler(responseHeader, finalPayload).ConfigureAwait(false); }
                        finally { _pushConcurrencyLimit.Release(); }
                    }, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown — but everything still in flight has to be failed, otherwise
            // those awaiters would hang forever: nothing will ever read their response
            // once this loop is gone (#80).
            FailAllPending(new ObjectDisposedException(nameof(TcpTransport), "The transport was disposed while requests were in flight."));
        }
        catch (Exception ex)
        {
            FailAllPending(ex);
        }
    }

    private void FailAllPending(Exception exception)
    {
        foreach (var kvp in _pendingRequests)
        {
            if (_pendingRequests.TryRemove(kvp.Key, out var pending))
            {
                pending.TrySetException(exception);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_readerCts != null)
        {
            await _readerCts.CancelAsync();
            if (_readerTask != null)
            {
                try
                {
                    await _readerTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }
            _readerCts.Dispose();
        }

        _stream?.Dispose();
        _client?.Dispose();
        _sendLock.Dispose();
        _pushConcurrencyLimit.Dispose();
    }
}
