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
    private readonly ConcurrentDictionary<uint, PendingRequest> _pendingRequests = new();
    private readonly ConcurrentBag<PendingRequest> _pendingRequestPool = new();
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

    private sealed class PendingRequest
    {
        public TaskCompletionSource<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> Completion { get; private set; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Reset the pending request for reuse by creating a new TCS.
        /// Cheaper than allocating a whole new PendingRequest object.
        /// </summary>
        public void Reset()
        {
            Completion = new TaskCompletionSource<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    private PendingRequest RentPendingRequest()
    {
        if (_pendingRequestPool.TryTake(out var request))
        {
            Interlocked.Decrement(ref _pendingRequestPoolSize);
            request.Reset();
            return request;
        }
        return new PendingRequest();
    }

    private void ReturnPendingRequest(PendingRequest request)
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

            if (compress && _options.EnableCompression && ServerSupportsCompression &&
                payload.Length >= NativeCompressionCodec.MinCompressionSize)
            {
                var (compressed, wasCompressed) = NativeCompressionCodec.CompressWithHeader(payload.Span);
                if (wasCompressed)
                {
                    actualPayload = compressed;
                    flags |= SurgewaveProtocolFlags.Compressed;
                }
            }

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

            return await pending.Completion.Task.WaitAsync(cancellationToken);
        }
        catch
        {
            _pendingRequests.TryRemove(requestId, out _);
            throw;
        }
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

            if (compress && _options.EnableCompression && ServerSupportsCompression &&
                payload.Length >= NativeCompressionCodec.MinCompressionSize)
            {
                var (compressed, wasCompressed) = NativeCompressionCodec.CompressWithHeader(payload.Span);
                if (wasCompressed)
                {
                    actualPayload = compressed;
                    flags |= SurgewaveProtocolFlags.Compressed;
                }
            }

            var header = new SurgewaveRequestHeader
            {
                Flags = flags,
                RequestId = requestId,
                OpCode = opCode,
                PayloadLength = actualPayload.Length
            };
            await NativeRequestFrameWriter.WriteAsync(_stream!, header, actualPayload, _requestHeaderBuffer, cancellationToken);

            await _stream!.ReadExactlyAsync(_responseHeaderBuffer, cancellationToken);
            var responseHeader = SurgewaveResponseHeader.ReadFrom(_responseHeaderBuffer);

            if (responseHeader.RequestId != requestId)
            {
                throw new InvalidOperationException(
                    $"Request ID mismatch: expected {requestId}, got {responseHeader.RequestId}");
            }

            // TODO: Optimize - use ArrayPool<byte>.Shared.Rent() for responsePayload when compressed,
            // since decompression creates a new array and the rented buffer could be returned immediately.
            // For uncompressed responses, the byte[] escapes to the caller as ReadOnlyMemory<byte>,
            // so ArrayPool is not safe without a return-buffer protocol.
            var responsePayload = new byte[responseHeader.PayloadLength];
            if (responseHeader.PayloadLength > 0)
            {
                await _stream.ReadExactlyAsync(responsePayload, cancellationToken);
            }

            ReadOnlyMemory<byte> finalPayload = responsePayload;
            if ((responseHeader.Flags & SurgewaveProtocolFlags.Compressed) != 0)
            {
                finalPayload = NativeCompressionCodec.DecompressWithHeader(responsePayload);
            }

            return (responseHeader, finalPayload);
        }
        finally
        {
            _sendLock.Release();
        }
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

                var responsePayload = new byte[responseHeader.PayloadLength];
                if (responseHeader.PayloadLength > 0)
                {
                    await _stream.ReadExactlyAsync(responsePayload, cancellationToken);
                }

                ReadOnlyMemory<byte> finalPayload = responsePayload;
                if ((responseHeader.Flags & SurgewaveProtocolFlags.Compressed) != 0)
                {
                    finalPayload = NativeCompressionCodec.DecompressWithHeader(responsePayload);
                }

                if (_pendingRequests.TryRemove(responseHeader.RequestId, out var pending))
                {
                    pending.Completion.TrySetResult((responseHeader, finalPayload));
                    ReturnPendingRequest(pending);
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
            // Normal shutdown
        }
        catch (Exception ex)
        {
            foreach (var kvp in _pendingRequests)
            {
                if (_pendingRequests.TryRemove(kvp.Key, out var pending))
                {
                    pending.Completion.TrySetException(ex);
                }
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
