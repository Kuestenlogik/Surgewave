using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Cryptography.X509Certificates;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport.Quic;

/// <summary>
/// Raw QUIC transport for the Surgewave native protocol. Same request/response and
/// pipelining semantics as <c>TcpTransport</c>, but rides on a single bidirectional
/// QUIC stream instead of a TCP socket.
/// </summary>
/// <remarks>
/// QUIC brings 0-RTT session resumption, per-stream flow control and packet-loss
/// resilience. On lossy networks this beats TCP because a dropped UDP packet does
/// not head-of-line-block the entire connection.
///
/// Requires msquic — Windows 11 / Server 2022+, or libmsquic on Linux.
/// </remarks>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
public sealed class QuicTransport : ISurgewaveTransport
{
    internal static readonly SslApplicationProtocol SurgewaveAlpn = new("surgewave/1");

    /// <summary>
    /// When set to <c>true</c>, the client skips server certificate validation.
    /// Only for dev and benchmark scenarios where the broker uses a self-signed
    /// certificate. Never enable in production — it disables all TLS integrity
    /// checks on the server identity.
    /// </summary>
    public static bool TrustAllCertificates { get; set; }

    private readonly TransportOptions _options;
    private QuicConnection? _connection;
    private QuicStream? _stream;
    private uint _requestIdCounter;
    private readonly byte[] _responseHeaderBuffer = new byte[SurgewaveResponseHeader.Size];
    private readonly byte[] _requestHeaderBuffer = new byte[SurgewaveNativeProtocol.HeaderSize];
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private readonly ConcurrentDictionary<uint, PendingResponse> _pendingRequests = new();
    private readonly ConcurrentBag<PendingResponse> _pendingRequestPool = new();
    private int _pendingRequestPoolSize;
    private const int MaxPendingRequestPoolSize = 100;
    private Task? _readerTask;
    private CancellationTokenSource? _readerCts;

    private readonly ConcurrentDictionary<SurgewaveOpCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task>> _pushHandlers = new();
    private readonly SemaphoreSlim _pushConcurrencyLimit = new(16, 16);

    public SurgewaveTransportType TransportType => SurgewaveTransportType.Quic;
    public bool IsConnected => _stream is not null && !_stream.ReadsClosed.IsCompleted;
    public bool ServerSupportsCompression { get; private set; }

    public QuicTransport(TransportOptions options)
    {
        _options = options;
    }

    public void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler)
        => _pushHandlers[opCode] = handler;

    public void UnregisterPushHandler(SurgewaveOpCode opCode)
        => _pushHandlers.TryRemove(opCode, out _);

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
        if (!QuicConnection.IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC is not supported on this platform. Install libmsquic on Linux or use Windows 11 / Windows Server 2022+.");
        }

        var clientAuth = new SslClientAuthenticationOptions
        {
            ApplicationProtocols = [SurgewaveAlpn],
            TargetHost = _options.Host,
            RemoteCertificateValidationCallback = _options.CertificateValidation ?? ValidateServerCertificate
        };

        if (_options.ClientCertificate is not null)
        {
            clientAuth.ClientCertificates = new X509CertificateCollection { _options.ClientCertificate };
        }

        var clientOptions = new QuicClientConnectionOptions
        {
            RemoteEndPoint = new DnsEndPoint(_options.Host, _options.Port),
            DefaultStreamErrorCode = 0x100,
            DefaultCloseErrorCode = 0x101,
            ClientAuthenticationOptions = clientAuth
        };

        _connection = await QuicConnection.ConnectAsync(clientOptions, cancellationToken).ConfigureAwait(false);
        _stream = await _connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, cancellationToken).ConfigureAwait(false);

        // Surgewave handshake: send magic + version, read header + capabilities.
        var handshakeBuffer = new byte[5];
        SurgewaveNativeProtocol.Magic.CopyTo(handshakeBuffer);
        handshakeBuffer[4] = SurgewaveNativeProtocol.Version;
        await _stream.WriteAsync(handshakeBuffer, cancellationToken).ConfigureAwait(false);
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);

        await _stream.ReadExactlyAsync(_responseHeaderBuffer, cancellationToken).ConfigureAwait(false);
        var header = SurgewaveResponseHeader.ReadFrom(_responseHeaderBuffer);

        if (header.ErrorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"Handshake failed: {header.ErrorCode}");
        }

        var payload = new byte[header.PayloadLength];
        await _stream.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);

        if (payload.Length >= 2)
        {
            ServerSupportsCompression = payload[1] != 0;
        }

        if (_options.EnablePipelining)
        {
            _readerCts = new CancellationTokenSource();
            _readerTask = Task.Run(() => ReaderLoopAsync(_readerCts.Token));
        }
    }

    /// <summary>
    /// Sends a request and materializes the response into memory the caller keeps (#80). The
    /// payload is copied out of the transport's pooled read so the buffer can go back immediately —
    /// this signature cannot express a loan. Callers on a hot path should use
    /// <see cref="SendRequestLeasedAsync"/>, which skips the copy.
    /// </summary>
    public async ValueTask<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        bool compress = true,
        CancellationToken cancellationToken = default)
    {
        using var lease = await SendRequestLeasedAsync(opCode, payload, compress, cancellationToken).ConfigureAwait(false);
        return (lease.Header, lease.Payload.ToArray());
    }

    /// <inheritdoc />
    public async ValueTask<SurgewaveResponseLease> SendRequestLeasedAsync(
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

    private async ValueTask<SurgewaveResponseLease> SendRequestPipelinedAsync(
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
                    await _stream!.FlushAsync(cancellationToken);
                }
                finally
                {
                    _sendLock.Release();
                }
            }
            finally
            {
                // Return before awaiting the response — see TcpTransport for the reasoning.
                if (compressionBuffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(compressionBuffer);
                }
            }

            return await AwaitResponseAsync(requestId, pending, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Only the party that removes the entry owns the completion.
            _pendingRequests.TryRemove(requestId, out _);
            throw;
        }
    }

    /// <summary>
    /// Awaits the pooled response source, honouring cancellation. See
    /// <see cref="PendingResponse"/> for the ownership contract: whoever removes the entry from
    /// the pending map may complete it, and a cancelled request is never recycled (#80).
    /// </summary>
    private async ValueTask<SurgewaveResponseLease> AwaitResponseAsync(
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
                ((QuicTransport, uint, PendingResponse, CancellationToken))state!;
            if (transport._pendingRequests.TryRemove(id, out _))
                source.TrySetCanceled(token);
        }, (this, requestId, pending, cancellationToken)).ConfigureAwait(false);

        var response = await pending.ValueTask.ConfigureAwait(false);
        ReturnPendingRequest(pending);
        return response;
    }

    private async ValueTask<SurgewaveResponseLease> SendRequestSynchronousAsync(
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
                await _stream!.FlushAsync(cancellationToken);
            }
            finally
            {
                if (compressionBuffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(compressionBuffer);
                }
            }

            await _stream.ReadExactlyAsync(_responseHeaderBuffer, cancellationToken);
            var responseHeader = SurgewaveResponseHeader.ReadFrom(_responseHeaderBuffer);

            if (responseHeader.RequestId != requestId)
            {
                throw new InvalidOperationException(
                    $"Request ID mismatch: expected {requestId}, got {responseHeader.RequestId}");
            }

            return await ReadPayloadAsync(_stream, responseHeader, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Reads one response payload off the wire into a lease (#80). An uncompressed frame is read
    /// into a pooled buffer that travels with the lease; a compressed frame is staged in a pool
    /// buffer and decompressed into an array the codec allocates anyway, so that lease owns nothing.
    /// On a failure mid-read the rent goes back before the exception leaves — nobody downstream
    /// will ever see this lease.
    /// </summary>
    private static async ValueTask<SurgewaveResponseLease> ReadPayloadAsync(
        Stream stream, SurgewaveResponseHeader header, CancellationToken cancellationToken)
    {
        var length = header.PayloadLength;
        if (length <= 0)
        {
            return new SurgewaveResponseLease(header, ReadOnlyMemory<byte>.Empty);
        }

        if ((header.Flags & SurgewaveProtocolFlags.Compressed) != 0)
        {
            var staging = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                await stream.ReadExactlyAsync(staging.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
                return new SurgewaveResponseLease(header, NativeCompressionCodec.DecompressWithHeader(staging.AsSpan(0, length)));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(staging);
            }
        }

        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            await stream.ReadExactlyAsync(buffer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }

        return new SurgewaveResponseLease(header, buffer, length);
    }

    /// <summary>
    /// Reads a server-push payload into memory the handler keeps. Push handlers are registered from
    /// outside the transport and their delegate contract says nothing about when the bytes stop
    /// being valid, so this path keeps allocating rather than lending a pooled buffer to code that
    /// may hold on to it (#80).
    /// </summary>
    private static async ValueTask<ReadOnlyMemory<byte>> ReadPushPayloadAsync(
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

                // Push frames (RequestId 0) go to handlers that own their bytes, so they are read
                // on the allocating path; everything else is a response and can be lent out.
                if (responseHeader.RequestId == 0)
                {
                    var pushPayload = await ReadPushPayloadAsync(_stream, responseHeader, cancellationToken).ConfigureAwait(false);
                    if (_pushHandlers.TryGetValue(responseHeader.OpCode, out var pushHandler))
                    {
                        _ = Task.Run(async () =>
                        {
                            await _pushConcurrencyLimit.WaitAsync(cancellationToken).ConfigureAwait(false);
                            try { await pushHandler(responseHeader, pushPayload).ConfigureAwait(false); }
                            finally { _pushConcurrencyLimit.Release(); }
                        }, cancellationToken);
                    }
                    continue;
                }

                var lease = await ReadPayloadAsync(_stream, responseHeader, cancellationToken).ConfigureAwait(false);

                // The lease holds a pooled buffer, so it needs an owner on every branch: the
                // waiter, or — when the waiter cancelled and took the entry — this loop (#80).
                if (_pendingRequests.TryRemove(responseHeader.RequestId, out var pending))
                {
                    // Recycling happens on the awaiting side once the result is consumed.
                    if (!pending.TrySetResult(lease))
                        lease.Dispose();
                }
                else
                {
                    lease.Dispose();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Fail everything still in flight: no one will read those responses now (#80).
            FailAllPending(new ObjectDisposedException(nameof(QuicTransport), "The transport was disposed while requests were in flight."));
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
                try { await _readerTask; }
                catch (OperationCanceledException) { }
            }
            _readerCts.Dispose();
        }

        if (_stream is not null)
        {
            await _stream.DisposeAsync();
        }

        if (_connection is not null)
        {
            try { await _connection.CloseAsync(0); } catch { /* best-effort */ }
            await _connection.DisposeAsync();
        }

        _sendLock.Dispose();
        _pushConcurrencyLimit.Dispose();
    }

    private bool ValidateServerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        // Per-instance override takes precedence over global static.
        if (_options.TrustAllCertificates == true)
            return true;
        if (_options.TrustAllCertificates == false)
            return sslPolicyErrors == SslPolicyErrors.None;

        // Fallback to global static flag.
        if (TrustAllCertificates)
            return true;

        return sslPolicyErrors == SslPolicyErrors.None;
    }
}
