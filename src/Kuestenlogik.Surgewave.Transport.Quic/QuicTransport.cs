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

    private readonly ConcurrentDictionary<uint, PendingRequest> _pendingRequests = new();
    private readonly ConcurrentBag<PendingRequest> _pendingRequestPool = new();
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

    private sealed class PendingRequest
    {
        public TaskCompletionSource<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> Completion { get; private set; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

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
                header.WriteTo(_requestHeaderBuffer);

                await _stream!.WriteAsync(_requestHeaderBuffer, cancellationToken);
                if (actualPayload.Length > 0)
                {
                    await _stream.WriteAsync(actualPayload, cancellationToken);
                }
                await _stream.FlushAsync(cancellationToken);
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
            header.WriteTo(_requestHeaderBuffer);

            await _stream!.WriteAsync(_requestHeaderBuffer, cancellationToken);
            if (actualPayload.Length > 0)
            {
                await _stream.WriteAsync(actualPayload, cancellationToken);
            }
            await _stream.FlushAsync(cancellationToken);

            await _stream.ReadExactlyAsync(_responseHeaderBuffer, cancellationToken);
            var responseHeader = SurgewaveResponseHeader.ReadFrom(_responseHeaderBuffer);

            if (responseHeader.RequestId != requestId)
            {
                throw new InvalidOperationException(
                    $"Request ID mismatch: expected {requestId}, got {responseHeader.RequestId}");
            }

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
                    _ = Task.Run(async () =>
                    {
                        await _pushConcurrencyLimit.WaitAsync(cancellationToken).ConfigureAwait(false);
                        try { await pushHandler(responseHeader, finalPayload).ConfigureAwait(false); }
                        finally { _pushConcurrencyLimit.Release(); }
                    }, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
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
