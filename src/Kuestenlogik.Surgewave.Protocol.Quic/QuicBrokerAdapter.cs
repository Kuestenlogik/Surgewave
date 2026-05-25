using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kuestenlogik.Surgewave.Protocol;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Protocol.Quic;

/// <summary>
/// Raw QUIC transport for Surgewave. Each accepted bidirectional stream is handed
/// to the broker's <see cref="ISurgewaveStreamHandler"/>, which auto-detects the
/// wire protocol (Surgewave native or Kafka) from the first four bytes and
/// dispatches accordingly. This adapter itself is protocol-agnostic.
/// </summary>
/// <remarks>
/// QUIC gives Surgewave three things raw TCP cannot:
/// (1) 0-RTT resumption cuts reconnect latency on lossy networks to zero;
/// (2) Per-stream flow control eliminates head-of-line blocking between
///     independent Produce/Fetch operations on the same connection;
/// (3) Connection migration survives NAT rebinding and wifi↔cellular handoffs.
///
/// Requires msquic — built into Windows 11 / Server 2022+ and available on
/// Linux via libmsquic. Disabled by default; enable via Surgewave:Quic:Enabled=true.
/// </remarks>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
public sealed partial class QuicBrokerAdapter : BackgroundService
{
    // Application-Layer Protocol Negotiation identifier for Surgewave-over-QUIC.
    // Clients must advertise this ALPN value to negotiate a session. One ALPN
    // covers every Surgewave wire protocol — the first 4 bytes of each bidirectional
    // stream select between Surgewave-native and Kafka, same as the TCP path.
    public static readonly SslApplicationProtocol SurgewaveAlpn = new("surgewave/1");

    private readonly QuicConfig _config;
    private readonly ILogger<QuicBrokerAdapter> _logger;
    private QuicListener? _listener;
    private int _activeConnections;

    public QuicBrokerAdapter(IOptions<QuicConfig> config, ILogger<QuicBrokerAdapter> logger)
    {
        _config = config.Value;
        _logger = logger;
    }

    /// <summary>Number of QUIC connections currently being served.</summary>
    public int ActiveConnections => Volatile.Read(ref _activeConnections);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            LogDisabled(_logger);
            return;
        }

        if (!QuicListener.IsSupported)
        {
            LogNotSupported(_logger);
            return;
        }

        var handler = SurgewaveStreamHandlerHolder.Instance;
        if (handler is null)
        {
            LogNoHandler(_logger);
            return;
        }

        X509Certificate2 cert;
        try
        {
            cert = LoadOrGenerateCertificate();
        }
        catch (Exception ex)
        {
            LogCertificateFailure(_logger, ex);
            return;
        }

        var listenerOptions = new QuicListenerOptions
        {
            ListenEndPoint = new IPEndPoint(IPAddress.Any, _config.Port),
            ApplicationProtocols = [SurgewaveAlpn],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = 0x100,
                DefaultCloseErrorCode = 0x101,
                IdleTimeout = TimeSpan.FromSeconds(_config.IdleTimeoutSeconds),
                MaxInboundBidirectionalStreams = _config.MaxStreamsPerConnection,
                MaxInboundUnidirectionalStreams = 0,
                ServerAuthenticationOptions = new SslServerAuthenticationOptions
                {
                    ApplicationProtocols = [SurgewaveAlpn],
                    ServerCertificate = cert,
                    ClientCertificateRequired = false
                }
            })
        };

        try
        {
            _listener = await QuicListener.ListenAsync(listenerOptions, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogListenerStartFailed(_logger, ex, _config.Port);
            return;
        }

        LogListening(_logger, _config.Port, _config.MaxStreamsPerConnection);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                QuicConnection connection;
                try
                {
                    connection = await _listener.AcceptConnectionAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (QuicException ex)
                {
                    LogAcceptFailed(_logger, ex);
                    continue;
                }

                if (Volatile.Read(ref _activeConnections) >= _config.MaxConnections)
                {
                    LogMaxConnectionsReached(_logger, _config.MaxConnections);
                    await connection.CloseAsync(0x102).ConfigureAwait(false);
                    await connection.DisposeAsync().ConfigureAwait(false);
                    continue;
                }

                _ = HandleConnectionAsync(connection, handler, stoppingToken);
            }
        }
        finally
        {
            await _listener.DisposeAsync().ConfigureAwait(false);
            LogStopped(_logger);
        }
    }

    // -------------------------------------------------------------------------
    // Per-connection lifecycle
    // -------------------------------------------------------------------------

    private async Task HandleConnectionAsync(
        QuicConnection connection,
        ISurgewaveStreamHandler handler,
        CancellationToken ct)
    {
        Interlocked.Increment(ref _activeConnections);
        var remoteEndPoint = connection.RemoteEndPoint;
        var clientHost = remoteEndPoint.Address.ToString();

        LogConnected(_logger, remoteEndPoint, Volatile.Read(ref _activeConnections));

        var streamTasks = new List<Task>();
        try
        {
            await using (connection)
            {
                while (!ct.IsCancellationRequested)
                {
                    QuicStream stream;
                    try
                    {
                        stream = await connection.AcceptInboundStreamAsync(ct).ConfigureAwait(false);
                    }
                    catch (QuicException ex) when (
                        ex.QuicError is QuicError.ConnectionAborted
                                     or QuicError.ConnectionIdle
                                     or QuicError.ConnectionTimeout
                                     or QuicError.OperationAborted)
                    {
                        break;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    if (stream.Type != QuicStreamType.Bidirectional)
                    {
                        stream.Abort(QuicAbortDirection.Both, 0x103);
                        await stream.DisposeAsync().ConfigureAwait(false);
                        continue;
                    }

                    streamTasks.Add(HandleStreamAsync(stream, handler, clientHost, remoteEndPoint, ct));
                }

                // Let in-flight stream workers finish before closing the connection.
                try { await Task.WhenAll(streamTasks).ConfigureAwait(false); }
                catch { /* per-stream errors already logged */ }
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (Exception ex)
        {
            LogConnectionError(_logger, ex, remoteEndPoint);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
            LogDisconnected(_logger, remoteEndPoint, Volatile.Read(ref _activeConnections));
        }
    }

    private async Task HandleStreamAsync(
        QuicStream stream,
        ISurgewaveStreamHandler handler,
        string clientHost,
        EndPoint endpoint,
        CancellationToken ct)
    {
        try
        {
            await using (stream)
            {
                await handler.HandleAsync(stream, clientHost, endpoint, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (QuicException) { /* peer reset / stream aborted — normal on disconnect */ }
        catch (Exception ex)
        {
            LogStreamError(_logger, ex, endpoint);
        }
    }

    // -------------------------------------------------------------------------
    // Certificate handling
    // -------------------------------------------------------------------------

    private X509Certificate2 LoadOrGenerateCertificate()
    {
        if (!string.IsNullOrEmpty(_config.CertificatePath))
        {
            return X509CertificateLoader.LoadPkcs12FromFile(_config.CertificatePath, _config.CertificatePassword);
        }

        // Dev/self-signed fallback. On Windows QUIC requires an exportable private
        // key, so we round-trip the generated cert through PKCS#12 to attach one.
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            new X500DistinguishedName("CN=surgewave-quic-dev"),
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1")], false));

        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        san.AddIpAddress(IPAddress.IPv6Loopback);
        request.CertificateExtensions.Add(san.Build());

        using var generated = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));

        LogUsingSelfSignedCert(_logger);
        return X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pkcs12), password: null);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        if (_listener is not null)
        {
            _listener.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        base.Dispose();
    }

    // -------------------------------------------------------------------------
    // Source-generated logging
    // -------------------------------------------------------------------------

    [LoggerMessage(Level = LogLevel.Debug, Message = "QUIC protocol adapter is disabled")]
    private static partial void LogDisabled(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "QUIC is not supported on this platform — install libmsquic on Linux or use Windows 11 / Windows Server 2022+.")]
    private static partial void LogNotSupported(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "QUIC protocol adapter cannot start — no ISurgewaveStreamHandler is registered. Ensure the broker assigns SurgewaveStreamHandlerHolder.Instance during startup.")]
    private static partial void LogNoHandler(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to load certificate for QUIC listener")]
    private static partial void LogCertificateFailure(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "QUIC listener failed to start on UDP port {Port}")]
    private static partial void LogListenerStartFailed(ILogger logger, Exception exception, int port);

    [LoggerMessage(Level = LogLevel.Information, Message = "QUIC transport listening on UDP :{Port} (ALPN=surgewave/1, maxStreamsPerConnection={MaxStreams})")]
    private static partial void LogListening(ILogger logger, int port, int maxStreams);

    [LoggerMessage(Level = LogLevel.Warning, Message = "QUIC accept failed")]
    private static partial void LogAcceptFailed(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "QUIC max connections reached ({Max}), closing incoming connection")]
    private static partial void LogMaxConnectionsReached(ILogger logger, int max);

    [LoggerMessage(Level = LogLevel.Information, Message = "QUIC protocol adapter stopped")]
    private static partial void LogStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "QUIC client connected: {Endpoint} (active: {Active})")]
    private static partial void LogConnected(ILogger logger, EndPoint endpoint, int active);

    [LoggerMessage(Level = LogLevel.Information, Message = "QUIC client disconnected: {Endpoint} (active: {Active})")]
    private static partial void LogDisconnected(ILogger logger, EndPoint endpoint, int active);

    [LoggerMessage(Level = LogLevel.Error, Message = "QUIC connection {Endpoint} error")]
    private static partial void LogConnectionError(ILogger logger, Exception exception, EndPoint endpoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "QUIC stream error ({Endpoint})")]
    private static partial void LogStreamError(ILogger logger, Exception exception, EndPoint endpoint);

    [LoggerMessage(Level = LogLevel.Warning, Message = "QUIC using self-signed dev certificate — set Surgewave:Quic:CertificatePath for production deployments")]
    private static partial void LogUsingSelfSignedCert(ILogger logger);
}
