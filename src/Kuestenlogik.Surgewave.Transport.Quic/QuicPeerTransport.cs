using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Kuestenlogik.Surgewave.Transport.Quic;

/// <summary>
/// Peer transport backed by raw QUIC. Each <see cref="IPeerConnection"/>
/// owns a <see cref="QuicConnection"/> plus a single bidirectional
/// <see cref="QuicStream"/>, mirroring the semantics of one long-lived TCP
/// socket per peer.
/// </summary>
/// <remarks>
/// QUIC adds 0-RTT resumption, per-stream flow control and packet-loss
/// resilience on top of inter-broker traffic — the things that hurt Raft
/// and replication most on lossy or high-latency links between datacenters.
///
/// Requires msquic — Windows 11 / Server 2022+, or libmsquic on Linux.
/// </remarks>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
public sealed class QuicPeerTransport : IPeerTransport
{
    public const string TransportName = "quic";
    internal static readonly SslApplicationProtocol PeerAlpn = new("surgewave-peer/1");

    // Shared lazily-created dev certificate for peer listeners. Real deployments
    // should register their own via Surgewave:Clustering:Quic:BrokerCertificatePath.
    private static X509Certificate2? _sharedDevCertificate;
    private static X509Certificate2? _loadedBrokerCertificate;
    private static X509Certificate2? _loadedCaCertificate;
    private static readonly object _certLock = new();

    /// <summary>
    /// Dev fallback: when <c>true</c>, skips server certificate validation on
    /// outbound peer connections and disables client cert requirement on inbound
    /// listeners. Never enable in production — it disables all TLS integrity
    /// checks between brokers. Use the shared-CA configuration instead.
    /// </summary>
    public static bool TrustAllCertificates { get; set; }

    /// <summary>
    /// Path to this broker's own PKCS#12 (.pfx) certificate — the identity
    /// presented to peers during the TLS handshake. Must be signed by the
    /// shared cluster CA when <see cref="CaCertificatePath"/> is set.
    /// </summary>
    public static string? BrokerCertificatePath { get; set; }

    /// <summary>Password for <see cref="BrokerCertificatePath"/>.</summary>
    public static string? BrokerCertificatePassword { get; set; }

    /// <summary>
    /// Path to the shared cluster CA certificate. When set, inbound and
    /// outbound peer connections only accept certs that chain to this CA.
    /// This is the recommended production configuration for mTLS between
    /// Surgewave brokers.
    /// </summary>
    public static string? CaCertificatePath { get; set; }

    // Retained for backwards compatibility with the earlier single-cert API.
    // New code should use BrokerCertificatePath instead.
    public static string? CertificatePath
    {
        get => BrokerCertificatePath;
        set => BrokerCertificatePath = value;
    }
    public static string? CertificatePassword
    {
        get => BrokerCertificatePassword;
        set => BrokerCertificatePassword = value;
    }

    /// <summary>
    /// True when a broker certificate path is configured and the CA certificate
    /// is configured, indicating the operator wants real mTLS rather than the
    /// self-signed dev fallback.
    /// </summary>
    public static bool HasMutualTlsConfig =>
        !string.IsNullOrEmpty(BrokerCertificatePath) && !string.IsNullOrEmpty(CaCertificatePath);

    private bool HasInstanceMutualTlsConfig =>
        _options is not null
        && !string.IsNullOrEmpty(_options.BrokerCertificatePath)
        && !string.IsNullOrEmpty(_options.CaCertificatePath);

    private readonly QuicPeerTransportOptions? _options;

    public QuicPeerTransport() { }

    public QuicPeerTransport(QuicPeerTransportOptions options)
    {
        _options = options;
    }

    public string Name => TransportName;

    public async ValueTask<IPeerConnection> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default)
    {
        if (!QuicConnection.IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC is not supported on this platform. Install libmsquic on Linux or use Windows 11 / Windows Server 2022+.");
        }

        var clientAuth = new SslClientAuthenticationOptions
        {
            ApplicationProtocols = [PeerAlpn],
            TargetHost = host,
            RemoteCertificateValidationCallback = _options?.CertificateValidation ?? ValidatePeerCertificate
        };

        // When mTLS is configured (instance or static), present this broker's
        // identity to the server.
        if (HasInstanceMutualTlsConfig || HasMutualTlsConfig)
        {
            var brokerCert = HasInstanceMutualTlsConfig
                ? LoadCertificateFromOptions(_options!)
                : LoadOrGenerateCertificate();
            clientAuth.ClientCertificates = new X509CertificateCollection { brokerCert };
        }

        var clientOptions = new QuicClientConnectionOptions
        {
            RemoteEndPoint = new DnsEndPoint(host, port),
            DefaultStreamErrorCode = 0x100,
            DefaultCloseErrorCode = 0x101,
            ClientAuthenticationOptions = clientAuth
        };

        QuicConnection? connection = null;
        try
        {
            connection = await QuicConnection.ConnectAsync(clientOptions, cancellationToken).ConfigureAwait(false);
            var stream = await connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, cancellationToken)
                .ConfigureAwait(false);
            var peerConnection = new QuicPeerConnection(connection, stream);
            connection = null; // ownership transferred
            PeerTransportMetrics.Instance.RecordConnectionOpened(TransportName);
            PeerTransportMetrics.Instance.IncrementActiveConnections(TransportName);
            return peerConnection;
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    public IPeerListener CreateListener(IPEndPoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return new QuicPeerListener(endpoint);
    }

    private bool ValidatePeerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        // Instance-level mTLS takes precedence over static config.
        if (HasInstanceMutualTlsConfig)
        {
            if (certificate is null) return false;
            var ca = LoadCaCertificateFromPath(_options!.CaCertificatePath!);
            return VerifyAgainstCa(certificate, ca);
        }

        // Instance-level trust-all override.
        if (_options?.TrustAllCertificates == true)
            return true;
        if (_options?.TrustAllCertificates == false)
            return sslPolicyErrors == SslPolicyErrors.None;

        // Static mTLS config.
        if (HasMutualTlsConfig)
        {
            if (certificate is null) return false;
            var ca = LoadCaCertificate();
            return VerifyAgainstCa(certificate, ca);
        }

        // Static trust-all flag.
        if (TrustAllCertificates)
            return true;

        return sslPolicyErrors == SslPolicyErrors.None;
    }

    internal static bool VerifyAgainstCa(X509Certificate leaf, X509Certificate2 ca)
    {
        // Build a chain that ignores the system trust store and only accepts
        // the provided CA as a root. This is exactly the shared-CA model:
        // every broker trusts cluster-CA-signed certs and nothing else.
        using var chain = new X509Chain
        {
            ChainPolicy =
            {
                RevocationMode = X509RevocationMode.NoCheck,
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority
            }
        };
        chain.ChainPolicy.CustomTrustStore.Add(ca);

        // Convert leaf to X509Certificate2 if needed.
        using var leaf2 = leaf as X509Certificate2 ?? X509CertificateLoader.LoadCertificate(leaf.GetRawCertData());
        if (!chain.Build(leaf2)) return false;

        // Walk the chain and require the root element to be our CA exactly.
        var root = chain.ChainElements[^1].Certificate;
        return root.Thumbprint == ca.Thumbprint;
    }

    internal static X509Certificate2 LoadCaCertificateForListener() => LoadCaCertificate();

    private static X509Certificate2 LoadCaCertificateFromPath(string path)
    {
        return X509CertificateLoader.LoadCertificateFromFile(path);
    }

    private static X509Certificate2 LoadCertificateFromOptions(QuicPeerTransportOptions options)
    {
        return X509CertificateLoader.LoadPkcs12FromFile(
            options.BrokerCertificatePath!, options.BrokerCertificatePassword);
    }

    private static X509Certificate2 LoadCaCertificate()
    {
        if (string.IsNullOrEmpty(CaCertificatePath))
        {
            throw new InvalidOperationException(
                "CaCertificatePath is not configured but mTLS validation was requested.");
        }

        lock (_certLock)
        {
            if (_loadedCaCertificate is not null)
            {
                return _loadedCaCertificate;
            }
            // CA cert has no private key attached and is typically distributed as
            // a plain .cer/.crt. LoadCertificateFromFile handles DER and PEM.
            _loadedCaCertificate = X509CertificateLoader.LoadCertificateFromFile(CaCertificatePath);
            return _loadedCaCertificate;
        }
    }

    internal static X509Certificate2 LoadOrGenerateCertificate()
    {
        // Production path: operator-supplied broker certificate signed by the cluster CA.
        if (!string.IsNullOrEmpty(BrokerCertificatePath))
        {
            lock (_certLock)
            {
                if (_loadedBrokerCertificate is not null)
                {
                    return _loadedBrokerCertificate;
                }
                _loadedBrokerCertificate = X509CertificateLoader.LoadPkcs12FromFile(
                    BrokerCertificatePath, BrokerCertificatePassword);
                return _loadedBrokerCertificate;
            }
        }

        // Dev fallback: generate a shared self-signed cert. Only usable when
        // TrustAllCertificates is also true, because peers can't chain-verify it.
        lock (_certLock)
        {
            if (_sharedDevCertificate is not null)
            {
                return _sharedDevCertificate;
            }

            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                new X500DistinguishedName("CN=surgewave-peer-dev"),
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

            // Round-trip through PKCS#12 so the key is exportable (required by QUIC on Windows).
            _sharedDevCertificate = X509CertificateLoader.LoadPkcs12(
                generated.Export(X509ContentType.Pkcs12), password: null);
            return _sharedDevCertificate;
        }
    }
}

[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
internal sealed class QuicPeerConnection : IPeerConnection
{
    private readonly QuicConnection _connection;
    private readonly QuicStream _stream;

    public QuicPeerConnection(QuicConnection connection, QuicStream stream)
    {
        _connection = connection;
        _stream = stream;
    }

    public Stream Stream => _stream;
    public EndPoint? RemoteEndPoint => _connection.RemoteEndPoint;
    public bool IsConnected => !_stream.ReadsClosed.IsCompleted && !_stream.WritesClosed.IsCompleted;

    public async ValueTask<IPeerStreamLease> AcquireStreamAsync(CancellationToken cancellationToken = default)
    {
        // True multi-stream: open a fresh outbound bidi stream per RPC.
        // Each stream has its own QUIC-level flow control and loss recovery,
        // so a retransmit stall on one Raft vote cannot delay an AppendEntries
        // on the same connection.
        var rpcStream = await _connection.OpenOutboundStreamAsync(
            QuicStreamType.Bidirectional, cancellationToken).ConfigureAwait(false);
        return new QuicPeerStreamLease(rpcStream, async () =>
        {
            try { await rpcStream.DisposeAsync().ConfigureAwait(false); }
            catch { /* best-effort */ }
        });
    }

    public async ValueTask<IPeerStreamLease> AcceptInboundStreamAsync(CancellationToken cancellationToken = default)
    {
        // Server-side: accept an inbound bidi stream opened by the remote peer.
        // Each accepted stream carries one RPC (request + response), so the
        // server can handle many concurrent RPCs on a single QUIC connection.
        var inboundStream = await _connection.AcceptInboundStreamAsync(cancellationToken).ConfigureAwait(false);
        return new QuicPeerStreamLease(inboundStream, async () =>
        {
            try { await inboundStream.DisposeAsync().ConfigureAwait(false); }
            catch { /* best-effort */ }
        });
    }

    public async ValueTask DisposeAsync()
    {
        try { await _stream.DisposeAsync().ConfigureAwait(false); }
        catch { /* best-effort */ }

        try { await _connection.CloseAsync(0).ConfigureAwait(false); }
        catch { /* best-effort */ }

        await _connection.DisposeAsync().ConfigureAwait(false);
        PeerTransportMetrics.Instance.RecordConnectionClosed(QuicPeerTransport.TransportName);
        PeerTransportMetrics.Instance.DecrementActiveConnections(QuicPeerTransport.TransportName);
    }

    private sealed class QuicPeerStreamLease : IPeerStreamLease
    {
        private readonly Func<ValueTask> _release;
        private int _disposed;

        public QuicPeerStreamLease(Stream stream, Func<ValueTask> release)
        {
            Stream = stream;
            _release = release;
        }

        public Stream Stream { get; }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                await _release().ConfigureAwait(false);
            }
        }
    }
}

[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
internal sealed class QuicPeerListener : IPeerListener
{
    private readonly IPEndPoint _endpoint;
    private QuicListener? _listener;

    public QuicPeerListener(IPEndPoint endpoint)
    {
        _endpoint = endpoint;
    }

    public IPEndPoint LocalEndPoint => _listener?.LocalEndPoint ?? _endpoint;

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is not null) return;

        if (!QuicListener.IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC listeners are not supported on this platform.");
        }

        var cert = QuicPeerTransport.LoadOrGenerateCertificate();
        var mutualTls = QuicPeerTransport.HasMutualTlsConfig;

        var options = new QuicListenerOptions
        {
            ListenEndPoint = _endpoint,
            ApplicationProtocols = [QuicPeerTransport.PeerAlpn],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                DefaultStreamErrorCode = 0x100,
                DefaultCloseErrorCode = 0x101,
                IdleTimeout = TimeSpan.FromSeconds(120),
                MaxInboundBidirectionalStreams = 128,
                MaxInboundUnidirectionalStreams = 0,
                ServerAuthenticationOptions = new SslServerAuthenticationOptions
                {
                    ApplicationProtocols = [QuicPeerTransport.PeerAlpn],
                    ServerCertificate = cert,
                    // Require a client cert when mTLS is configured so both
                    // directions of the handshake chain to the cluster CA.
                    ClientCertificateRequired = mutualTls,
                    RemoteCertificateValidationCallback = mutualTls
                        ? ValidateIncomingPeerCertificate
                        : null
                }
            })
        };

        _listener = await QuicListener.ListenAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IPeerConnection> AcceptAsync(CancellationToken cancellationToken = default)
    {
        if (_listener is null)
        {
            throw new InvalidOperationException("Listener has not been started.");
        }

        QuicConnection? connection = null;
        try
        {
            connection = await _listener.AcceptConnectionAsync(cancellationToken).ConfigureAwait(false);
            var stream = await connection.AcceptInboundStreamAsync(cancellationToken).ConfigureAwait(false);
            var peerConnection = new QuicPeerConnection(connection, stream);
            connection = null;
            return peerConnection;
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_listener is not null)
        {
            await _listener.DisposeAsync().ConfigureAwait(false);
            _listener = null;
        }
    }

    // Server-side equivalent of QuicPeerTransport.ValidatePeerCertificate.
    // Kept as an instance method so it can be passed as a delegate to
    // SslServerAuthenticationOptions.RemoteCertificateValidationCallback.
    private static bool ValidateIncomingPeerCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        if (certificate is null) return false;
        var ca = QuicPeerTransport.LoadCaCertificateForListener();
        return QuicPeerTransport.VerifyAgainstCa(certificate, ca);
    }
}
