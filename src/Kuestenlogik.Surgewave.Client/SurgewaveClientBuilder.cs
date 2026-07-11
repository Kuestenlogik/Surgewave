using System.Reflection;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Transport;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Fluent builder for creating Surgewave clients with protocol selection.
/// </summary>
/// <example>
/// <code>
/// // Auto-detect protocol (tries Surgewave Native first)
/// await using var client = await SurgewaveClient.Create("localhost:9092")
///     .WithClientId("my-app")
///     .BuildAsync();
///
/// // Explicit Surgewave Native protocol
/// await using var client = await SurgewaveClient.Create("localhost:9092")
///     .UseSurgewaveProtocol()
///     .BuildAsync();
///
/// // Explicit Kafka protocol
/// await using var client = await SurgewaveClient.Create("localhost:9092")
///     .UseKafkaProtocol()
///     .BuildAsync();
/// </code>
/// </example>
public sealed class SurgewaveClientBuilder
{
    private readonly string _bootstrapServers;
    private ProtocolType _protocol = ProtocolType.Auto;
    private string? _clientId;
    private SurgewaveTransportType _transport = SurgewaveTransportType.Auto;
    private SslOptions? _ssl;
    private SaslOptions? _sasl;

    internal SurgewaveClientBuilder(string bootstrapServers)
    {
        if (string.IsNullOrWhiteSpace(bootstrapServers))
            throw new ArgumentException("Bootstrap servers are required", nameof(bootstrapServers));

        _bootstrapServers = bootstrapServers;
    }

    /// <summary>
    /// Use Surgewave Native protocol for maximum performance.
    /// Only works when connecting to a Surgewave broker.
    /// </summary>
    public SurgewaveClientBuilder UseSurgewaveProtocol()
    {
        _protocol = ProtocolType.SurgewaveNative;
        return this;
    }

    /// <summary>
    /// Use Kafka-compatible protocol.
    /// Use when connecting to real Kafka clusters or for compatibility.
    /// </summary>
    public SurgewaveClientBuilder UseKafkaProtocol()
    {
        _protocol = ProtocolType.Kafka;
        return this;
    }

    /// <summary>
    /// Auto-detect the best protocol based on broker capabilities.
    /// Tries Surgewave Native first, falls back to Kafka if not supported.
    /// This is the default behavior.
    /// </summary>
    public SurgewaveClientBuilder UseAutoDetect()
    {
        _protocol = ProtocolType.Auto;
        return this;
    }

    /// <summary>
    /// Set the client ID for identification.
    /// </summary>
    public SurgewaveClientBuilder WithClientId(string clientId)
    {
        _clientId = clientId;
        return this;
    }

    /// <summary>
    /// Set the transport type for Surgewave Native protocol.
    /// Has no effect when using Kafka protocol.
    /// </summary>
    public SurgewaveClientBuilder WithTransport(SurgewaveTransportType transport)
    {
        _transport = transport;
        return this;
    }

    /// <summary>
    /// Configure mTLS client-cert authentication. Both <paramref name="certificatePem"/>
    /// and <paramref name="privateKeyPem"/> are PEM-encoded strings; the
    /// passphrase decrypts an encrypted PKCS#8 key when present. The
    /// optional CA certificate pins the server's trust anchor (private
    /// PKI). Currently honoured only by the Kafka-protocol client; the
    /// native Surgewave client throws <see cref="NotSupportedException"/>
    /// if mTLS is requested — TLS support there is a follow-up slice.
    /// </summary>
    public SurgewaveClientBuilder WithSslPem(
        string certificatePem,
        string privateKeyPem,
        string? passphrase = null,
        string? caCertificatePem = null,
        bool allowSelfSigned = false)
    {
        _ssl = new SslOptions
        {
            CertificatePem = certificatePem,
            PrivateKeyPem = privateKeyPem,
            Passphrase = passphrase,
            CaCertificatePem = caCertificatePem,
            AllowSelfSigned = allowSelfSigned,
        };
        return this;
    }

    /// <summary>
    /// Configure SASL authentication. Sent during the Kafka SASL
    /// handshake (<c>SaslHandshakeRequest</c> + <c>SaslAuthenticateRequest</c>)
    /// before any produce/fetch frame. <see cref="SaslMechanism.Plain"/>
    /// is implemented today; SCRAM and OAUTHBEARER are reserved for
    /// follow-up slices and throw at handshake time.
    /// </summary>
    public SurgewaveClientBuilder WithSasl(SaslMechanism mechanism, string username, string password)
    {
        _sasl = new SaslOptions
        {
            Mechanism = mechanism,
            Username = username,
            Password = password,
        };
        return this;
    }

    /// <summary>
    /// Build the client asynchronously.
    /// For Auto protocol, this will attempt to detect the best protocol.
    /// The returned client is already connected.
    /// </summary>
    public async Task<ISurgewaveClient> BuildAsync(CancellationToken cancellationToken = default)
    {
        var protocol = _protocol;

        if (protocol == ProtocolType.Auto)
        {
            // Native-first: probe the Surgewave native protocol and, if it
            // connects, reuse that connection (no second connect). The native
            // protocol has no TLS/SASL yet (see CreateNativeClient), so a secured
            // Auto build skips the probe and uses Kafka directly. Anything else
            // falls back to Kafka.
            if (_ssl is null && _sasl is null)
            {
                var native = await TryConnectNativeAsync(cancellationToken);
                if (native is not null)
                    return native;
            }
            protocol = ProtocolType.Kafka;
        }

        ISurgewaveClient client = protocol switch
        {
            ProtocolType.SurgewaveNative => CreateNativeClient(),
            ProtocolType.Kafka => CreateKafkaClient(),
            _ => throw new InvalidOperationException($"Unknown protocol: {protocol}")
        };

        await client.ConnectAsync(cancellationToken);
        return client;
    }

    // #59: the Kafka wire client (KafkaClient/KafkaConsumer/KafkaProducer/KafkaTransport)
    // lives in the optional Kuestenlogik.Surgewave.Client.Kafka assembly so native-only
    // consumers — the broker, stream/connect apps — never drag the Kafka protocol, and
    // thus Protocol.Kafka, into their binary. It is resolved lazily by reflection: present
    // ⇒ works exactly as before; absent ⇒ a clear, actionable error. Client construction
    // is a cold path, so the one-time reflection cost is irrelevant.
    private static Func<string, string?, SslOptions?, SaslOptions?, ISurgewaveClient>? _kafkaClientFactory;

    private ISurgewaveClient CreateKafkaClient()
        => (_kafkaClientFactory ??= ResolveKafkaClientFactory())(_bootstrapServers, _clientId, _ssl, _sasl);

    private static Func<string, string?, SslOptions?, SaslOptions?, ISurgewaveClient> ResolveKafkaClientFactory()
    {
        var type = Type.GetType("Kuestenlogik.Surgewave.Client.Kafka.KafkaClient, Kuestenlogik.Surgewave.Client.Kafka")
            ?? throw new InvalidOperationException(
                "The Kafka wire protocol requires the 'Kuestenlogik.Surgewave.Client.Kafka' package. " +
                "Add a reference to it, or select the native protocol with UseSurgewaveProtocol().");
        var ctor = type.GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
            binder: null,
            [typeof(string), typeof(string), typeof(SslOptions), typeof(SaslOptions)],
            modifiers: null)
            ?? throw new InvalidOperationException(
                "Incompatible 'Kuestenlogik.Surgewave.Client.Kafka': expected KafkaClient(string, string?, SslOptions?, SaslOptions?).");
        return (bs, cid, ssl, sasl) => (ISurgewaveClient)ctor.Invoke([bs, cid, ssl, sasl]);
    }

    private SurgewaveClient CreateNativeClient()
    {
        // Surgewave-native TLS / SASL aren't wired yet — bail loudly so the
        // caller gets a clear error instead of a silent unauthenticated
        // connection. Kafka-mode is the supported path for SSL/SASL today.
        if (_ssl is not null)
            throw new NotSupportedException(
                "TLS for the Surgewave-native protocol is not implemented yet. Use UseKafkaProtocol() for TLS-secured connections.");
        if (_sasl is not null)
            throw new NotSupportedException(
                "SASL for the Surgewave-native protocol is not implemented yet. Use UseKafkaProtocol() for SASL-authenticated connections.");
        return new SurgewaveClient(_bootstrapServers, _clientId, _transport);
    }

    /// <summary>
    /// Build the client synchronously.
    /// For Auto protocol, this will block on protocol detection.
    /// </summary>
    /// <remarks>
    /// The caller is responsible for disposing the returned client.
    /// </remarks>
#pragma warning disable CA2000 // Dispose objects before losing scope - caller owns the result
    public ISurgewaveClient Build()
        => BuildAsync().GetAwaiter().GetResult();
#pragma warning restore CA2000

    /// <summary>
    /// Probe the Surgewave native protocol and return the CONNECTED client for
    /// reuse, or <c>null</c> if native isn't reachable (the caller then falls back
    /// to Kafka). Bounded by a short detection timeout — and that token is actually
    /// passed to <c>ConnectAsync</c>, so a hung native connect can't stall Auto
    /// selection. The probe client is the same one Auto would return, so a
    /// successful probe avoids a second connect.
    /// </summary>
    // CA2000: on success the connected client is RETURNED and owned by the caller
    // (same "caller owns the result" pattern as Build/BuildAsync); every failure
    // path below disposes the probe. The analyzer can't prove the ownership
    // transfer through the try/catch, so suppress it here.
#pragma warning disable CA2000
    private async Task<ISurgewaveClient?> TryConnectNativeAsync(CancellationToken cancellationToken)
    {
        var client = new SurgewaveClient(_bootstrapServers, _clientId, _transport);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(5)); // Quick timeout for detection
            await client.ConnectAsync(cts.Token);
            return client; // caller owns the connected client
        }
        catch when (!cancellationToken.IsCancellationRequested)
        {
            // A failed / timed-out native probe just means "fall back to Kafka".
            await client.DisposeAsync();
            return null;
        }
        catch
        {
            // Real caller cancellation: dispose the probe and propagate.
            await client.DisposeAsync();
            throw;
        }
    }
#pragma warning restore CA2000
}
