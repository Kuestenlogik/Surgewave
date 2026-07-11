using System.Buffers;
using System.Buffers.Binary;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

namespace Kuestenlogik.Surgewave.Client.Security;

/// <summary>
/// Opens a Kafka-protocol-compatible transport against a single broker
/// address: TCP connect, optional TLS handshake, optional SASL handshake.
/// The result is a <see cref="Stream"/> that producer / consumer code
/// uses identically to a bare <see cref="NetworkStream"/>.
/// <para>
/// Surgewave's Kafka-wire client is hand-rolled (no librdkafka), so the
/// auth handshakes happen here in managed code: <see cref="SslStream"/>
/// for TLS, the existing <see cref="SaslHandshakeRequest"/> +
/// <see cref="SaslAuthenticateRequest"/> Kafka frames for SASL. PLAIN
/// is implemented in this slice; the SCRAM and OAUTHBEARER mechanisms
/// throw a clear "not yet implemented" error.
/// </para>
/// </summary>
internal static class KafkaTransport
{
    /// <summary>
    /// Connect to the first broker in <paramref name="bootstrapServers"/>,
    /// run TLS + SASL handshakes if configured, and return the wired
    /// <see cref="OpenedTransport"/>. Caller disposes via
    /// <see cref="OpenedTransport.Dispose"/>.
    /// </summary>
    public static OpenedTransport Open(
        string bootstrapServers,
        string clientId,
        SslOptions? ssl,
        SaslOptions? sasl)
    {
        var broker = BrokerAddress.ParseFirst(bootstrapServers);

        var tcp = new TcpClient();
        try
        {
            tcp.Connect(broker.Host, broker.Port);
        }
        catch
        {
            tcp.Dispose();
            throw;
        }

        Stream stream;
        SslStream? sslStream = null;
        X509Certificate2? clientCert = null;
        X509Certificate2? caCert = null;
        try
        {
            if (ssl is not null)
            {
                (sslStream, clientCert, caCert) = OpenSsl(tcp.GetStream(), broker.Host, ssl);
                stream = sslStream;
            }
            else
            {
                stream = tcp.GetStream();
            }

            if (sasl is not null)
            {
                RunSaslHandshake(stream, clientId, sasl);
            }

            return new OpenedTransport(tcp, sslStream, stream, clientCert, caCert);
        }
        catch
        {
            sslStream?.Dispose();
            clientCert?.Dispose();
            caCert?.Dispose();
            tcp.Dispose();
            throw;
        }
    }

    private static (SslStream, X509Certificate2 ClientCert, X509Certificate2? CaCert) OpenSsl(
        NetworkStream net, string host, SslOptions ssl)
    {
        X509Certificate2 clientCert = string.IsNullOrEmpty(ssl.Passphrase)
            ? X509Certificate2.CreateFromPem(ssl.CertificatePem, ssl.PrivateKeyPem)
            : X509Certificate2.CreateFromEncryptedPem(ssl.CertificatePem, ssl.PrivateKeyPem, ssl.Passphrase);

        // X509Certificate2.CreateFromPem on Windows yields an ephemeral
        // key that SslStream can't use for client auth — re-export and
        // re-import via PKCS#12 to get a persistable copy.
        var persistable = X509CertificateLoader.LoadPkcs12(clientCert.Export(X509ContentType.Pkcs12), null);
        clientCert.Dispose();
        clientCert = persistable;

        X509Certificate2? caCert = null;
        if (!string.IsNullOrEmpty(ssl.CaCertificatePem))
        {
            caCert = X509Certificate2.CreateFromPem(ssl.CaCertificatePem);
        }

        var allowSelfSigned = ssl.AllowSelfSigned;
        var capturedCa = caCert;
        var sslStream = new SslStream(net, leaveInnerStreamOpen: false,
            userCertificateValidationCallback: (_, serverCert, chain, errors) =>
            {
                if (allowSelfSigned) return true;
                if (errors == SslPolicyErrors.None) return true;

                // CA pinning — only forgive untrusted-root / partial-chain;
                // hostname mismatches and revocation failures still reject.
                if (capturedCa is null || serverCert is null || chain is null) return false;
                var unknownRootOnly = errors == SslPolicyErrors.RemoteCertificateChainErrors
                    && chain.ChainStatus.All(s =>
                        s.Status == X509ChainStatusFlags.NoError
                        || s.Status == X509ChainStatusFlags.UntrustedRoot
                        || s.Status == X509ChainStatusFlags.PartialChain);
                if (!unknownRootOnly) return false;
                using var pinned = new X509Chain();
                pinned.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                pinned.ChainPolicy.CustomTrustStore.Add(capturedCa);
                pinned.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                return pinned.Build(new X509Certificate2(serverCert));
            });

        try
        {
            sslStream.AuthenticateAsClient(new SslClientAuthenticationOptions
            {
                TargetHost = host,
                ClientCertificates = new X509CertificateCollection { clientCert },
                // SslProtocols.None lets the OS pick the best supported
                // version (TLS 1.2 / 1.3 today, future-proofed). CA5398
                // discourages hardcoded versions for exactly this reason.
                EnabledSslProtocols = SslProtocols.None,
            });
        }
        catch
        {
            sslStream.Dispose();
            clientCert.Dispose();
            caCert?.Dispose();
            throw;
        }

        return (sslStream, clientCert, caCert);
    }

    private static void RunSaslHandshake(Stream stream, string clientId, SaslOptions sasl)
    {
        var mechanism = sasl.Mechanism switch
        {
            SaslMechanism.Plain => "PLAIN",
            SaslMechanism.ScramSha256 => throw new NotSupportedException(
                "SASL SCRAM-SHA-256 is reserved for a follow-up slice. Use Plain for now."),
            SaslMechanism.ScramSha512 => throw new NotSupportedException(
                "SASL SCRAM-SHA-512 is reserved for a follow-up slice. Use Plain for now."),
            SaslMechanism.OAuthBearer => throw new NotSupportedException(
                "SASL OAUTHBEARER is reserved for a follow-up slice."),
            _ => throw new NotSupportedException($"Unknown SASL mechanism: {sasl.Mechanism}"),
        };

        // Step 1: SaslHandshakeRequest — picks the mechanism. v1 is the
        // "framed-handshake" version where the AuthenticateRequest that
        // follows is wrapped in the Kafka request envelope; v0 (legacy)
        // sent the auth bytes raw on the socket. Surgewave's broker
        // implementation expects v1, so we pin that.
        var handshake = new SaslHandshakeRequest
        {
            ApiKey = ApiKey.SaslHandshake,
            ApiVersion = 1,
            CorrelationId = 1,
            ClientId = clientId,
            Mechanism = mechanism,
        };
        WriteRequest(stream, handshake);
        var handshakeResp = ReadHandshakeResponse(stream);
        if (handshakeResp.errorCode != 0)
        {
            throw new InvalidOperationException(
                $"SASL handshake rejected mechanism '{mechanism}'. Broker advertised: {string.Join(", ", handshakeResp.enabled)}");
        }

        // Step 2: SaslAuthenticateRequest — mechanism-specific bytes.
        // PLAIN: \0 + username + \0 + password (UTF-8). Single round-trip.
        var authBytes = sasl.Mechanism switch
        {
            SaslMechanism.Plain => BuildPlainAuthBytes(sasl.Username, sasl.Password),
            _ => throw new NotSupportedException(),
        };

        var auth = new SaslAuthenticateRequest
        {
            ApiKey = ApiKey.SaslAuthenticate,
            ApiVersion = 1,
            CorrelationId = 2,
            ClientId = clientId,
            AuthBytes = authBytes,
        };
        WriteRequest(stream, auth);
        var authResp = ReadAuthenticateResponse(stream);
        if (authResp.errorCode != 0)
        {
            throw new InvalidOperationException(
                $"SASL authenticate failed: {authResp.errorMessage ?? "(no message)"} (code {authResp.errorCode})");
        }
    }

    private static byte[] BuildPlainAuthBytes(string username, string password)
    {
        // RFC 4616: authzid \0 authcid \0 password — Bowire / Kafka
        // clients leave authzid empty (the broker uses authcid as identity).
        var u = Encoding.UTF8.GetBytes(username);
        var p = Encoding.UTF8.GetBytes(password);
        var bytes = new byte[1 + u.Length + 1 + p.Length];
        bytes[0] = 0x00;
        u.CopyTo(bytes, 1);
        bytes[1 + u.Length] = 0x00;
        p.CopyTo(bytes, 2 + u.Length);
        return bytes;
    }

    private static void WriteRequest(Stream stream, KafkaRequest request)
    {
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var span = writer.WrittenSpan;
        var totalLen = 4 + span.Length;
        var buf = ArrayPool<byte>.Shared.Rent(totalLen);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(buf, span.Length);
            span.CopyTo(buf.AsSpan(4));
            stream.Write(buf, 0, totalLen);
            stream.Flush();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buf);
        }
    }

    private static (short errorCode, string[] enabled) ReadHandshakeResponse(Stream stream)
    {
        var (correlationId, body) = ReadResponse(stream);
        _ = correlationId;
        using var ms = new MemoryStream(body);
        var errorCode = ReadInt16BigEndian(ms);
        var arrayLen = ReadInt32BigEndian(ms);
        var enabled = new string[Math.Max(0, arrayLen)];
        for (var i = 0; i < enabled.Length; i++)
        {
            var strLen = ReadInt16BigEndian(ms);
            var strBytes = new byte[strLen];
            ms.ReadExactly(strBytes, 0, strLen);
            enabled[i] = Encoding.UTF8.GetString(strBytes);
        }
        return (errorCode, enabled);
    }

    private static (short errorCode, string? errorMessage) ReadAuthenticateResponse(Stream stream)
    {
        var (correlationId, body) = ReadResponse(stream);
        _ = correlationId;
        using var ms = new MemoryStream(body);
        var errorCode = ReadInt16BigEndian(ms);
        var msgLen = ReadInt16BigEndian(ms);
        string? msg = null;
        if (msgLen >= 0)
        {
            var msgBytes = new byte[msgLen];
            ms.ReadExactly(msgBytes, 0, msgLen);
            msg = Encoding.UTF8.GetString(msgBytes);
        }
        return (errorCode, msg);
    }

    private static (int correlationId, byte[] body) ReadResponse(Stream stream)
    {
        var sizeBuf = new byte[4];
        stream.ReadExactly(sizeBuf, 0, 4);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuf);
        var payload = new byte[size];
        stream.ReadExactly(payload, 0, size);
        var correlationId = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(0, 4));
        var body = new byte[size - 4];
        Buffer.BlockCopy(payload, 4, body, 0, body.Length);
        return (correlationId, body);
    }

    private static short ReadInt16BigEndian(Stream s)
    {
        Span<byte> b = stackalloc byte[2];
        s.ReadExactly(b);
        return BinaryPrimitives.ReadInt16BigEndian(b);
    }

    private static int ReadInt32BigEndian(Stream s)
    {
        Span<byte> b = stackalloc byte[4];
        s.ReadExactly(b);
        return BinaryPrimitives.ReadInt32BigEndian(b);
    }

    /// <summary>
    /// Bundle of resources the transport opened. Disposing this object
    /// disposes the stream + TCP socket + any X509 resources held for
    /// the TLS handshake. Producer/Consumer hold one of these per
    /// connection.
    /// </summary>
    public sealed class OpenedTransport : IDisposable
    {
        public TcpClient Client { get; }
        public Stream Stream { get; }
        private readonly SslStream? _sslStream;
        private readonly X509Certificate2? _clientCert;
        private readonly X509Certificate2? _caCert;
        private bool _disposed;

        internal OpenedTransport(
            TcpClient client,
            SslStream? sslStream,
            Stream stream,
            X509Certificate2? clientCert,
            X509Certificate2? caCert)
        {
            Client = client;
            _sslStream = sslStream;
            Stream = stream;
            _clientCert = clientCert;
            _caCert = caCert;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _sslStream?.Dispose();
            Client.Dispose();
            _clientCert?.Dispose();
            _caCert?.Dispose();
        }
    }
}
