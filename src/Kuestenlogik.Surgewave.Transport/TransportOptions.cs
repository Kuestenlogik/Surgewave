using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Configuration options for Surgewave transports.
/// </summary>
public sealed class TransportOptions
{
    /// <summary>
    /// The broker host to connect to.
    /// </summary>
    public required string Host { get; init; }

    /// <summary>
    /// The broker port to connect to.
    /// </summary>
    public required int Port { get; init; }

    /// <summary>
    /// Enable request pipelining for higher throughput.
    /// Default: true.
    /// </summary>
    public bool EnablePipelining { get; init; } = true;

    /// <summary>
    /// Enable compression for large payloads.
    /// Default: true.
    /// </summary>
    public bool EnableCompression { get; init; } = true;

    /// <summary>
    /// TCP send buffer size in bytes.
    /// Default: 65536 (64KB).
    /// </summary>
    public int SendBufferSize { get; init; } = 65536;

    /// <summary>
    /// TCP receive buffer size in bytes.
    /// Default: 65536 (64KB).
    /// </summary>
    public int ReceiveBufferSize { get; init; } = 65536;

    /// <summary>
    /// Custom server certificate validation callback for QUIC/TLS connections.
    /// When set, overrides the default validation logic (including TrustAllCertificates).
    /// </summary>
    public RemoteCertificateValidationCallback? CertificateValidation { get; init; }

    /// <summary>
    /// Optional client certificate to present during the TLS handshake.
    /// Used for mutual TLS with the broker.
    /// </summary>
    public X509Certificate2? ClientCertificate { get; init; }

    /// <summary>
    /// When <c>true</c>, skips server certificate validation for this connection only.
    /// Overrides the static <c>QuicTransport.TrustAllCertificates</c> for fine-grained control.
    /// </summary>
    public bool? TrustAllCertificates { get; init; }
}
