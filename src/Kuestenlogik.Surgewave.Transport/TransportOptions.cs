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
    /// How long a request frame may take to reach the peer before the connection is considered
    /// dead. Default: 30 seconds. <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> disables
    /// the deadline.
    ///
    /// <para><b>Why a deadline is needed at all.</b> An in-flight socket send cannot be cancelled —
    /// the cancellation token is only observed before a write starts. So when a peer stops draining
    /// its receive buffer, the write blocks and the caller's own timeout has no effect: it waits
    /// forever on a connection that will never make progress. Tearing the socket down is the only
    /// thing that releases such a write, which is what this deadline does (#117).</para>
    ///
    /// <para>It is a connection-level backstop, not a request timeout: a peer that has not accepted
    /// a single frame within this window is gone, whereas a slow-but-progressing peer is normal and
    /// must not be disconnected.</para>
    /// </summary>
    public TimeSpan WriteTimeout { get; init; } = TimeSpan.FromSeconds(30);

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
