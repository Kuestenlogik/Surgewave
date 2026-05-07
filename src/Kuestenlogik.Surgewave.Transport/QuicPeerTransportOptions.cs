using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Per-instance configuration for QUIC peer transport connections.
/// When set on a <c>QuicPeerTransport</c> instance, these options override
/// the static properties for that instance only — enabling fine-grained
/// trust pinning per connection.
/// </summary>
public sealed class QuicPeerTransportOptions
{
    /// <summary>
    /// Path to this broker's own PKCS#12 (.pfx) certificate.
    /// Overrides <c>QuicPeerTransport.BrokerCertificatePath</c> for this instance.
    /// </summary>
    public string? BrokerCertificatePath { get; init; }

    /// <summary>Password for <see cref="BrokerCertificatePath"/>.</summary>
    public string? BrokerCertificatePassword { get; init; }

    /// <summary>
    /// Path to the trusted CA certificate for peer validation.
    /// Overrides <c>QuicPeerTransport.CaCertificatePath</c> for this instance.
    /// </summary>
    public string? CaCertificatePath { get; init; }

    /// <summary>
    /// When <c>true</c>, skips certificate validation for this instance.
    /// Overrides the static <c>QuicPeerTransport.TrustAllCertificates</c>.
    /// </summary>
    public bool? TrustAllCertificates { get; init; }

    /// <summary>
    /// Custom certificate validation callback. When set, replaces all built-in
    /// validation logic (mTLS, TrustAll, system store) for this instance.
    /// </summary>
    public RemoteCertificateValidationCallback? CertificateValidation { get; init; }
}
