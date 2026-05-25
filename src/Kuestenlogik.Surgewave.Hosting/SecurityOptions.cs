namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Security configuration options.
/// </summary>
public sealed class SecurityOptions
{
    /// <summary>
    /// Enable TLS/SSL encryption.
    /// </summary>
    public bool EnableTls { get; set; }

    /// <summary>
    /// Path to TLS certificate file.
    /// </summary>
    public string? CertificatePath { get; set; }

    /// <summary>
    /// Path to TLS private key file.
    /// </summary>
    public string? KeyPath { get; set; }

    /// <summary>
    /// Enable SASL authentication.
    /// </summary>
    public bool EnableSasl { get; set; }

    /// <summary>
    /// SASL mechanism: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512".
    /// </summary>
    public string? SaslMechanism { get; set; }

    /// <summary>
    /// Enable ACL authorization.
    /// </summary>
    public bool EnableAcl { get; set; }
}
