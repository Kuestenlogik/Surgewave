namespace Kuestenlogik.Surgewave.Client.Security;

/// <summary>
/// PEM-encoded TLS material for client-authenticated connections.
/// The client cert + key are presented during the TLS handshake;
/// the optional CA cert pins the server's trust anchor when a private
/// PKI is in use. Mirrors the shape Bowire ships its
/// <c>__bowireMtls__</c> marker in, and the same shape Confluent.Kafka
/// exposes via <c>SslCertificatePem</c> / <c>SslKeyPem</c> / <c>SslCaPem</c>
/// — only here Surgewave's hand-rolled wire client consumes it directly
/// instead of routing through librdkafka.
/// </summary>
public sealed record SslOptions
{
    /// <summary>
    /// Client certificate as a PEM-encoded string
    /// (<c>-----BEGIN CERTIFICATE-----</c> … block). Required.
    /// </summary>
    public required string CertificatePem { get; init; }

    /// <summary>
    /// Client private key as a PEM-encoded string. Either an unencrypted
    /// PKCS#8 key (<c>-----BEGIN PRIVATE KEY-----</c>) or, when
    /// <see cref="Passphrase"/> is supplied, an encrypted PKCS#8 key
    /// (<c>-----BEGIN ENCRYPTED PRIVATE KEY-----</c>).
    /// </summary>
    public required string PrivateKeyPem { get; init; }

    /// <summary>
    /// Optional passphrase decrypting <see cref="PrivateKeyPem"/>.
    /// Empty / null means the key is in plaintext form.
    /// </summary>
    public string? Passphrase { get; init; }

    /// <summary>
    /// Optional CA certificate (PEM) used to validate the server's
    /// certificate chain when the system trust store doesn't already
    /// trust it (private dev/internal CA). When unset, the system
    /// trust store is used as-is.
    /// </summary>
    public string? CaCertificatePem { get; init; }

    /// <summary>
    /// When true, server certificate validation is skipped entirely.
    /// Use only against test environments — defeats the purpose of TLS
    /// in production.
    /// </summary>
    public bool AllowSelfSigned { get; init; }
}
