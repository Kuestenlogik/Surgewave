namespace Kuestenlogik.Surgewave.Control.Models.TrustStore;

/// <summary>
/// View-model for the <c>/security/signer</c> diagnostic panel — returned by
/// <c>GET /api/plugins/trusted-keys</c> on the Broker.
/// </summary>
public sealed record TrustStoreStatus(
    string? TrustedKeysDir,
    bool RequireSigned,
    string ProviderName,
    IReadOnlyList<TrustedKeyInfo> Keys);

public sealed record TrustedKeyInfo(
    string Name,
    string? Fingerprint,
    DateTimeOffset LastModifiedUtc,
    long SizeBytes);

/// <summary>
/// Result of <c>POST /api/plugins/trusted-keys/generate</c>. The
/// <see cref="PrivateKeyPem"/> is only present in the response — the broker
/// never persists it. The Control UI streams it back to the operator as a
/// one-time download.
/// </summary>
public sealed record GeneratedKeyPair(
    string Name,
    string Fingerprint,
    string PublicKeyPem,
    string PrivateKeyPem);
