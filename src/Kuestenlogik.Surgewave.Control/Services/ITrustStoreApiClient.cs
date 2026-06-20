using Kuestenlogik.Surgewave.Control.Models.TrustStore;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Thin HTTP client over the Broker's <c>/api/plugins/trusted-keys</c>
/// surface. Operations target the file-system trust store of
/// <c>BuiltinEcdsaSigner</c>, not the marketplace's publisher trust store.
/// </summary>
public interface ITrustStoreApiClient
{
    Task<TrustStoreStatus> GetStatusAsync(CancellationToken ct = default);

    Task<TrustedKeyInfo> UploadAsync(string keyName, Stream pemContent, CancellationToken ct = default);

    Task DeleteAsync(string keyName, CancellationToken ct = default);

    Task<GeneratedKeyPair> GenerateAsync(string keyName, CancellationToken ct = default);
}
