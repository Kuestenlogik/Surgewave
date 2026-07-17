using System.Collections.Frozen;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Cached <see cref="ApiKey"/> → name strings for per-request metrics labels.
/// Avoids one <c>Enum.ToString()</c> allocation per request on the metrics tap.
/// </summary>
internal static class ApiKeyName
{
    private static readonly FrozenDictionary<ApiKey, string> s_names =
        Enum.GetValues<ApiKey>().Distinct().ToFrozenDictionary(static k => k, static k => k.ToString());

    /// <summary>Returns the cached name, falling back to <c>ToString()</c> for undefined values.</summary>
    public static string Of(ApiKey apiKey)
        => s_names.TryGetValue(apiKey, out var name) ? name : apiKey.ToString();
}
