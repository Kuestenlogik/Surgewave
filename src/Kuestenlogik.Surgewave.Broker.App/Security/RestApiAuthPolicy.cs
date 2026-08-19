using Microsoft.AspNetCore.Http;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Pure authorization decision for the broker's HTTP surface, extracted from the
/// middleware so it is unit-testable. Default-deny: every path is protected
/// except the anonymous allowlist, so gRPC (/v3, raw gRPC), Schema Registry
/// (/subjects, …), /bowire and REST (/admin, /api) are all covered by
/// construction. Protected paths require authentication; mutating REST methods
/// additionally require the configured role. gRPC calls (all POST) require
/// authentication only, since the HTTP method cannot distinguish read from
/// write.
/// </summary>
public sealed class RestApiAuthPolicy
{
    private readonly RestApiAuthConfig _config;

    public RestApiAuthPolicy(RestApiAuthConfig config) => _config = config;

    public RestApiAuthDecision Evaluate(string path, string method, bool isGrpc, bool isAuthenticated, bool isInRequiredRole)
    {
        if (!IsProtected(path))
            return RestApiAuthDecision.Allow;

        if (!isAuthenticated)
            return RestApiAuthDecision.Unauthenticated;

        // gRPC is always POST; role granularity by method is impossible, so a
        // valid token is the bar (Control performs fine-grained authz).
        if (isGrpc)
            return RestApiAuthDecision.Allow;

        if (IsMutating(method) && !isInRequiredRole)
            return RestApiAuthDecision.Forbidden;

        return RestApiAuthDecision.Allow;
    }

    /// <summary>Everything is protected except the explicit anonymous allowlist (default-deny).</summary>
    public bool IsProtected(string path) =>
        !_config.AnonymousPathPrefixes.Any(prefix => MatchesPrefix(path, prefix));

    private static bool IsMutating(string method) =>
        HttpMethods.IsPost(method) || HttpMethods.IsPut(method) ||
        HttpMethods.IsDelete(method) || HttpMethods.IsPatch(method);

    // Segment-boundary match so "/health" matches "/health" and "/health/ready"
    // but not "/health-internal".
    private static bool MatchesPrefix(string path, string prefix)
    {
        if (!path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return false;
        return path.Length == prefix.Length || path[prefix.Length] == '/';
    }
}
