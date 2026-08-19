using Microsoft.AspNetCore.Http;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Decides whether a mutating request to a privileged management endpoint may
/// proceed while REST authentication is <em>disabled</em>. Pure decision logic,
/// extracted from the middleware so it is unit-testable.
/// </summary>
/// <remarks>
/// <para>
/// This is the second, much narrower gate next to <see cref="RestApiAuthPolicy"/>.
/// When <see cref="RestApiAuthConfig.Enabled"/> is true, that policy already
/// covers the whole surface default-deny and this one stands down entirely.
/// </para>
/// <para>
/// It exists because auth is opt-in while a handful of endpoints are effectively
/// privilege-granting: the trust store decides which signing keys a
/// <c>.swpkg</c> may carry, and the repository list decides where packages are
/// fetched from. An anonymous caller who can write either of those can get code
/// executed inside the broker at plugin load. Restricting those writes to
/// loopback keeps the common all-in-one deployment (broker and Control on one
/// host) working while removing the network-reachable path.
/// </para>
/// <para>
/// <strong>Reverse proxies:</strong> the decision is made on the transport peer
/// address. Behind a proxy that is itself on loopback, every request looks local
/// and this gate stops protecting anything. Such deployments must enable REST
/// auth — or configure forwarded-headers processing so the peer address is the
/// real client.
/// </para>
/// </remarks>
public sealed class PrivilegedWritePolicy
{
    private readonly RestApiAuthConfig _config;

    public PrivilegedWritePolicy(RestApiAuthConfig config) => _config = config;

    /// <summary>
    /// True when the request must be rejected with 403.
    /// </summary>
    /// <param name="path">Request path.</param>
    /// <param name="method">HTTP method.</param>
    /// <param name="isLoopback">
    /// Whether the transport peer is a loopback address. Callers must treat an
    /// unknown peer as non-loopback so the gate fails closed.
    /// </param>
    public bool ShouldBlock(string path, string method, bool isLoopback)
    {
        // The real auth gate owns the surface when it is on.
        if (_config.Enabled)
            return false;

        // Operator opted out deliberately (e.g. Control on a separate host).
        if (_config.AllowUnauthenticatedRemoteWrites)
            return false;

        if (!IsMutating(method))
            return false;

        if (!IsPrivileged(path))
            return false;

        return !isLoopback;
    }

    /// <summary>Reads are left alone — they leak configuration, not privilege.</summary>
    public static bool IsMutating(string method) =>
        HttpMethods.IsPost(method)
        || HttpMethods.IsPut(method)
        || HttpMethods.IsDelete(method)
        || HttpMethods.IsPatch(method);

    /// <summary>Matches the configured privilege-granting path prefixes, case-insensitively.</summary>
    public bool IsPrivileged(string path)
    {
        foreach (var prefix in _config.PrivilegedWritePathPrefixes)
        {
            if (string.IsNullOrEmpty(prefix))
                continue;

            if (path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
