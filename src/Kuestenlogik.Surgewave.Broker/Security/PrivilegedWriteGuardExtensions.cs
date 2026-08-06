using System.Net;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Wires <see cref="PrivilegedWritePolicy"/> into the pipeline: while REST auth
/// is disabled, mutating calls to the privilege-granting management endpoints
/// are accepted from loopback only.
/// </summary>
public static class PrivilegedWriteGuardExtensions
{
    /// <summary>
    /// Insert the loopback gate. No-op when REST auth is enabled (that gate is
    /// strictly stronger) or when the operator opted out explicitly.
    /// </summary>
    public static void UseSurgewavePrivilegedWriteGuard(
        this WebApplication app,
        RestApiAuthConfig config,
        ILogger logger)
    {
        if (config.Enabled)
            return;

        if (config.AllowUnauthenticatedRemoteWrites)
        {
            logger.LogWarning(
                "REST API auth is disabled and AllowUnauthenticatedRemoteWrites is set — any client that can reach " +
                "this broker's HTTP port may upload plugin signing keys and repository sources, which is equivalent " +
                "to granting code execution at plugin load. Enable Surgewave:Security:RestApiAuth:Enabled instead " +
                "unless the port is reachable only by trusted operators.");
            return;
        }

        var policy = new PrivilegedWritePolicy(config);

        app.Use(async (context, next) =>
        {
            var path = context.Request.Path.Value ?? "/";

            if (policy.ShouldBlock(path, context.Request.Method, IsLoopback(context.Connection.RemoteIpAddress)))
            {
                logger.LogWarning(
                    "Rejected unauthenticated {Method} {Path} from {RemoteIp}: privileged management writes are " +
                    "loopback-only while Surgewave:Security:RestApiAuth:Enabled is false",
                    LogSanitizer.Sanitize(context.Request.Method),
                    LogSanitizer.Sanitize(path),
                    context.Connection.RemoteIpAddress?.ToString() ?? "unknown");

                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                await context.Response.WriteAsJsonAsync(new
                {
                    error = "Privileged management writes are restricted to loopback while REST API authentication " +
                            "is disabled. Enable Surgewave:Security:RestApiAuth:Enabled, or set " +
                            "Surgewave:Security:RestApiAuth:AllowUnauthenticatedRemoteWrites if this port is only " +
                            "reachable by trusted operators.",
                });
                return;
            }

            await next();
        });
    }

    /// <summary>
    /// Loopback check over the transport peer. An unknown peer counts as remote
    /// so the gate fails closed. IPv4-mapped IPv6 (<c>::ffff:127.0.0.1</c>) is
    /// normalised first — <see cref="IPAddress.IsLoopback"/> does not recognise
    /// it on its own.
    /// </summary>
    private static bool IsLoopback(IPAddress? remote)
    {
        if (remote is null)
            return false;

        if (remote.IsIPv4MappedToIPv6)
            remote = remote.MapToIPv4();

        return IPAddress.IsLoopback(remote);
    }
}
