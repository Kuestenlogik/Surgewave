using System.Text;

namespace Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;

/// <summary>
/// Decodes the SASL/OAUTHBEARER client first message (RFC 7628) and dispatches the
/// extracted bearer token to the configured <see cref="IOAuthBearerTokenValidator"/>.
/// The wire format is a GS2-style framing:
/// <code>
/// n,a=username,\x01auth=Bearer &lt;token&gt;\x01\x01
/// </code>
/// where <c>\x01</c> is a literal SOH byte. The username is advisory; the broker
/// trusts the principal claim from the validated token instead.
/// </summary>
public sealed class OAuthBearerAuthenticator(IOAuthBearerTokenValidator validator, OAuthBearerConfig config)
{
    /// <summary>
    /// Authenticate a single SASL/OAUTHBEARER step. Returns the authenticated
    /// principal name on success; <see cref="SaslAuthenticationResult.Failed"/> with
    /// a generic reason otherwise — the detailed failure reason is logged inside
    /// the validator, never sent on the wire.
    /// </summary>
    public async Task<(SaslAuthenticationResult Result, DateTimeOffset? ExpiresAt)> AuthenticateAsync(
        byte[] authBytes,
        CancellationToken cancellationToken)
    {
        if (!TryExtractToken(authBytes, out var token))
        {
            return (SaslAuthenticationResult.Failed("Malformed OAUTHBEARER frame"), null);
        }

        var result = await validator.ValidateAsync(token, cancellationToken).ConfigureAwait(false);
        if (!result.IsValid)
        {
            return (SaslAuthenticationResult.Failed("Invalid OAUTHBEARER token"), null);
        }

        var principalName = result.Principal!.FindFirst(config.PrincipalClaim)?.Value
            ?? result.Principal.Identity?.Name
            ?? "oauthbearer-anonymous";

        return (SaslAuthenticationResult.Success(principalName), result.ExpiresAt);
    }

    /// <summary>
    /// Extracts the bearer token from a SASL/OAUTHBEARER client first message.
    /// Returns false on any malformed input rather than throwing — auth failures
    /// must not crash the connection handler.
    /// </summary>
    internal static bool TryExtractToken(byte[] authBytes, out string token)
    {
        token = string.Empty;
        if (authBytes is null || authBytes.Length == 0) return false;

        var text = Encoding.UTF8.GetString(authBytes);

        // SOH-delimited key=value pairs follow the GS2 header.
        var parts = text.Split('\x01', StringSplitOptions.RemoveEmptyEntries);
        foreach (var part in parts)
        {
            if (part.StartsWith("auth=Bearer ", StringComparison.Ordinal))
            {
                token = part["auth=Bearer ".Length..].Trim();
                return token.Length > 0;
            }
        }
        return false;
    }
}
