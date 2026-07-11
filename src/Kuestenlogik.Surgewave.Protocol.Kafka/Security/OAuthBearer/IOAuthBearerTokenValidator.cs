using System.Security.Claims;

namespace Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;

/// <summary>
/// Validates an OAuth2 bearer token presented over the SASL OAUTHBEARER mechanism
/// (KIP-936). Implementations can do offline signature verification against a JWKS
/// endpoint, online introspection, or anything in between. The result is a
/// principal with extracted claims plus an absolute lifetime so the SASL response
/// can advertise the right <c>session_lifetime_ms</c> back to the client (KIP-368).
/// </summary>
public interface IOAuthBearerTokenValidator
{
    /// <summary>
    /// Validates <paramref name="token"/>. Returns a successful result with the
    /// extracted principal and lifetime, or a failed result with a human-readable
    /// reason. Implementations MUST NOT throw on bad-token input.
    /// </summary>
    Task<OAuthBearerValidationResult> ValidateAsync(string token, CancellationToken cancellationToken);
}

/// <summary>
/// Outcome of validating an OAUTHBEARER token. <see cref="IsValid"/> separates
/// "good token, here's who you are" from "bad token, here's why".
/// </summary>
public sealed record OAuthBearerValidationResult
{
    public bool IsValid { get; init; }

    /// <summary>The authenticated principal — null when <see cref="IsValid"/> is false.</summary>
    public ClaimsPrincipal? Principal { get; init; }

    /// <summary>
    /// Time at which the token (or the local cache of it) is no longer valid. Used
    /// to fill <c>SessionLifetimeMs</c> in the SASL response so the client knows
    /// when to re-authenticate.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>Diagnostic detail when validation failed. Safe to log but NOT to send to the client.</summary>
    public string? FailureReason { get; init; }

    public static OAuthBearerValidationResult Success(ClaimsPrincipal principal, DateTimeOffset expiresAt) =>
        new() { IsValid = true, Principal = principal, ExpiresAt = expiresAt };

    public static OAuthBearerValidationResult Failure(string reason) =>
        new() { IsValid = false, FailureReason = reason };
}
