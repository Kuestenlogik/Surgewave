namespace Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;

/// <summary>
/// Broker-side OAUTHBEARER configuration (KIP-936). Bound from
/// <c>Surgewave:Sasl:OAuthBearer</c>. When <see cref="Enabled"/> is false the
/// <c>OAUTHBEARER</c> mechanism is rejected during the SASL handshake, so callers
/// can drop the dependency entirely without wiring an empty validator.
/// </summary>
public sealed class OAuthBearerConfig
{
    public bool Enabled { get; init; }

    /// <summary>OIDC discovery URL (the <c>.well-known/openid-configuration</c> endpoint).</summary>
    public string? OidcAuthority { get; init; }

    /// <summary>Direct JWKS URL — used when <see cref="OidcAuthority"/> is not set.</summary>
    public string? JwksUri { get; init; }

    /// <summary>Expected <c>iss</c> claim. When set, tokens with a different issuer are rejected.</summary>
    public string? ValidIssuer { get; init; }

    /// <summary>Expected <c>aud</c> claim values. Empty array → audience validation is skipped.</summary>
    public string[] ValidAudiences { get; init; } = [];

    /// <summary>Name of the claim used as the principal's username after a successful validation.</summary>
    public string PrincipalClaim { get; init; } = "sub";

    /// <summary>
    /// How long the broker may reuse an unmodified JWKS document. Defaults to
    /// 30 minutes; a shorter value means faster propagation of IdP key rotation
    /// at the cost of more JWKS fetches.
    /// </summary>
    public TimeSpan JwksRefreshInterval { get; init; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Require HTTPS for the OIDC discovery / JWKS endpoint. Defaults to <c>true</c>
    /// — IdPs in production must always be HTTPS. Setting this to <c>false</c> is
    /// only meaningful for an in-process IdP fixture in integration tests, or for
    /// a localhost dev IdP. Production deployments must leave this untouched.
    /// </summary>
    public bool RequireHttpsMetadata { get; init; } = true;
}
