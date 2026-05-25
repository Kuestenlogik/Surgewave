namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Naming conventions for authentication schemes in multi-IdP setups.
/// </summary>
public static class SchemeNames
{
    /// <summary>
    /// Claim type used to store the provider name in the cookie session.
    /// </summary>
    public const string ProviderClaimType = "surgewave:idp";

    /// <summary>
    /// Returns the named OIDC scheme for a given provider, e.g. "oidc-keycloak".
    /// </summary>
    public static string OidcScheme(string providerName) => $"oidc-{providerName}";
}
