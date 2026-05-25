using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Routes claims transformation to the appropriate provider-specific transformer
/// based on the <c>surgewave:idp</c> claim stored in the cookie session.
/// </summary>
public sealed class CompositeClaimsTransformation(
    IOptions<SurgewaveAuthConfig> config,
    KeycloakClaimsTransformation keycloak,
    EntraIdClaimsTransformation entraId,
    SamlClaimsTransformation saml,
    OktaClaimsTransformation okta,
    GoogleClaimsTransformation google,
    LdapClaimsTransformation ldap) : IClaimsTransformation
{
    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
    {
        if (principal.Identity is not ClaimsIdentity identity)
            return Task.FromResult(principal);

        // Read the provider name from the surgewave:idp claim injected during login
        var providerName = identity.FindFirst(SchemeNames.ProviderClaimType)?.Value;
        if (string.IsNullOrEmpty(providerName))
            return Task.FromResult(principal);

        // Find the matching provider configuration
        var providerConfig = config.Value.Providers
            .FirstOrDefault(p => string.Equals(p.Name, providerName, StringComparison.OrdinalIgnoreCase));

        if (providerConfig is null)
            return Task.FromResult(principal);

        return providerConfig.Type switch
        {
            AuthProviderType.EntraId => entraId.TransformAsync(principal, providerConfig),
            AuthProviderType.Saml => saml.TransformAsync(principal, providerConfig),
            AuthProviderType.Okta => okta.TransformAsync(principal, providerConfig),
            AuthProviderType.Google => google.TransformAsync(principal, providerConfig),
            AuthProviderType.Ldap => ldap.TransformAsync(principal, providerConfig),
            _ => keycloak.TransformAsync(principal, providerConfig)
        };
    }
}
