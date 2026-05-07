using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Per-provider configuration for a single identity provider in a multi-IdP setup.
/// </summary>
public sealed class IdpProviderConfig : IValidatableConfig
{
    /// <summary>
    /// Unique name for this provider (used in scheme names and routes).
    /// </summary>
    [Required]
    [MinLength(1)]
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Display name shown on the login page.
    /// </summary>
    public string? DisplayName { get; set; }

    /// <summary>
    /// MudBlazor icon identifier for the login button (e.g. "Microsoft", "Google").
    /// </summary>
    public string? Icon { get; set; }

    /// <summary>
    /// Authentication provider type (Oidc, EntraId, Saml, Okta, Google).
    /// </summary>
    public AuthProviderType Type { get; set; } = AuthProviderType.Oidc;

    /// <summary>
    /// OIDC Issuer URL (e.g., https://keycloak.example.com/realms/surgewave).
    /// </summary>
    public string Authority { get; set; } = string.Empty;

    /// <summary>
    /// OIDC Client ID.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// Optional Client Secret for confidential clients.
    /// </summary>
    public string? ClientSecret { get; set; }

    /// <summary>
    /// OIDC scopes to request.
    /// </summary>
    public string[] Scopes { get; set; } = ["openid", "profile", "roles"];

    /// <summary>
    /// Claim type containing roles. Keycloak uses "realm_access" by default.
    /// </summary>
    public string RoleClaimType { get; set; } = "realm_access";

    /// <summary>
    /// Require HTTPS for OIDC metadata endpoint.
    /// </summary>
    public bool RequireHttpsMetadata { get; set; } = true;

    /// <summary>
    /// OAuth2 callback path for sign-in. Auto-derived from Name if not set.
    /// </summary>
    public string? CallbackPath { get; set; }

    /// <summary>
    /// OAuth2 callback path for sign-out. Auto-derived from Name if not set.
    /// </summary>
    public string? SignedOutCallbackPath { get; set; }

    /// <summary>
    /// Azure AD / Entra ID Tenant ID (for EntraId type).
    /// </summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Group-to-role mappings in "groupObjectId=surgewave-role" format.
    /// </summary>
    public string[] GroupRoleMappings { get; set; } = [];

    /// <summary>
    /// Okta organization domain, e.g. "dev-12345.okta.com" (for Okta type).
    /// </summary>
    public string? OktaDomain { get; set; }

    /// <summary>
    /// Google Workspace hosted domain restriction, e.g. "example.com" (for Google type).
    /// </summary>
    public string? GoogleHostedDomain { get; set; }

    /// <summary>
    /// SAML 2.0 configuration (for Saml type).
    /// </summary>
    public SamlAuthConfig Saml { get; set; } = new();

    /// <summary>
    /// LDAP/AD Bind configuration (for Ldap type).
    /// </summary>
    public LdapAuthConfig Ldap { get; set; } = new();

    /// <summary>
    /// Returns the effective callback path, auto-derived from Name if not explicitly set.
    /// </summary>
    public string EffectiveCallbackPath => CallbackPath ?? $"/signin-oidc-{Name}";

    /// <summary>
    /// Returns the effective signed-out callback path, auto-derived from Name if not explicitly set.
    /// </summary>
    public string EffectiveSignedOutCallbackPath => SignedOutCallbackPath ?? $"/signout-callback-oidc-{Name}";

    /// <summary>
    /// Returns the display name, falling back to Name if not set.
    /// </summary>
    public string EffectiveDisplayName => DisplayName ?? Name;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
