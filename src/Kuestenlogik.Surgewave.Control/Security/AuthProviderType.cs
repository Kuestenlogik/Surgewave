namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Supported authentication provider types for Surgewave Control UI.
/// </summary>
public enum AuthProviderType
{
    /// <summary>
    /// Generic OpenID Connect (default, e.g. Keycloak).
    /// </summary>
    Oidc,

    /// <summary>
    /// Azure AD / Microsoft Entra ID via OIDC.
    /// </summary>
    EntraId,

    /// <summary>
    /// Generic SAML 2.0 identity provider (e.g. ADFS, Shibboleth).
    /// </summary>
    Saml,

    /// <summary>
    /// Okta via OIDC.
    /// </summary>
    Okta,

    /// <summary>
    /// Google Workspace via OIDC.
    /// </summary>
    Google,

    /// <summary>
    /// LDAP/AD Bind — direct LDAP authentication without OIDC.
    /// </summary>
    Ldap
}
