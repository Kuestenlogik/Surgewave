namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Configuration for authentication in Surgewave Control UI.
/// Bound to the "Auth" section in appsettings.json.
/// Supports multiple identity providers simultaneously via <see cref="Providers"/>.
/// </summary>
public sealed class SurgewaveAuthConfig
{
    public const string SectionName = "Auth";

    /// <summary>
    /// Enable authentication. When false, the UI runs without any auth (default).
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Role name for full admin access (superuser).
    /// </summary>
    public string AdminRole { get; set; } = "surgewave-admin";

    /// <summary>
    /// Configured identity providers. Each entry defines a separate IdP with its own scheme.
    /// </summary>
    public IdpProviderConfig[] Providers { get; set; } = [];

    /// <summary>
    /// True when exactly one provider is configured (auto-redirect, no login page selection needed).
    /// </summary>
    public bool IsSingleProvider => Providers.Length == 1;

    /// <summary>
    /// True when any configured provider uses SAML (requires MVC controllers).
    /// </summary>
    public bool HasSamlProvider => Providers.Any(p => p.Type == AuthProviderType.Saml);

    /// <summary>
    /// True when any configured provider uses LDAP/AD Bind (requires MVC controllers).
    /// </summary>
    public bool HasLdapProvider => Providers.Any(p => p.Type == AuthProviderType.Ldap);
}
