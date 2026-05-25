using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Configuration for SAML 2.0 authentication. Bound to "Auth:Saml" section.
/// </summary>
public sealed class SamlAuthConfig : IValidatableConfig
{
    /// <summary>
    /// IdP Metadata URL (e.g. ADFS Federation Metadata endpoint).
    /// </summary>
    public string IdpMetadataUrl { get; set; } = string.Empty;

    /// <summary>
    /// IdP Entity ID.
    /// </summary>
    public string IdpEntityId { get; set; } = string.Empty;

    /// <summary>
    /// Service Provider Entity ID (this application).
    /// </summary>
    public string SpEntityId { get; set; } = string.Empty;

    /// <summary>
    /// Assertion Consumer Service URL (e.g. https://surgewave.example.com/saml/acs).
    /// </summary>
    public string AssertionConsumerServiceUrl { get; set; } = string.Empty;

    /// <summary>
    /// Single Logout Service URL (optional).
    /// </summary>
    public string? SingleLogoutServiceUrl { get; set; }

    /// <summary>
    /// Path to SP certificate file for signing requests.
    /// </summary>
    public string? CertificateFile { get; set; }

    /// <summary>
    /// Password for the SP certificate.
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// SAML attribute name containing roles.
    /// </summary>
    public string RoleAttribute { get; set; } = "http://schemas.microsoft.com/ws/2008/06/identity/claims/role";

    /// <summary>
    /// Optional SAML attribute name containing groups (for group-to-role mapping).
    /// </summary>
    public string? GroupAttribute { get; set; }

    /// <summary>
    /// Whether to require signed SAML assertions.
    /// </summary>
    public bool WantAssertionsSigned { get; set; } = true;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
