using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Configuration for LDAP/AD Bind authentication.
/// Supports two modes: Direct Bind (BindDnPattern) and Search-then-Bind (ServiceAccountDn).
/// </summary>
public sealed class LdapAuthConfig : IValidatableConfig
{
    /// <summary>
    /// LDAP server hostname or IP address.
    /// </summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>
    /// LDAP server port (default: 389, LDAPS: 636).
    /// </summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 389;

    /// <summary>
    /// Use SSL/LDAPS for the connection.
    /// </summary>
    public bool UseSsl { get; set; }

    /// <summary>
    /// Use StartTLS to upgrade the connection to TLS.
    /// </summary>
    public bool StartTls { get; set; }

    /// <summary>
    /// DN pattern for Direct Bind mode. {0} is replaced with the username.
    /// Example: "uid={0},ou=users,dc=example,dc=com"
    /// </summary>
    public string? BindDnPattern { get; set; }

    /// <summary>
    /// Service account DN for Search-then-Bind mode.
    /// </summary>
    public string? ServiceAccountDn { get; set; }

    /// <summary>
    /// Service account password for Search-then-Bind mode.
    /// </summary>
    public string? ServiceAccountPassword { get; set; }

    /// <summary>
    /// Search base for finding users (e.g. "ou=users,dc=example,dc=com").
    /// </summary>
    public string? SearchBase { get; set; }

    /// <summary>
    /// LDAP search filter to locate the user. {0} is replaced with the username.
    /// Default: "(sAMAccountName={0})" for Active Directory.
    /// </summary>
    public string SearchFilter { get; set; } = "(sAMAccountName={0})";

    /// <summary>
    /// Optional separate search base for group lookups.
    /// </summary>
    public string? GroupSearchBase { get; set; }

    /// <summary>
    /// LDAP attribute containing the user's group memberships.
    /// </summary>
    public string GroupAttribute { get; set; } = "memberOf";

    /// <summary>
    /// LDAP attribute for the user's display name.
    /// </summary>
    public string DisplayNameAttribute { get; set; } = "displayName";

    /// <summary>
    /// LDAP attribute for the user's email address.
    /// </summary>
    public string EmailAttribute { get; set; } = "mail";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
