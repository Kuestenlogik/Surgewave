using System.DirectoryServices.Protocols;
using System.Net;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Performs LDAP authentication via Direct Bind or Search-then-Bind.
/// Returns user attributes (display name, email, groups) on success.
/// </summary>
public sealed class LdapAuthenticationService(ILogger<LdapAuthenticationService> logger)
{
    // Whitelist-Allowlist fuer LDAP-Benutzernamen: Buchstaben, Ziffern,
    // Punkt, Bindestrich, Unterstrich, At-Zeichen (fuer UPN-Form
    // user@domain). Backslash, Klammern, Sternchen und andere LDAP-
    // Metazeichen sind verboten. Max 256 Zeichen.
    private static readonly Regex ValidUsername =
        new(@"^[A-Za-z0-9._\-@]{1,256}$", RegexOptions.Compiled);


    /// <summary>
    /// Authenticates a user against the configured LDAP server.
    /// </summary>
    public async Task<LdapAuthResult> AuthenticateAsync(
        string username, string password, IdpProviderConfig config)
    {
        var ldap = config.Ldap;

        if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            return LdapAuthResult.Failure("Username and password are required.");

        // Defense in depth gegen LDAP-Injection: zusaetzlich zum
        // RFC-4515-Escaping (EscapeLdapFilterValue) verlangen wir einen
        // strikten Whitelist-Allowlist fuer den Benutzernamen. Damit
        // koennen LDAP-Filter-Metazeichen den Filter nicht mehr
        // erreichen — die `Replace`-basierten Escapes laufen nur noch
        // als Belt-and-Suspenders.
        if (!ValidUsername.IsMatch(username))
        {
            logger.LogWarning("Rejected LDAP authentication: username contains disallowed characters");
            return LdapAuthResult.Failure("Invalid username or password.");
        }

        try
        {
            if (!string.IsNullOrEmpty(ldap.BindDnPattern))
                return await DirectBindAsync(username, password, ldap);

            if (!string.IsNullOrEmpty(ldap.ServiceAccountDn))
                return await SearchThenBindAsync(username, password, ldap);

            return LdapAuthResult.Failure("LDAP configuration error: neither BindDnPattern nor ServiceAccountDn is set.");
        }
        catch (LdapException ex) when (ex.ErrorCode == 49) // InvalidCredentials
        {
            logger.LogWarning("LDAP bind failed for user {Username}: invalid credentials", LogSanitizer.Sanitize(username));
            return LdapAuthResult.Failure("Invalid username or password.");
        }
        catch (LdapException ex)
        {
            logger.LogError(ex, "LDAP error for user {Username}: {Message}", LogSanitizer.Sanitize(username), ex.Message);
            return LdapAuthResult.Failure("LDAP server error. Please try again later.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error during LDAP authentication for {Username}", LogSanitizer.Sanitize(username));
            return LdapAuthResult.Failure("Authentication service error.");
        }
    }

    private Task<LdapAuthResult> DirectBindAsync(string username, string password, LdapAuthConfig ldap)
    {
        var userDn = ldap.BindDnPattern!.Replace("{0}", EscapeLdapDnValue(username));
        logger.LogDebug("LDAP Direct Bind for DN: {UserDn}", userDn);

        using var connection = CreateConnection(ldap);
        connection.Credential = new NetworkCredential(userDn, password);
        connection.Bind();

        var attrs = ReadUserAttributes(connection, userDn, ldap);
        return Task.FromResult(LdapAuthResult.Success(userDn, attrs));
    }

    private Task<LdapAuthResult> SearchThenBindAsync(string username, string password, LdapAuthConfig ldap)
    {
        // Phase 1: Bind with service account and search for the user
        using var searchConnection = CreateConnection(ldap);
        searchConnection.Credential = new NetworkCredential(ldap.ServiceAccountDn, ldap.ServiceAccountPassword);
        searchConnection.Bind();

        var filter = ldap.SearchFilter.Replace("{0}", EscapeLdapFilterValue(username));
        var searchBase = ldap.SearchBase ?? string.Empty;

        logger.LogDebug("LDAP search for user: base={SearchBase}, filter={Filter}", searchBase, filter);

        var searchRequest = new SearchRequest(
            searchBase,
            filter,
            SearchScope.Subtree,
            ldap.DisplayNameAttribute, ldap.EmailAttribute, ldap.GroupAttribute);

        var searchResponse = (SearchResponse)searchConnection.SendRequest(searchRequest);

        if (searchResponse.Entries.Count == 0)
        {
            logger.LogWarning("LDAP user not found for username {Username}", LogSanitizer.Sanitize(username));
            return Task.FromResult(LdapAuthResult.Failure("Invalid username or password."));
        }

        var entry = searchResponse.Entries[0];
        var userDn = entry.DistinguishedName;

        // Phase 2: Re-bind with user credentials to verify password
        using var userConnection = CreateConnection(ldap);
        userConnection.Credential = new NetworkCredential(userDn, password);
        userConnection.Bind();

        var attrs = ExtractAttributes(entry, ldap);
        return Task.FromResult(LdapAuthResult.Success(userDn, attrs));
    }

    private static LdapConnection CreateConnection(LdapAuthConfig ldap)
    {
        var identifier = new LdapDirectoryIdentifier(ldap.Host, ldap.Port);
        var connection = new LdapConnection(identifier)
        {
            AuthType = AuthType.Basic,
        };

        connection.SessionOptions.ProtocolVersion = 3;

        if (ldap.UseSsl)
            connection.SessionOptions.SecureSocketLayer = true;

        if (ldap.StartTls)
            connection.SessionOptions.StartTransportLayerSecurity(null);

        return connection;
    }

    private static LdapUserAttributes ReadUserAttributes(
        LdapConnection connection, string userDn, LdapAuthConfig ldap)
    {
        var searchRequest = new SearchRequest(
            userDn,
            "(objectClass=*)",
            SearchScope.Base,
            ldap.DisplayNameAttribute, ldap.EmailAttribute, ldap.GroupAttribute);

        var response = (SearchResponse)connection.SendRequest(searchRequest);

        if (response.Entries.Count == 0)
            return new LdapUserAttributes();

        return ExtractAttributes(response.Entries[0], ldap);
    }

    private static LdapUserAttributes ExtractAttributes(SearchResultEntry entry, LdapAuthConfig ldap)
    {
        var displayName = GetFirstAttributeValue(entry, ldap.DisplayNameAttribute);
        var email = GetFirstAttributeValue(entry, ldap.EmailAttribute);
        var groups = GetAllAttributeValues(entry, ldap.GroupAttribute);

        return new LdapUserAttributes
        {
            DisplayName = displayName,
            Email = email,
            Groups = groups,
        };
    }

    private static string? GetFirstAttributeValue(SearchResultEntry entry, string attributeName)
    {
        if (!entry.Attributes.Contains(attributeName))
            return null;

        var values = entry.Attributes[attributeName];
        return values.Count > 0 ? values[0]?.ToString() : null;
    }

    private static string[] GetAllAttributeValues(SearchResultEntry entry, string attributeName)
    {
        if (!entry.Attributes.Contains(attributeName))
            return [];

        var attr = entry.Attributes[attributeName];
        var result = new string[attr.Count];
        for (var i = 0; i < attr.Count; i++)
            result[i] = attr[i]?.ToString() ?? string.Empty;

        return result;
    }

    /// <summary>
    /// Escapes special characters in DN values per RFC 4514.
    /// </summary>
    private static string EscapeLdapDnValue(string value)
    {
        return value
            .Replace("\\", "\\\\")
            .Replace(",", "\\,")
            .Replace("+", "\\+")
            .Replace("\"", "\\\"")
            .Replace("<", "\\<")
            .Replace(">", "\\>")
            .Replace(";", "\\;");
    }

    /// <summary>
    /// Escapes special characters in LDAP filter values per RFC 4515.
    /// </summary>
    private static string EscapeLdapFilterValue(string value)
    {
        return value
            .Replace("\\", "\\5c")
            .Replace("*", "\\2a")
            .Replace("(", "\\28")
            .Replace(")", "\\29")
            .Replace("\0", "\\00");
    }
}

/// <summary>
/// Result of an LDAP authentication attempt.
/// </summary>
public sealed class LdapAuthResult
{
    public bool Succeeded { get; private init; }
    public string? UserDn { get; private init; }
    public string? ErrorMessage { get; private init; }
    public LdapUserAttributes Attributes { get; private init; } = new();

    public static LdapAuthResult Success(string userDn, LdapUserAttributes attributes) =>
        new() { Succeeded = true, UserDn = userDn, Attributes = attributes };

    public static LdapAuthResult Failure(string error) =>
        new() { Succeeded = false, ErrorMessage = error };
}

/// <summary>
/// User attributes retrieved from LDAP after successful authentication.
/// </summary>
public sealed class LdapUserAttributes
{
    public string? DisplayName { get; init; }
    public string? Email { get; init; }
    public string[] Groups { get; init; } = [];
}
