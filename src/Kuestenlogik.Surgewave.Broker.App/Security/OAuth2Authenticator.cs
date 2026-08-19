using System.Security.Claims;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Result of OAuth2 token validation.
/// </summary>
public sealed class OAuth2ValidationResult
{
    /// <summary>
    /// Whether the token is valid.
    /// </summary>
    public bool IsValid { get; init; }

    /// <summary>
    /// The principal (username) extracted from the token.
    /// </summary>
    public string? Principal { get; init; }

    /// <summary>
    /// Groups/roles extracted from the token.
    /// </summary>
    public IReadOnlyList<string> Groups { get; init; } = [];

    /// <summary>
    /// All claims from the token.
    /// </summary>
    public IReadOnlyDictionary<string, string> Claims { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Error message if validation failed.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// Token expiration time.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    public static OAuth2ValidationResult Success(
        string principal,
        IReadOnlyList<string> groups,
        IReadOnlyDictionary<string, string> claims,
        DateTimeOffset? expiresAt) => new()
    {
        IsValid = true,
        Principal = principal,
        Groups = groups,
        Claims = claims,
        ExpiresAt = expiresAt
    };

    public static OAuth2ValidationResult Failure(string error) => new()
    {
        IsValid = false,
        Error = error
    };
}

/// <summary>
/// OAuth2/OIDC authenticator for JWT token validation.
/// Supports JWKS-based key discovery and claims extraction.
/// </summary>
public sealed class OAuth2Authenticator : IDisposable
{
    private readonly OAuth2Config _config;
    private readonly ILogger<OAuth2Authenticator>? _logger;
    private readonly JsonWebTokenHandler _tokenHandler;
    private readonly ConfigurationManager<OpenIdConnectConfiguration>? _configManager;
    private readonly SemaphoreSlim _keyLock = new(1, 1);
    private JsonWebKeySet? _cachedJwks;
    private DateTime _jwksCacheExpiry = DateTime.MinValue;
    private bool _disposed;

    /// <summary>
    /// Whether OAuth2 authentication is enabled and configured.
    /// </summary>
    public bool IsEnabled => _config.Enabled && !string.IsNullOrEmpty(_config.Issuer);

    public OAuth2Authenticator(OAuth2Config config, ILogger<OAuth2Authenticator>? logger = null)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _logger = logger;
        _tokenHandler = new JsonWebTokenHandler();

        if (IsEnabled)
        {
            var metadataAddress = _config.JwksUri ?? $"{_config.Issuer?.TrimEnd('/')}/.well-known/openid-configuration";

            _configManager = new ConfigurationManager<OpenIdConnectConfiguration>(
                metadataAddress,
                new OpenIdConnectConfigurationRetriever(),
                new HttpDocumentRetriever { RequireHttps = _config.RequireHttpsMetadata })
            {
                AutomaticRefreshInterval = _config.JwksCacheDuration
            };

            _logger?.LogInformation("OAuth2 authenticator initialized with issuer: {Issuer}", _config.Issuer);
        }
    }

    /// <summary>
    /// Validate a JWT token and extract claims.
    /// </summary>
    public async Task<OAuth2ValidationResult> ValidateTokenAsync(
        string token,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
        {
            return OAuth2ValidationResult.Failure("OAuth2 authentication is not enabled");
        }

        if (string.IsNullOrEmpty(token))
        {
            return OAuth2ValidationResult.Failure("Token is empty");
        }

        // Remove "Bearer " prefix if present
        if (token.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            token = token[7..];
        }

        try
        {
            var keys = await GetSigningKeysAsync(cancellationToken);
            if (keys == null || keys.Count == 0)
            {
                return OAuth2ValidationResult.Failure("No signing keys available");
            }

            var validationParameters = new TokenValidationParameters
            {
                ValidIssuer = _config.Issuer,
                ValidAudience = _config.Audience,
                ValidateIssuer = !string.IsNullOrEmpty(_config.Issuer),
                ValidateAudience = !string.IsNullOrEmpty(_config.Audience),
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                IssuerSigningKeys = keys,
                ClockSkew = _config.ClockSkew,
                ValidAlgorithms = _config.AllowedAlgorithms
            };

            var result = await _tokenHandler.ValidateTokenAsync(token, validationParameters);

            if (!result.IsValid)
            {
                var errorMessage = result.Exception?.Message ?? "Token validation failed";
                _logger?.LogDebug("Token validation failed: {Error}", errorMessage);
                return OAuth2ValidationResult.Failure(errorMessage);
            }

            // Extract claims
            var claimsIdentity = result.ClaimsIdentity;
            var claims = new Dictionary<string, string>();

            foreach (var claim in claimsIdentity.Claims)
            {
                claims[claim.Type] = claim.Value;
            }

            // Extract principal
            var principal = GetClaimValue(claimsIdentity, _config.UsernameClaim)
                ?? GetClaimValue(claimsIdentity, "sub")
                ?? "unknown";

            // Extract groups
            var groups = GetGroupClaims(claimsIdentity, _config.GroupsClaim);

            // Get expiration
            DateTimeOffset? expiresAt = null;
            if (result.SecurityToken is JsonWebToken jwt)
            {
                expiresAt = jwt.ValidTo;
            }

            _logger?.LogDebug("Token validated successfully for principal: {Principal}", principal);

            return OAuth2ValidationResult.Success(
                $"User:{principal}",
                groups,
                claims,
                expiresAt);
        }
        catch (SecurityTokenException ex)
        {
            _logger?.LogDebug(ex, "Security token exception during validation");
            return OAuth2ValidationResult.Failure(ex.Message);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Unexpected error during token validation");
            return OAuth2ValidationResult.Failure("Token validation error");
        }
    }

    /// <summary>
    /// Validate a token synchronously (blocks).
    /// </summary>
    public OAuth2ValidationResult ValidateToken(string token)
    {
        return ValidateTokenAsync(token).GetAwaiter().GetResult();
    }

    private async Task<ICollection<SecurityKey>?> GetSigningKeysAsync(CancellationToken cancellationToken)
    {
        if (_configManager == null)
        {
            return null;
        }

        try
        {
            var config = await _configManager.GetConfigurationAsync(cancellationToken);
            return config.SigningKeys;
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to get OIDC configuration");

            // Try direct JWKS fetch as fallback
            if (!string.IsNullOrEmpty(_config.JwksUri))
            {
                return await FetchJwksDirectlyAsync(_config.JwksUri, cancellationToken);
            }

            return null;
        }
    }

    private async Task<ICollection<SecurityKey>?> FetchJwksDirectlyAsync(
        string jwksUri,
        CancellationToken cancellationToken)
    {
        await _keyLock.WaitAsync(cancellationToken);
        try
        {
            // Check cache
            if (_cachedJwks != null && DateTime.UtcNow < _jwksCacheExpiry)
            {
                return _cachedJwks.GetSigningKeys();
            }

            using var httpClient = new HttpClient();
            var response = await httpClient.GetStringAsync(new Uri(jwksUri), cancellationToken);
            _cachedJwks = new JsonWebKeySet(response);
            _jwksCacheExpiry = DateTime.UtcNow.Add(_config.JwksCacheDuration);

            return _cachedJwks.GetSigningKeys();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to fetch JWKS from {Uri}", jwksUri);
            return _cachedJwks?.GetSigningKeys();
        }
        finally
        {
            _keyLock.Release();
        }
    }

    private static string? GetClaimValue(ClaimsIdentity identity, string claimType)
    {
        return identity.FindFirst(claimType)?.Value
            ?? identity.FindFirst(c => c.Type.EndsWith($"/{claimType}", StringComparison.OrdinalIgnoreCase))?.Value;
    }

    private static List<string> GetGroupClaims(ClaimsIdentity identity, string groupsClaim)
    {
        var groups = new List<string>();

        // Try direct claim
        var groupClaims = identity.FindAll(groupsClaim);
        foreach (var claim in groupClaims)
        {
            // Handle JSON array values
            if (claim.Value.StartsWith('['))
            {
                try
                {
                    var array = JsonSerializer.Deserialize<string[]>(claim.Value);
                    if (array != null)
                    {
                        groups.AddRange(array);
                    }
                }
                catch
                {
                    groups.Add(claim.Value);
                }
            }
            else
            {
                groups.Add(claim.Value);
            }
        }

        // Try nested realm_access/roles (Keycloak format)
        var realmAccess = identity.FindFirst("realm_access")?.Value;
        if (!string.IsNullOrEmpty(realmAccess))
        {
            try
            {
                using var doc = JsonDocument.Parse(realmAccess);
                if (doc.RootElement.TryGetProperty("roles", out var roles) && roles.ValueKind == JsonValueKind.Array)
                {
                    foreach (var role in roles.EnumerateArray())
                    {
                        var roleValue = role.GetString();
                        if (!string.IsNullOrEmpty(roleValue))
                        {
                            groups.Add(roleValue);
                        }
                    }
                }
            }
            catch
            {
                // Ignore parsing errors
            }
        }

        return groups.Distinct().ToList();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _keyLock.Dispose();
    }
}
