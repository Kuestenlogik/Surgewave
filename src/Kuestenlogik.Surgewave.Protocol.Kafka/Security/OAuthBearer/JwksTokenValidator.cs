using System.Security.Claims;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;

/// <summary>
/// JWT-bearer token validator backed by a remote JWKS endpoint. Supports both
/// modes that operators commonly mix:
/// <list type="bullet">
///   <item>full OIDC discovery via <see cref="OAuthBearerConfig.OidcAuthority"/> — the validator pulls
///         the JWKS URI, issuer, and signing keys from the well-known document; or</item>
///   <item>direct JWKS via <see cref="OAuthBearerConfig.JwksUri"/> when the IdP doesn't expose discovery.</item>
/// </list>
/// JWKS material is cached for <see cref="OAuthBearerConfig.JwksRefreshInterval"/> so a token
/// flood doesn't hammer the IdP.
/// </summary>
public sealed class JwksTokenValidator : IOAuthBearerTokenValidator
{
    private readonly OAuthBearerConfig _config;
    private readonly ILogger<JwksTokenValidator> _logger;
    private readonly Lazy<ConfigurationManager<OpenIdConnectConfiguration>>? _oidcManager;
    private readonly HttpClient? _jwksHttpClient;
    private readonly SemaphoreSlim _jwksLock = new(1, 1);
    private JsonWebKeySet? _cachedJwks;
    private DateTime _jwksCacheExpiry = DateTime.MinValue;
    private readonly JsonWebTokenHandler _handler = new();

    public JwksTokenValidator(OAuthBearerConfig config, ILogger<JwksTokenValidator> logger, HttpClient httpClient)
    {
        _config = config;
        _logger = logger;

        if (!string.IsNullOrEmpty(config.OidcAuthority))
        {
            var metadataAddress = config.OidcAuthority.TrimEnd('/') + "/.well-known/openid-configuration";
            // Microsoft.IdentityModel's HttpDocumentRetriever defaults RequireHttps=true;
            // for in-process IdP fixtures and local dev with http://localhost we honour
            // the operator's explicit RequireHttpsMetadata=false setting.
            var docRetriever = new HttpDocumentRetriever(httpClient) { RequireHttps = config.RequireHttpsMetadata };

            _oidcManager = new Lazy<ConfigurationManager<OpenIdConnectConfiguration>>(
                () => new ConfigurationManager<OpenIdConnectConfiguration>(
                    metadataAddress,
                    new OpenIdConnectConfigurationRetriever(),
                    docRetriever)
                {
                    AutomaticRefreshInterval = config.JwksRefreshInterval,
                });
        }
        else if (!string.IsNullOrEmpty(config.JwksUri))
        {
            // OpenIdConnectConfigurationRetriever does NOT parse a raw JWKS — it
            // expects a full discovery document — so for the direct-JWKS path we
            // fetch the document ourselves and cache it for JwksRefreshInterval.
            _jwksHttpClient = httpClient;
        }
        else
        {
            throw new InvalidOperationException(
                "JwksTokenValidator requires either Surgewave:Security:OAuthBearer:OidcAuthority or :JwksUri to be set.");
        }
    }

    public async Task<OAuthBearerValidationResult> ValidateAsync(string token, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(token))
        {
            return OAuthBearerValidationResult.Failure("Empty token");
        }

        IEnumerable<SecurityKey> signingKeys;
        string? discoveredIssuer = null;
        try
        {
            if (_oidcManager is not null)
            {
                var discovery = await _oidcManager.Value.GetConfigurationAsync(cancellationToken).ConfigureAwait(false);
                signingKeys = discovery.SigningKeys;
                discoveredIssuer = discovery.Issuer;
            }
            else
            {
                signingKeys = await GetCachedJwksKeysAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "OAUTHBEARER: failed to refresh JWKS / discovery");
            return OAuthBearerValidationResult.Failure("JWKS unavailable");
        }

        var validationParameters = new TokenValidationParameters
        {
            ValidateIssuerSigningKey = true,
            IssuerSigningKeys = signingKeys,
            ValidateIssuer = !string.IsNullOrEmpty(_config.ValidIssuer),
            ValidIssuer = _config.ValidIssuer ?? discoveredIssuer,
            ValidateAudience = _config.ValidAudiences.Length > 0,
            ValidAudiences = _config.ValidAudiences,
            ValidateLifetime = true,
            // Allow modest clock skew so a freshly-minted token isn't rejected on race.
            ClockSkew = TimeSpan.FromMinutes(2),
        };

        var validation = await _handler.ValidateTokenAsync(token, validationParameters).ConfigureAwait(false);
        if (!validation.IsValid)
        {
            // The exception carries the actual reason (signature mismatch, expired, etc.); we
            // swallow it from the wire so a bad client can't fingerprint our IdP setup.
            _logger.LogDebug(validation.Exception, "OAUTHBEARER: token validation failed");
            return OAuthBearerValidationResult.Failure(validation.Exception?.Message ?? "Invalid token");
        }

        var principal = new ClaimsPrincipal(validation.ClaimsIdentity);
        var expiresAt = ExtractExpiry(principal);
        return OAuthBearerValidationResult.Success(principal, expiresAt);
    }

    private async Task<IEnumerable<SecurityKey>> GetCachedJwksKeysAsync(CancellationToken cancellationToken)
    {
        await _jwksLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cachedJwks is not null && DateTime.UtcNow < _jwksCacheExpiry)
            {
                return _cachedJwks.GetSigningKeys();
            }

            if (_config.RequireHttpsMetadata
                && !_config.JwksUri!.StartsWith("https:", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    "RequireHttpsMetadata is true but JwksUri is not HTTPS — refusing to fetch.");
            }

            var json = await _jwksHttpClient!.GetStringAsync(new Uri(_config.JwksUri!), cancellationToken)
                .ConfigureAwait(false);
            _cachedJwks = new JsonWebKeySet(json);
            _jwksCacheExpiry = DateTime.UtcNow.Add(_config.JwksRefreshInterval);
            return _cachedJwks.GetSigningKeys();
        }
        finally
        {
            _jwksLock.Release();
        }
    }

    private static DateTimeOffset ExtractExpiry(ClaimsPrincipal principal)
    {
        var expClaim = principal.FindFirst("exp")?.Value;
        if (expClaim is not null && long.TryParse(expClaim, out var unix))
        {
            return DateTimeOffset.FromUnixTimeSeconds(unix);
        }
        // Fall back to 1h if the token has no exp (RFC 7519 says it's optional).
        return DateTimeOffset.UtcNow + TimeSpan.FromHours(1);
    }
}
