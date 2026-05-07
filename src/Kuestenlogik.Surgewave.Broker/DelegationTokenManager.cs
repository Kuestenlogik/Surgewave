using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Manages delegation tokens for authentication delegation.
/// Tokens are HMAC-signed bearer tokens that can be used for authentication.
/// </summary>
public sealed class DelegationTokenManager
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly ConcurrentDictionary<string, DelegationToken> _tokens = new();
    private readonly DelegationTokenConfig _config;
    private readonly byte[] _secretKey;
    private readonly string _persistencePath;
    private readonly ILogger<DelegationTokenManager>? _logger;
    private readonly Lock _persistenceLock = new();

    public DelegationTokenManager(DelegationTokenConfig config, string dataDirectory, ILogger<DelegationTokenManager>? logger = null)
    {
        _config = config;
        _logger = logger;
        _persistencePath = Path.Combine(dataDirectory, ".metadata", "delegation-tokens.json");

        // Initialize secret key
        if (!string.IsNullOrEmpty(config.SecretKey))
        {
            _secretKey = Convert.FromBase64String(config.SecretKey);
        }
        else
        {
            // Generate a random secret key if not configured.
            // Only warn when the feature is actually enabled — otherwise the
            // generated key is harmless filler that nothing will ever use.
            _secretKey = RandomNumberGenerator.GetBytes(32);
            if (config.Enabled)
            {
                _logger?.LogWarning("Delegation tokens are enabled but no secret key is configured. Generated a random key (issued tokens will be invalidated on broker restart). Set Surgewave:DelegationTokens:SecretKey to a stable Base64 value.");
            }
        }

        // Load persisted tokens
        LoadTokens();
    }

    /// <summary>
    /// Configuration for delegation tokens.
    /// </summary>
    public DelegationTokenConfig Config => _config;

    /// <summary>
    /// Create a new delegation token.
    /// </summary>
    public DelegationToken CreateToken(
        string ownerPrincipalType,
        string ownerPrincipalName,
        string? requesterPrincipalType,
        string? requesterPrincipalName,
        List<TokenRenewer>? renewers,
        long maxLifetimeMs)
    {
        var tokenId = Guid.NewGuid().ToString();
        var issueTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Apply default max lifetime if not specified or invalid
        var effectiveMaxLifetime = maxLifetimeMs <= 0
            ? _config.DefaultMaxLifetimeMs
            : Math.Min(maxLifetimeMs, _config.MaxMaxLifetimeMs);

        var expiryTimestamp = issueTimestamp + _config.DefaultRenewalPeriodMs;
        var maxTimestamp = issueTimestamp + effectiveMaxLifetime;

        // Ensure expiry doesn't exceed max
        expiryTimestamp = Math.Min(expiryTimestamp, maxTimestamp);

        // Generate HMAC
        var hmac = GenerateHmac(tokenId, ownerPrincipalType, ownerPrincipalName, issueTimestamp);

        var token = new DelegationToken
        {
            TokenId = tokenId,
            Hmac = hmac,
            OwnerPrincipalType = ownerPrincipalType,
            OwnerPrincipalName = ownerPrincipalName,
            RequesterPrincipalType = requesterPrincipalType ?? ownerPrincipalType,
            RequesterPrincipalName = requesterPrincipalName ?? ownerPrincipalName,
            IssueTimestampMs = issueTimestamp,
            ExpiryTimestampMs = expiryTimestamp,
            MaxTimestampMs = maxTimestamp,
            Renewers = renewers ?? []
        };

        _tokens[tokenId] = token;
        PersistTokens();

        _logger?.LogInformation("Created delegation token {TokenId} for {Owner}, expires at {Expiry}",
            tokenId, $"{ownerPrincipalType}:{ownerPrincipalName}",
            DateTimeOffset.FromUnixTimeMilliseconds(expiryTimestamp));

        return token;
    }

    /// <summary>
    /// Renew a delegation token by its HMAC.
    /// </summary>
    public (DelegationToken? Token, string? Error) RenewToken(byte[] hmac, long renewPeriodMs)
    {
        var token = FindTokenByHmac(hmac);
        if (token == null)
        {
            return (null, "Token not found");
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Check if token has already expired
        if (token.ExpiryTimestampMs < now)
        {
            return (null, "Token has expired");
        }

        // Calculate new expiry
        var effectiveRenewalPeriod = renewPeriodMs <= 0
            ? _config.DefaultRenewalPeriodMs
            : renewPeriodMs;

        var newExpiry = now + effectiveRenewalPeriod;

        // Cannot extend beyond max timestamp
        newExpiry = Math.Min(newExpiry, token.MaxTimestampMs);

        // Update token
        token.ExpiryTimestampMs = newExpiry;
        PersistTokens();

        _logger?.LogInformation("Renewed delegation token {TokenId}, new expiry at {Expiry}",
            token.TokenId, DateTimeOffset.FromUnixTimeMilliseconds(newExpiry));

        return (token, null);
    }

    /// <summary>
    /// Expire a delegation token by its HMAC.
    /// </summary>
    public (long ExpiryTimestamp, string? Error) ExpireToken(byte[] hmac, long expiryTimePeriodMs)
    {
        var token = FindTokenByHmac(hmac);
        if (token == null)
        {
            return (-1, "Token not found");
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        long newExpiry;
        if (expiryTimePeriodMs < 0)
        {
            // Negative means expire immediately
            newExpiry = now;
        }
        else
        {
            newExpiry = now + expiryTimePeriodMs;
        }

        // Cannot extend beyond max timestamp
        newExpiry = Math.Min(newExpiry, token.MaxTimestampMs);

        token.ExpiryTimestampMs = newExpiry;
        PersistTokens();

        _logger?.LogInformation("Set expiry for delegation token {TokenId} to {Expiry}",
            token.TokenId, DateTimeOffset.FromUnixTimeMilliseconds(newExpiry));

        return (newExpiry, null);
    }

    /// <summary>
    /// Describe delegation tokens, optionally filtered by owner.
    /// </summary>
    public List<DelegationToken> DescribeTokens(List<TokenOwner>? owners)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var result = new List<DelegationToken>();

        foreach (var token in _tokens.Values)
        {
            // Skip expired tokens
            if (token.ExpiryTimestampMs < now)
            {
                continue;
            }

            // If owners filter specified, check if token matches
            if (owners != null && owners.Count > 0)
            {
                var matches = owners.Any(o =>
                    o.PrincipalType == token.OwnerPrincipalType &&
                    o.PrincipalName == token.OwnerPrincipalName);

                if (!matches)
                {
                    continue;
                }
            }

            result.Add(token);
        }

        return result;
    }

    /// <summary>
    /// Validate a token HMAC and return the associated token if valid.
    /// </summary>
    public DelegationToken? ValidateToken(byte[] hmac)
    {
        var token = FindTokenByHmac(hmac);
        if (token == null)
        {
            return null;
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (token.ExpiryTimestampMs < now)
        {
            return null; // Token expired
        }

        return token;
    }

    /// <summary>
    /// Get a token by ID (for internal use).
    /// </summary>
    public DelegationToken? GetToken(string tokenId)
    {
        return _tokens.TryGetValue(tokenId, out var token) ? token : null;
    }

    /// <summary>
    /// Clean up expired tokens.
    /// </summary>
    public int CleanupExpiredTokens()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var expiredIds = _tokens
            .Where(kvp => kvp.Value.ExpiryTimestampMs < now)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var id in expiredIds)
        {
            _tokens.TryRemove(id, out _);
        }

        if (expiredIds.Count > 0)
        {
            PersistTokens();
            _logger?.LogInformation("Cleaned up {Count} expired delegation tokens", expiredIds.Count);
        }

        return expiredIds.Count;
    }

    private DelegationToken? FindTokenByHmac(byte[] hmac)
    {
        return _tokens.Values.FirstOrDefault(t => t.Hmac.SequenceEqual(hmac));
    }

    private byte[] GenerateHmac(string tokenId, string principalType, string principalName, long timestamp)
    {
        var data = $"{tokenId}:{principalType}:{principalName}:{timestamp}";
        using var hmacAlgorithm = new HMACSHA256(_secretKey);
        return hmacAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(data));
    }

    private void LoadTokens()
    {
        try
        {
            if (!File.Exists(_persistencePath))
            {
                return;
            }

            var json = File.ReadAllText(_persistencePath);
            var tokens = JsonSerializer.Deserialize<List<DelegationToken>>(json);
            if (tokens != null)
            {
                foreach (var token in tokens)
                {
                    _tokens[token.TokenId] = token;
                }
                _logger?.LogInformation("Loaded {Count} delegation tokens from storage", tokens.Count);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to load delegation tokens from storage");
        }
    }

    private void PersistTokens()
    {
        try
        {
            lock (_persistenceLock)
            {
                var directory = Path.GetDirectoryName(_persistencePath);
                if (!string.IsNullOrEmpty(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var json = JsonSerializer.Serialize(_tokens.Values.ToList(), JsonOptions);
                File.WriteAllText(_persistencePath, json);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to persist delegation tokens");
        }
    }
}

/// <summary>
/// Configuration for delegation tokens.
/// </summary>
public sealed class DelegationTokenConfig
{
    /// <summary>
    /// Whether delegation tokens are enabled. Default: false — opt-in security feature.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Base64-encoded secret key for HMAC generation.
    /// If not set, a random key is generated (tokens won't survive restarts).
    /// </summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// Default token max lifetime in milliseconds. Default: 7 days.
    /// </summary>
    public long DefaultMaxLifetimeMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Maximum allowed max lifetime in milliseconds. Default: 7 days.
    /// </summary>
    public long MaxMaxLifetimeMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Default renewal period in milliseconds. Default: 24 hours.
    /// </summary>
    public long DefaultRenewalPeriodMs { get; set; } = 24 * 60 * 60 * 1000L;
}

/// <summary>
/// Represents a delegation token.
/// </summary>
public sealed class DelegationToken
{
    public required string TokenId { get; init; }
    public required byte[] Hmac { get; init; }
    public required string OwnerPrincipalType { get; init; }
    public required string OwnerPrincipalName { get; init; }
    public required string RequesterPrincipalType { get; init; }
    public required string RequesterPrincipalName { get; init; }
    public required long IssueTimestampMs { get; init; }
    public long ExpiryTimestampMs { get; set; }
    public required long MaxTimestampMs { get; init; }
    public required List<TokenRenewer> Renewers { get; init; }
}

/// <summary>
/// Represents a principal that can renew a delegation token.
/// </summary>
public sealed class TokenRenewer
{
    public required string PrincipalType { get; init; }
    public required string PrincipalName { get; init; }
}

/// <summary>
/// Represents a token owner for filtering.
/// </summary>
public sealed class TokenOwner
{
    public required string PrincipalType { get; init; }
    public required string PrincipalName { get; init; }
}
