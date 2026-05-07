using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for DelegationTokenManager: create, renew, expire, validate, and cleanup.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DelegationTokenTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DelegationTokenConfig _config;
    private readonly DelegationTokenManager _manager;

    public DelegationTokenTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-dt-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        _config = new DelegationTokenConfig
        {
            Enabled = true,
            DefaultMaxLifetimeMs = 7 * 24 * 60 * 60 * 1000L,   // 7 days
            MaxMaxLifetimeMs     = 7 * 24 * 60 * 60 * 1000L,
            DefaultRenewalPeriodMs = 24 * 60 * 60 * 1000L        // 24 hours
        };
        _manager = new DelegationTokenManager(_config, _tempDir, NullLogger<DelegationTokenManager>.Instance);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); }
        catch { /* ignore */ }
    }

    // ── CreateToken ──────────────────────────────────────────────────────────

    [Fact]
    public void CreateToken_ReturnsUniqueTokenIds()
    {
        var t1 = _manager.CreateToken("User", "alice", null, null, null, 0);
        var t2 = _manager.CreateToken("User", "bob", null, null, null, 0);

        Assert.NotEqual(t1.TokenId, t2.TokenId);
    }

    [Fact]
    public void CreateToken_HmacIsNotEmpty()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);

        Assert.NotNull(token.Hmac);
        Assert.NotEmpty(token.Hmac);
    }

    [Fact]
    public void CreateToken_OwnerFieldsAreSet()
    {
        var token = _manager.CreateToken("User", "alice", "ServiceAccount", "deployer", null, 0);

        Assert.Equal("User", token.OwnerPrincipalType);
        Assert.Equal("alice", token.OwnerPrincipalName);
        Assert.Equal("ServiceAccount", token.RequesterPrincipalType);
        Assert.Equal("deployer", token.RequesterPrincipalName);
    }

    [Fact]
    public void CreateToken_RequesterDefaultsToOwner_WhenNull()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);

        Assert.Equal("User", token.RequesterPrincipalType);
        Assert.Equal("alice", token.RequesterPrincipalName);
    }

    [Fact]
    public void CreateToken_MaxLifetimeCappedByConfig()
    {
        // Request an excessive max lifetime – should be capped
        var token = _manager.CreateToken("User", "alice", null, null, null,
            maxLifetimeMs: 999 * 24 * 60 * 60 * 1000L); // 999 days

        var maxAllowed = _config.MaxMaxLifetimeMs;
        Assert.True(token.MaxTimestampMs <= token.IssueTimestampMs + maxAllowed + 1000,
            "MaxTimestamp should be capped at MaxMaxLifetimeMs");
    }

    [Fact]
    public void CreateToken_DefaultMaxLifetime_UsedWhenZeroOrNegative()
    {
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var token = _manager.CreateToken("User", "alice", null, null, null, maxLifetimeMs: 0);
        var after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var expectedMax = before + _config.DefaultMaxLifetimeMs;
        Assert.True(token.MaxTimestampMs >= expectedMax - 1000);
        Assert.True(token.MaxTimestampMs <= after + _config.DefaultMaxLifetimeMs + 1000);
    }

    [Fact]
    public void CreateToken_ExpiryDoesNotExceedMaxTimestamp()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);
        Assert.True(token.ExpiryTimestampMs <= token.MaxTimestampMs);
    }

    [Fact]
    public void CreateToken_Renewers_StoredCorrectly()
    {
        var renewers = new List<TokenRenewer>
        {
            new() { PrincipalType = "User", PrincipalName = "admin" }
        };
        var token = _manager.CreateToken("User", "alice", null, null, renewers, 0);

        Assert.Single(token.Renewers);
        Assert.Equal("admin", token.Renewers[0].PrincipalName);
    }

    // ── GetToken ─────────────────────────────────────────────────────────────

    [Fact]
    public void GetToken_ExistingId_ReturnsToken()
    {
        var created = _manager.CreateToken("User", "alice", null, null, null, 0);
        var retrieved = _manager.GetToken(created.TokenId);

        Assert.NotNull(retrieved);
        Assert.Equal(created.TokenId, retrieved.TokenId);
    }

    [Fact]
    public void GetToken_UnknownId_ReturnsNull()
    {
        var result = _manager.GetToken("nonexistent-token-id");
        Assert.Null(result);
    }

    // ── ValidateToken ─────────────────────────────────────────────────────────

    [Fact]
    public void ValidateToken_ValidHmac_ReturnsToken()
    {
        var created = _manager.CreateToken("User", "alice", null, null, null, 0);
        var validated = _manager.ValidateToken(created.Hmac);

        Assert.NotNull(validated);
        Assert.Equal(created.TokenId, validated.TokenId);
    }

    [Fact]
    public void ValidateToken_UnknownHmac_ReturnsNull()
    {
        var result = _manager.ValidateToken(new byte[32]); // all-zeros HMAC
        Assert.Null(result);
    }

    [Fact]
    public void ValidateToken_ExpiredToken_ReturnsNull()
    {
        // Create a token with a very short lifetime and expire it manually
        var token = _manager.CreateToken("User", "alice", null, null, null, maxLifetimeMs: 1000);

        // Force-expire by setting ExpiryTimestampMs in the past
        token.ExpiryTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1;

        var validated = _manager.ValidateToken(token.Hmac);
        Assert.Null(validated);
    }

    // ── RenewToken ────────────────────────────────────────────────────────────

    [Fact]
    public void RenewToken_ValidToken_ExtendsExpiry()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);

        // Reduce expiry to "soon" so renewal makes a visible difference
        token.ExpiryTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000;
        var expiryBeforeRenewal = token.ExpiryTimestampMs;

        var (renewed, error) = _manager.RenewToken(token.Hmac, renewPeriodMs: 3_600_000);

        Assert.Null(error);
        Assert.NotNull(renewed);
        Assert.True(renewed.ExpiryTimestampMs > expiryBeforeRenewal,
            "Expiry should be extended after renewal");
    }

    [Fact]
    public void RenewToken_UnknownHmac_ReturnsError()
    {
        var (token, error) = _manager.RenewToken(new byte[32], renewPeriodMs: 0);

        Assert.Null(token);
        Assert.NotNull(error);
        Assert.Contains("not found", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RenewToken_ExpiredToken_ReturnsError()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);
        // Force expiry
        token.ExpiryTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1;

        var (renewed, error) = _manager.RenewToken(token.Hmac, renewPeriodMs: 3_600_000);

        Assert.Null(renewed);
        Assert.NotNull(error);
        Assert.Contains("expired", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RenewToken_CannotExceedMaxTimestamp()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);
        var maxTs = token.MaxTimestampMs;

        // Try to renew for a very long period
        var (renewed, _) = _manager.RenewToken(token.Hmac, renewPeriodMs: 999 * 24 * 60 * 60 * 1000L);

        Assert.NotNull(renewed);
        Assert.True(renewed.ExpiryTimestampMs <= maxTs);
    }

    [Fact]
    public void RenewToken_ZeroPeriod_UsesDefaultRenewalPeriod()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);
        token.ExpiryTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000;

        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var (renewed, error) = _manager.RenewToken(token.Hmac, renewPeriodMs: 0);
        var after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Assert.Null(error);
        Assert.NotNull(renewed);
        // new expiry should be around now + DefaultRenewalPeriodMs
        var expectedMin = before + _config.DefaultRenewalPeriodMs - 5000;
        var expectedMax = Math.Min(after + _config.DefaultRenewalPeriodMs + 5000, token.MaxTimestampMs);
        Assert.True(renewed.ExpiryTimestampMs >= expectedMin);
        Assert.True(renewed.ExpiryTimestampMs <= expectedMax);
    }

    // ── ExpireToken ───────────────────────────────────────────────────────────

    [Fact]
    public void ExpireToken_NegativePeriod_ExpiresImmediately()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var (expiry, error) = _manager.ExpireToken(token.Hmac, expiryTimePeriodMs: -1);

        Assert.Null(error);
        Assert.True(expiry <= now + 1000, "Token should be expired immediately");
    }

    [Fact]
    public void ExpireToken_UnknownHmac_ReturnsError()
    {
        var (expiry, error) = _manager.ExpireToken(new byte[32], expiryTimePeriodMs: -1);

        Assert.Equal(-1, expiry);
        Assert.NotNull(error);
    }

    [Fact]
    public void ExpireToken_CannotExceedMaxTimestamp()
    {
        var token = _manager.CreateToken("User", "alice", null, null, null, 0);
        var maxTs = token.MaxTimestampMs;

        var (expiry, error) = _manager.ExpireToken(token.Hmac, expiryTimePeriodMs: 999 * 24 * 60 * 60 * 1000L);

        Assert.Null(error);
        Assert.True(expiry <= maxTs);
    }

    // ── DescribeTokens ────────────────────────────────────────────────────────

    [Fact]
    public void DescribeTokens_NoFilter_ReturnsAllValid()
    {
        _manager.CreateToken("User", "alice", null, null, null, 0);
        _manager.CreateToken("User", "bob", null, null, null, 0);

        var tokens = _manager.DescribeTokens(null);

        Assert.Equal(2, tokens.Count);
    }

    [Fact]
    public void DescribeTokens_OwnerFilter_ReturnsMatchingOnly()
    {
        _manager.CreateToken("User", "alice", null, null, null, 0);
        _manager.CreateToken("User", "bob", null, null, null, 0);

        var tokens = _manager.DescribeTokens(
        [
            new TokenOwner { PrincipalType = "User", PrincipalName = "alice" }
        ]);

        Assert.Single(tokens);
        Assert.Equal("alice", tokens[0].OwnerPrincipalName);
    }

    [Fact]
    public void DescribeTokens_ExcludesExpiredTokens()
    {
        var t1 = _manager.CreateToken("User", "alice", null, null, null, 0);
        _manager.CreateToken("User", "bob", null, null, null, 0);

        // Force-expire t1
        t1.ExpiryTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1;

        var tokens = _manager.DescribeTokens(null);

        Assert.Single(tokens); // Only bob's token
        Assert.Equal("bob", tokens[0].OwnerPrincipalName);
    }

    // ── CleanupExpiredTokens ─────────────────────────────────────────────────

    [Fact]
    public void CleanupExpiredTokens_RemovesExpiredTokens()
    {
        var t1 = _manager.CreateToken("User", "alice", null, null, null, 0);
        _manager.CreateToken("User", "bob", null, null, null, 0);

        // Expire t1
        t1.ExpiryTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1;

        var removed = _manager.CleanupExpiredTokens();

        Assert.Equal(1, removed);

        // alice's token is gone
        Assert.Null(_manager.GetToken(t1.TokenId));
    }

    [Fact]
    public void CleanupExpiredTokens_NoExpiredTokens_ReturnsZero()
    {
        _manager.CreateToken("User", "alice", null, null, null, 0);
        var removed = _manager.CleanupExpiredTokens();
        Assert.Equal(0, removed);
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    [Fact]
    public void CreateToken_PersistsToDisk()
    {
        _manager.CreateToken("User", "alice", null, null, null, 0);

        var metadataDir = Path.Combine(_tempDir, ".metadata");
        var file = Path.Combine(metadataDir, "delegation-tokens.json");
        Assert.True(File.Exists(file), "Token file should be written to disk");
    }

    [Fact]
    public void DelegationTokenConfig_Defaults()
    {
        var config = new DelegationTokenConfig();

        // Delegation tokens are an opt-in security feature — default is disabled.
        Assert.False(config.Enabled);
        Assert.Equal(7 * 24 * 60 * 60 * 1000L, config.DefaultMaxLifetimeMs);
        Assert.Equal(7 * 24 * 60 * 60 * 1000L, config.MaxMaxLifetimeMs);
        Assert.Equal(24 * 60 * 60 * 1000L, config.DefaultRenewalPeriodMs);
        Assert.Null(config.SecretKey);
    }
}
