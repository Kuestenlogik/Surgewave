using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for security features including OAuth2, mTLS configuration, and security config.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class SecurityTests
{
    #region OAuth2 Configuration Tests

    [Fact]
    public void OAuth2Config_DefaultValues_AreCorrect()
    {
        var config = new OAuth2Config();

        Assert.False(config.Enabled);
        Assert.Null(config.Issuer);
        Assert.Null(config.JwksUri);
        Assert.Null(config.Audience);
        Assert.Equal("preferred_username", config.UsernameClaim);
        Assert.Equal("groups", config.GroupsClaim);
        Assert.Equal(5, config.ClockSkewMinutes);
        Assert.Equal(1, config.JwksCacheHours);
        Assert.True(config.RequireHttpsMetadata);
        Assert.Contains("RS256", config.AllowedAlgorithms);
        Assert.Contains("ES256", config.AllowedAlgorithms);
    }

    [Fact]
    public void OAuth2Config_ClockSkew_ReturnsCorrectTimeSpan()
    {
        var config = new OAuth2Config { ClockSkewMinutes = 10 };

        Assert.Equal(TimeSpan.FromMinutes(10), config.ClockSkew);
    }

    [Fact]
    public void OAuth2Config_JwksCacheDuration_ReturnsCorrectTimeSpan()
    {
        var config = new OAuth2Config { JwksCacheHours = 2 };

        Assert.Equal(TimeSpan.FromHours(2), config.JwksCacheDuration);
    }

    #endregion

    #region OAuth2 Authenticator Tests

    [Fact]
    public void OAuth2Authenticator_WhenDisabled_IsEnabledReturnsFalse()
    {
        var config = new OAuth2Config { Enabled = false };
        using var authenticator = new OAuth2Authenticator(config);

        Assert.False(authenticator.IsEnabled);
    }

    [Fact]
    public void OAuth2Authenticator_WhenEnabledWithoutIssuer_IsEnabledReturnsFalse()
    {
        var config = new OAuth2Config { Enabled = true, Issuer = null };
        using var authenticator = new OAuth2Authenticator(config);

        Assert.False(authenticator.IsEnabled);
    }

    [Fact]
    public void OAuth2Authenticator_WhenEnabledWithIssuer_IsEnabledReturnsTrue()
    {
        var config = new OAuth2Config
        {
            Enabled = true,
            Issuer = "https://auth.example.com/realms/test"
        };
        using var authenticator = new OAuth2Authenticator(config);

        Assert.True(authenticator.IsEnabled);
    }

    [Fact]
    public async Task OAuth2Authenticator_WhenDisabled_ValidateReturnsFailure()
    {
        var config = new OAuth2Config { Enabled = false };
        using var authenticator = new OAuth2Authenticator(config);

        var result = await authenticator.ValidateTokenAsync("some-token");

        Assert.False(result.IsValid);
        Assert.Equal("OAuth2 authentication is not enabled", result.Error);
    }

    [Fact]
    public async Task OAuth2Authenticator_EmptyToken_ReturnsFailure()
    {
        var config = new OAuth2Config
        {
            Enabled = true,
            Issuer = "https://auth.example.com"
        };
        using var authenticator = new OAuth2Authenticator(config);

        var result = await authenticator.ValidateTokenAsync("");

        Assert.False(result.IsValid);
        Assert.Equal("Token is empty", result.Error);
    }

    [Fact]
    public async Task OAuth2Authenticator_NullToken_ReturnsFailure()
    {
        var config = new OAuth2Config
        {
            Enabled = true,
            Issuer = "https://auth.example.com"
        };
        using var authenticator = new OAuth2Authenticator(config);

        var result = await authenticator.ValidateTokenAsync(null!);

        Assert.False(result.IsValid);
        Assert.Equal("Token is empty", result.Error);
    }

    #endregion

    #region OAuth2 Validation Result Tests

    [Fact]
    public void OAuth2ValidationResult_Success_HasCorrectValues()
    {
        var groups = new List<string> { "admin", "users" };
        var claims = new Dictionary<string, string> { { "sub", "user123" } };
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1);

        var result = OAuth2ValidationResult.Success("alice", groups, claims, expiresAt);

        Assert.True(result.IsValid);
        Assert.Equal("alice", result.Principal);
        Assert.Equal(groups, result.Groups);
        Assert.Equal(claims, result.Claims);
        Assert.Equal(expiresAt, result.ExpiresAt);
        Assert.Null(result.Error);
    }

    [Fact]
    public void OAuth2ValidationResult_Failure_HasCorrectValues()
    {
        var result = OAuth2ValidationResult.Failure("Invalid signature");

        Assert.False(result.IsValid);
        Assert.Null(result.Principal);
        Assert.Empty(result.Groups);
        Assert.Empty(result.Claims);
        Assert.Null(result.ExpiresAt);
        Assert.Equal("Invalid signature", result.Error);
    }

    #endregion

    #region Security Config Tests

    [Fact]
    public void SecurityConfig_DefaultValues_AreCorrect()
    {
        var config = new SecurityConfig();

        Assert.False(config.SaslEnabled);
        Assert.False(config.TlsEnabled);
        Assert.False(config.AclEnabled);
        Assert.Null(config.CertificatePath);
        Assert.Null(config.TrustedCaCertificatePath);
        Assert.False(config.RequireClientCertificate);
        Assert.False(config.AllowAnonymous);
        Assert.Equal("TLS12", config.MinTlsVersion);
    }

    [Fact]
    public void SecurityConfig_OAuth2Config_IsInitialized()
    {
        var config = new SecurityConfig();

        Assert.NotNull(config.OAuth2);
        Assert.False(config.OAuth2.Enabled);
    }

    [Fact]
    public void SecurityConfig_CanEnablemTLS()
    {
        var config = new SecurityConfig
        {
            TlsEnabled = true,
            CertificatePath = "/path/to/cert.pfx",
            CertificatePassword = "password",
            RequireClientCertificate = true,
            TrustedCaCertificatePath = "/path/to/ca.pem"
        };

        Assert.True(config.TlsEnabled);
        Assert.True(config.RequireClientCertificate);
        Assert.Equal("/path/to/cert.pfx", config.CertificatePath);
        Assert.Equal("/path/to/ca.pem", config.TrustedCaCertificatePath);
    }

    [Fact]
    public void SecurityConfig_CanEnableACL()
    {
        var config = new SecurityConfig
        {
            AclEnabled = true,
            SuperUsers = ["User:admin"],
            AllowIfNoAclFound = false
        };

        Assert.True(config.AclEnabled);
        Assert.Contains("User:admin", config.SuperUsers);
        Assert.False(config.AllowIfNoAclFound);
    }

    [Fact]
    public void SecurityConfig_CanEnableSASL()
    {
        var config = new SecurityConfig
        {
            SaslEnabled = true,
            SaslMechanisms = ["PLAIN", "SCRAM-SHA-256"],
            Users = ["alice:password123", "bob:secret456"]
        };

        Assert.True(config.SaslEnabled);
        Assert.Equal(2, config.SaslMechanisms.Length);
        Assert.Contains("PLAIN", config.SaslMechanisms);
        Assert.Contains("SCRAM-SHA-256", config.SaslMechanisms);
        Assert.Equal(2, config.Users.Length);
    }

    #endregion

    #region ACL Pattern Type Tests

    [Fact]
    public void AclPatternType_HasExpectedValues()
    {
        Assert.Equal(0, (int)AclPatternType.Unknown);
        Assert.Equal(1, (int)AclPatternType.Any);
        Assert.Equal(2, (int)AclPatternType.Match);
        Assert.Equal(3, (int)AclPatternType.Literal);
        Assert.Equal(4, (int)AclPatternType.Prefixed);
        Assert.Equal(5, (int)AclPatternType.Suffix);
        Assert.Equal(6, (int)AclPatternType.Regex);
    }

    #endregion
}
