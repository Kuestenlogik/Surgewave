using System.Security.Claims;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-936 OAUTHBEARER: covers the wire-format parser and the validator dispatch
/// path. The HTTP-backed JwksTokenValidator is not exercised here — that needs
/// an IdP fixture and lives in the deferred conformance suite (9f).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class OAuthBearerAuthenticatorTests
{
    private const char Soh = '';

    [Fact]
    public void TryExtractToken_ParsesStandardOauthBearerFrame()
    {
        // RFC 7628 client first message: n,a=alice,<SOH>auth=Bearer xyz<SOH><SOH>
        var frame = $"n,a=alice,{Soh}auth=Bearer xyz{Soh}{Soh}";
        var ok = OAuthBearerAuthenticator.TryExtractToken(Encoding.UTF8.GetBytes(frame), out var token);

        Assert.True(ok);
        Assert.Equal("xyz", token);
    }

    [Fact]
    public void TryExtractToken_FailsOnMalformedFrame()
    {
        var ok = OAuthBearerAuthenticator.TryExtractToken(Encoding.UTF8.GetBytes("garbage-without-auth"), out var token);
        Assert.False(ok);
        Assert.Equal(string.Empty, token);
    }

    [Fact]
    public void TryExtractToken_FailsOnEmptyInput()
    {
        Assert.False(OAuthBearerAuthenticator.TryExtractToken([], out _));
        Assert.False(OAuthBearerAuthenticator.TryExtractToken(null!, out _));
    }

    [Fact]
    public async Task AuthenticateAsync_ValidToken_ReturnsSuccessWithPrincipalClaim()
    {
        var validator = new StubValidator(success: true, principalSub: "alice@example.com");
        var config = new OAuthBearerConfig { Enabled = true, PrincipalClaim = "sub" };
        var authenticator = new OAuthBearerAuthenticator(validator, config);

        var frame = Encoding.UTF8.GetBytes($"n,,{Soh}auth=Bearer fake-token{Soh}{Soh}");
        var (result, expiresAt) = await authenticator.AuthenticateAsync(frame, CancellationToken.None);

        Assert.True(result.IsSuccess);
        Assert.Equal("alice@example.com", result.Username);
        Assert.NotNull(expiresAt);
    }

    [Fact]
    public async Task AuthenticateAsync_InvalidToken_ReturnsGenericFailure()
    {
        var validator = new StubValidator(success: false, principalSub: null);
        var config = new OAuthBearerConfig { Enabled = true };
        var authenticator = new OAuthBearerAuthenticator(validator, config);

        var frame = Encoding.UTF8.GetBytes($"n,,{Soh}auth=Bearer expired-token{Soh}{Soh}");
        var (result, expiresAt) = await authenticator.AuthenticateAsync(frame, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Null(expiresAt);
        // The wire-facing error must NOT echo the validator's internal reason.
        Assert.Equal("Invalid OAUTHBEARER token", result.ErrorMessage);
    }

    [Fact]
    public async Task AuthenticateAsync_MalformedFrame_FailsBeforeValidatorIsInvoked()
    {
        var validator = new StubValidator(success: true, principalSub: "ignored");
        var authenticator = new OAuthBearerAuthenticator(validator, new OAuthBearerConfig { Enabled = true });

        var (result, _) = await authenticator.AuthenticateAsync(Encoding.UTF8.GetBytes("not-a-frame"), CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(0, validator.Calls); // We never reached the validator.
    }

    /// <summary>Stub validator — counts invocations and returns the configured outcome.</summary>
    private sealed class StubValidator : IOAuthBearerTokenValidator
    {
        private readonly bool _success;
        private readonly string? _principalSub;
        public int Calls { get; private set; }

        public StubValidator(bool success, string? principalSub)
        {
            _success = success;
            _principalSub = principalSub;
        }

        public Task<OAuthBearerValidationResult> ValidateAsync(string token, CancellationToken cancellationToken)
        {
            Calls++;
            if (!_success) return Task.FromResult(OAuthBearerValidationResult.Failure("stub failure"));

            var identity = new ClaimsIdentity("test");
            if (_principalSub is not null) identity.AddClaim(new Claim("sub", _principalSub));
            var principal = new ClaimsPrincipal(identity);

            return Task.FromResult(OAuthBearerValidationResult.Success(principal, DateTimeOffset.UtcNow.AddHours(1)));
        }
    }
}
