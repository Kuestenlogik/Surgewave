using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Direct probe: bypasses the SASL wire and calls JwksTokenValidator against the
/// fixture's in-process IdP. When the e2e test fails, this isolates whether the
/// problem is the validator (token format, JWKS fetch, validation parameters) or
/// the SASL plumbing.
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection("OAuthBearerBroker")]
public sealed class OAuthBearerValidatorProbeTests
{
    private readonly OAuthBearerBrokerFixture _fixture;
    public OAuthBearerValidatorProbeTests(OAuthBearerBrokerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task IdpEndpointsAreReachable()
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var discovery = await http.GetStringAsync(_fixture.OidcAuthority + "/.well-known/openid-configuration");
        Assert.Contains("\"jwks_uri\"", discovery);
        var jwks = await http.GetStringAsync(_fixture.JwksUri);
        Assert.Contains("\"keys\"", jwks);
    }

    [Fact]
    public async Task ValidatorAcceptsFreshlyMintedToken()
    {
        var config = new OAuthBearerConfig
        {
            Enabled = true,
            OidcAuthority = _fixture.OidcAuthority,
            ValidIssuer = OAuthBearerBrokerFixture.TestIssuer,
            ValidAudiences = [OAuthBearerBrokerFixture.TestAudience],
            PrincipalClaim = "sub",
            RequireHttpsMetadata = false,
        };

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var validator = new JwksTokenValidator(config, NullLogger<JwksTokenValidator>.Instance, http);

        var token = _fixture.MintToken();
        var result = await validator.ValidateAsync(token, CancellationToken.None);

        Assert.True(result.IsValid,
            $"Validator rejected freshly-minted token. Reason: {result.FailureReason ?? "(none)"}; token: {token}");
    }

    [Fact]
    public async Task ValidatorWithDirectJwksAcceptsToken()
    {
        // Bypass OIDC discovery entirely — point the validator straight at /jwks.
        var config = new OAuthBearerConfig
        {
            Enabled = true,
            JwksUri = _fixture.JwksUri,
            ValidIssuer = OAuthBearerBrokerFixture.TestIssuer,
            ValidAudiences = [OAuthBearerBrokerFixture.TestAudience],
            PrincipalClaim = "sub",
            RequireHttpsMetadata = false,
        };

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var validator = new JwksTokenValidator(config, NullLogger<JwksTokenValidator>.Instance, http);

        var token = _fixture.MintToken();
        var result = await validator.ValidateAsync(token, CancellationToken.None);

        Assert.True(result.IsValid,
            $"Direct-JWKS validator rejected freshly-minted token. Reason: {result.FailureReason ?? "(none)"}");
    }
}
