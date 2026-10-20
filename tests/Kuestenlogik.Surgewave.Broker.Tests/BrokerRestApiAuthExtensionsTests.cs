using System.Security.Claims;
using System.Text;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Startup-guard tests for broker REST auth registration (#37): enabling auth
/// without an issuer must fail fast rather than stand up a validator that trusts
/// any issuer; a token's roles claim gives the caller the role; disabling it registers nothing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BrokerRestApiAuthExtensionsTests
{
    [Fact]
    public void Enabled_WithoutIssuer_ThrowsAtStartup()
    {
        var services = new ServiceCollection();
        var config = new RestApiAuthConfig { Enabled = true }; // no issuer, no OAuth2 fallback

        var ex = Assert.Throws<InvalidOperationException>(
            () => services.AddSurgewaveRestApiAuth(config, new OAuth2Config()));
        Assert.Contains("issuer", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Enabled_WithOAuth2IssuerFallback_DoesNotThrow()
    {
        var services = new ServiceCollection();
        var config = new RestApiAuthConfig { Enabled = true };
        var oauth2 = new OAuth2Config { Issuer = "https://idp.test/realms/surgewave" };

        services.AddSurgewaveRestApiAuth(config, oauth2);

        Assert.Contains(services, s => s.ServiceType == typeof(Microsoft.AspNetCore.Authentication.IAuthenticationService));
    }

    [Fact]
    public async Task TokenWithTheRolesClaim_GivesTheCallerTheRequiredRole()
    {
        const string issuer = "https://idp.test/realms/surgewave";
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("broker-rest-auth-tests-signing-key-0123456789"));
        var config = new RestApiAuthConfig { Enabled = true, Issuer = issuer };
        var services = new ServiceCollection();
        services.AddSurgewaveRestApiAuth(config, new OAuth2Config());
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<JwtBearerOptions>>().Get(JwtBearerDefaults.AuthenticationScheme);
        var parameters = options.TokenValidationParameters.Clone();
        parameters.IssuerSigningKey = key;

        var token = new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Subject = new ClaimsIdentity([new Claim("preferred_username", "admin"), new Claim(config.RolesClaim, config.RequiredRole)]),
            Expires = DateTime.UtcNow.AddMinutes(5),
            SigningCredentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256),
        });

        // The handler the broker validates with, as JwtBearer configures it.
        var result = await Assert.Single(options.TokenHandlers).ValidateTokenAsync(token, parameters);

        Assert.True(result.IsValid, result.Exception?.Message);
        Assert.True(new ClaimsPrincipal(result.ClaimsIdentity).IsInRole(config.RequiredRole));
    }

    [Fact]
    public void Disabled_RegistersNothing()
    {
        var services = new ServiceCollection();

        services.AddSurgewaveRestApiAuth(new RestApiAuthConfig { Enabled = false }, new OAuth2Config());

        Assert.DoesNotContain(services, s => s.ServiceType == typeof(Microsoft.AspNetCore.Authentication.IAuthenticationService));
    }
}
