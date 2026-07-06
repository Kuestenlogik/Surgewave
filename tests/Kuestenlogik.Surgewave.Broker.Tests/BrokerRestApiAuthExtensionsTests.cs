using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Startup-guard tests for broker REST auth registration (#37): enabling auth
/// without an issuer must fail fast rather than stand up a validator that trusts
/// any issuer; disabling it registers nothing.
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
    public void Disabled_RegistersNothing()
    {
        var services = new ServiceCollection();

        services.AddSurgewaveRestApiAuth(new RestApiAuthConfig { Enabled = false }, new OAuth2Config());

        Assert.DoesNotContain(services, s => s.ServiceType == typeof(Microsoft.AspNetCore.Authentication.IAuthenticationService));
    }
}
