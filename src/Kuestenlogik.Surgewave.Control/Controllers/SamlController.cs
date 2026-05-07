using System.Security.Claims;
using ITfoxtec.Identity.Saml2;
using ITfoxtec.Identity.Saml2.Claims;
using ITfoxtec.Identity.Saml2.MvcCore;
using ITfoxtec.Identity.Saml2.Schemas;
using ITfoxtec.Identity.Saml2.Schemas.Metadata;
using Kuestenlogik.Surgewave.Control.Security;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens.Saml2;

namespace Kuestenlogik.Surgewave.Control.Controllers;

/// <summary>
/// MVC Controller for SAML 2.0 endpoints (Login, ACS, Logout, SLO, Metadata).
/// Required because ITfoxtec SAML2 uses POST bindings that need traditional MVC form handling.
/// Routes are per-provider: /saml/{providerName}/login, /saml/{providerName}/acs, etc.
/// </summary>
[AllowAnonymous]
[Route("saml/{providerName}")]
public sealed class SamlController(
    IOptions<SurgewaveAuthConfig> authOptions,
    ILogger<SamlController> logger) : Controller
{
    private IdpProviderConfig GetProviderConfig(string providerName) =>
        authOptions.Value.Providers.First(p =>
            string.Equals(p.Name, providerName, StringComparison.OrdinalIgnoreCase)
            && p.Type == AuthProviderType.Saml);

    private Saml2Configuration GetSaml2Config(string providerName) =>
        HttpContext.RequestServices.GetRequiredKeyedService<Saml2Configuration>(providerName);

    /// <summary>
    /// Initiates SAML AuthnRequest — redirects user to IdP.
    /// </summary>
    [HttpGet("login")]
    public IActionResult Login(string providerName, string? returnUrl = null)
    {
        var providerConfig = GetProviderConfig(providerName);
        var saml2Config = GetSaml2Config(providerName);
        var binding = new Saml2RedirectBinding();

        var saml2AuthnRequest = new Saml2AuthnRequest(saml2Config)
        {
            AssertionConsumerServiceUrl = new Uri(providerConfig.Saml.AssertionConsumerServiceUrl),
            ForceAuthn = false,
        };

        binding.Bind(saml2AuthnRequest);

        logger.LogInformation("SAML AuthnRequest initiated for provider {Provider}, redirecting to IdP", providerName);

        // Store returnUrl in relay state
        binding.RelayState = returnUrl ?? "/";

        return binding.ToActionResult();
    }

    /// <summary>
    /// Assertion Consumer Service — processes SAML Response from IdP.
    /// </summary>
    [HttpPost("acs")]
    public async Task<IActionResult> AssertionConsumerService(string providerName)
    {
        var saml2Config = GetSaml2Config(providerName);
        var binding = new Saml2PostBinding();
        var saml2AuthnResponse = new Saml2AuthnResponse(saml2Config);

        binding.ReadSamlResponse(Request.ToGenericHttpRequest(), saml2AuthnResponse);

        if (saml2AuthnResponse.Status != Saml2StatusCodes.Success)
        {
            logger.LogWarning("SAML Response status for provider {Provider}: {Status}", providerName, saml2AuthnResponse.Status);
            return Redirect("/Account/AccessDenied");
        }

        binding.Unbind(Request.ToGenericHttpRequest(), saml2AuthnResponse);

        var claimsIdentity = saml2AuthnResponse.ClaimsIdentity;

        // Inject the surgewave:idp claim so the session knows which provider authenticated the user
        claimsIdentity.AddClaim(new Claim(SchemeNames.ProviderClaimType, providerName));

        var claimsPrincipal = new ClaimsPrincipal(claimsIdentity);

        await HttpContext.SignInAsync(
            CookieAuthenticationDefaults.AuthenticationScheme,
            claimsPrincipal,
            new AuthenticationProperties
            {
                IsPersistent = true,
                AllowRefresh = true,
            });

        logger.LogInformation("SAML login successful for {NameId} via provider {Provider}", claimsIdentity.Name, providerName);

        var returnUrl = binding.RelayState;
        return Redirect(string.IsNullOrEmpty(returnUrl) ? "/" : returnUrl);
    }

    /// <summary>
    /// Initiates SAML LogoutRequest — redirects user to IdP for single logout.
    /// </summary>
    [HttpGet("logout")]
    public async Task<IActionResult> Logout(string providerName)
    {
        var providerConfig = GetProviderConfig(providerName);
        var saml2Config = GetSaml2Config(providerName);

        if (string.IsNullOrEmpty(providerConfig.Saml.SingleLogoutServiceUrl))
        {
            // No SLO configured — just sign out locally
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            return Redirect("/");
        }

        var binding = new Saml2RedirectBinding();

        var nameId = User.Claims.FirstOrDefault(c => c.Type == ClaimTypes.NameIdentifier);
        var sessionIndex = User.Claims.FirstOrDefault(c => c.Type == Saml2ClaimTypes.SessionIndex);

        var saml2LogoutRequest = new Saml2LogoutRequest(saml2Config, User)
        {
            Destination = new Uri(providerConfig.Saml.SingleLogoutServiceUrl),
        };

        if (nameId is not null)
        {
            saml2LogoutRequest.NameId = new Saml2NameIdentifier(nameId.Value);
        }

        if (sessionIndex is not null)
        {
            saml2LogoutRequest.SessionIndex = sessionIndex.Value;
        }

        binding.Bind(saml2LogoutRequest);

        await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);

        logger.LogInformation("SAML logout initiated for provider {Provider}", providerName);

        return binding.ToActionResult();
    }

    /// <summary>
    /// Single Logout Service — processes LogoutResponse from IdP.
    /// </summary>
    [HttpPost("slo")]
    public IActionResult SingleLogoutService(string providerName)
    {
        var saml2Config = GetSaml2Config(providerName);
        var binding = new Saml2PostBinding();
        var saml2LogoutResponse = new Saml2LogoutResponse(saml2Config);

        binding.Unbind(Request.ToGenericHttpRequest(), saml2LogoutResponse);

        if (saml2LogoutResponse.Status != Saml2StatusCodes.Success)
        {
            logger.LogWarning("SAML SLO Response status for provider {Provider}: {Status}", providerName, saml2LogoutResponse.Status);
        }

        return Redirect("/");
    }

    /// <summary>
    /// SP Metadata endpoint — provides metadata XML for IdP configuration.
    /// </summary>
    [HttpGet("metadata")]
    public IActionResult Metadata(string providerName)
    {
        var providerConfig = GetProviderConfig(providerName);
        var saml2Config = GetSaml2Config(providerName);

        var entityDescriptor = new EntityDescriptor(saml2Config)
        {
            SPSsoDescriptor = new SPSsoDescriptor
            {
                AuthnRequestsSigned = saml2Config.SigningCertificate is not null,
                WantAssertionsSigned = providerConfig.Saml.WantAssertionsSigned,
                AssertionConsumerServices =
                [
                    new AssertionConsumerService
                    {
                        Binding = ProtocolBindings.HttpPost,
                        Location = new Uri(providerConfig.Saml.AssertionConsumerServiceUrl),
                        IsDefault = true,
                    }
                ],
            }
        };

        if (!string.IsNullOrEmpty(providerConfig.Saml.SingleLogoutServiceUrl))
        {
            entityDescriptor.SPSsoDescriptor.SingleLogoutServices =
            [
                new SingleLogoutService
                {
                    Binding = ProtocolBindings.HttpPost,
                    Location = new Uri(providerConfig.Saml.SingleLogoutServiceUrl),
                }
            ];
        }

        if (saml2Config.SigningCertificate is not null)
        {
            entityDescriptor.SPSsoDescriptor.SigningCertificates = [saml2Config.SigningCertificate];
        }

        return new Saml2Metadata(entityDescriptor).CreateMetadata().ToActionResult();
    }
}
