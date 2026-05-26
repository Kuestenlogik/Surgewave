using System.Security.Claims;
using Kuestenlogik.Surgewave.Control.Security;
using Kuestenlogik.Surgewave.Core.Util;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Control.Controllers;

/// <summary>
/// MVC Controller for LDAP/AD Bind authentication endpoints.
/// Handles username/password POST login since LDAP has no browser redirect flow.
/// Routes are per-provider: /ldap/{providerName}/login.
/// </summary>
[AllowAnonymous]
[Route("ldap/{providerName}")]
public sealed class LdapController(
    IOptions<SurgewaveAuthConfig> authOptions,
    LdapAuthenticationService ldapService,
    ILogger<LdapController> logger) : Controller
{
    private IdpProviderConfig GetProviderConfig(string providerName) =>
        authOptions.Value.Providers.First(p =>
            string.Equals(p.Name, providerName, StringComparison.OrdinalIgnoreCase)
            && p.Type == AuthProviderType.Ldap);

    /// <summary>
    /// Authenticates the user against the LDAP server and signs them in via cookie.
    /// </summary>
    [HttpPost("login")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Login(
        string providerName,
        [FromForm] string username,
        [FromForm] string password,
        [FromForm] string? returnUrl)
    {
        var providerConfig = GetProviderConfig(providerName);
        var result = await ldapService.AuthenticateAsync(username, password, providerConfig);

        // Open-Redirect-Schutz: returnUrl muss eine *lokale* URL sein.
        // Url.IsLocalUrl lehnt absolute URLs (http://evil.com) und
        // protokoll-relative URLs (//evil.com) ab; akzeptiert sind
        // pfad-relative URLs ("/foo") und Fragmentes.
        var safeReturnUrl = Url.IsLocalUrl(returnUrl) ? returnUrl! : "/";

        if (!result.Succeeded)
        {
            logger.LogWarning("LDAP login failed for user {Username} via provider {Provider}", LogSanitizer.Sanitize(username), LogSanitizer.Sanitize(providerName));

            var loginUrl = $"/Account/Login?provider={Uri.EscapeDataString(providerName)}" +
                           $"&returnUrl={Uri.EscapeDataString(safeReturnUrl)}" +
                           "&error=invalid_credentials";
            return Redirect(loginUrl);
        }

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, result.Attributes.DisplayName ?? username),
            new(ClaimTypes.NameIdentifier, result.UserDn!),
            new(SchemeNames.ProviderClaimType, providerName),
        };

        if (!string.IsNullOrEmpty(result.Attributes.Email))
            claims.Add(new Claim(ClaimTypes.Email, result.Attributes.Email));

        // Store group memberships as ldap:groups claims for the ClaimsTransformation pipeline
        foreach (var group in result.Attributes.Groups)
            claims.Add(new Claim("ldap:groups", group));

        var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
        var principal = new ClaimsPrincipal(identity);

        await HttpContext.SignInAsync(
            CookieAuthenticationDefaults.AuthenticationScheme,
            principal,
            new AuthenticationProperties
            {
                IsPersistent = true,
                AllowRefresh = true,
            });

        logger.LogInformation("LDAP login successful for {Username} via provider {Provider}", LogSanitizer.Sanitize(username), LogSanitizer.Sanitize(providerName));

        return Redirect(safeReturnUrl);
    }
}
