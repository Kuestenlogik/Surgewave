using System.Security.Claims;
using ITfoxtec.Identity.Saml2;
using ITfoxtec.Identity.Saml2.MvcCore.Configuration;
using ITfoxtec.Identity.Saml2.Util;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace Kuestenlogik.Surgewave.Control.Security;

/// <summary>
/// Extension methods for configuring provider-specific authentication schemes.
/// </summary>
public static class AuthenticationBuilderExtensions
{
    /// <summary>
    /// Registers the shared cookie scheme once, then adds named OIDC/SAML schemes for every configured provider.
    /// </summary>
    public static AuthenticationBuilder AddSurgewaveProviders(
        this AuthenticationBuilder builder,
        SurgewaveAuthConfig authConfig,
        IServiceCollection services)
    {
        // Single cookie scheme shared by all providers
        builder.AddCookie(CookieAuthenticationDefaults.AuthenticationScheme, options =>
        {
            options.AccessDeniedPath = "/Account/AccessDenied";
            options.LoginPath = "/Account/Login";
        });

        foreach (var provider in authConfig.Providers)
        {
            switch (provider.Type)
            {
                case AuthProviderType.EntraId:
                    builder.AddSurgewaveEntraId(provider);
                    break;
                case AuthProviderType.Okta:
                    builder.AddSurgewaveOkta(provider);
                    break;
                case AuthProviderType.Google:
                    builder.AddSurgewaveGoogle(provider);
                    break;
                case AuthProviderType.Saml:
                    builder.AddSurgewaveSaml(provider, services);
                    break;
                case AuthProviderType.Ldap:
                    // LDAP uses direct bind — no OIDC scheme needed.
                    // Register LdapAuthenticationService for the controller.
                    services.AddSingleton<LdapAuthenticationService>();
                    break;
                default:
                    builder.AddSurgewaveOidc(provider);
                    break;
            }
        }

        return builder;
    }

    /// <summary>
    /// Adds generic OIDC authentication (Keycloak or any standard OIDC provider).
    /// </summary>
    public static AuthenticationBuilder AddSurgewaveOidc(this AuthenticationBuilder builder, IdpProviderConfig config)
    {
        var schemeName = SchemeNames.OidcScheme(config.Name);

        builder.AddOpenIdConnect(schemeName, config.EffectiveDisplayName, options =>
        {
            options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            options.Authority = config.Authority;
            options.ClientId = config.ClientId;
            options.ClientSecret = config.ClientSecret;
            options.ResponseType = OpenIdConnectResponseType.Code;
            options.SaveTokens = true;
            options.GetClaimsFromUserInfoEndpoint = true;
            options.RequireHttpsMetadata = config.RequireHttpsMetadata;
            options.CallbackPath = config.EffectiveCallbackPath;
            options.SignedOutCallbackPath = config.EffectiveSignedOutCallbackPath;

            foreach (var scope in config.Scopes)
            {
                options.Scope.Add(scope);
            }

            options.TokenValidationParameters.RoleClaimType = ClaimTypes.Role;

            InjectProviderClaim(options, config.Name);
        });

        return builder;
    }

    /// <summary>
    /// Adds Azure AD / Entra ID authentication via OIDC with Entra-specific defaults.
    /// </summary>
    public static AuthenticationBuilder AddSurgewaveEntraId(this AuthenticationBuilder builder, IdpProviderConfig config)
    {
        var schemeName = SchemeNames.OidcScheme(config.Name);

        var authority = string.IsNullOrEmpty(config.Authority)
            ? $"https://login.microsoftonline.com/{config.TenantId}/v2.0"
            : config.Authority;

        builder.AddOpenIdConnect(schemeName, config.EffectiveDisplayName, options =>
        {
            options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            options.Authority = authority;
            options.ClientId = config.ClientId;
            options.ClientSecret = config.ClientSecret;
            options.ResponseType = OpenIdConnectResponseType.Code;
            options.SaveTokens = true;
            options.GetClaimsFromUserInfoEndpoint = true;
            options.RequireHttpsMetadata = config.RequireHttpsMetadata;
            options.CallbackPath = config.EffectiveCallbackPath;
            options.SignedOutCallbackPath = config.EffectiveSignedOutCallbackPath;

            // Entra ID standard scopes
            options.Scope.Clear();
            options.Scope.Add("openid");
            options.Scope.Add("profile");
            options.Scope.Add("email");

            options.TokenValidationParameters.RoleClaimType = ClaimTypes.Role;
            options.TokenValidationParameters.ValidateIssuer = true;

            // Entra ID sends roles/groups as individual claims, not as JSON
            options.ClaimActions.MapJsonKey("roles", "roles");
            options.ClaimActions.MapJsonKey("groups", "groups");
            options.ClaimActions.MapJsonKey("wids", "wids");

            InjectProviderClaim(options, config.Name);
        });

        return builder;
    }

    /// <summary>
    /// Adds Okta OIDC authentication with Okta-specific defaults.
    /// </summary>
    public static AuthenticationBuilder AddSurgewaveOkta(this AuthenticationBuilder builder, IdpProviderConfig config)
    {
        var schemeName = SchemeNames.OidcScheme(config.Name);

        var authority = string.IsNullOrEmpty(config.Authority)
            ? $"https://{config.OktaDomain}/oauth2/default"
            : config.Authority;

        builder.AddOpenIdConnect(schemeName, config.EffectiveDisplayName, options =>
        {
            options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            options.Authority = authority;
            options.ClientId = config.ClientId;
            options.ClientSecret = config.ClientSecret;
            options.ResponseType = OpenIdConnectResponseType.Code;
            options.SaveTokens = true;
            options.GetClaimsFromUserInfoEndpoint = true;
            options.RequireHttpsMetadata = config.RequireHttpsMetadata;
            options.CallbackPath = config.EffectiveCallbackPath;
            options.SignedOutCallbackPath = config.EffectiveSignedOutCallbackPath;

            // Okta standard scopes — "groups" scope required for group claims
            options.Scope.Clear();
            options.Scope.Add("openid");
            options.Scope.Add("profile");
            options.Scope.Add("email");
            options.Scope.Add("groups");

            options.TokenValidationParameters.RoleClaimType = ClaimTypes.Role;

            InjectProviderClaim(options, config.Name);
        });

        return builder;
    }

    /// <summary>
    /// Adds Google Workspace OIDC authentication with Google-specific defaults.
    /// </summary>
    public static AuthenticationBuilder AddSurgewaveGoogle(this AuthenticationBuilder builder, IdpProviderConfig config)
    {
        var schemeName = SchemeNames.OidcScheme(config.Name);

        builder.AddOpenIdConnect(schemeName, config.EffectiveDisplayName, options =>
        {
            options.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme;
            options.Authority = "https://accounts.google.com";
            options.ClientId = config.ClientId;
            options.ClientSecret = config.ClientSecret;
            options.ResponseType = OpenIdConnectResponseType.Code;
            options.SaveTokens = true;
            options.GetClaimsFromUserInfoEndpoint = true;
            options.RequireHttpsMetadata = config.RequireHttpsMetadata;
            options.CallbackPath = config.EffectiveCallbackPath;
            options.SignedOutCallbackPath = config.EffectiveSignedOutCallbackPath;

            // Google standard scopes
            options.Scope.Clear();
            options.Scope.Add("openid");
            options.Scope.Add("profile");
            options.Scope.Add("email");

            options.TokenValidationParameters.RoleClaimType = ClaimTypes.Role;

            // Restrict to hosted domain if configured
            if (!string.IsNullOrEmpty(config.GoogleHostedDomain))
            {
                options.Events = new OpenIdConnectEvents
                {
                    OnRedirectToIdentityProvider = context =>
                    {
                        context.ProtocolMessage.SetParameter("hd", config.GoogleHostedDomain);
                        return Task.CompletedTask;
                    }
                };
            }

            InjectProviderClaim(options, config.Name);
        });

        return builder;
    }

    /// <summary>
    /// Adds SAML 2.0 authentication with keyed Saml2Configuration (ITfoxtec SAML2).
    /// </summary>
    public static AuthenticationBuilder AddSurgewaveSaml(
        this AuthenticationBuilder builder,
        IdpProviderConfig config,
        IServiceCollection services)
    {
        var samlConfig = config.Saml;

        services.AddKeyedSingleton<Saml2Configuration>(config.Name, (_, _) =>
        {
            var saml2 = new Saml2Configuration
            {
                Issuer = samlConfig.SpEntityId,
                SignatureAlgorithm = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256",
            };

            if (!string.IsNullOrEmpty(samlConfig.CertificateFile))
            {
                saml2.SigningCertificate = CertificateUtil.Load(
                    samlConfig.CertificateFile,
                    samlConfig.CertificatePassword);
            }

            saml2.AllowedAudienceUris.Add(samlConfig.SpEntityId);

            return saml2;
        });

        // Also register the first SAML provider's config as non-keyed for backward compat
        services.TryAddSingleton<Saml2Configuration>(sp =>
            sp.GetRequiredKeyedService<Saml2Configuration>(config.Name));

        services.AddSaml2();

        return builder;
    }

    /// <summary>
    /// Hooks into OnTokenValidated to inject the <see cref="SchemeNames.ProviderClaimType"/> claim
    /// so the session cookie knows which provider authenticated the user.
    /// </summary>
    private static void InjectProviderClaim(OpenIdConnectOptions options, string providerName)
    {
        var existingHandler = options.Events?.OnTokenValidated;

        options.Events ??= new OpenIdConnectEvents();
        options.Events.OnTokenValidated = context =>
        {
            if (context.Principal?.Identity is ClaimsIdentity identity)
            {
                identity.AddClaim(new Claim(SchemeNames.ProviderClaimType, providerName));
            }

            return existingHandler?.Invoke(context) ?? Task.CompletedTask;
        };
    }
}
