using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Wires JWT bearer authentication + a default-deny authorization gate onto the
/// broker's HTTP surface (#37). This closes the "anyone who can reach the
/// broker's HTTP port can manage it" hole while staying config-gated so
/// existing deployments — which run with no HTTP auth — are unaffected.
/// </summary>
public static class BrokerRestApiAuthExtensions
{
    private const string GrpcContentTypePrefix = "application/grpc";

    /// <summary>Register JWT bearer auth if REST auth is enabled. No-op otherwise.</summary>
    /// <exception cref="InvalidOperationException">Auth is enabled but no issuer is configured.</exception>
    public static void AddSurgewaveRestApiAuth(
        this IServiceCollection services,
        RestApiAuthConfig config,
        OAuth2Config oauth2)
    {
        if (!config.Enabled)
            return;

        var issuer = config.Issuer ?? oauth2.Issuer;
        var audience = config.Audience ?? oauth2.Audience;
        var jwksUri = config.JwksUri ?? oauth2.JwksUri;

        // Fail fast rather than stand up a validator that trusts any issuer.
        if (string.IsNullOrWhiteSpace(issuer))
        {
            throw new InvalidOperationException(
                "Surgewave:Security:RestApiAuth is enabled but no issuer is configured. " +
                "Set Surgewave:Security:RestApiAuth:Issuer (or Security:OAuth2:Issuer) to the OIDC issuer URL.");
        }

        services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            })
            .AddJwtBearer(options =>
            {
                options.Authority = issuer;
                options.RequireHttpsMetadata = config.RequireHttpsMetadata;

                // Prefer an explicit JWKS metadata endpoint when configured;
                // otherwise the authority's OIDC discovery document is used.
                if (!string.IsNullOrEmpty(jwksUri))
                    options.MetadataAddress = jwksUri;

                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidIssuer = issuer,
                    ValidateAudience = !string.IsNullOrEmpty(audience),
                    ValidAudience = audience,
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromMinutes(config.ClockSkewMinutes),
                    RoleClaimType = config.RolesClaim,
                    NameClaimType = "preferred_username",
                };
            });

        services.AddAuthorization();
    }

    /// <summary>
    /// Insert authentication + the default-deny authorization gate into the
    /// pipeline. Must run before the endpoints are mapped. No-op when disabled.
    /// </summary>
    public static void UseSurgewaveRestApiAuth(
        this WebApplication app,
        RestApiAuthConfig config,
        OAuth2Config oauth2,
        ILogger logger)
    {
        if (!config.Enabled)
            return;

        // Route first so the gate can inspect the resolved endpoint's metadata
        // (used to distinguish real gRPC from a spoofed Content-Type). Then
        // authenticate so context.User is populated before the gate reads it.
        app.UseRouting();
        app.UseAuthentication();

        if (string.IsNullOrWhiteSpace(config.Audience ?? oauth2.Audience))
        {
            logger.LogWarning(
                "REST API auth: no audience configured — tokens minted for any audience of the issuer are accepted. " +
                "Set Surgewave:Security:RestApiAuth:Audience to restrict to this broker.");
        }

        var policy = new RestApiAuthPolicy(config);
        app.Use(async (context, next) =>
        {
            // Only relax the role check for a *real* gRPC call: the resolved
            // endpoint must be a gRPC method AND carry the gRPC content type.
            // Spoofing Content-Type: application/grpc on a REST endpoint no
            // longer bypasses the role gate — that endpoint has no gRPC metadata.
            // (Match by metadata type name to avoid coupling to the exact gRPC
            // assembly type, which varies across Grpc.AspNetCore versions.)
            var contentTypeGrpc = context.Request.ContentType?.StartsWith(GrpcContentTypePrefix, StringComparison.OrdinalIgnoreCase) == true;
            var endpointIsGrpc = context.GetEndpoint()?.Metadata
                .Any(m => m.GetType().Name == "GrpcMethodMetadata") == true;
            var isGrpc = contentTypeGrpc && endpointIsGrpc;

            var decision = policy.Evaluate(
                context.Request.Path.Value ?? "/",
                context.Request.Method,
                isGrpc,
                context.User.Identity?.IsAuthenticated == true,
                context.User.IsInRole(config.RequiredRole));

            switch (decision)
            {
                case RestApiAuthDecision.Unauthenticated:
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                case RestApiAuthDecision.Forbidden:
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return;
                default:
                    await next();
                    break;
            }
        });

        logger.LogInformation(
            "  - REST API auth:       enabled (default-deny; mutations require role '{Role}')", config.RequiredRole);
    }
}
