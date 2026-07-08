using System.Net.Http;
using System.Security.Cryptography;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Holds the two optional in-memory SCRAM credential stores as a single DI service so
/// the <see cref="SaslAuthenticator"/> and the Kafka SecurityApiHandler
/// (AlterUserScramCredentials, KIP-554) share the SAME instances. Either entry is null
/// when the corresponding mechanism is not in the SASL allow-list.
/// </summary>
internal sealed record ScramStores(ScramCredentialStore? Sha256, ScramCredentialStore? Sha512);

/// <summary>
/// Registers the Kafka-protocol auth stack (SASL / SCRAM / ACL) in the broker's DI
/// (#59 phase 2b). <see cref="AclAuthorizer"/> is deliberately a SHARED broker-core
/// service — the native gRPC SecurityServiceImpl and the REST <c>/admin/acls</c> surface
/// consume it too, so it must resolve even when Kafka is disabled. SASL + the SCRAM
/// stores are Kafka-only (SecurityApiHandler). Conditional registration mirrors the
/// previous post-build gating on <c>config.Security.*</c>; construction logic and its
/// informational logging move verbatim into the factories.
/// </summary>
internal static class KafkaSecurityRegistration
{
    public static IServiceCollection AddKafkaSecurityServices(this IServiceCollection services, BrokerConfig bootstrap)
    {
        // SCRAM stores holder — always registered; both entries null unless SASL is on
        // and the mechanism is in the allow-list. Registered even when SASL is off so
        // the SecurityApiHandler can resolve it unconditionally (it gets nulls).
        services.AddSingleton(sp =>
        {
            var sec = sp.GetRequiredService<BrokerConfig>().Security;
            if (!sec.SaslEnabled)
                return new ScramStores(null, null);

            var logger = sp.GetRequiredService<ILogger<SaslAuthenticator>>();
            ScramCredentialStore? sha256 = null;
            ScramCredentialStore? sha512 = null;
            if (sec.SaslMechanisms.Contains(SaslAuthenticator.MechanismScramSha256, StringComparer.OrdinalIgnoreCase))
            {
                sha256 = new ScramCredentialStore(hashAlgorithm: HashAlgorithmName.SHA256);
                logger.LogInformation("SCRAM-SHA-256 store initialised (in-memory)");
            }
            if (sec.SaslMechanisms.Contains(SaslAuthenticator.MechanismScramSha512, StringComparer.OrdinalIgnoreCase))
            {
                sha512 = new ScramCredentialStore(hashAlgorithm: HashAlgorithmName.SHA512);
                logger.LogInformation("SCRAM-SHA-512 store initialised (in-memory)");
            }
            return new ScramStores(sha256, sha512);
        });

        if (bootstrap.Security.SaslEnabled)
            services.AddSingleton(BuildSaslAuthenticator);

        if (bootstrap.Security.AclEnabled)
            services.AddSingleton(BuildAclAuthorizer);

        return services;
    }

    private static SaslAuthenticator BuildSaslAuthenticator(IServiceProvider sp)
    {
        var sec = sp.GetRequiredService<BrokerConfig>().Security;
        var logger = sp.GetRequiredService<ILogger<SaslAuthenticator>>();

        var credentialStore = new CredentialStore(sec.CredentialsFile);
        foreach (var userEntry in sec.Users)
        {
            var parts = userEntry.Split(':');
            if (parts.Length == 2)
                credentialStore.AddUser(parts[0], parts[1]);
        }

        // OAUTHBEARER (KIP-936): stand up a JWT validator + frame parser only when the
        // mechanism is in the allow-list and the OIDC/JWKS config is present; otherwise
        // the SaslAuthenticator rejects the mechanism cleanly.
        OAuthBearerAuthenticator? oauthBearer = null;
        if (sec.OAuthBearer.Enabled
            && sec.SaslMechanisms.Contains(SaslAuthenticator.MechanismOAuthBearer, StringComparer.OrdinalIgnoreCase))
        {
            var oauthHttp = sp.GetRequiredService<IHttpClientFactory>().CreateClient("oauthbearer-jwks");
            oauthHttp.Timeout = TimeSpan.FromSeconds(30);
            var validator = new JwksTokenValidator(sec.OAuthBearer, sp.GetRequiredService<ILogger<JwksTokenValidator>>(), oauthHttp);
            oauthBearer = new OAuthBearerAuthenticator(validator, sec.OAuthBearer);
            logger.LogInformation(
                "OAUTHBEARER enabled (issuer={Issuer}, jwksUri={Jwks}, principalClaim={Claim})",
                sec.OAuthBearer.ValidIssuer ?? sec.OAuthBearer.OidcAuthority ?? "(none)",
                sec.OAuthBearer.JwksUri ?? "(via discovery)",
                sec.OAuthBearer.PrincipalClaim);
        }

        var scram = sp.GetRequiredService<ScramStores>();
        var authenticator = new SaslAuthenticator(
            credentialStore,
            sec.SaslMechanisms,
            scramSha256Store: scram.Sha256,
            scramSha512Store: scram.Sha512,
            oauthBearer: oauthBearer);
        logger.LogInformation("SASL authentication enabled with mechanisms: {Mechanisms}",
            string.Join(", ", sec.SaslMechanisms));
        return authenticator;
    }

    private static AclAuthorizer BuildAclAuthorizer(IServiceProvider sp)
    {
        var sec = sp.GetRequiredService<BrokerConfig>().Security;
        var logger = sp.GetRequiredService<ILogger<AclAuthorizer>>();
        var authorizer = new AclAuthorizer(
            logger: logger,
            allowIfNoAclFound: sec.AllowIfNoAclFound,
            superUsers: sec.SuperUsers,
            aclFilePath: sec.AclFile);
        logger.LogInformation("ACL authorization enabled (AllowIfNoAclFound: {AllowIfNoAclFound}, SuperUsers: {SuperUsers})",
            sec.AllowIfNoAclFound,
            sec.SuperUsers.Length > 0 ? string.Join(", ", sec.SuperUsers) : "none");
        return authorizer;
    }
}
