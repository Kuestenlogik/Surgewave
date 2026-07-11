using Kuestenlogik.Surgewave.Broker.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Registers the host-side ACL authorizer (#59 b5). <see cref="AclAuthorizer"/> is SHARED
/// broker-core — the native gRPC SecurityServiceImpl and the REST <c>/admin/acls</c> surface
/// consume it, so it stays in the broker host and must resolve even when the Kafka plugin is
/// absent. It is also exposed through the neutral <see cref="IAuthorizer"/> seam so the relocated
/// Kafka Security/Data handlers (now in the Kafka plugin) can resolve authorization without naming
/// the concrete type. The SASL / SCRAM / OAUTHBEARER stack moved into the Kafka plugin
/// (Protocol.Kafka) together with the SASL request handler.
/// </summary>
internal static class KafkaSecurityRegistration
{
    public static IServiceCollection AddAclAuthorizer(this IServiceCollection services, BrokerConfig bootstrap)
    {
        if (bootstrap.Security.AclEnabled)
        {
            services.AddSingleton(BuildAclAuthorizer);
            services.AddSingleton<IAuthorizer>(sp => sp.GetRequiredService<AclAuthorizer>());
        }

        return services;
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
