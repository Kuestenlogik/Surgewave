using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// Extension methods for registering core broker services.
/// </summary>
public static class CoreServicesRegistration
{
    /// <summary>
    /// Registers core broker services including configuration, storage, and gRPC.
    /// </summary>
    public static IServiceCollection AddCoreBrokerServices(this IServiceCollection services, IConfiguration configuration)
    {
        // Bind configuration from appsettings.json
        services.Configure<BrokerConfig>(configuration.GetSection(BrokerConfig.SectionName));

        // Register BrokerConfig as singleton for direct injection
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<BrokerConfig>>().Value);

        // Register log segment factory based on storage engine
        services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<BrokerConfig>();
            return StorageEngineFactory.Create(config.StorageEngine);
        });

        // Shared log manager for both protocols
        services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<BrokerConfig>();
            var segmentFactory = sp.GetRequiredService<ILogSegmentFactory>();
            var retentionPolicy = new RetentionPolicy
            {
                RetentionHours = config.LogRetentionHours,
                RetentionBytes = config.LogRetentionBytes
            };
            // When Raft is enabled, don't persist topics to file - Raft log is the source of truth
            var persistTopicsToFile = !config.UseRaftConsensus;
            return new LogManager(config.DataDirectory, segmentFactory, retentionPolicy: retentionPolicy, persistTopicsToFile: persistTopicsToFile);
        });

        // Configure Kestrel with options
        services.AddSingleton<IConfigureOptions<KestrelServerOptions>>(sp =>
        {
            var config = sp.GetRequiredService<BrokerConfig>();
            return new ConfigureNamedOptions<KestrelServerOptions>(null, options =>
            {
                options.ListenAnyIP(config.GrpcPort, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });
        });

        return services;
    }

    /// <summary>
    /// Registers gRPC services with late-bound dependencies via holders.
    /// </summary>
    public static IServiceCollection AddGrpcServiceHolders(this IServiceCollection services)
    {
        services.AddGrpc();

        services.AddSingleton(_ => ProducerServiceImplHolder.Instance
            ?? throw new InvalidOperationException("ProducerServiceImpl not initialized"));
        services.AddSingleton(_ => ConsumerServiceImplHolder.Instance
            ?? throw new InvalidOperationException("ConsumerServiceImpl not initialized"));
        services.AddSingleton(_ => TopicServiceImplHolder.Instance
            ?? throw new InvalidOperationException("TopicServiceImpl not initialized"));
        services.AddSingleton(_ => AdminServiceImplHolder.Instance
            ?? throw new InvalidOperationException("AdminServiceImpl not initialized"));
        services.AddSingleton(_ => ConsumerGroupServiceImplHolder.Instance
            ?? throw new InvalidOperationException("ConsumerGroupServiceImpl not initialized"));
        services.AddSingleton(_ => ClusterServiceImplHolder.Instance
            ?? throw new InvalidOperationException("ClusterServiceImpl not initialized"));
        services.AddSingleton(_ => TransactionServiceImplHolder.Instance
            ?? throw new InvalidOperationException("TransactionServiceImpl not initialized"));
        services.AddSingleton(_ => QuotaServiceImplHolder.Instance
            ?? throw new InvalidOperationException("QuotaServiceImpl not initialized"));
        services.AddSingleton(_ => SecurityServiceImplHolder.Instance
            ?? throw new InvalidOperationException("SecurityServiceImpl not initialized"));
        services.AddSingleton(_ => SchemaRegistryServiceImplHolder.Instance
            ?? throw new InvalidOperationException("SchemaRegistryServiceImpl not initialized"));
        services.AddSingleton(_ => ConnectServiceImplHolder.Instance
            ?? throw new InvalidOperationException("ConnectServiceImpl not initialized"));

        return services;
    }
}
