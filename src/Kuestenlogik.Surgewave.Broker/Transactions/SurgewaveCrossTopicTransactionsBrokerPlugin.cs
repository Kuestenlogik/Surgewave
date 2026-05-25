using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// <see cref="IBrokerPlugin"/> that activates the cross-topic atomic transaction manager.
/// Opt-in via <c>Surgewave:CrossTopicTransactions:Enabled=true</c>.
///
/// <para>
/// The manager is registered as a DI singleton in <see cref="ConfigureServices"/> so
/// <c>Program.cs</c> can resolve it (nullable) and pass it to the <c>SurgewaveBroker</c>
/// constructor. The REST API is mapped in <see cref="Configure"/>.
/// </para>
/// </summary>
public sealed class SurgewaveCrossTopicTransactionsBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.CrossTopicTransactions";

    /// <inheritdoc />
    public string DisplayName => "Cross-Topic Transactions";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:CrossTopicTransactions:Enabled", false);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(sp =>
        {
            var config = sp.GetRequiredService<BrokerConfig>();
            var logManager = sp.GetRequiredService<LogManager>();
            var serializer = sp.GetRequiredService<RecordBatchSerializer>();
            var logger = sp.GetRequiredService<ILogger<CrossTopicTransactionManager>>();
            return new CrossTopicTransactionManager(logManager, serializer, config.CrossTopicTransactions, logger);
        });
    }

    /// <inheritdoc />
    public void Configure(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var manager = services.GetRequiredService<CrossTopicTransactionManager>();
        var logger = services.GetRequiredService<ILogger<Program>>();
        var config = services.GetRequiredService<BrokerConfig>();

        app.MapCrossTopicTransactions(manager);
        logger.LogInformation("  - Cross-Topic Txn API: {Host}:{GrpcPort}/api/transactions",
            config.Host, config.GrpcPort);
    }
}
