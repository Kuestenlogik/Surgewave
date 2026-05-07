using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// <see cref="IBrokerPlugin"/> that activates the Kafka Connect framework inside the broker:
/// local Surgewave client, <see cref="ConnectWorker"/>, <see cref="PipelineOrchestrator"/>,
/// gRPC service holder, and the Pipeline REST API at <c>/api/pipelines</c>.
///
/// <para>
/// This is the first plugin to use <see cref="IBrokerPlugin.ConfigureAsync"/> — the Connect
/// framework needs async lifecycle (client connection, worker start, pipeline init).
/// </para>
/// </summary>
public sealed class SurgewaveConnectBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.Connect";

    /// <inheritdoc />
    public string DisplayName => "Kafka Connect";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:Connect:Enabled", false);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // ConnectWorkerConfig from broker config
        services.AddSingleton(sp =>
        {
            var brokerConfig = sp.GetRequiredService<BrokerConfig>();
            var cfg = sp.GetRequiredService<IConfiguration>();
            return new ConnectWorkerConfig
            {
                BootstrapServers = $"{brokerConfig.Host}:{brokerConfig.Port}",
                GroupId = brokerConfig.Connect.GroupId,
                ConfigTopic = brokerConfig.Connect.ConfigTopic,
                OffsetsTopic = brokerConfig.Connect.OffsetsTopic,
                StatusTopic = brokerConfig.Connect.StatusTopic,
                PluginsDirectory = cfg.GetValue<string>("Surgewave:Connect:PluginsDirectory")
                    ?? brokerConfig.Connect.PluginsDirectory ?? "plugins"
            };
        });

        services.AddSingleton<PluginLoader>();
        services.AddSingleton<PluginDiscovery>();
        services.AddSurgewavePipelines();
    }

    /// <inheritdoc />
    public async Task ConfigureAsync(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var config = services.GetRequiredService<BrokerConfig>();
        var connectConfig = services.GetRequiredService<ConnectWorkerConfig>();
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();

        // Discover connectors from plugins directory
        var pluginDiscovery = services.GetRequiredService<PluginDiscovery>();
        var pluginsDirectory = connectConfig.PluginsDirectory;
        if (!string.IsNullOrEmpty(pluginsDirectory))
        {
            var fullPluginsPath = Path.GetFullPath(pluginsDirectory);
            if (Directory.Exists(fullPluginsPath))
            {
                app.Logger.LogInformation("Scanning plugins directory for connectors: {Directory}", fullPluginsPath);
                pluginDiscovery.DiscoverPlugins(fullPluginsPath, useDefaultContext: false);
            }
        }

        // Pipeline metrics + worker services
        var pipelineMetrics = new PipelineMetricsCollector();
        var workerServices = new ConnectWorkerServices { MetricsCollector = pipelineMetrics };

        // Local Surgewave client (loopback to the broker's own Kafka port)
        var localClient = await SurgewaveClient
            .Create($"localhost:{config.Port}")
            .UseKafkaProtocol()
            .BuildAsync();
        await localClient.ConnectAsync();

        var connectLogger = loggerFactory.CreateLogger<ConnectWorker>();
#pragma warning disable CA2000 // Lifecycle managed by the application (runs until broker shutdown)
        var connectWorker = new ConnectWorker(connectConfig, localClient, connectLogger, workerServices, ownsClient: true);
#pragma warning restore CA2000
        connectWorker.SetTypeResolver(className => pluginDiscovery.LoadPluginType(className));
        await connectWorker.StartAsync();
        app.Logger.LogInformation("Kafka Connect framework started");

        // Pipeline orchestrator
        var pipelineStore = services.GetRequiredService<PipelineStore>();
        var pipelineTopicManager = services.GetRequiredService<PipelineTopicManager>();
        var pipelineLogger = loggerFactory.CreateLogger<PipelineOrchestrator>();
        var pipelineOrchestrator = new PipelineOrchestrator(
            pipelineStore, pipelineTopicManager, connectWorker, pluginDiscovery, pipelineLogger, pipelineMetrics);
        PipelineOrchestratorHolder.Instance = pipelineOrchestrator;
        await pipelineOrchestrator.InitializeAsync();
        app.Logger.LogInformation("Pipeline Orchestration framework started");

        // Wire gRPC service holder
        ConnectServiceImplHolder.Instance = new ConnectServiceImpl(
            listConnectors: () => connectWorker.ListConnectors().ToList(),
            getConnector: name =>
            {
                var info = connectWorker.GetConnectorStatus(name);
                if (info == null) return null;
                return new ConnectorInfoDto(
                    info.Name, info.Type, info.State, info.WorkerId,
                    new Dictionary<string, string>(info.Config),
                    info.Tasks.Select(t => new TaskStatusDto(t.Id, t.State, t.WorkerId, null)).ToList());
            },
            createConnector: async (name, cfg) =>
            {
                var connectorClass = cfg.GetValueOrDefault("connector.class")
                    ?? throw new RpcException(new Status(StatusCode.InvalidArgument, "Missing connector.class"));
                return await connectWorker.CreateConnectorAsync(name, connectorClass, cfg);
            },
            deleteConnector: name => connectWorker.StopConnectorAsync(name),
            updateConnectorConfig: async (name, cfg) =>
            {
                await connectWorker.RestartConnectorAsync(name);
                var info = connectWorker.GetConnectorStatus(name);
                if (info == null) return null;
                return new ConnectorInfoDto(
                    info.Name, info.Type, info.State, info.WorkerId,
                    new Dictionary<string, string>(info.Config),
                    info.Tasks.Select(t => new TaskStatusDto(t.Id, t.State, t.WorkerId, null)).ToList());
            },
            restartConnector: (name, _, _) => connectWorker.RestartConnectorAsync(name),
            pauseConnector: name => connectWorker.PauseConnectorAsync(name),
            resumeConnector: name => connectWorker.ResumeConnectorAsync(name),
            restartConnectorTask: (connector, taskId) => connectWorker.RestartTaskAsync(connector, taskId),
            listConnectorPlugins: (includeSink, includeSource) =>
            {
                return pluginDiscovery.GetAllPlugins()
                    .Where(p => (includeSink && p.Type.Equals("sink", StringComparison.OrdinalIgnoreCase)) ||
                               (includeSource && p.Type.Equals("source", StringComparison.OrdinalIgnoreCase)) ||
                               (!includeSink && !includeSource))
                    .Select(p => new ConnectorPluginDto(p.Class, p.Type, p.Version))
                    .ToList();
            });

        // REST + gRPC
        app.MapGrpcService<ConnectServiceImpl>();
        app.MapSurgewavePipelines();
        app.Logger.LogInformation("Pipeline REST API mapped at /api/pipelines");

        programLogger.LogInformation("  - Connect API:         {Host}:{GrpcPort}/connectors (gRPC + REST + Pipelines)",
            config.Host, config.GrpcPort);
    }
}
