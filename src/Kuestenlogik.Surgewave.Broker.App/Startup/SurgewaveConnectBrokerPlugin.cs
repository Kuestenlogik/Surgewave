using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Packaging;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Connect.Pipelines.Chat;
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
/// The worker is a client of the broker it runs in, so it starts once that broker accepts clients, beside the host's
/// start and not as a step of it (#268). <see cref="ConfigureAsync"/> maps the endpoints, which a host can only do
/// before it starts, and returns; the client connection, the worker and the pipelines follow on the listener's accept
/// signal. It used to do all of it before the host went on: a broker accepts clients only once it has caught up with
/// its metadata log, so one that waited for a quorum waited inside this plugin, with its HTTP port unbound and its
/// liveness probe unanswered, and one whose worker waited for a topic was never ready. Until the worker has started,
/// the Connect and pipeline endpoints answer 503.
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
                    ?? brokerConfig.Connect.PluginsDirectory ?? ""
            };
        });

        services.AddSingleton<PluginLoader>();
        services.AddSingleton<PluginDiscovery>();
        services.AddSurgewavePipelines();
    }

    // The worker once it has started; the Connect service answers UNAVAILABLE, which REST transcodes to 503, before.
    private volatile ConnectWorker? _worker;

    private ConnectWorker Worker() => _worker
        ?? throw new RpcException(new Status(StatusCode.Unavailable, "The Connect worker has not started yet"));

    /// <inheritdoc />
    public Task ConfigureAsync(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var config = services.GetRequiredService<BrokerConfig>();
        var connectConfig = services.GetRequiredService<ConnectWorkerConfig>();
        var programLogger = services.GetRequiredService<ILogger<Program>>();

        // Discover connectors from plugins directory
        var pluginDiscovery = services.GetRequiredService<PluginDiscovery>();
        // Scan every connector scope, not one working-directory-relative path. The
        // configured value still wins and still means that directory alone.
        var connectorDirectories = SurgewaveConnectorDirectories.SearchOrder(
            connectConfig.PluginsDirectory);

        foreach (var directory in connectorDirectories)
        {
            if (!Directory.Exists(directory)) continue;

            app.Logger.LogInformation("Scanning for connectors: {Directory}", directory);
            pluginDiscovery.DiscoverPlugins(directory, useDefaultContext: false);
        }

        // Wire gRPC service holder. The service is resolved once, so it is this one from the start and reaches the
        // worker through Worker(), which refuses until the worker has started.
        ConnectServiceImplHolder.Instance = new ConnectServiceImpl(
            listConnectors: () => Worker().ListConnectors().ToList(),
            getConnector: name =>
            {
                var info = Worker().GetConnectorStatus(name);
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
                return await Worker().CreateConnectorAsync(name, connectorClass, cfg);
            },
            deleteConnector: name => Worker().StopConnectorAsync(name),
            updateConnectorConfig: async (name, cfg) =>
            {
                var worker = Worker();
                await worker.RestartConnectorAsync(name);
                var info = worker.GetConnectorStatus(name);
                if (info == null) return null;
                return new ConnectorInfoDto(
                    info.Name, info.Type, info.State, info.WorkerId,
                    new Dictionary<string, string>(info.Config),
                    info.Tasks.Select(t => new TaskStatusDto(t.Id, t.State, t.WorkerId, null)).ToList());
            },
            restartConnector: (name, _, _) => Worker().RestartConnectorAsync(name),
            pauseConnector: name => Worker().PauseConnectorAsync(name),
            resumeConnector: name => Worker().ResumeConnectorAsync(name),
            restartConnectorTask: (connector, taskId) => Worker().RestartTaskAsync(connector, taskId),
            listConnectorPlugins: (includeSink, includeSource) =>
            {
                return pluginDiscovery.GetAllPlugins()
                    .Where(p => (includeSink && p.Type.Equals("sink", StringComparison.OrdinalIgnoreCase)) ||
                               (includeSource && p.Type.Equals("source", StringComparison.OrdinalIgnoreCase)) ||
                               (!includeSink && !includeSource))
                    .Select(p => new ConnectorPluginDto(p.Class, p.Type, p.Version))
                    .ToList();
            });

        // REST + gRPC, mapped now: a host maps its endpoints before it starts. /api/pipelines (+ /chat) answers 503
        // until the orchestrator is there.
        app.MapGrpcService<ConnectServiceImpl>();
        app.MapSurgewavePipelines();
        app.MapSurgewavePipelineChat();
        app.Logger.LogInformation("Pipeline REST API mapped at /api/pipelines (+ /chat)");

        programLogger.LogInformation("  - Connect API:         {Host}:{GrpcPort}/connectors (gRPC + REST + Pipelines)",
            config.Host, config.GrpcPort);

        // Not awaited: the host goes on to bind its HTTP port and to say ready without the worker.
        _ = StartOnceTheBrokerAcceptsAsync(app, services, pluginDiscovery);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Connects the worker's client, starts the worker and the pipelines, once the broker accepts clients. A start
    /// that fails stops the broker, as it did when it was a step of the host's start: a broker that runs with Connect
    /// enabled and no worker would answer for connectors it does not run.
    /// </summary>
    private async Task StartOnceTheBrokerAcceptsAsync(WebApplication app, IServiceProvider services, PluginDiscovery pluginDiscovery)
    {
        var stopping = app.Lifetime.ApplicationStopping;
        try
        {
            await services.GetRequiredService<ClientListenerSignal>().WaitUntilAcceptingAsync(stopping).ConfigureAwait(false);
            await StartAsync(app, services, pluginDiscovery, stopping).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // The broker stopped before it accepted a client, or while the worker started: nothing to run.
        }
        catch (Exception ex)
        {
            app.Logger.LogCritical(ex, "The Connect worker did not start; stopping the broker");
            Environment.ExitCode = 1;
            app.Lifetime.StopApplication();
        }
    }

    private async Task StartAsync(WebApplication app, IServiceProvider services, PluginDiscovery pluginDiscovery, CancellationToken stopping)
    {
        var config = services.GetRequiredService<BrokerConfig>();
        var connectConfig = services.GetRequiredService<ConnectWorkerConfig>();
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();

        // Pipeline metrics + worker services
        var pipelineMetrics = new PipelineMetricsCollector();
        var workerServices = new ConnectWorkerServices { MetricsCollector = pipelineMetrics };

        // Local loopback client to the broker's own listener. Uses the native Surgewave
        // protocol (same socket, magic-byte dispatched alongside the Kafka wire) so the
        // broker does not depend on the Kafka wire *client* (Client.Kafka) and its binary
        // stays Protocol.Kafka-free (#59). A loopback to the co-located broker never needs
        // the Kafka wire; native is also the faster path.
        var localClient = await SurgewaveClient
            .Create(config.LocalListenerAddress())
            .UseSurgewaveProtocol()
            .BuildAsync(stopping).ConfigureAwait(false);
        await localClient.ConnectAsync(stopping).ConfigureAwait(false);

        var connectLogger = loggerFactory.CreateLogger<ConnectWorker>();
#pragma warning disable CA2000 // Lifecycle managed by the application (runs until broker shutdown)
        var connectWorker = new ConnectWorker(connectConfig, localClient, connectLogger, workerServices, ownsClient: true);
#pragma warning restore CA2000
        connectWorker.SetTypeResolver(className => pluginDiscovery.LoadPluginType(className));
        await connectWorker.StartAsync(stopping).ConfigureAwait(false);
        _worker = connectWorker;
        app.Logger.LogInformation("Kafka Connect framework started");

        // Pipeline orchestrator
        var pipelineStore = services.GetRequiredService<PipelineStore>();
        var pipelineTopicManager = services.GetRequiredService<PipelineTopicManager>();
        var pipelineLogger = loggerFactory.CreateLogger<PipelineOrchestrator>();
        var pipelineOrchestrator = new PipelineOrchestrator(
            pipelineStore, pipelineTopicManager, connectWorker, pluginDiscovery, pipelineLogger, pipelineMetrics);
        await pipelineOrchestrator.InitializeAsync(stopping).ConfigureAwait(false);

        // Pipeline chat (agent pipelines): the manager behind /api/pipelines/{id}/chat. Without the holder
        // assignment every chat endpoint answers 503.
        PipelineChatManagerHolder.Instance = new PipelineChatManager(connectConfig, loggerFactory.CreateLogger<PipelineChatManager>());

        // Last: from here /api/pipelines answers, with the pipelines loaded.
        PipelineOrchestratorHolder.Instance = pipelineOrchestrator;
        app.Logger.LogInformation("Pipeline Orchestration framework started");
    }
}
