using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Distributed;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;
using Microsoft.Extensions.Logging;

var builder = WebApplication.CreateBuilder(args);

// Parse command line arguments
var bootstrapServers = args.Length > 0 ? args[0] : "localhost:9092";
var restPort = 8083;
string? pluginPathArg = null;     // null = not set on the command line
var protocol = "auto"; // auto, surgewave, kafka
var distributed = false;

for (int i = 1; i < args.Length; i++)
{
    if (args[i] == "--port" && i + 1 < args.Length)
        restPort = int.Parse(args[++i]);
    else if (args[i] == "--plugin-path" && i + 1 < args.Length)
        pluginPathArg = args[++i];
    else if (args[i] == "--protocol" && i + 1 < args.Length)
        protocol = args[++i].ToLowerInvariant();
    else if (args[i] == "--distributed")
        distributed = true;
}

// Resolve the plugins directory with the broker's precedence:
//   1. --plugin-path CLI argument (highest)
//   2. Surgewave:PluginsDirectory in IConfiguration (appsettings.json + env vars)
//   3. ./plugins relative to the working directory (lowest)
// Builder.Configuration is already populated with appsettings/env/CLI sources at
// this point, so reading the setting here gives the same effective value the
// broker would see.
var pluginPath = pluginPathArg
    ?? builder.Configuration["Surgewave:PluginsDirectory"]
    ?? "./plugins";

// Layer plugin-bundled defaults BENEATH the worker's own appsettings.json so
// plugin authors can ship recommended defaults that user values still override.
builder.Configuration.AddPluginDefaults(Path.GetFullPath(pluginPath));

// Configure Kestrel
builder.WebHost.UseUrls($"http://localhost:{restPort}");

// Add logging
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

// Add gRPC with JSON transcoding
builder.Services.AddGrpc().AddJsonTranscoding();

// Add services
builder.Services.AddSingleton<PluginLoader>();
builder.Services.AddSingleton<PluginDiscovery>();

// Register ConnectServiceImpl with a holder pattern for late binding
builder.Services.AddSingleton(_ => ConnectServiceHolder.Instance
    ?? throw new InvalidOperationException("ConnectServiceImpl not initialized"));

var app = builder.Build();
var logger = app.Services.GetRequiredService<ILogger<Program>>();

logger.LogInformation("Surgewave Connect Worker v0.1.0 — {Mode} Kafka Connect runtime",
    distributed ? "Distributed" : "Standalone");

// Initialize plugin discovery
var pluginLoader = app.Services.GetRequiredService<PluginLoader>();
var pluginDiscovery = app.Services.GetRequiredService<PluginDiscovery>();

// Scan plugin directories
foreach (var dir in pluginPath.Split(';', StringSplitOptions.RemoveEmptyEntries))
{
    var fullPath = Path.GetFullPath(dir);
    if (Directory.Exists(fullPath))
    {
        logger.LogInformation("Scanning plugin directory: {Path}", fullPath);
        pluginDiscovery.DiscoverPlugins(fullPath);
    }
}

// All connectors (including built-in) are discovered from plugin directories only

var plugins = pluginDiscovery.GetAllPlugins();
logger.LogInformation("Discovered {Count} connector plugin(s)", plugins.Count);

// Create Surgewave client
logger.LogInformation("Connecting to broker: {Servers}", bootstrapServers);
var clientBuilder = SurgewaveClient.Create(bootstrapServers)
    .WithClientId($"surgewave-connect-{Environment.MachineName}");

if (protocol == "surgewave")
    clientBuilder.UseSurgewaveProtocol();
else if (protocol == "kafka")
    clientBuilder.UseKafkaProtocol();
else
    clientBuilder.UseAutoDetect();

var surgewaveClient = await clientBuilder.BuildAsync();
await surgewaveClient.ConnectAsync();
logger.LogInformation("Connected using {Protocol} protocol", surgewaveClient.Protocol);

// Create Connect worker config
var workerConfig = new ConnectWorkerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = "surgewave-connect",
    ConfigTopic = "_connect-configs",
    OffsetsTopic = "_connect-offsets",
    StatusTopic = "_connect-status",
    DistributedMode = distributed
};

// Create Connect worker
var workerServices = new ConnectWorkerServices
{
    SchemaRegistry = surgewaveClient.NativeClient?.Schema
};
await using var connectWorker = new ConnectWorker(workerConfig, surgewaveClient, app.Services.GetRequiredService<ILogger<ConnectWorker>>(), workerServices, ownsClient: true);
connectWorker.SetTypeResolver(className => pluginDiscovery.LoadPluginType(className));

// Create worker coordinator for distributed mode
WorkerCoordinator? coordinator = null;
if (distributed)
{
    var restUrl = $"http://localhost:{restPort}";
    coordinator = new WorkerCoordinator(workerConfig, restUrl, app.Services.GetRequiredService<ILogger<WorkerCoordinator>>());
    coordinator.SetSurgewaveClient(surgewaveClient);

    // Handle task assignments from coordinator
    coordinator.TasksAssigned += async (_, e) =>
    {
        logger.LogInformation("Received task assignment for connector {Connector}", e.Assignment.ConnectorName);
        // In distributed mode, the coordinator manages which connectors run on which worker
    };

    coordinator.TasksRevoked += (_, e) =>
    {
        logger.LogInformation("Tasks revoked: {Tasks}", string.Join(", ", e.TaskIds));
    };
}

// Create and register gRPC service with delegates wired to ConnectWorker
ConnectServiceHolder.Instance = new ConnectServiceImpl(
    listConnectors: () => connectWorker.ListConnectors().ToList(),
    getConnector: name =>
    {
        var status = connectWorker.GetConnectorStatus(name);
        if (status == null) return null;
        return new ConnectorInfoDto(
            status.Name,
            status.Type,
            status.State,
            status.WorkerId,
            new Dictionary<string, string>(status.Config),
            status.Tasks.Select(t => new TaskStatusDto(t.Id, t.State, t.WorkerId, t.Trace)).ToList());
    },
    createConnector: async (name, config) =>
    {
        if (!config.TryGetValue("connector.class", out var connectorClass))
            throw new ArgumentException("Missing connector.class in config");
        return await connectWorker.CreateConnectorAsync(name, connectorClass, config);
    },
    deleteConnector: connectWorker.StopConnectorAsync,
    updateConnectorConfig: async (name, config) =>
    {
        // For now, we don't support config updates - would need to stop and recreate
        var status = connectWorker.GetConnectorStatus(name);
        if (status == null) return null;
        return new ConnectorInfoDto(
            status.Name,
            status.Type,
            status.State,
            status.WorkerId,
            new Dictionary<string, string>(status.Config),
            status.Tasks.Select(t => new TaskStatusDto(t.Id, t.State, t.WorkerId, t.Trace)).ToList());
    },
    restartConnector: async (name, includeTasks, onlyFailed) => await connectWorker.RestartConnectorAsync(name),
    pauseConnector: connectWorker.PauseConnectorAsync,
    resumeConnector: connectWorker.ResumeConnectorAsync,
    restartConnectorTask: connectWorker.RestartTaskAsync,
    listConnectorPlugins: (includeSink, includeSource) =>
    {
        var allPlugins = pluginDiscovery.GetAllPlugins();
        return allPlugins
            .Where(p => (includeSink && p.Type == "sink") || (includeSource && p.Type == "source") || (!includeSink && !includeSource))
            .Select(p => new ConnectorPluginDto(p.Class, p.Type, p.Version))
            .ToList();
    });

// Map gRPC service with JSON transcoding (REST at /v3/connectors)
app.MapGrpcService<ConnectServiceImpl>();

// Root endpoint for health/info
app.MapGet("/", () => Results.Ok(new
{
    version = "0.1.0",
    protocol = surgewaveClient.Protocol.ToString().ToLowerInvariant(),
    grpc = true,
    distributed,
    workerId = coordinator?.WorkerId,
    isLeader = coordinator?.IsLeader,
    workers = coordinator?.Workers.Count
}));

// Endpoint to list workers (distributed mode)
app.MapGet("/v3/workers", () =>
{
    if (coordinator == null)
        return Results.Ok(new { workers = Array.Empty<object>(), mode = "standalone" });

    return Results.Ok(new
    {
        workers = coordinator.Workers.Select(w => new
        {
            workerId = w.WorkerId,
            restUrl = w.RestUrl,
            connectors = w.AssignedConnectors
        }),
        mode = "distributed",
        leader = coordinator.LeaderId
    });
});

// Start the Connect worker
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

await connectWorker.StartAsync(cts.Token);

// Start coordinator in distributed mode
if (coordinator != null)
{
    await coordinator.StartAsync(cts.Token);
}

logger.LogInformation("Surgewave Connect worker ready");
logger.LogInformation("  Mode: {Mode}", distributed ? "Distributed" : "Standalone");
if (coordinator != null)
{
    logger.LogInformation("  Worker ID: {WorkerId}", coordinator.WorkerId);
}
logger.LogInformation("  Bootstrap servers: {Servers}", bootstrapServers);
logger.LogInformation("  Protocol: {Protocol}", surgewaveClient.Protocol);
logger.LogInformation("  REST API: http://localhost:{Port}/v3/connectors", restPort);
logger.LogInformation("  gRPC API: http://localhost:{Port} (ConnectService)", restPort);
logger.LogInformation("Press Ctrl+C to shutdown");

try
{
    await app.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // Expected on shutdown
}

// Graceful shutdown
if (coordinator != null)
{
    await coordinator.LeaveGroupAsync();
    await coordinator.DisposeAsync();
}

await connectWorker.StopAsync();
logger.LogInformation("Surgewave Connect worker stopped");

return 0;

/// <summary>
/// Holder for late-bound ConnectServiceImpl instance.
/// </summary>
static class ConnectServiceHolder
{
    public static ConnectServiceImpl? Instance { get; set; }
}
