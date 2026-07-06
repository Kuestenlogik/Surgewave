using System.Runtime;
using Kuestenlogik.Bowire;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Handlers;
using Kuestenlogik.Surgewave.Broker.Plugins;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Native.Coordination;
using Kuestenlogik.Surgewave.Broker.KeyValue;
using Kuestenlogik.Surgewave.Broker.Queue;
using Kuestenlogik.Surgewave.Broker.Startup;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Clustering.GeoReplication;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;
using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Broker.CruiseControl;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine;
using Kuestenlogik.Surgewave.Storage.Engine.FileSystem;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Storage.Engine.ObjectStore;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using Grpc.Core;
using Kuestenlogik.Surgewave.Core.Telemetry;
using Kuestenlogik.Surgewave.Api.GraphQL;
// Enterprise plugin: Kuestenlogik.Surgewave.Replication
using Kuestenlogik.Surgewave.Clustering.Upgrades;
using Kuestenlogik.Surgewave.Wasm;
using Kuestenlogik.Surgewave.Broker.IntentConfig;
using Kuestenlogik.Surgewave.Broker.Quotas;
using Kuestenlogik.Surgewave.Broker.Transactions;
using Kuestenlogik.Surgewave.Plugins.Licensing;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

// --- Plugin defaults layering ---
// Each installed plugin may ship a default-settings file next to its DLLs (the
// filename is whatever the plugin's plugin.json manifest declares via pluginSettings,
// defaulting to "pluginsettings.json"). They get layered in as the LOWEST-priority
// sources so plugin defaults take effect only when the user has not overridden them:
//
//   tier 1 (lowest)  — plugin defaults from plugins/<id>/<settings-file>   (this call)
//   tier 2           — broker appsettings.json + appsettings.{Env}.json    (already added)
//   tier 3 (highest) — environment variables + command-line args           (already added)
var brokerPluginsDir = builder.Configuration["Surgewave:PluginsDirectory"]
    ?? Path.Combine(AppContext.BaseDirectory, "plugins");

builder.Configuration.AddPluginDefaults(brokerPluginsDir);

// Signer verifier used by the plugin upload endpoint. Registry is a singleton over the
// plugins dir so AssemblyLoadContexts stay alive for the life of the host; the ISppSigner
// itself is rebuilt lazily from Surgewave:Plugins:Signer config.
builder.Services.AddSurgewavePluginSigner(builder.Configuration, brokerPluginsDir);

// --- Licensing ---
// Licensing: discover ILicenseProvider from loaded assemblies (e.g., Surgewave.Licensing).
// Must run before plugin activation so enterprise features can be gated.
// Without a provider, the broker runs as Community Edition.
var license = LicenseProviderDiscovery.Discover(builder.Services, builder.Configuration);

// Configure GC for low latency - reduces P99 latency spikes from GC pauses
// This is done early before most allocations occur
// Read from config if available, otherwise use SustainedLowLatency as default
var gcLatencyMode = builder.Configuration.GetValue("Surgewave:Gc:LatencyMode", "SustainedLowLatency");
GCSettings.LatencyMode = gcLatencyMode switch
{
    "LowLatency" => GCLatencyMode.LowLatency,
    "SustainedLowLatency" => GCLatencyMode.SustainedLowLatency,
    "Batch" => GCLatencyMode.Batch,
    "NoGCRegion" => GCLatencyMode.NoGCRegion,
    _ => GCLatencyMode.Interactive
};

// Bind configuration from appsettings.json
builder.Services.Configure<BrokerConfig>(
    builder.Configuration.GetSection(BrokerConfig.SectionName));

// HttpClient factory — used by OAUTHBEARER JWKS refresh and any future
// outbound HTTP path. The factory manages handler lifetime so we don't
// fall foul of socket exhaustion (the well-known HttpClient gotcha).
builder.Services.AddHttpClient();

// KIP-714 client telemetry ingestor — singleton so its Meter lives for
// the broker's lifetime and DI handles Dispose on shutdown.
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor>();
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Broker.Telemetry.ITelemetryIngestor>(sp =>
    sp.GetRequiredService<Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor>());

// Optional: flip the gRPC / REST endpoint from cleartext HTTP to HTTPS without editing the
// Kestrel section manually. When Surgewave:GrpcUseTls=true, override the Grpc endpoint URL to
// https://*:{GrpcPort} and (if supplied) wire up the cert path / password. Falling back to
// the .NET dev certificate when no path is given keeps the toggle one-line for development.
// The existing Kestrel log-level suppression is lifted when TLS is on, since the HTTP/2-
// without-ALPN warning that motivated it no longer applies once ALPN is in use.
var grpcUseTls = builder.Configuration.GetValue("Surgewave:GrpcUseTls", defaultValue: false);
if (grpcUseTls)
{
    var grpcPort = builder.Configuration.GetValue("Surgewave:GrpcPort", defaultValue: 9093);
    builder.Configuration["Kestrel:Endpoints:Grpc:Url"] = $"https://*:{grpcPort}";

    var certPath = builder.Configuration["Surgewave:GrpcCertificatePath"];
    if (!string.IsNullOrEmpty(certPath))
    {
        builder.Configuration["Kestrel:Endpoints:Grpc:Certificate:Path"] = certPath;
        var certPassword = builder.Configuration["Surgewave:GrpcCertificatePassword"];
        if (!string.IsNullOrEmpty(certPassword))
            builder.Configuration["Kestrel:Endpoints:Grpc:Certificate:Password"] = certPassword;
    }

    // HTTP/2 needs ALPN, which only works over TLS. The suppression in appsettings.json
    // was needed to silence Kestrel's cleartext-HTTP/2 warning; with HTTPS we want normal
    // Kestrel logging back.
    builder.Configuration["Logging:LogLevel:Microsoft.AspNetCore.Server.Kestrel"] = "Warning";
}

// Register BrokerConfig as singleton for direct injection
builder.Services.AddSingleton(sp =>
    sp.GetRequiredService<IOptions<BrokerConfig>>().Value);

// Register log segment factory based on storage engine.
// Community engines (File, Memory, ZeroCopy*) are built-in.
// Enterprise engines (Arrow, DuckDb, Parquet, NvmeDirect) are discovered via IStorageEnginePlugin.
builder.Services.AddSingleton<ILogSegmentFactory>(sp =>
{
    var config = sp.GetRequiredService<BrokerConfig>();
    var engineName = config.StorageEngine;

    // Built-in community storage engines (case-insensitive)
    ILogSegmentFactory? factory = engineName switch
    {
        _ when string.Equals(engineName, StorageEngines.Memory, StringComparison.OrdinalIgnoreCase) => new MemoryLogSegmentFactory(),
        _ when string.Equals(engineName, StorageEngines.File, StringComparison.OrdinalIgnoreCase) => FileLogSegmentFactory.Create(useMmap: true),
        _ when string.Equals(engineName, StorageEngines.ZeroCopyWal, StringComparison.OrdinalIgnoreCase) => FileLogSegmentFactory.Create(useMmap: true),
        _ when string.Equals(engineName, StorageEngines.ZeroCopyMemory, StringComparison.OrdinalIgnoreCase) => ZeroCopyMemoryLogSegmentFactory.Create(),
        _ => null
    };

    if (factory is not null)
        return factory;

    // Try plugin-provided storage engines (Arrow, DuckDb, Parquet, NvmeDirect)
    var storagePlugins = BrokerPluginActivator.Discover<IStorageEnginePlugin>();
    var match = storagePlugins.FirstOrDefault(p =>
        p.SupportedModes.Any(m => m.Equals(engineName, StringComparison.OrdinalIgnoreCase)));

    if (match is not null)
        return match.CreateFactory(engineName, builder.Configuration);

    throw new InvalidOperationException(
        $"Storage engine '{engineName}' requires a storage engine plugin. " +
        $"Install the corresponding .swpkg package to the plugins directory.");
});

// Shared log manager for both protocols
builder.Services.AddSingleton(sp =>
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

// Kestrel endpoint configuration from appsettings.json (Kestrel section)
// Dev HTTPS: change Url to "https://*:9093" --— uses .NET dev certificate (dotnet dev-certs https --trust)
// Prod HTTPS: add Certificate:Path + Password to the endpoint
// HTTP/3: set Protocols to "Http1AndHttp2AndHttp3" (requires HTTPS)

builder.Services.AddGrpc().AddJsonTranscoding();
builder.Services.AddGrpcReflection();

// gRPC services are registered with late-bound dependencies via holders
builder.Services.AddSingleton(_ => ProducerServiceImplHolder.Instance
    ?? throw new InvalidOperationException("ProducerServiceImpl not initialized"));
builder.Services.AddSingleton(_ => ConsumerServiceImplHolder.Instance
    ?? throw new InvalidOperationException("ConsumerServiceImpl not initialized"));
builder.Services.AddSingleton(_ => TopicServiceImplHolder.Instance
    ?? throw new InvalidOperationException("TopicServiceImpl not initialized"));
builder.Services.AddSingleton(_ => AdminServiceImplHolder.Instance
    ?? throw new InvalidOperationException("AdminServiceImpl not initialized"));
builder.Services.AddSingleton(_ => ConsumerGroupServiceImplHolder.Instance
    ?? throw new InvalidOperationException("ConsumerGroupServiceImpl not initialized"));
builder.Services.AddSingleton(_ => ClusterServiceImplHolder.Instance
    ?? throw new InvalidOperationException("ClusterServiceImpl not initialized"));
builder.Services.AddSingleton(_ => TransactionServiceImplHolder.Instance
    ?? throw new InvalidOperationException("TransactionServiceImpl not initialized"));
builder.Services.AddSingleton(_ => QuotaServiceImplHolder.Instance
    ?? throw new InvalidOperationException("QuotaServiceImpl not initialized"));
builder.Services.AddSingleton(_ => SecurityServiceImplHolder.Instance
    ?? throw new InvalidOperationException("SecurityServiceImpl not initialized"));
builder.Services.AddSingleton(_ => SchemaRegistryServiceImplHolder.Instance
    ?? throw new InvalidOperationException("SchemaRegistryServiceImpl not initialized"));
builder.Services.AddSingleton(_ => ConnectServiceImplHolder.Instance
    ?? throw new InvalidOperationException("ConnectServiceImpl not initialized"));

// Add health checks
builder.Services.AddHealthChecks();

// JWT auth for the HTTP REST surface (#37). Config-gated (default off) so
// existing deployments keep their open admin API; when enabled, management
// endpoints require a valid bearer token. Reuses the OAuth2 issuer/audience.
var restApiAuthConfig = builder.Configuration
    .GetSection($"{BrokerConfig.SectionName}:Security:RestApiAuth").Get<RestApiAuthConfig>() ?? new RestApiAuthConfig();
var restApiOAuth2Config = builder.Configuration
    .GetSection($"{BrokerConfig.SectionName}:Security:OAuth2").Get<OAuth2Config>() ?? new OAuth2Config();
builder.Services.AddSurgewaveRestApiAuth(restApiAuthConfig, restApiOAuth2Config);

// SQL Query Service
builder.Services.Configure<Kuestenlogik.Surgewave.Broker.Sql.SqlServiceConfig>(
    builder.Configuration.GetSection(Kuestenlogik.Surgewave.Broker.Sql.SqlServiceConfig.SectionName));
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Broker.Sql.SurgewaveSqlService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<Kuestenlogik.Surgewave.Broker.Sql.SurgewaveSqlService>());

// Add Schema Registry if enabled (done early so config is available)
// The actual services will be registered after BrokerConfig is available

// Configure OpenTelemetry
var telemetryConfig = builder.Configuration
    .GetSection(TelemetryConfig.SectionName)
    .Get<TelemetryConfig>() ?? new TelemetryConfig { ServiceName = "Kuestenlogik.Surgewave.Broker" };

// Override with standard OTEL env vars if present
if (Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT") is { } otlpEndpoint)
{
    telemetryConfig.Otlp.Enabled = true;
    telemetryConfig.Otlp.Endpoint = otlpEndpoint;
}
if (Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_PROTOCOL") is { } otlpProtocol)
{
    telemetryConfig.Otlp.Protocol = otlpProtocol.Equals("http/protobuf", StringComparison.OrdinalIgnoreCase)
        ? "HttpProtobuf" : "Grpc";
}

var otelBuilder = builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource
        .AddService(
            serviceName: telemetryConfig.ServiceName,
            serviceVersion: telemetryConfig.ServiceVersion)
        .AddAttributes(new Dictionary<string, object>
        {
            ["deployment.environment"] = builder.Environment.EnvironmentName
        }));

// Configure Metrics
otelBuilder.WithMetrics(metrics =>
{
    metrics
        .AddMeter(BrokerMetrics.MeterName)
        .AddAspNetCoreInstrumentation();

    if (telemetryConfig.Prometheus.Enabled)
    {
        metrics.AddPrometheusExporter();
    }

    if (telemetryConfig.Otlp.Enabled)
    {
        metrics.AddOtlpExporter(options =>
        {
            options.Endpoint = new Uri(telemetryConfig.Otlp.Endpoint);
            options.Protocol = telemetryConfig.Otlp.Protocol.Equals("HttpProtobuf", StringComparison.OrdinalIgnoreCase)
                ? OtlpExportProtocol.HttpProtobuf
                : OtlpExportProtocol.Grpc;
            options.TimeoutMilliseconds = telemetryConfig.Otlp.TimeoutMs;
            if (!string.IsNullOrEmpty(telemetryConfig.Otlp.Headers))
            {
                options.Headers = telemetryConfig.Otlp.Headers;
            }
        });
    }
});

// Configure Tracing
otelBuilder.WithTracing(tracing =>
{
    if (telemetryConfig.Tracing.SamplingRatio < 1.0)
    {
        tracing.SetSampler(new TraceIdRatioBasedSampler(telemetryConfig.Tracing.SamplingRatio));
    }

    tracing.AddSource(BrokerMetrics.ActivitySourceName);

    if (telemetryConfig.Tracing.IncludeAspNetCore)
    {
        tracing.AddAspNetCoreInstrumentation(options => options.RecordException = true);
    }

    if (telemetryConfig.Otlp.Enabled)
    {
        tracing.AddOtlpExporter(options =>
        {
            options.Endpoint = new Uri(telemetryConfig.Otlp.Endpoint);
            options.Protocol = telemetryConfig.Otlp.Protocol.Equals("HttpProtobuf", StringComparison.OrdinalIgnoreCase)
                ? OtlpExportProtocol.HttpProtobuf
                : OtlpExportProtocol.Grpc;
            options.TimeoutMilliseconds = telemetryConfig.Otlp.TimeoutMs;
            if (!string.IsNullOrEmpty(telemetryConfig.Otlp.Headers))
            {
                options.Headers = telemetryConfig.Otlp.Headers;
            }
        });
    }
});

// Connect + Pipeline Orchestration: now activated as an IBrokerPlugin
// (SurgewaveConnectBrokerPlugin) via BrokerPluginActivator.

// Schema Registry: now activated as an IBrokerPlugin (SurgewaveSchemaRegistryBrokerPlugin) via
// BrokerPluginActivator.ActivatePlugins + ConfigureBrokerPlugins. The plugin's
// ConfigureServices() registers all schema store + handlers + inference + evolution +
// migration + linking services, and Configure() wires the gRPC holder + maps endpoints.

// ── Protocol Plugins (IProtocolPlugin discovery) ─────────────────────────────
// Scans loaded assemblies for IProtocolPlugin implementations (MQTT, WebSocket, AMQP, etc.)
var activatedProtocols = BrokerPluginActivator.ActivateProtocols(
    builder.Services, builder.Configuration);


// --- Enterprise Broker Plugins (IBrokerPlugin discovery) ---
// Scans loaded assemblies for IBrokerPlugin implementations, checks config + license,
// and calls ConfigureServices on each enabled plugin. Replaces per-feature if-blocks.
var activatedPlugins = BrokerPluginActivator.ActivatePlugins(
    builder.Services, builder.Configuration, license);
var activatedFeatures = new HashSet<string>(activatedPlugins.Select(p => p.FeatureId));

// Add QueueView semantics (always-on; provides RabbitMQ-style visibility timeouts)
{
    var queueViewConfig = new QueueViewConfig();
    builder.Configuration.GetSection(QueueViewConfig.SectionName).Bind(queueViewConfig);
    builder.Services.AddSingleton(queueViewConfig);
    builder.Services.AddSingleton<QueueViewManager>();
    builder.Services.AddSingleton<IQueueViewManager>(sp => sp.GetRequiredService<QueueViewManager>());
}

// Add AMQP 0.9.1 protocol adapter (disabled by default --— enable via Surgewave:Amqp:Enabled=true)

// Register core broker singletons into DI so IBrokerPlugin implementations can
// resolve them in their Configure() phase. This is the incremental DI-migration:
// the objects are still used as local variables below (via GetRequiredService), but
// plugins no longer need Program.cs locals passed in as constructor args.
builder.Services.AddSingleton<BrokerMetrics>();
builder.Services.AddSingleton<ClusterState>();
// Singleton, weil er cluster.id auf Platte persistiert — zwei Instanzen
// wären ein Race auf dieselbe Datei.
builder.Services.AddSingleton<ClusterIdManager>();

// Broker-side observability — a single channel-multiplexer that
// lets in-process consumers (Bowire's surgewave://embedded tap is the
// reference one) subscribe to pipeline events. Registered once as
// the concrete type and re-exposed as the public interface so
// pipeline code can call .Publish() directly while external
// consumers only see ObserveAsync().
// Config-driven: Surgewave:Observability:Enabled=false skips registration entirely so the
// hot-path `_observability?.HasSubscribers == true` check short-circuits on null. When
// enabled, SurgewaveBrokerObservability is registered as both the concrete publisher-facing
// type and the consumer-facing interface.
Kuestenlogik.Surgewave.Core.Observability.SurgewaveObservabilityExtensions.AddSurgewaveBrokerObservability(
    builder.Services, builder.Configuration);
builder.Services.AddSingleton(sp => new DynamicBrokerConfig(
    sp.GetRequiredService<BrokerConfig>(),
    sp.GetRequiredService<ILogger<DynamicBrokerConfig>>()));

// Clustering + reassignment chain — registered as DI factory singletons so they
// resolve lazily (after app.Build) and IBrokerPlugin.Configure() can depend on them.
builder.Services.AddSingleton(sp =>
{
    var c = sp.GetRequiredService<BrokerConfig>();
    return ClusteringConfig.Create(
        brokerId: c.BrokerId, host: c.Host, port: c.Port, rack: c.Rack,
        clusterId: c.ClusterId, dataDirectory: c.DataDirectory,
        clusterNodes: c.ClusterNodes, replicationPort: c.ReplicationPort,
        minInSyncReplicas: c.MinInSyncReplicas,
        allowAutoLeaderRebalance: c.AllowAutoLeaderRebalance,
        leaderImbalanceCheckIntervalSeconds: c.LeaderImbalanceCheckIntervalSeconds,
        controlledShutdownMaxRetries: c.ControlledShutdownMaxRetries,
        heartbeatIntervalMs: c.HeartbeatIntervalMs,
        heartbeatTimeoutMs: c.HeartbeatTimeoutMs,
        maxHeartbeatFailures: c.MaxHeartbeatFailures,
        useRaftConsensus: c.UseRaftConsensus,
        raftDataDirectory: c.RaftDataDirectory,
        raftElectionTimeoutMinMs: c.RaftElectionTimeoutMinMs,
        raftElectionTimeoutMaxMs: c.RaftElectionTimeoutMaxMs,
        raftHeartbeatIntervalMs: c.RaftHeartbeatIntervalMs,
        raftPeerDiscoveryTimeoutSeconds: c.RaftPeerDiscoveryTimeoutSeconds,
        autoRebalanceEnabled: c.AutoRebalanceEnabled,
        rebalanceCheckIntervalSeconds: c.RebalanceCheckIntervalSeconds,
        rebalanceImbalanceThreshold: c.RebalanceImbalanceThreshold,
        reassignmentThrottleBytesPerSec: c.ReassignmentThrottleBytesPerSec,
        reassignmentMaxConcurrent: c.ReassignmentMaxConcurrent,
        interBrokerTransport: c.InterBrokerTransport,
        interBrokerCertificatePath: c.InterBrokerCertificatePath,
        interBrokerCertificatePassword: c.InterBrokerCertificatePassword,
        interBrokerCaCertificatePath: c.InterBrokerCaCertificatePath);
});
// Inter-broker transport (TCP by default, QUIC when Surgewave:InterBrokerTransport=quic).
// Explicitly invoke both registrations so the module initializers run even if no
// other call site in this assembly forces the TCP/QUIC assemblies to be loaded.
// Idempotent — safe to call repeatedly.
Kuestenlogik.Surgewave.Transport.Tcp.TcpTransportRegistration.Register();
if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
{
    Kuestenlogik.Surgewave.Transport.Quic.QuicTransportRegistration.Register();
}

builder.Services.AddSingleton<Kuestenlogik.Surgewave.Transport.IPeerTransport>(sp =>
{
    var cfg = sp.GetRequiredService<ClusteringConfig>();
    var bootLogger = sp.GetRequiredService<ILogger<Program>>();
    var requested = cfg.InterBrokerTransport;

    // Pre-check QUIC-on-this-host before even entering the factory, so we can
    // emit a precise warning pointing at msquic rather than a generic
    // PlatformNotSupportedException from deep in the listener.
    if (string.Equals(requested, "quic", StringComparison.OrdinalIgnoreCase))
    {
        var quicSupported = (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            && System.Net.Quic.QuicListener.IsSupported
            && System.Net.Quic.QuicConnection.IsSupported;

        if (!quicSupported)
        {
            bootLogger.LogWarning(
                "Surgewave:InterBrokerTransport is set to 'quic' but QUIC is not available on this host. "
                + "Install libmsquic on Linux, or run on Windows 11 / Windows Server 2022+. "
                + "Falling back to TCP — this broker will not participate in a QUIC cluster.");
            requested = "tcp";
        }
        else if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            // Feed cert config into the static properties the transport uses
            // during connect/listen. When both broker cert and CA are set,
            // mTLS is active and TrustAllCertificates is ignored.
            Kuestenlogik.Surgewave.Transport.Quic.QuicPeerTransport.BrokerCertificatePath = cfg.InterBrokerCertificatePath;
            Kuestenlogik.Surgewave.Transport.Quic.QuicPeerTransport.BrokerCertificatePassword = cfg.InterBrokerCertificatePassword;
            Kuestenlogik.Surgewave.Transport.Quic.QuicPeerTransport.CaCertificatePath = cfg.InterBrokerCaCertificatePath;

            if (Kuestenlogik.Surgewave.Transport.Quic.QuicPeerTransport.HasMutualTlsConfig)
            {
                bootLogger.LogInformation(
                    "QUIC inter-broker mTLS enabled — broker cert: {Cert}, CA: {Ca}",
                    cfg.InterBrokerCertificatePath, cfg.InterBrokerCaCertificatePath);
            }
            else
            {
                bootLogger.LogWarning(
                    "QUIC inter-broker transport is enabled without mTLS. Using TrustAllCertificates "
                    + "as a dev fallback — NEVER run this in production. Configure "
                    + "Surgewave:InterBrokerCertificatePath + InterBrokerCaCertificatePath to enable mTLS.");
                Kuestenlogik.Surgewave.Transport.Quic.QuicPeerTransport.TrustAllCertificates = true;
            }
        }
    }

    var transport = Kuestenlogik.Surgewave.Transport.PeerTransportFactory.CreateWithFallback(requested, "tcp", out var fellBack);
    if (fellBack)
    {
        bootLogger.LogWarning(
            "Peer transport '{Requested}' not available — using 'tcp' fallback.", requested);
    }
    return transport;
});

builder.Services.AddSingleton(sp => new ReplicaManager(
    sp.GetRequiredService<ILogger<ReplicaManager>>(),
    sp.GetRequiredService<ClusterState>(),
    sp.GetRequiredService<LogManager>(),
    sp.GetRequiredService<ClusteringConfig>(),
    sp.GetRequiredService<Kuestenlogik.Surgewave.Transport.IPeerTransport>(),
    sp.GetRequiredService<BrokerMetrics>()));
builder.Services.AddSingleton(sp => new ClusterController(
    sp.GetRequiredService<ILogger<ClusterController>>(),
    sp.GetRequiredService<ClusterState>(),
    sp.GetRequiredService<ReplicaManager>(),
    sp.GetRequiredService<ClusteringConfig>()));
builder.Services.AddSingleton(sp => new ReplicationServer(
    sp.GetRequiredService<ILogger<ReplicationServer>>(),
    sp.GetRequiredService<ClusterState>(),
    sp.GetRequiredService<LogManager>(),
    sp.GetRequiredService<ReplicaManager>(),
    sp.GetRequiredService<ClusteringConfig>(),
    sp.GetRequiredService<Kuestenlogik.Surgewave.Transport.IPeerTransport>()));
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentPlanner>();
builder.Services.AddSingleton(sp =>
{
    var c = sp.GetRequiredService<BrokerConfig>();
    return new Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentConfig
    {
        DefaultThrottleRateBytesPerSec = (int)c.ReassignmentThrottleBytesPerSec,
        MaxConcurrentReassignments = c.ReassignmentMaxConcurrent
    };
});
builder.Services.AddSingleton(sp => new Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager(
    sp.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>>(),
    sp.GetRequiredService<ClusterState>(),
    sp.GetRequiredService<ClusterController>(),
    sp.GetRequiredService<ReplicaManager>(),
    sp.GetRequiredService<LogManager>(),
    sp.GetRequiredService<ClusteringConfig>()));
builder.Services.AddSingleton(sp => new Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentExecutor(
    sp.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentExecutor>>(),
    sp.GetRequiredService<ClusterState>(),
    sp.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>(),
    sp.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentPlanner>(),
    sp.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentConfig>()));
builder.Services.AddSingleton(sp => new RecordBatchSerializer(
    sp.GetRequiredService<ILogger<RecordBatchSerializer>>()));
builder.Services.AddSingleton(sp => new BandwidthQuotaManager(
    sp.GetRequiredService<BrokerConfig>().BandwidthQuota,
    sp.GetRequiredService<ILogger<BandwidthQuotaManager>>()));

var app = builder.Build();

// Get dependencies
var logger = app.Services.GetRequiredService<ILogger<Program>>();
var config = app.Services.GetRequiredService<BrokerConfig>();
var logManager = app.Services.GetRequiredService<LogManager>();

// REST auth gate must sit ahead of the endpoint mappings below (#37).
app.UseSurgewaveRestApiAuth(config.Security.RestApiAuth, config.Security.OAuth2, logger);

// Apply SIMD configuration from appsettings
SimdBigEndian.MinBatchSize = config.SimdBatchThreshold;

logger.LogInformation("Surgewave Broker v{Version} — event streaming platform for .NET",
    Kuestenlogik.Surgewave.Clustering.Upgrades.BrokerVersion.Current);



logger.LogInformation("SIMD BigEndian: {Implementation} (threshold: {Threshold})",
    SimdBigEndian.Implementation, config.SimdBatchThreshold);
logger.LogInformation("Storage engine: {StorageEngine} ({Persistent})",
    config.StorageEngine, string.Equals(config.StorageEngine, StorageEngines.File, StringComparison.OrdinalIgnoreCase) ? "persistent" : "ephemeral");


// Register and initialize tiered storage
TieredStorageInitializer.RegisterProviders();
var tieredStorageManager = TieredStorageInitializer.Create(config, logger);

// Resolve the DI-registered singletons (all registered as factory singletons before
// app.Build so IBrokerPlugin.Configure can also resolve them via IServiceProvider).
var metrics = app.Services.GetRequiredService<BrokerMetrics>();
var clusterState = app.Services.GetRequiredService<ClusterState>();
var clusteringConfig = app.Services.GetRequiredService<ClusteringConfig>();
var replicaManager = app.Services.GetRequiredService<ReplicaManager>();
var clusterController = app.Services.GetRequiredService<ClusterController>();
var replicationServer = app.Services.GetRequiredService<ReplicationServer>();

// Initialize Raft consensus if enabled
RaftNode? raftNode = null;
RaftTransport? raftTransport = null;
RaftPersistence? raftPersistence = null;
if (config.UseRaftConsensus)
{
    logger.LogInformation("Initializing Raft consensus mode...");

    // Create Raft components
    var raftPersistenceLogger = app.Services.GetRequiredService<ILogger<RaftPersistence>>();
    raftPersistence = new RaftPersistence(raftPersistenceLogger, clusteringConfig);

    var raftTransportLogger = app.Services.GetRequiredService<ILogger<RaftTransport>>();
    var peerTransport = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Transport.IPeerTransport>();
    raftTransport = new RaftTransport(raftTransportLogger, clusterState, clusteringConfig, peerTransport);

    var stateMachineLogger = app.Services.GetRequiredService<ILogger<MetadataStateMachine>>();
    var stateMachine = new MetadataStateMachine(stateMachineLogger, clusterState);

    var raftNodeLogger = app.Services.GetRequiredService<ILogger<RaftNode>>();
    raftNode = new RaftNode(raftNodeLogger, clusteringConfig, raftPersistence, raftTransport, stateMachine);

    // Wire up Raft to replication components
    replicationServer.SetRaftNode(raftNode);
    clusterController.SetRaftNode(raftNode);

    logger.LogInformation("Raft consensus initialized (BrokerId={BrokerId}, DataDir={DataDir})",
        config.BrokerId, config.RaftDataDirectory);
}

// Initialize rolling upgrade infrastructure
var rollingUpgradeConfig = new RollingUpgradeConfig
{
    GracefulShutdownTimeout = TimeSpan.FromSeconds(config.ShutdownTimeoutSeconds),
};
var versionCompatibilityChecker = new VersionCompatibilityChecker();
var leadershipTransferLogger = app.Services.GetRequiredService<ILogger<LeadershipTransfer>>();
var leadershipTransfer = new LeadershipTransfer(
    leadershipTransferLogger, clusterState, clusterController, clusteringConfig, rollingUpgradeConfig);
var shutdownOrchestratorLogger = app.Services.GetRequiredService<ILogger<GracefulShutdownOrchestrator>>();
var shutdownOrchestrator = new GracefulShutdownOrchestrator(
    shutdownOrchestratorLogger, clusteringConfig, rollingUpgradeConfig,
    clusterState, leadershipTransfer, raftNode);

logger.LogInformation("Rolling upgrade infrastructure initialized (version={Version})", BrokerVersion.Current);

// Start Surgewave broker (Kafka wire protocol over TCP)
var brokerLogger = app.Services.GetRequiredService<ILogger<SurgewaveBroker>>();
var serializerLogger = app.Services.GetRequiredService<ILogger<RecordBatchSerializer>>();
var coordinatorLogger = app.Services.GetRequiredService<ILogger<ConsumerGroupCoordinator>>();
var txnCoordinatorLogger = app.Services.GetRequiredService<ILogger<TransactionCoordinator>>();
var txnStateStoreLogger = app.Services.GetRequiredService<ILogger<TransactionStateStore>>();
var quotaManagerLogger = app.Services.GetRequiredService<ILogger<QuotaManager>>();
var recordBatchSerializer = app.Services.GetRequiredService<RecordBatchSerializer>();
var offsetStoreLogger = app.Services.GetRequiredService<ILogger<OffsetStore>>();
var offsetStore = new OffsetStore(config.DataDirectory, offsetStoreLogger);

// Register gRPC service implementations with required dependencies
ProducerServiceImplHolder.Instance = new ProducerServiceImpl(logManager, recordBatchSerializer.SerializeMessages);
ConsumerServiceImplHolder.Instance = new ConsumerServiceImpl(
    logManager,
    recordBatchSerializer.ParseRecordBatch,
    offsetStore.CommitOffset,
    (groupId, topic, partition) => (long?)offsetStore.GetCommittedOffset(groupId, topic, partition));
TopicServiceImplHolder.Instance = new TopicServiceImpl(logManager);
AdminServiceImplHolder.Instance = new AdminServiceImpl(
    config.BrokerId,
    config.Host,
    config.Port,
    config.GrpcPort,
    getPartitionInfo: (topic, partition) =>
    {
        var tp = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = topic, Partition = partition };
        var log = logManager.GetLog(tp);
        if (log == null) return null;
        return new PartitionInfoDto(
            partition,
            config.BrokerId,
            [config.BrokerId],
            [config.BrokerId],
            log.HighWatermark,
            log.LogStartOffset);
    },
    describeBrokerConfig: () =>
    {
        // Effektive Broker-Config aus DynamicBrokerConfig (statische Werte +
        // persistierte dynamische Overrides) statt des frueheren In-Memory-Fakes.
        var dyn = app.Services.GetRequiredService<DynamicBrokerConfig>();
        var overrides = dyn.GetDynamicConfigs();
        var entries = new List<BrokerConfigEntryDto>();
        foreach (var key in DynamicBrokerConfig.DynamicConfigKeys
                     .Concat(DynamicBrokerConfig.ReadOnlyConfigKeys)
                     .Distinct(StringComparer.OrdinalIgnoreCase)
                     .OrderBy(k => k, StringComparer.OrdinalIgnoreCase))
        {
            var value = dyn.GetConfig(key) ?? "";
            var isSensitive = key.Contains("password", StringComparison.OrdinalIgnoreCase)
                || key.Contains("secret", StringComparison.OrdinalIgnoreCase)
                || key.Contains("token", StringComparison.OrdinalIgnoreCase);
            entries.Add(new BrokerConfigEntryDto(
                key,
                isSensitive && value.Length > 0 ? "********" : value,
                IsDefault: !overrides.ContainsKey(key),
                IsReadOnly: DynamicBrokerConfig.ReadOnlyConfigKeys.Contains(key),
                IsSensitive: isSensitive));
        }
        return entries;
    },
    setBrokerConfig: (key, value) =>
        app.Services.GetRequiredService<DynamicBrokerConfig>().SetConfig(key, value),
    electLeader: (topic, partition) =>
        clusterController.ElectLeaderAsync(
            new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = topic, Partition = partition },
            preferredLeader: null,
            CancellationToken.None));

var shareGroupCoordinatorLogger = app.Services.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator>>();
var queueViewManager = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Core.Queue.IQueueViewManager>();
var shareGroupCoordinator = new Kuestenlogik.Surgewave.Broker.ShareGroups.ShareGroupCoordinator(shareGroupCoordinatorLogger, logManager, queueViewManager);

var consumerGroupV2CoordinatorLogger = app.Services.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator>>();
var consumerGroupV2Coordinator = new Kuestenlogik.Surgewave.Broker.ConsumerGroupV2.ConsumerGroupV2Coordinator(consumerGroupV2CoordinatorLogger, logManager);

// Classic coordinator gets a reference to the v2 coordinator so OffsetCommit/Fetch
// from a KIP-848 consumer falls through to v2 epoch validation when the group is
// not in the classic state map.
var consumerGroupCoordinator = new ConsumerGroupCoordinator(
    coordinatorLogger, offsetStore, logManager,
    aclAuthorizer: null,
    observability: app.Services.GetService<Kuestenlogik.Surgewave.Core.Observability.SurgewaveBrokerObservability>(),
    v2Coordinator: consumerGroupV2Coordinator);

var streamsGroupCoordinatorLogger = app.Services.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator>>();
var streamsGroupCoordinator = new Kuestenlogik.Surgewave.Broker.StreamsGroups.StreamsGroupCoordinator(streamsGroupCoordinatorLogger, logManager);

// Background sweep so groups whose last member silently dies eventually GC.
// The sweep service self-disposes inside the Task.Run so its lifetime is bounded
// by ApplicationStopping; ownership of the constructed instance is transferred
// to that task and the host's stopping callback drives shutdown.
Kuestenlogik.Surgewave.Broker.Lifecycle.GroupCoordinatorSweepRunner.Start(
    app,
    consumerGroupV2Coordinator,
    shareGroupCoordinator,
    streamsGroupCoordinator);

var nativeGroupCoordinatorLogger = app.Services.GetRequiredService<ILogger<NativeGroupCoordinator>>();
var nativeGroupCoordinator = new NativeGroupCoordinator(nativeGroupCoordinatorLogger, offsetStore);

// Register ConsumerGroupServiceImpl with delegates wired to NativeGroupCoordinator
ConsumerGroupServiceImplHolder.Instance = new ConsumerGroupServiceImpl(
    joinGroup: (groupId, memberId, groupInstanceId, clientId, protocolType, sessionTimeoutMs, rebalanceTimeoutMs, protocols) =>
    {
        var protoList = protocols.Select(p => new GroupProtocol(p.Name, p.Metadata)).ToList();
        var result = nativeGroupCoordinator.JoinGroup(groupId, memberId, groupInstanceId, clientId, protocolType, sessionTimeoutMs, rebalanceTimeoutMs, protoList);
        return new JoinGroupResultDto(
            result.ErrorCode,
            result.GenerationId,
            result.ProtocolName,
            result.LeaderId,
            result.MemberId,
            result.Members.Select(m => new JoinGroupMemberDto(m.MemberId, m.GroupInstanceId, m.Metadata)).ToList());
    },
    syncGroup: (groupId, memberId, generationId, assignments) =>
    {
        var assignList = assignments.Select(a => new MemberAssignment(a.MemberId, a.Assignment)).ToList();
        var result = nativeGroupCoordinator.SyncGroup(groupId, memberId, generationId, assignList);
        return new SyncGroupResultDto(result.ErrorCode, result.Assignment);
    },
    heartbeat: (groupId, memberId, generationId) =>
    {
        var result = nativeGroupCoordinator.Heartbeat(groupId, memberId, generationId);
        return new HeartbeatResultDto(result.ErrorCode);
    },
    leaveGroup: (groupId, memberId) =>
    {
        var result = nativeGroupCoordinator.LeaveGroup(groupId, memberId);
        return new LeaveGroupResultDto(result.ErrorCode);
    },
    listGroups: () => nativeGroupCoordinator.ListGroups()
        .Select(g => new GroupInfoDto(g.GroupId, g.ProtocolType, g.State)).ToList(),
    describeGroup: (groupId) =>
    {
        var result = nativeGroupCoordinator.DescribeGroup(groupId);
        return new DescribeGroupResultDto(
            result.ErrorCode,
            result.GroupId,
            result.State,
            result.ProtocolType,
            result.ProtocolName,
            result.GenerationId,
            result.Members.Select(m => new GroupMemberInfoDto(m.MemberId, m.GroupInstanceId, m.ClientId, m.Metadata, m.Assignment)).ToList());
    },
    deleteGroup: (groupId) =>
    {
        var result = nativeGroupCoordinator.DeleteGroup(groupId);
        return new DeleteGroupResultDto(result.ErrorCode);
    },
    findCoordinator: (key, keyType) =>
    {
        var result = nativeGroupCoordinator.FindCoordinator(key, (byte)keyType);
        return new FindCoordinatorResultDto(result.ErrorCode, result.CoordinatorId, result.Host, result.Port);
    });

// Register ClusterServiceImpl with delegates
ClusterServiceImplHolder.Instance = new ClusterServiceImpl(
    getClusterInfo: () =>
    {
        var topics = logManager.ListTopics().ToList();
        var totalPartitions = topics.Sum(t => t.PartitionCount);
        return new ClusterInfoDto(config.BrokerId, config.Host, config.Port, topics.Count, totalPartitions);
    },
    listBrokers: () =>
    {
        // Echte Broker-Liste aus dem ClusterState statt hartkodiertem
        // Single-Broker-Eintrag; im Single-Node-Betrieb registriert der
        // ClusterController diesen Broker selbst.
        var brokers = clusterState.Brokers.Values
            .Select(b => new BrokerInfoDto(
                b.BrokerId, b.Host, b.Port,
                IsController: b.BrokerId == clusterState.ControllerId,
                IsAlive: true,
                b.Rack,
                PeerTransport: clusteringConfig.InterBrokerTransport))
            .OrderBy(b => b.BrokerId)
            .ToList();

        return brokers.Count > 0
            ? brokers
            : [new BrokerInfoDto(config.BrokerId, config.Host, config.Port, true, true, null,
                PeerTransport: clusteringConfig.InterBrokerTransport)];
    },
    getMetadata: (topicNames) =>
    {
        var allTopics = logManager.ListTopics().ToList();
        var filteredTopics = topicNames != null && topicNames.Count > 0
            ? allTopics.Where(t => topicNames.Contains(t.Name)).ToList()
            : allTopics;

        return filteredTopics.Select(t => new TopicMetadataDto(
            t.Name,
            t.PartitionCount,
            Enumerable.Range(0, t.PartitionCount)
                .Select(p => new PartitionMetadataDto(p, config.BrokerId, [config.BrokerId], [config.BrokerId]))
                .ToList()
        )).ToList();
    },
    alterReassignments: (reassignments) =>
    {
        // Echte Ausführung über den PartitionReassignmentManager (gleicher
        // Pfad wie der Kafka-Wire ClusterAdminHandler) statt Fake-Erfolg.
        var manager = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>();
        var plan = new Kuestenlogik.Surgewave.Clustering.Cluster.ReassignmentPlan
        {
            Version = 1,
            Partitions = [.. reassignments.Select(r => new Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignment
            {
                Topic = r.Topic,
                Partition = r.Partition,
                Replicas = r.Replicas
            })]
        };
        try
        {
            manager.ExecuteReassignmentAsync(plan, CancellationToken.None).GetAwaiter().GetResult();
            return reassignments.Select(r => new ReassignmentResultDto(r.Topic, r.Partition, true, null)).ToList();
        }
        catch (Exception ex)
        {
            return reassignments.Select(r => new ReassignmentResultDto(r.Topic, r.Partition, false, ex.Message)).ToList();
        }
    },
    listReassignments: () =>
    {
        var manager = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>();
        return [.. manager.GetActiveReassignments().Select(s => new OngoingReassignmentDto(
            s.Topic, s.Partition, s.TargetReplicas, s.AddingReplicas, s.RemovingReplicas))];
    },
    triggerCompaction: async (partitions) =>
    {
        var result = await logManager.ApplyCompactionAsync(CancellationToken.None);
        return new CompactionResultDto(true, (int)result.RecordsRemoved, result.BytesRemoved, result.SegmentsCompacted);
    },
    getCompactionStatus: (partitions) =>
    {
        var topics = logManager.ListTopics()
            .Where(t => t.CleanupPolicy.HasFlag(CleanupPolicy.Compact))
            .ToList();

        var result = new List<CompactionStatusDto>();
        foreach (var topic in topics)
        {
            for (int p = 0; p < topic.PartitionCount; p++)
            {
                var tp = new Kuestenlogik.Surgewave.Core.Models.TopicPartition { Topic = topic.Name, Partition = p };
                var stats = logManager.GetCompactionStats(tp);
                var dirtyRatio = logManager.GetDirtyRatio(tp) ?? 0.0;
                result.Add(new CompactionStatusDto(
                    topic.Name, p,
                    CompactionEnabled: true,
                    LastCompactionTime: stats?.LastCompaction.ToUnixTimeMilliseconds() ?? 0,
                    CompactionRatio: dirtyRatio));
            }
        }
        return result;
    });

var producerStateManager = new ProducerStateManager();
var transactionIndex = new TransactionIndex();
var transactionStateStore = new TransactionStateStore(config.DataDirectory, txnStateStoreLogger);
var transactionCoordinator = new TransactionCoordinator(producerStateManager, logManager, transactionIndex, offsetStore, transactionStateStore, txnCoordinatorLogger);

// Register TransactionServiceImpl with delegates wired to TransactionCoordinator
TransactionServiceImplHolder.Instance = new TransactionServiceImpl(
    initProducerId: (transactionalId, transactionTimeoutMs, producerId, producerEpoch) =>
    {
        var request = new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.InitProducerIdRequest
        {
            ApiKey = Kuestenlogik.Surgewave.Protocol.Kafka.ApiKey.InitProducerId,
            CorrelationId = 0,
            ApiVersion = 4,
            ClientId = "grpc",
            TransactionalId = transactionalId,
            TransactionTimeoutMs = transactionTimeoutMs,
            ProducerId = producerId,
            ProducerEpoch = (short)producerEpoch
        };
        var response = transactionCoordinator.HandleInitProducerIdAsync(request, CancellationToken.None).GetAwaiter().GetResult();
        return new InitProducerIdResultDto((int)response.ErrorCode, response.ProducerId, response.ProducerEpoch);
    },
    addPartitionsToTxn: (transactionalId, producerId, producerEpoch, partitions) =>
    {
        var topicsDict = partitions.GroupBy(p => p.Topic)
            .ToDictionary(g => g.Key, g => g.Select(p => p.Partition).ToList());
        var request = new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.AddPartitionsToTxnRequest
        {
            ApiKey = Kuestenlogik.Surgewave.Protocol.Kafka.ApiKey.AddPartitionsToTxn,
            CorrelationId = 0,
            ApiVersion = 3,
            ClientId = "grpc",
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = (short)producerEpoch,
            Topics = topicsDict
        };
        var response = transactionCoordinator.HandleAddPartitionsToTxn(request);
        var results = response.Results.SelectMany(kvp =>
            kvp.Value.Select(p => (kvp.Key, p.Partition, (int)p.ErrorCode))).ToList();
        return new AddPartitionsToTxnResultDto(results);
    },
    addOffsetsToTxn: (transactionalId, producerId, producerEpoch, groupId) =>
    {
        var request = new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.AddOffsetsToTxnRequest
        {
            ApiKey = Kuestenlogik.Surgewave.Protocol.Kafka.ApiKey.AddOffsetsToTxn,
            CorrelationId = 0,
            ApiVersion = 3,
            ClientId = "grpc",
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = (short)producerEpoch,
            GroupId = groupId
        };
        var response = transactionCoordinator.HandleAddOffsetsToTxn(request);
        return new AddOffsetsToTxnResultDto((int)response.ErrorCode);
    },
    txnOffsetCommit: (transactionalId, groupId, producerId, producerEpoch, generationId, memberId, offsets) =>
    {
        // gRPC path is name-keyed; v6 (TopicId) is a wire-only concern.
        var topicEntries = offsets.GroupBy(o => o.Topic)
            .Select(g => new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.TxnOffsetCommitRequest.TxnOffsetCommitTopic
            {
                Name = g.Key,
                Partitions = g.Select(o => new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.TxnOffsetCommitRequest.TxnOffsetCommitPartition
                {
                    Partition = o.Partition,
                    CommittedOffset = o.Offset,
                    Metadata = o.Metadata
                }).ToList()
            })
            .ToList();
        var request = new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.TxnOffsetCommitRequest
        {
            ApiKey = Kuestenlogik.Surgewave.Protocol.Kafka.ApiKey.TxnOffsetCommit,
            CorrelationId = 0,
            ApiVersion = 3,
            ClientId = "grpc",
            TransactionalId = transactionalId,
            GroupId = groupId,
            ProducerId = producerId,
            ProducerEpoch = (short)producerEpoch,
            GenerationId = generationId,
            MemberId = memberId,
            Topics = topicEntries
        };
        var response = transactionCoordinator.HandleTxnOffsetCommit(request);
        var results = response.Topics.SelectMany(t =>
            t.Partitions.Select(p => (t.Name ?? string.Empty, p.Partition, (int)p.ErrorCode))).ToList();
        return new TxnOffsetCommitResultDto(results);
    },
    endTxn: (transactionalId, producerId, producerEpoch, commit) =>
    {
        var request = new Kuestenlogik.Surgewave.Protocol.Kafka.Requests.EndTxnRequest
        {
            ApiKey = Kuestenlogik.Surgewave.Protocol.Kafka.ApiKey.EndTxn,
            CorrelationId = 0,
            ApiVersion = 3,
            ClientId = "grpc",
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = (short)producerEpoch,
            Committed = commit
        };
        var response = transactionCoordinator.HandleEndTxnAsync(request, CancellationToken.None).GetAwaiter().GetResult();
        return new EndTxnResultDto((int)response.ErrorCode);
    },
    listTransactions: (statesFilter, producerIdFilter) =>
    {
        var results = transactionCoordinator.ListTransactions(statesFilter, producerIdFilter);
        return results.Select(t => new TransactionListingDto(t.TransactionalId, t.ProducerId, t.State)).ToList();
    },
    describeTransactions: (transactionalIds) =>
    {
        var results = transactionCoordinator.DescribeTransactions(transactionalIds);
        return results.Select(t => new TransactionDescriptionDto(
            t.TransactionalId,
            t.State,
            t.ProducerId,
            t.ProducerEpoch,
            t.TransactionTimeoutMs,
            t.TransactionStartTimeMs,
            t.Partitions,
            t.ErrorCode)).ToList();
    });

var quotaManager = new QuotaManager(config.Quotas, quotaManagerLogger, config.DataDirectory);

// Bandwidth quota manager — DI singleton (always-on, toggle is internal)
var bandwidthQuotaManager = app.Services.GetRequiredService<BandwidthQuotaManager>();

var delegationTokenManagerLogger = app.Services.GetRequiredService<ILogger<DelegationTokenManager>>();
var delegationTokenManager = new DelegationTokenManager(config.DelegationTokens, config.DataDirectory, delegationTokenManagerLogger);

// Register QuotaServiceImpl with delegates wired to QuotaManager
QuotaServiceImplHolder.Instance = new QuotaServiceImpl(
    getQuotaConfig: () =>
    {
        var cfg = quotaManager.Config;
        return new QuotaConfigDto(
            cfg.Enabled,
            cfg.ProducerBytesPerSecond,
            cfg.ProducerBurstBytes,
            cfg.ConsumerBytesPerSecond,
            cfg.ConsumerBurstBytes,
            cfg.MaxThrottleTimeMs,
            cfg.ClientInactivityTimeoutMinutes);
    },
    setQuotaConfig: (enabled, producerBytesPerSecond, producerBurstBytes, consumerBytesPerSecond, consumerBurstBytes, maxThrottleTimeMs, clientInactivityTimeoutMinutes) =>
    {
        quotaManager.UpdateConfig(
            enabled,
            producerBytesPerSecond,
            producerBurstBytes,
            consumerBytesPerSecond,
            consumerBurstBytes,
            maxThrottleTimeMs,
            clientInactivityTimeoutMinutes);
    },
    describeClientQuotas: (clientId) =>
    {
        var stats = quotaManager.GetClientStats(clientId);
        if (stats == null) return null;
        return new ClientQuotaStatsDto(
            clientId,
            stats.TotalProducedBytes,
            stats.TotalFetchedBytes,
            stats.ProduceThrottleCount,
            stats.FetchThrottleCount,
            stats.AvailableProduceTokens,
            stats.AvailableFetchTokens,
            new DateTimeOffset(stats.LastActivity).ToUnixTimeMilliseconds());
    },
    listClientQuotas: () =>
    {
        return quotaManager.GetAllClientStats()
            .Select(x => new ClientQuotaStatsDto(
                x.ClientId,
                x.Stats.TotalProducedBytes,
                x.Stats.TotalFetchedBytes,
                x.Stats.ProduceThrottleCount,
                x.Stats.FetchThrottleCount,
                x.Stats.AvailableProduceTokens,
                x.Stats.AvailableFetchTokens,
                new DateTimeOffset(x.Stats.LastActivity).ToUnixTimeMilliseconds()))
            .ToList();
    });

IProtocolHandler protocolHandler = new KafkaProtocolHandler();

// Initialize security components if enabled
SaslAuthenticator? saslAuthenticator = null;
AclAuthorizer? aclAuthorizer = null;
ScramCredentialStore? scramSha256Store = null;
ScramCredentialStore? scramSha512Store = null;

if (config.Security.SaslEnabled)
{
    var credentialStore = new CredentialStore(config.Security.CredentialsFile);

    // Add users from config if specified
    foreach (var userEntry in config.Security.Users)
    {
        var parts = userEntry.Split(':');
        if (parts.Length == 2)
        {
            credentialStore.AddUser(parts[0], parts[1]);
        }
    }

    // OAUTHBEARER (KIP-936): if the operator listed the mechanism in SaslMechanisms
    // and supplied an OIDC authority or a JWKS URI, stand up a JWT validator and
    // wire the SASL frame parser. Without the config block we leave OAUTHBEARER
    // unbound — the SaslAuthenticator will reject the mechanism cleanly.
    OAuthBearerAuthenticator? oauthBearerAuthenticator = null;
    var oauthBearerEnabled = config.Security.OAuthBearer.Enabled
        && config.Security.SaslMechanisms.Contains(SaslAuthenticator.MechanismOAuthBearer, StringComparer.OrdinalIgnoreCase);
    if (oauthBearerEnabled)
    {
        var httpFactory = app.Services.GetRequiredService<IHttpClientFactory>();
        var oauthHttp = httpFactory.CreateClient("oauthbearer-jwks");
        oauthHttp.Timeout = TimeSpan.FromSeconds(30);
        var jwksLogger = app.Services.GetRequiredService<ILogger<JwksTokenValidator>>();
        var validator = new JwksTokenValidator(config.Security.OAuthBearer, jwksLogger, oauthHttp);
        oauthBearerAuthenticator = new OAuthBearerAuthenticator(validator, config.Security.OAuthBearer);
        logger.LogInformation(
            "OAUTHBEARER enabled (issuer={Issuer}, jwksUri={Jwks}, principalClaim={Claim})",
            config.Security.OAuthBearer.ValidIssuer ?? config.Security.OAuthBearer.OidcAuthority ?? "(none)",
            config.Security.OAuthBearer.JwksUri ?? "(via discovery)",
            config.Security.OAuthBearer.PrincipalClaim);
    }

    // SCRAM credential stores. We stand them up in-memory whenever the
    // mechanism is in the allow-list so AlterUserScramCredentials (KIP-554)
    // can populate them at runtime — the admin RPC is the canonical way
    // to provision SCRAM users; static config files would always lag the
    // wire path.
    if (config.Security.SaslMechanisms.Contains(SaslAuthenticator.MechanismScramSha256, StringComparer.OrdinalIgnoreCase))
    {
        scramSha256Store = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA256);
        logger.LogInformation("SCRAM-SHA-256 store initialised (in-memory)");
    }
    if (config.Security.SaslMechanisms.Contains(SaslAuthenticator.MechanismScramSha512, StringComparer.OrdinalIgnoreCase))
    {
        scramSha512Store = new ScramCredentialStore(hashAlgorithm: System.Security.Cryptography.HashAlgorithmName.SHA512);
        logger.LogInformation("SCRAM-SHA-512 store initialised (in-memory)");
    }

    saslAuthenticator = new SaslAuthenticator(
        credentialStore,
        config.Security.SaslMechanisms,
        scramSha256Store: scramSha256Store,
        scramSha512Store: scramSha512Store,
        oauthBearer: oauthBearerAuthenticator);
    logger.LogInformation("SASL authentication enabled with mechanisms: {Mechanisms}",
        string.Join(", ", config.Security.SaslMechanisms));
}

if (config.Security.AclEnabled)
{
    aclAuthorizer = new AclAuthorizer(
        logger: app.Services.GetRequiredService<ILogger<AclAuthorizer>>(),
        allowIfNoAclFound: config.Security.AllowIfNoAclFound,
        superUsers: config.Security.SuperUsers,
        aclFilePath: config.Security.AclFile);

    logger.LogInformation("ACL authorization enabled (AllowIfNoAclFound: {AllowIfNoAclFound}, SuperUsers: {SuperUsers})",
        config.Security.AllowIfNoAclFound,
        config.Security.SuperUsers.Length > 0 ? string.Join(", ", config.Security.SuperUsers) : "none");
}

// Register SecurityServiceImpl with delegates wired to AclAuthorizer
// Note: aclAuthorizer may be null if ACL is disabled, so we handle this in the delegates
SecurityServiceImplHolder.Instance = new SecurityServiceImpl(
    describeAcls: (filter) =>
    {
        if (aclAuthorizer == null)
            return [];

        var aclFilter = filter != null ? AclEnumMapper.CreateAclFilter(filter) : null;
        return aclAuthorizer.ListAcls(aclFilter)
            .Select(AclEnumMapper.ConvertToAclBindingDto)
            .ToList();
    },
    createAcls: (acls) =>
    {
        if (aclAuthorizer == null)
            // Ehrlich statt Fake-Erfolg — gleiche Semantik wie SecurityApiHandler (SECURITY_DISABLED).
            return acls.Select(_ => AclErrorCodes.SecurityDisabled).ToList();

        var errorCodes = new List<int>();
        foreach (var acl in acls)
        {
            try
            {
                aclAuthorizer.AddAcl(new AclEntry
                {
                    Principal = acl.Principal,
                    Host = acl.Host,
                    ResourceType = AclEnumMapper.MapProtoToInternalResourceType(acl.ResourceType),
                    ResourceName = acl.ResourceName,
                    PatternType = AclEnumMapper.MapProtoToInternalPatternType(acl.PatternType),
                    Operation = AclEnumMapper.MapProtoToInternalOperation(acl.Operation),
                    Permission = AclEnumMapper.MapProtoToInternalPermission(acl.Permission)
                });
                errorCodes.Add(0); // Success
            }
            catch
            {
                errorCodes.Add(-1); // Unknown error
            }
        }
        return errorCodes;
    },
    deleteAcls: (filters) =>
    {
        if (aclAuthorizer == null)
            return filters.Select(_ => (new List<AclBindingDto>(), AclErrorCodes.SecurityDisabled)).ToList();

        var results = new List<(List<AclBindingDto> MatchingAcls, int ErrorCode)>();
        foreach (var filter in filters)
        {
            try
            {
                var aclFilter = AclEnumMapper.CreateAclFilter(filter);

                // Find matching ACLs before deletion
                var matchingAcls = aclAuthorizer.ListAcls(aclFilter)
                    .Select(AclEnumMapper.ConvertToAclBindingDto)
                    .ToList();

                // Remove matching ACLs
                aclAuthorizer.RemoveAcls(aclFilter);

                results.Add((matchingAcls, 0));
            }
            catch
            {
                results.Add(([], -1));
            }
        }
        return results;
    });

// Create dynamic broker config for runtime config modifications
var dynamicBrokerConfig = app.Services.GetRequiredService<DynamicBrokerConfig>();

// Auto-tuning: now activated as an IBrokerPlugin (SurgewaveAutoTuningBrokerPlugin) via
// BrokerPluginActivator.ActivatePlugins + ConfigureBrokerPlugins. The plugin's
// Configure() resolves BrokerMetrics, DynamicBrokerConfig and BrokerConfig from DI,
// builds AutoTuningService, starts the background loop, and maps the REST API.

// Create audit logger. The optional topic sink mirrors events into a Surgewave
// topic for SIEM / compliance pipelines (G13). It rides on top of the file
// sink — never replaces it — so a topic-write failure can't drop a record.
var auditLoggerInstance = app.Services.GetRequiredService<ILogger<AuditLogger>>();
AuditTopicSink? auditTopicSink = null;
if (config.Audit.Enabled && config.Audit.TopicSinkEnabled)
{
    var sinkLogger = app.Services.GetRequiredService<ILogger<AuditTopicSink>>();
    auditTopicSink = new AuditTopicSink(logManager, recordBatchSerializer, config.Audit, sinkLogger);
    logger.LogInformation("Audit topic sink enabled — mirroring events to {Topic}", config.Audit.TopicName);
}
var auditLogger = new AuditLogger(config, auditLoggerInstance, auditTopicSink);
await auditLogger.InitializeAsync();

// Initialize deduplication manager (if enabled)
DeduplicationManager? deduplicationManager = null;
if (config.Deduplication.Enabled)
{
    var dedupLogger = app.Services.GetRequiredService<ILogger<DeduplicationManager>>();
    deduplicationManager = new DeduplicationManager(config.Deduplication, metrics, dedupLogger);
    metrics.RegisterDedupAccessor(() => deduplicationManager.TotalEntries);
    logger.LogInformation("Content-based deduplication enabled (window={WindowMs}ms, max={MaxEntries}/partition)",
        config.Deduplication.WindowSizeMs, config.Deduplication.MaxEntriesPerPartition);
}

// Initialize delay index (if enabled)
DelayIndex? delayIndex = null;
if (config.DelayDelivery.Enabled)
{
    var delayLogger = app.Services.GetRequiredService<ILogger<DelayIndex>>();
    delayIndex = new DelayIndex(config.DelayDelivery, metrics, delayLogger);
    metrics.RegisterDelayAccessor(() => delayIndex.PendingCount);
    logger.LogInformation("Delayed message delivery enabled (maxDelay={MaxDelayMs}ms)",
        config.DelayDelivery.MaxDelayMs);
}

// Initialize TTL index (if enabled)
TtlIndex? ttlIndex = null;
if (config.Ttl.Enabled)
{
    var ttlLogger = app.Services.GetRequiredService<ILogger<TtlIndex>>();
    ttlIndex = new TtlIndex(config.Ttl, metrics, ttlLogger);
    metrics.RegisterTtlAccessor(() => ttlIndex.TrackedCount);
    logger.LogInformation("Per-message TTL enabled (maxTtl={MaxTtlMs}ms, defaultTtl={DefaultTtlMs}ms)",
        config.Ttl.MaxTtlMs, config.Ttl.DefaultTtlMs);
}

// Initialize broker-level DLQ manager (if enabled)
DlqManager? dlqManager = null;
if (config.BrokerDlq.Enabled)
{
    var dlqManagerLogger = app.Services.GetRequiredService<ILogger<DlqManager>>();
    dlqManager = new DlqManager(config.BrokerDlq, logManager, delayIndex, metrics, dlqManagerLogger);
    logger.LogInformation("Broker-level DLQ enabled (maxRetries={MaxRetries}, backoff={BackoffMs}ms, suffix={Suffix})",
        config.BrokerDlq.MaxRetries, config.BrokerDlq.RetryBackoffMs, config.BrokerDlq.TopicSuffix);
}

// Create request handlers
var dataApiLogger = app.Services.GetRequiredService<ILogger<DataApiHandler>>();
var metadataApiLogger = app.Services.GetRequiredService<ILogger<MetadataApiHandler>>();
var topicAdminLogger = app.Services.GetRequiredService<ILogger<TopicAdminHandler>>();
var configApiLogger = app.Services.GetRequiredService<ILogger<ConfigApiHandler>>();
var securityApiLogger = app.Services.GetRequiredService<ILogger<SecurityApiHandler>>();
var interBrokerApiLogger = app.Services.GetRequiredService<ILogger<InterBrokerApiHandler>>();
var consumerGroupApiLogger = app.Services.GetRequiredService<ILogger<ConsumerGroupApiHandler>>();
var shareGroupApiLogger = app.Services.GetRequiredService<ILogger<ShareGroupApiHandler>>();
var consumerGroupV2ApiLogger = app.Services.GetRequiredService<ILogger<ConsumerGroupV2ApiHandler>>();
var streamsGroupApiLogger = app.Services.GetRequiredService<ILogger<StreamsGroupApiHandler>>();
var raftApiLogger = app.Services.GetRequiredService<ILogger<RaftApiHandler>>();
var quotaApiLogger = app.Services.GetRequiredService<ILogger<QuotaApiHandler>>();
var delegationTokenApiLogger = app.Services.GetRequiredService<ILogger<DelegationTokenApiHandler>>();
var telemetryApiLogger = app.Services.GetRequiredService<ILogger<TelemetryApiHandler>>();

// KIP-714 client telemetry (G9 follow-up): default ingestor logs + meters every
// PushTelemetry payload. Operators flip Surgewave:Telemetry:Enabled=true to start
// advertising a real subscription set; the disabled path keeps the pre-G9
// stub semantics so a config-flag-only change doesn't surprise existing clients.
Kuestenlogik.Surgewave.Broker.Telemetry.ITelemetryIngestor telemetryIngestor =
    app.Services.GetRequiredService<Kuestenlogik.Surgewave.Broker.Telemetry.LoggingTelemetryIngestor>();
if (config.Telemetry.Enabled && config.Telemetry.TopicSinkEnabled)
{
    var sinkLogger = app.Services.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Broker.Telemetry.TelemetryTopicSink>>();
    var telemetrySink = new Kuestenlogik.Surgewave.Broker.Telemetry.TelemetryTopicSink(
        logManager, recordBatchSerializer, config.Telemetry, sinkLogger);
    telemetryIngestor = new Kuestenlogik.Surgewave.Broker.Telemetry.TopicForwardingTelemetryIngestor(telemetryIngestor, telemetrySink);
    logger.LogInformation("Telemetry topic sink enabled — mirroring OTLP payloads to {Topic}", config.Telemetry.TopicName);
}

IKafkaRequestHandler[] handlers =
[
    new DataApiHandler(config, logManager, transactionCoordinator, quotaManager, recordBatchSerializer, aclAuthorizer, deduplicationManager, delayIndex, ttlIndex, metrics, dataApiLogger, bandwidthQuotaManager,
        app.Services.GetService<Kuestenlogik.Surgewave.Core.Observability.SurgewaveBrokerObservability>(),
        coldStartProfiler: app.Services.GetService<Kuestenlogik.Surgewave.Broker.AutoTuning.ColdStartWorkloadProfiler>()),
    new MetadataApiHandler(config, logManager, metadataApiLogger),
    new TopicAdminHandler(config, logManager, quotaManager, auditLogger, topicAdminLogger),
    new ConfigApiHandler(config, dynamicBrokerConfig, logManager),
    new SecurityApiHandler(config, saslAuthenticator, aclAuthorizer, auditLogger, securityApiLogger,
        scramSha256Store: scramSha256Store, scramSha512Store: scramSha512Store),
    new InterBrokerApiHandler(config, clusterState, replicaManager, logManager, interBrokerApiLogger, transactionCoordinator),
    new ConsumerGroupApiHandler(consumerGroupCoordinator, consumerGroupApiLogger),
    new ShareGroupApiHandler(shareGroupCoordinator, shareGroupApiLogger),
    new ConsumerGroupV2ApiHandler(consumerGroupV2Coordinator, consumerGroupV2ApiLogger),
    new StreamsGroupApiHandler(streamsGroupCoordinator, streamsGroupApiLogger),
    new RaftApiHandler(config, raftNode, raftPersistence, clusterState, raftApiLogger),
    new ClusterAdminHandler(clusterController,
        app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>(),
        clusterState,
        app.Services.GetRequiredService<ILogger<ClusterAdminHandler>>()),
    new QuotaApiHandler(quotaManager, quotaApiLogger),
    new DelegationTokenApiHandler(delegationTokenManager, delegationTokenApiLogger),
    new TelemetryApiHandler(telemetryApiLogger, config.Telemetry, telemetryIngestor),
    new ClusterMembershipHandler(
        app.Services.GetRequiredService<ClusterIdManager>(),
        clusterState,
        app.Services.GetRequiredService<ILogger<ClusterMembershipHandler>>())
];
var dispatcher = new RequestDispatcher(handlers);

// Get PluginDiscovery for connector plugins (available even if Connect is disabled)
var pluginDiscoveryForBroker = app.Services.GetService<PluginDiscovery>();

// Cross-topic transaction manager: registered by SurgewaveCrossTopicTransactionsBrokerPlugin
// when enabled. Resolve nullable — null when disabled, passed to SurgewaveBroker constructor.
var crossTopicTxnManager = app.Services.GetService<Kuestenlogik.Surgewave.Broker.Transactions.CrossTopicTransactionManager>();

// Create KV Bucket Manager (broker-native Key-Value Store)
var kvBucketManagerLogger = app.Services.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Broker.KeyValue.KvBucketManager>>();
var kvBucketManager = new Kuestenlogik.Surgewave.Broker.KeyValue.KvBucketManager(logManager, kvBucketManagerLogger);
await kvBucketManager.RestoreFromTopicsAsync(app.Lifetime.ApplicationStopping);

// Consumer-Lag-Berechnung: ein Calculator ueber dem gemeinsamen OffsetStore
// (Kafka- UND Native-Consumer committen dorthin). Gruppen-Metadaten kommen aus
// beiden Koordinatoren; Gruppen, die nur noch Offsets haben (keine aktiven
// Member), erscheinen als "Empty".
IEnumerable<(string GroupId, string State, int MemberCount)> GetGroupInfosForLag()
{
    var result = new Dictionary<string, (string GroupId, string State, int MemberCount)>(StringComparer.Ordinal);
    foreach (var g in consumerGroupCoordinator.GetGroupSummaries())
        result[g.GroupId] = g;
    foreach (var g in nativeGroupCoordinator.ListGroups())
        result.TryAdd(g.GroupId, (g.GroupId, g.State, 0));
    foreach (var id in offsetStore.GetGroupIds())
        result.TryAdd(id, (id, "Empty", 0));
    return result.Values;
}
var lagCalculator = new Kuestenlogik.Surgewave.Core.Monitoring.DefaultLagCalculator(
    new OffsetStoreProvider(offsetStore, GetGroupInfosForLag),
    new LogManagerWatermarkProvider(logManager));

var surgewaveBroker = new SurgewaveBroker(config, logManager, recordBatchSerializer, consumerGroupCoordinator, shareGroupCoordinator, nativeGroupCoordinator, transactionCoordinator, quotaManager, protocolHandler, metrics, dispatcher, brokerLogger, consumerGroupV2Coordinator: consumerGroupV2Coordinator, streamsGroupCoordinator: streamsGroupCoordinator, pluginDiscovery: pluginDiscoveryForBroker, dlqManager: dlqManager, crossTopicTxnManager: crossTopicTxnManager, kvBucketManager: kvBucketManager, lagCalculator: lagCalculator);

// Publish the broker as the Surgewave stream handler so alternative transports
// (QUIC, shared memory, ...) can pump connections into the shared pipeline.
// The handler auto-detects Surgewave-native vs Kafka from the first 4 bytes on
// the stream, so transports don't need to know which protocol is inside.
Kuestenlogik.Surgewave.Protocol.SurgewaveStreamHandlerHolder.Instance = surgewaveBroker;

// Register GraphQL broker service (needs LogManager, serializer, and group coordinator).
// FeatureId literal matches the FeatureId the GraphQL plugin declares for itself.
if (activatedFeatures.Contains("Surgewave.Api.GraphQL"))
{
    var graphqlBrokerServiceLogger = app.Services.GetRequiredService<ILogger<Kuestenlogik.Surgewave.Api.GraphQL.Services.GraphQLBrokerService>>();
    var graphqlBrokerService = new Kuestenlogik.Surgewave.Api.GraphQL.Services.GraphQLBrokerService(
        logManager,
        serializeMessages: recordBatchSerializer.SerializeMessages,
        listGroups: () => nativeGroupCoordinator.ListGroups()
            .Select(g => new Kuestenlogik.Surgewave.Api.GraphQL.Services.GroupInfoDto(g.GroupId, g.State, 0, g.ProtocolType))
            .ToList(),
        getClusterInfo: () => new Kuestenlogik.Surgewave.Api.GraphQL.Services.ClusterInfoDto(
            config.BrokerId, config.Host, config.Port,
            logManager.ListTopics().Count(),
            logManager.ListTopics().Sum(t => t.PartitionCount)),
        graphqlBrokerServiceLogger);
    GraphQLBrokerServiceHolder.Instance = graphqlBrokerService;
}

// Start replication components
await clusterController.StartAsync(CancellationToken.None);
await replicaManager.StartAsync(CancellationToken.None);
await replicationServer.StartAsync(CancellationToken.None);

logger.LogInformation("Replication components started (Controller: {IsController})", clusterController.IsController);

var brokerTask = Task.Run(() => surgewaveBroker.StartAsync(CancellationToken.None));

logger.LogInformation("Kafka wire protocol listener started on {Host}:{Port}", config.Host, config.Port);

app.MapGrpcService<ProducerServiceImpl>();
app.MapGrpcService<ConsumerServiceImpl>();
app.MapGrpcService<TopicServiceImpl>();
app.MapGrpcService<AdminServiceImpl>();
app.MapGrpcService<ConsumerGroupServiceImpl>();
app.MapGrpcService<ClusterServiceImpl>();
app.MapGrpcService<TransactionServiceImpl>();
app.MapGrpcService<QuotaServiceImpl>();
app.MapGrpcService<SecurityServiceImpl>();
app.MapGrpcReflectionService();
app.MapBowire("/bowire", options =>
{
    options.Title = "Surgewave gRPC API";
    options.Description = "Interactive gRPC browser for Surgewave Broker";
});
app.MapPrometheusScrapingEndpoint();

// Schema Registry endpoint mapping and gRPC holder init are handled by
// SurgewaveSchemaRegistryBrokerPlugin.Configure() via BrokerPluginActivator.ConfigureBrokerPlugins.
// When Schema Registry is disabled, initialize the gRPC holder with no-op delegates so
// gRPC clients get a clean Unavailable error instead of a null-ref.
if (!builder.Configuration.GetValue("Surgewave:SchemaRegistry:Enabled", true))
{
    SchemaRegistryServiceImplHolder.Instance = new SchemaRegistryServiceImpl(
        listSubjects: (_) => [],
        getSubjectVersions: (_, _) => [],
        registerSchema: (_, _, _, _) => throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.Unavailable, "Schema Registry is disabled")),
        getSchemaById: (_) => null,
        getSchemaByVersion: (_, _) => null,
        deleteSubject: (_, _) => [],
        deleteSchemaVersion: (_, _, _) => null,
        checkCompatibility: (_, _, _, _, _) => new CompatibilityResultDto(true, null),
        getCompatibilityConfig: (_) => 0,
        setCompatibilityConfig: (_, level) => level,
        getSchemaTypes: () => []);
}

// Connect: service init, connector discovery, pipeline orchestrator, gRPC holder and
// REST API mapping are all handled by SurgewaveConnectBrokerPlugin.ConfigureAsync() via
// BrokerPluginActivator.ConfigureBrokerPluginsAsync. When Connect is disabled, set the
// gRPC holder to no-op delegates so clients get a clean Unavailable error.
if (!config.Connect.Enabled)
{
    ConnectServiceImplHolder.Instance = new ConnectServiceImpl(
        listConnectors: () => [],
        getConnector: _ => null,
        createConnector: (_, _) => throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.Unavailable, "Kafka Connect is not enabled. Set Surgewave:Connect:Enabled=true in appsettings.json")),
        deleteConnector: _ => Task.CompletedTask,
        updateConnectorConfig: (_, _) => throw new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.Unavailable, "Kafka Connect is not enabled")),
        restartConnector: (_, _, _) => Task.CompletedTask,
        pauseConnector: _ => Task.CompletedTask,
        resumeConnector: _ => Task.CompletedTask,
        restartConnectorTask: (_, _) => Task.CompletedTask,
        listConnectorPlugins: (_, _) => []);
}
app.MapGrpcService<ConnectServiceImpl>();

// Health check endpoints (Kubernetes ready/live probes)
app.MapHealthChecks("/health", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    ResponseWriter = async (context, report) =>
    {
        context.Response.ContentType = "application/json";
        var response = new
        {
            status = report.Status.ToString(),
            timestamp = DateTime.UtcNow,
            broker_id = config.BrokerId,
            topics_count = clusterState.Topics.Count,
            brokers_count = clusterState.Brokers.Count,
            raft_enabled = config.UseRaftConsensus,
            raft_state = raftNode?.State.ToString(),
            raft_leader_id = raftNode?.LeaderId
        };
        await context.Response.WriteAsJsonAsync(response);
    }
});

// Liveness probe - just checks if the app is running
app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = _ => false // Don't run any checks, just return healthy if app is running
});

// Readiness probe - checks if the app is ready to serve traffic
app.MapHealthChecks("/health/ready");

// Configure activated protocol plugins (endpoint mapping, middleware)
BrokerPluginActivator.ConfigureProtocols(app, app.Services, activatedProtocols);

// Map Audit REST API
app.MapSurgewaveAudit(auditLogger);
logger.LogInformation("  - Audit API:           {Host}:{GrpcPort}/admin/audit", config.Host, config.GrpcPort);

// Map ACL REST API
if (aclAuthorizer != null)
{
    app.MapSurgewaveAcl(aclAuthorizer);
    logger.LogInformation("  - ACL API:             {Host}:{GrpcPort}/admin/acls", config.Host, config.GrpcPort);
}

// Map Quota REST API
app.MapSurgewaveQuota(quotaManager);
logger.LogInformation("  - Quota API:           {Host}:{GrpcPort}/admin/quotas", config.Host, config.GrpcPort);

// Map Bandwidth Quota REST API (always-on — the manager handles the enabled/disabled toggle internally)
app.MapSurgewaveBandwidthQuota(bandwidthQuotaManager);
logger.LogInformation("  - Bandwidth Quota API: {Host}:{GrpcPort}/api/quotas/bandwidth{Suffix}",
    config.Host, config.GrpcPort, config.BandwidthQuota.Enabled ? "" : " (disabled)");

// Map Message Browser REST API (read + produce + offset-for-timestamp)
app.MapMessageBrowser(logManager, recordBatchSerializer);
logger.LogInformation("  - Message Browser:     {Host}:{GrpcPort}/admin/messages", config.Host, config.GrpcPort);

// Map Queue Inspector REST API (QueueViewManager is always-on)
app.MapSurgewaveQueue(app.Services.GetRequiredService<QueueViewManager>());
logger.LogInformation("  - Queue API:           {Host}:{GrpcPort}/api/queue", config.Host, config.GrpcPort);

// Map Consumer-Lag REST API (Lag-Dashboard + Offset-Reset in Control)
int GetActiveMemberCount(string groupId)
{
    var kafkaMembers = consumerGroupCoordinator.GetGroupSummaries()
        .FirstOrDefault(g => g.GroupId == groupId).MemberCount;
    var nativeGroup = nativeGroupCoordinator.DescribeGroup(groupId);
    return kafkaMembers + (nativeGroup?.Members.Count ?? 0);
}
app.MapConsumerLag(lagCalculator, offsetStore, logManager, GetActiveMemberCount);
logger.LogInformation("  - Consumer Lag API:    {Host}:{GrpcPort}/api/consumer-groups/lag", config.Host, config.GrpcPort);

// Map SQL Query REST API
Kuestenlogik.Surgewave.Broker.Sql.SqlRestApi.MapSqlRestApi(app);
logger.LogInformation("  - SQL Query API:       {Host}:{GrpcPort}/api/sql", config.Host, config.GrpcPort);

// Cross-Topic Transaction REST API: mapped by SurgewaveCrossTopicTransactionsBrokerPlugin.Configure()

// Map Online Partition Reassignment REST API — all components from DI
var reassignmentConfig = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentConfig>();
var reassignmentPlanner = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentPlanner>();
var reassignmentManagerForApi = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Cluster.PartitionReassignmentManager>();
var reassignmentExecutor = app.Services.GetRequiredService<Kuestenlogik.Surgewave.Clustering.Reassignment.ReassignmentExecutor>();
_ = reassignmentExecutor.StartAsync(app.Lifetime.ApplicationStopping);
app.MapSurgewaveReassignment(reassignmentExecutor, reassignmentPlanner, reassignmentConfig);
logger.LogInformation("  - Reassignment API:    {Host}:{GrpcPort}/api/partitions/reassign, /balance, /decommission/*", config.Host, config.GrpcPort);

// Map Rolling Upgrade REST API
app.MapRollingUpgrade(clusterState, clusteringConfig, shutdownOrchestrator, versionCompatibilityChecker);
logger.LogInformation("  - Rolling Upgrade API: {Host}:{GrpcPort}/api/cluster/version, /api/cluster/upgrade/*", config.Host, config.GrpcPort);

// Auto-Tuning: REST API mapping is handled by SurgewaveAutoTuningBrokerPlugin.Configure()
// via the BrokerPluginActivator.ConfigureBrokerPlugins call below.

// Cruise Control: now activated as an IBrokerPlugin (SurgewaveCruiseControlBrokerPlugin)
// via BrokerPluginActivator.ActivatePlugins + ConfigureBrokerPlugins. The plugin's
// Configure() resolves ClusterState, ReassignmentPlanner, ReassignmentExecutor from
// DI, builds CruiseControlService, starts the background loop, and maps the REST API.

// Live config validation endpoint — answers 'does the config the broker is actually
// running on still validate?' against every IValidatableConfig type discoverable in
// the loaded assemblies (broker + protocol plugins + broker plugins).
app.MapConfigValidation();
logger.LogInformation("  - Config Validate API: {Host}:{GrpcPort}/api/config/validate", config.Host, config.GrpcPort);

app.MapLicenseApi(license, activatedPlugins);
logger.LogInformation("  - License API:        {Host}:{GrpcPort}/api/license", config.Host, config.GrpcPort);

// Configure activated broker plugins (endpoint mapping, middleware)
await BrokerPluginActivator.ConfigureBrokerPluginsAsync(app, app.Services, activatedPlugins);
foreach (var bp in activatedPlugins)
    logger.LogInformation("  - {Plugin} API:  active", bp.DisplayName);

// Map Intent-Based Configuration REST API
var intentEngine = new Kuestenlogik.Surgewave.Broker.IntentConfig.IntentConfigEngine();
app.MapIntentConfig(intentEngine);
logger.LogInformation("  - Intent Config API:   {Host}:{GrpcPort}/api/intent", config.Host, config.GrpcPort);

// Map KV Store & Object Store REST API
app.MapSurgewaveKv(kvBucketManager);
logger.LogInformation("  - KV Store API:        {Host}:{GrpcPort}/api/kv/buckets, /api/kv/objects", config.Host, config.GrpcPort);

// Map Plugin REST API (upload, download, list)
var pluginPackageManager = new Kuestenlogik.Surgewave.Plugins.Packaging.PluginPackageManager();
var pluginPackageCacheDir = Path.Combine(config.DataDirectory, "plugin-packages");
var pluginDiscoveryForApi = app.Services.GetService<Kuestenlogik.Surgewave.Plugins.PluginDiscovery>();
var uploadVerifier = app.Services.GetService<Kuestenlogik.Surgewave.Plugins.Packaging.ISppSigner>();
var signerOptions = app.Services.GetService<Microsoft.Extensions.Options.IOptions<Kuestenlogik.Surgewave.Plugins.Packaging.Hosting.SignerOptions>>()?.Value;
app.MapSurgewavePlugins(
    pluginPackageManager,
    config.Connect.PluginsDirectory,
    pluginPackageCacheDir,
    pluginDiscoveryForApi,
    verifier: uploadVerifier,
    requireSigned: signerOptions?.RequireSignedPackages ?? false);
app.MapSurgewaveTrustedKeys(signerOptions);
var repositoryStore = new Kuestenlogik.Surgewave.Plugins.Repository.RepositoryStore(
    Path.Combine(config.DataDirectory, "surgewave-repositories.json"));
// Hydrate the broker's marketplace-search repositories from the canonical
// store so the operator's /plugins/sources edits actually flow into the
// SearchPlugins Native op. Without this, the manager keeps the hard-coded
// NuGet.org default and the Control's marketplace browse silently ignores
// configured feeds. MVP: startup-only sync — live re-sync on store mutate
// is a follow-up.
surgewaveBroker.ConnectorRepositoryManager.SyncFromStore(repositoryStore);
app.MapSurgewaveRepositories(repositoryStore);
logger.LogInformation("  - Plugin API:          {Host}:{GrpcPort}/api/plugins", config.Host, config.GrpcPort);

// Prometheus HTTP service discovery endpoint
// Returns targets in Prometheus HTTP SD format: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#http_sd_config
app.MapGet("/sd-targets", () =>
{
    var targets = new List<string>();

    // Add known brokers from cluster state
    foreach (var broker in clusterState.Brokers.Values)
    {
        targets.Add($"{broker.Host}:{config.GrpcPort}");
    }

    // Always include self if not already in list
    var selfTarget = $"{config.Host}:{config.GrpcPort}";
    if (!targets.Contains(selfTarget))
    {
        targets.Add(selfTarget);
    }

    return Results.Ok(new[]
    {
        new
        {
            targets,
            labels = new Dictionary<string, string>
            {
                ["cluster"] = "surgewave-raft",
                ["job"] = "surgewave-broker"
            }
        }
    });
});

// Geo-replication: now activated as an IBrokerPlugin (SurgewaveGeoReplicationBrokerPlugin).

// Enterprise plugin: Kuestenlogik.Surgewave.Replication
// Active-active multi-DC replication and Cluster Linking require the Kuestenlogik.Surgewave.Replication package.

logger.LogInformation("gRPC server started on port {GrpcPort}", config.GrpcPort);
logger.LogInformation("Surgewave broker ready to accept connections");
logger.LogInformation("  - Kafka wire protocol: {Host}:{Port}", config.Host, config.Port);
logger.LogInformation("  - gRPC API:            {Host}:{GrpcPort}", config.Host, config.GrpcPort);
logger.LogInformation("  - Metrics:             {Host}:{GrpcPort}/metrics", config.Host, config.GrpcPort);
foreach (var proto in activatedProtocols)
    logger.LogInformation("  - {Protocol}:  port {Port}", proto.DisplayName, proto.DefaultPort > 0 ? proto.DefaultPort : "shared");
// Schema Registry startup log is now emitted by SurgewaveSchemaRegistryBrokerPlugin.Configure()
logger.LogInformation("Press Ctrl+C to shutdown");

// Handle shutdown gracefully --— use GracefulShutdownOrchestrator for rolling upgrade safety
var cts = new CancellationTokenSource();
Console.CancelKeyPress += (sender, eventArgs) =>
{
    logger.LogInformation("Shutting down broker (initiating graceful shutdown with leadership transfer)...");
    eventArgs.Cancel = true;

    // Initiate graceful shutdown in background before cancelling the main token
    _ = Task.Run(async () =>
    {
        try
        {
            var result = await shutdownOrchestrator.InitiateGracefulShutdownAsync(ct: CancellationToken.None);
            if (result.Success)
            {
                logger.LogInformation(
                    "Graceful shutdown completed: {Transferred} partitions transferred in {Duration:F1}s",
                    result.PartitionsTransferred, result.Duration.TotalSeconds);
            }
            else
            {
                logger.LogWarning(
                    "Graceful shutdown completed with warnings: {Warnings}",
                    string.Join("; ", result.Warnings));
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Graceful shutdown failed, proceeding with hard shutdown");
        }
        finally
        {
            cts.Cancel();
        }
    });
};

try
{
    await app.RunAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // Normal shutdown
    logger.LogInformation("Broker shutdown completed");
}
catch (Exception ex)
{
    logger.LogCritical(ex, "Fatal error occurred");
    return 1;
}

// Cleanup — dispose plugins first (they may hold connections/background services)
await BrokerPluginActivator.DisposeBrokerPluginsAsync(activatedPlugins, logger);
await reassignmentExecutor.DisposeAsync();
await reassignmentManagerForApi.DisposeAsync();
await surgewaveBroker.DisposeAsync();
// Enterprise plugin: Kuestenlogik.Surgewave.Replication
// ClusterLinkManager lifecycle is managed by SurgewaveGeoReplicationBrokerPlugin.
// ConnectWorker lifecycle is managed by SurgewaveConnectBrokerPlugin.
if (raftNode != null)
    await raftNode.DisposeAsync();
if (raftTransport != null)
    await raftTransport.DisposeAsync();
await replicationServer.DisposeAsync();
await replicaManager.DisposeAsync();
await clusterController.DisposeAsync();
if (tieredStorageManager != null)
    await tieredStorageManager.DisposeAsync();
// AutoTuningService lifecycle is managed by SurgewaveAutoTuningBrokerPlugin.
// Shutdown is handled by the application's hosted service shutdown sequence.
deduplicationManager?.Dispose();
delayIndex?.Dispose();
ttlIndex?.Dispose();
dlqManager?.Dispose();
quotaManager.Dispose();
bandwidthQuotaManager.Dispose();
metrics.Dispose();
transactionStateStore.Dispose();
offsetStore.Dispose();
logManager.Dispose();
cts.Dispose();

logger.LogInformation("Surgewave broker stopped");
return 0;

// ACL enum mapping functions moved to AclEnumMapper class in Startup folder
