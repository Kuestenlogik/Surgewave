using Kuestenlogik.Surgewave.Core.Telemetry;
using Kuestenlogik.Surgewave.Gateway;
using Kuestenlogik.Surgewave.Gateway.Services;
using Kuestenlogik.Surgewave.Gateway.WebSocket;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Kuestenlogik.Bowire;

var builder = WebApplication.CreateBuilder(args);

// Load configuration
var config = builder.Configuration.GetSection("Gateway").Get<GatewayConfig>() ?? new GatewayConfig();
builder.Services.AddSingleton(config);

// Load telemetry configuration
var telemetryConfig = builder.Configuration
    .GetSection(TelemetryConfig.SectionName)
    .Get<TelemetryConfig>() ?? new TelemetryConfig { ServiceName = "Kuestenlogik.Surgewave.Gateway" };

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

// Register GatewayMetrics as singleton (DI container manages disposal)
builder.Services.AddSingleton<GatewayMetrics>();

// Create and register the cluster registry (manages multiple broker connections)
builder.Services.AddSingleton<ClusterRegistry>();

// Register as hosted service to manage connection lifecycle
builder.Services.AddHostedService<SurgewaveClientHostedService>();

// WebSocket services
builder.Services.AddSingleton<WebSocketSessionManager>();
builder.Services.AddTransient<WebSocketHandler>();
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers.SubscribeHandler>();
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers.ProduceHandler>();
builder.Services.AddSingleton<Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers.AdminHandler>();
builder.Services.AddHostedService<WebSocketCleanupService>();

// Add gRPC with JSON transcoding
builder.Services.AddGrpc().AddJsonTranscoding();
builder.Services.AddGrpcReflection();

// Configure OpenTelemetry
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
        .AddMeter(GatewayMetrics.MeterName)
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

    tracing.AddSource(GatewayMetrics.ActivitySourceName);

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

// Add Health Checks
builder.Services.AddHealthChecks();

// Add OpenAPI
builder.Services.AddOpenApi("v3", options =>
{
    options.AddDocumentTransformer((document, context, cancellationToken) =>
    {
        document.Info.Title = "Surgewave Gateway";
        document.Info.Version = "v3";
        document.Info.Description = "Confluent REST Proxy compatible API for Surgewave message broker. " +
                                    "Provides REST and gRPC access via JSON transcoding.";
        return Task.CompletedTask;
    });
});

var app = builder.Build();

// Wire up cluster count accessor for metrics
var clusterRegistry = app.Services.GetRequiredService<ClusterRegistry>();
var gatewayMetrics = app.Services.GetRequiredService<GatewayMetrics>();
gatewayMetrics.RegisterClusterCountAccessor(() => clusterRegistry.ClusterCount);

// Enable WebSocket support
if (config.WebSocket.Enabled)
{
    app.UseWebSockets(new WebSocketOptions
    {
        KeepAliveInterval = TimeSpan.FromMilliseconds(config.WebSocket.HeartbeatIntervalMs)
    });

    // Map WebSocket routes
    app.Map("/ws", wsApp => wsApp.UseSurgewaveWebSocket());
    app.Map("/ws/clusters/{clusterId}", wsApp => wsApp.UseSurgewaveWebSocket());
}

// Enable Prometheus metrics endpoint
if (telemetryConfig.Prometheus.Enabled)
{
    app.MapPrometheusScrapingEndpoint(telemetryConfig.Prometheus.Path);
}

// Enable OpenAPI (consumed by the Bowire workbench below alongside the gRPC services)
app.MapOpenApi();

// Map gRPC services with JSON transcoding
app.MapGrpcService<ProducerGatewayService>();
app.MapGrpcService<ConsumerGatewayService>();
app.MapGrpcService<TopicGatewayService>();
app.MapGrpcService<ClusterGatewayService>();
app.MapGrpcService<ConsumerGroupGatewayService>();
app.MapGrpcService<TransactionGatewayService>();
app.MapGrpcReflectionService();
app.MapBowire("/bowire", options =>
{
    options.Title = "Surgewave Gateway";
    options.Description = "Interactive workbench for the Surgewave Gateway gRPC + REST surface";
});

// Map Health endpoints
app.MapHealthChecks("/health", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    ResponseWriter = async (context, report) =>
    {
        context.Response.ContentType = "application/json";

        // Get cluster info from the default cluster
        var client = clusterRegistry.GetClient(null);
        var clusterConfig = clusterRegistry.GetConfig(null);

        int brokerId = 0, topicsCount = 0, brokersCount = 0, controllerId = -1;
        bool isConnected = false;

        try
        {
            var clusterInfo = await client.Cluster.GetClusterInfoAsync();
            var brokers = await client.Cluster.ListBrokersAsync();
            brokerId = brokers.FirstOrDefault()?.BrokerId ?? 0;
            topicsCount = clusterInfo.TopicCount;
            brokersCount = brokers.Count;
            controllerId = clusterInfo.ControllerId;
            isConnected = true;
        }
        catch
        {
            // Broker not reachable
        }

        var response = new
        {
            status = isConnected ? "Healthy" : "Unhealthy",
            timestamp = DateTime.UtcNow,
            broker_id = brokerId,
            host = clusterConfig?.BrokerHost ?? "localhost",
            port = clusterConfig?.BrokerPort ?? 9092,
            grpc_port = 9093,
            topics_count = topicsCount,
            brokers_count = brokersCount,
            controller_id = controllerId,
            raft_enabled = false,
            raft_state = (string?)null,
            raft_leader_id = (int?)null,
            checks = new
            {
                broker = isConnected ? "Healthy" : "Unhealthy",
                grpc = "Healthy", // If we're responding, gRPC is healthy
                storage = isConnected ? "Healthy" : "Unknown"
            }
        };
        await context.Response.WriteAsJsonAsync(response, new System.Text.Json.JsonSerializerOptions
        {
            PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.SnakeCaseLower
        });
    }
});

// Liveness probe
app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = _ => false
});

// Readiness probe
app.MapHealthChecks("/health/ready");

app.Run();
