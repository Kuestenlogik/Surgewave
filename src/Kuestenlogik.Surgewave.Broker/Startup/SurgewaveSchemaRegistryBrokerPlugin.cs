using Kuestenlogik.Surgewave.Api.Grpc.Server;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Startup;

/// <summary>
/// <see cref="IBrokerPlugin"/> that wires up the entire Schema Registry subsystem:
/// schema store, compatibility checker, all built-in format handlers (Avro, JSON,
/// Protobuf, FlatBuffers), schema inference (topic sampling + auto-registration),
/// schema evolution (AI-assisted analysis), zero-downtime schema migration, and
/// cross-cluster schema linking.
///
/// <para>
/// Before this refactoring, all of the above was a ~108-line
/// <c>if (schemaRegistryEnabled)</c> block in <c>Program.cs</c> (ConfigureServices
/// phase) plus a ~75-line block (endpoint mapping phase). The plugin encapsulates
/// both phases under the standard <see cref="IBrokerPlugin"/> lifecycle so the
/// discovery loop handles activation, and <c>Program.cs</c> is uncluttered.
/// </para>
///
/// <para>
/// Default: <b>enabled</b>. Schema Registry is a community feature and is on by
/// default (<c>Surgewave:SchemaRegistry:Enabled</c> defaults to <c>true</c>). Operators
/// who do not need it can set <c>Enabled=false</c> in their <c>appsettings.json</c>
/// to skip all registration and endpoint mapping — zero runtime cost.
/// </para>
/// </summary>
public sealed class SurgewaveSchemaRegistryBrokerPlugin : IBrokerPlugin
{
    /// <inheritdoc />
    public string FeatureId => "Surgewave.SchemaRegistry";

    /// <inheritdoc />
    public string DisplayName => "Schema Registry";

    /// <inheritdoc />
    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue("Surgewave:SchemaRegistry:Enabled", true);

    /// <inheritdoc />
    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // External mode: ExternalUrl is set → proxy + RemoteSchemaStore
        if (HasExternalUrl(configuration))
        {
            var externalUrl = configuration.GetValue<string>("Surgewave:SchemaRegistry:ExternalUrl")!.TrimEnd('/');
            services.AddHttpClient("SchemaRegistryProxy", c => c.BaseAddress = new Uri(externalUrl + "/"));
            services.AddSingleton<ISchemaStore>(sp =>
            {
                var factory = sp.GetRequiredService<IHttpClientFactory>();
                return new RemoteSchemaStore(factory.CreateClient("SchemaRegistryProxy"));
            });

            // Inference keeps running locally (has topic access), registers at remote
            services.AddSingleton<ITopicMessageSampler, TopicMessageSampler>();
            services.AddSurgewaveSchemaInference(new SchemaInferenceConfig
            {
                Enabled = configuration.GetValue("Surgewave:SchemaRegistry:Inference:Enabled", true),
                SampleSize = configuration.GetValue("Surgewave:SchemaRegistry:Inference:SampleSize", 100),
                RefreshIntervalSeconds = configuration.GetValue("Surgewave:SchemaRegistry:Inference:RefreshIntervalSeconds", 60),
                AutoRegister = configuration.GetValue("Surgewave:SchemaRegistry:Inference:AutoRegister", true)
            });
            return;
        }

        var schemaDataPath = configuration.GetValue<string>("Surgewave:SchemaRegistry:DataPath")
            ?? Path.Combine(configuration.GetValue<string>("Surgewave:DataDirectory") ?? "./data", "schemas");
        var defaultCompatibility = configuration.GetValue<string>("Surgewave:SchemaRegistry:DefaultCompatibility") ?? "Backward";

        _ = Enum.TryParse<CompatibilityMode>(defaultCompatibility, ignoreCase: true, out var compatibilityMode);
        services.AddSurgewaveSchemaRegistry(new Kuestenlogik.Surgewave.Schema.Registry.SchemaRegistryConfig
        {
            DataPath = schemaDataPath,
            DefaultCompatibility = compatibilityMode
        });

        // Built-in schema format handlers
        services.AddAvroSchemaHandler();
        services.AddJsonSchemaHandler();
        services.AddProtobufSchemaHandler();
        services.AddFlatBuffersSchemaHandler();

        // Schema inference
        var inferenceEnabled = configuration.GetValue("Surgewave:SchemaRegistry:Inference:Enabled", true);
        var inferenceSampleSize = configuration.GetValue("Surgewave:SchemaRegistry:Inference:SampleSize", 100);
        var inferenceRefreshInterval = configuration.GetValue("Surgewave:SchemaRegistry:Inference:RefreshIntervalSeconds", 60);
        var inferenceAutoRegister = configuration.GetValue("Surgewave:SchemaRegistry:Inference:AutoRegister", true);

        services.AddSingleton<ITopicMessageSampler, TopicMessageSampler>();
        services.AddSurgewaveSchemaInference(new SchemaInferenceConfig
        {
            Enabled = inferenceEnabled,
            SampleSize = inferenceSampleSize,
            RefreshIntervalSeconds = inferenceRefreshInterval,
            AutoRegister = inferenceAutoRegister
        });

        // Schema evolution analysis (AI-assisted)
        var evolutionEnabled = configuration.GetValue("Surgewave:SchemaEvolution:Enabled", false);
        var evolutionInterval = configuration.GetValue("Surgewave:SchemaEvolution:CheckIntervalSeconds", 60);
        var evolutionAutoCode = configuration.GetValue("Surgewave:SchemaEvolution:AutoGenerateCode", true);
        var evolutionNotifyAssistant = configuration.GetValue("Surgewave:SchemaEvolution:NotifyAssistant", true);
        var evolutionNamespace = configuration.GetValue<string>("Surgewave:SchemaEvolution:DefaultNamespace") ?? "Surgewave.Models";

        services.AddSurgewaveSchemaEvolution(new SchemaEvolutionConfig
        {
            Enabled = evolutionEnabled,
            CheckIntervalSeconds = evolutionInterval,
            AutoGenerateCode = evolutionAutoCode,
            NotifyAssistant = evolutionNotifyAssistant,
            DefaultNamespace = evolutionNamespace
        });

        // Zero-downtime schema migration
        var migrationEnabled = configuration.GetValue("Surgewave:SchemaMigration:Enabled", false);
        var migrationAutoRead = configuration.GetValue("Surgewave:SchemaMigration:AutoMigrateOnRead", true);
        var migrationAutoWrite = configuration.GetValue("Surgewave:SchemaMigration:AutoMigrateOnWrite", false);
        var migrationMissing = configuration.GetValue<string>("Surgewave:SchemaMigration:MissingFieldStrategy") ?? "UseDefault";
        var migrationExtra = configuration.GetValue<string>("Surgewave:SchemaMigration:ExtraFieldStrategy") ?? "Ignore";
        var migrationTypeMismatch = configuration.GetValue<string>("Surgewave:SchemaMigration:TypeMismatchStrategy") ?? "Coerce";
        var migrationMaxCache = configuration.GetValue("Surgewave:SchemaMigration:MaxCachedMigrators", 100);

        services.AddSurgewaveSchemaMigration(new SchemaMigrationConfig
        {
            Enabled = migrationEnabled,
            AutoMigrateOnRead = migrationAutoRead,
            AutoMigrateOnWrite = migrationAutoWrite,
            MissingFieldStrategy = Enum.TryParse<MissingFieldStrategy>(migrationMissing, ignoreCase: true, out var missingStrat)
                ? missingStrat
                : MissingFieldStrategy.UseDefault,
            ExtraFieldStrategy = Enum.TryParse<ExtraFieldStrategy>(migrationExtra, ignoreCase: true, out var extraStrat)
                ? extraStrat
                : ExtraFieldStrategy.Ignore,
            TypeMismatchStrategy = Enum.TryParse<TypeMismatchStrategy>(migrationTypeMismatch, ignoreCase: true, out var typeStrat)
                ? typeStrat
                : TypeMismatchStrategy.Coerce,
            MaxCachedMigrators = migrationMaxCache
        });

        // Cross-cluster schema linking
        var linkingEnabled = configuration.GetValue("Surgewave:SchemaLinking:Enabled", false);
        var linkingInterval = configuration.GetValue("Surgewave:SchemaLinking:SyncIntervalSeconds", 30);
        var linkingSyncMode = configuration.GetValue<string>("Surgewave:SchemaLinking:SyncMode") ?? "Bidirectional";
        var linkingSyncCompat = configuration.GetValue("Surgewave:SchemaLinking:SyncCompatibilityConfig", true);
        var linkingConflict = configuration.GetValue<string>("Surgewave:SchemaLinking:ConflictResolution") ?? "HighestVersion";
        var linkingPatterns = configuration.GetSection("Surgewave:SchemaLinking:SubjectPatterns").Get<List<string>>() ?? ["*"];
        var linkingRemotes = configuration.GetSection("Surgewave:SchemaLinking:RemoteRegistries").Get<List<LinkedSchemaRegistry>>() ?? [];
        var linkingStatePath = Path.Combine(
            configuration.GetValue<string>("Surgewave:DataDirectory") ?? "./data", "schema-linking-state.json");

        services.AddSurgewaveSchemaLinking(
            new SchemaLinkingConfig
            {
                Enabled = linkingEnabled,
                SyncIntervalSeconds = linkingInterval,
                SyncMode = Enum.TryParse<SchemaSyncMode>(linkingSyncMode, ignoreCase: true, out var syncMode)
                    ? syncMode
                    : SchemaSyncMode.Bidirectional,
                SyncCompatibilityConfig = linkingSyncCompat,
                ConflictResolution = Enum.TryParse<ConflictResolution>(linkingConflict, ignoreCase: true, out var conflictRes)
                    ? conflictRes
                    : ConflictResolution.HighestVersion,
                SubjectPatterns = linkingPatterns,
                RemoteRegistries = linkingRemotes
            },
            localClusterId: configuration.GetValue<string>("Surgewave:ClusterId"),
            statePath: linkingStatePath);
    }

    /// <inheritdoc />
    public void Configure(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var logger = services.GetRequiredService<ILogger<Program>>();
        var config = services.GetRequiredService<BrokerConfig>();

        // External mode: proxy schema requests to standalone instance
        if (!string.IsNullOrEmpty(config.SchemaRegistry.ExternalUrl))
        {
            var externalUrl = config.SchemaRegistry.ExternalUrl.TrimEnd('/');
            var httpClientFactory = services.GetRequiredService<IHttpClientFactory>();
            var httpClient = httpClientFactory.CreateClient("SchemaRegistryProxy");
            httpClient.BaseAddress = new Uri(externalUrl + "/");

            foreach (var prefix in new[] { "/subjects", "/schemas", "/config", "/compatibility" })
            {
                app.Map(prefix + "/{**rest}", async (HttpContext ctx) =>
                {
                    var targetPath = ctx.Request.Path + ctx.Request.QueryString;
                    var request = new HttpRequestMessage(new HttpMethod(ctx.Request.Method), targetPath);
                    if (ctx.Request.ContentLength > 0)
                    {
                        request.Content = new StreamContent(ctx.Request.Body);
                        request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(
                            ctx.Request.ContentType ?? "application/json");
                    }
                    var response = await httpClient.SendAsync(request, ctx.RequestAborted);
                    ctx.Response.StatusCode = (int)response.StatusCode;
                    ctx.Response.ContentType = response.Content.Headers.ContentType?.ToString() ?? "application/json";
                    await response.Content.CopyToAsync(ctx.Response.Body, ctx.RequestAborted);
                });
            }

            logger.LogInformation("Schema Registry proxy → {ExternalUrl}", externalUrl);
            return;
        }
        var schemaStore = services.GetRequiredService<SchemaStore>();
        var compatibilityChecker = services.GetRequiredService<CompatibilityChecker>();

        // Wire the gRPC service holder
        SchemaRegistryServiceImplHolder.Instance = new SchemaRegistryServiceImpl(
            listSubjects: (includeDeleted) => schemaStore.GetSubjects(includeDeleted).ToList(),
            getSubjectVersions: (subject, includeDeleted) => schemaStore.GetVersions(subject, includeDeleted).ToList(),
            registerSchema: (subject, schema, schemaType, references) =>
            {
                var refs = references?.Select(r => new SchemaReference(r.Name, r.Subject, r.Version)).ToList();
                var result = schemaStore.RegisterSchema(subject, schema, (SchemaType)schemaType, refs);
                return new SchemaDto(
                    result.Id, result.Version, result.Subject, result.SchemaString,
                    (int)result.SchemaType,
                    result.References?.Select(r => new SchemaReferenceDto(r.Name, r.Subject, r.Version)).ToList());
            },
            getSchemaById: (id) =>
            {
                var schema = schemaStore.GetSchemaById(id);
                return schema == null ? null : new SchemaDto(
                    schema.Id, schema.Version, schema.Subject, schema.SchemaString,
                    (int)schema.SchemaType,
                    schema.References?.Select(r => new SchemaReferenceDto(r.Name, r.Subject, r.Version)).ToList());
            },
            getSchemaByVersion: (subject, version) =>
            {
                var schema = schemaStore.GetSchema(subject, version);
                return schema == null ? null : new SchemaDto(
                    schema.Id, schema.Version, schema.Subject, schema.SchemaString,
                    (int)schema.SchemaType,
                    schema.References?.Select(r => new SchemaReferenceDto(r.Name, r.Subject, r.Version)).ToList());
            },
            deleteSubject: (subject, permanent) => schemaStore.DeleteSubject(subject, permanent).ToList(),
            deleteSchemaVersion: (subject, version, permanent) => schemaStore.DeleteVersion(subject, version, permanent),
            checkCompatibility: (subject, schema, schemaType, version, references) =>
            {
                var mode = schemaStore.GetCompatibility(subject);
                var existingSchemas = schemaStore.GetSchemasForCompatibilityCheck(subject, mode);
                var result = compatibilityChecker.CheckCompatibility(schema, (SchemaType)schemaType, existingSchemas, mode);
                return new CompatibilityResultDto(result.IsCompatible, result.Messages?.ToList());
            },
            getCompatibilityConfig: (subject) => (int)schemaStore.GetCompatibility(subject),
            setCompatibilityConfig: (subject, level) =>
            {
                schemaStore.SetCompatibility(subject, (CompatibilityMode)level);
                return level;
            },
            getSchemaTypes: () => compatibilityChecker.GetSupportedTypes().ToList());

        // Map REST + gRPC endpoints
        app.MapSurgewaveSchemaRegistry();
        app.MapGrpcService<SchemaRegistryServiceImpl>();

        // Schema Linking endpoints
        var schemaLinkingConfig = services.GetService<SchemaLinkingConfig>();
        if (schemaLinkingConfig?.Enabled == true)
        {
            app.MapSurgewaveSchemaLinking();
            logger.LogInformation("Schema Linking enabled (cross-cluster schema synchronization)");
        }

        logger.LogInformation("Schema Registry enabled at /subjects, /schemas, /config (gRPC + REST)");
        logger.LogInformation("  - Schema Registry:     {Host}:{GrpcPort}/subjects", config.Host, config.GrpcPort);
    }

    private static bool HasExternalUrl(IConfiguration configuration)
        => !string.IsNullOrEmpty(configuration.GetValue<string>("Surgewave:SchemaRegistry:ExternalUrl"));
}
