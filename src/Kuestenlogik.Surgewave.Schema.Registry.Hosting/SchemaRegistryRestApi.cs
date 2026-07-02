using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Kuestenlogik.Bowire;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// REST API for Schema Registry, compatible with Confluent Schema Registry API.
/// Uses Bowire as the OpenAPI workbench (Surgewave-wide default — replaced
/// the per-surface Scalar dependency).
/// </summary>
public static class SchemaRegistryRestApi
{
    /// <summary>
    /// Adds Schema Registry services to the service collection.
    /// </summary>
    public static IServiceCollection AddSurgewaveSchemaRegistry(this IServiceCollection services, SchemaRegistryConfig? config = null)
    {
        config ??= new SchemaRegistryConfig();

        services.AddSingleton(config);
        services.AddSingleton<SchemaStore>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<SchemaStore>>();
            return new SchemaStore(logger, config.DataPath);
        });
        services.AddSingleton<ISchemaStore>(sp => sp.GetRequiredService<SchemaStore>());

        // Register the handler registry (collects all ISchemaTypeHandler implementations)
        services.AddSingleton<ISchemaTypeHandlerRegistry, SchemaTypeHandlerRegistry>();

        // Register the compatibility checker (uses the handler registry)
        services.AddSingleton<CompatibilityChecker>();

        services.AddOpenApi(options =>
        {
            options.AddDocumentTransformer((document, context, cancellationToken) =>
            {
                document.Info.Title = "Surgewave Schema Registry API";
                document.Info.Version = "1.0.0";
                document.Info.Description = "Confluent Schema Registry compatible REST API";
                return Task.CompletedTask;
            });
        });

        return services;
    }

    /// <summary>
    /// Adds a custom schema type handler to the service collection.
    /// Call this before AddSurgewaveSchemaRegistry to register custom handlers.
    /// </summary>
    public static IServiceCollection AddSchemaTypeHandler<THandler>(this IServiceCollection services)
        where THandler : class, ISchemaTypeHandler
    {
        services.AddSingleton<ISchemaTypeHandler, THandler>();
        return services;
    }

    /// <summary>
    /// Adds schema inference services to the service collection.
    /// Must be called after AddSurgewaveSchemaRegistry.
    /// </summary>
    public static IServiceCollection AddSurgewaveSchemaInference(
        this IServiceCollection services,
        SchemaInferenceConfig? config = null)
    {
        config ??= new SchemaInferenceConfig();

        services.AddSingleton(config);
        services.AddSingleton<SchemaInferenceEngine>();
        services.AddSingleton<SchemaInferenceService>();

        if (config.Enabled)
        {
            services.AddHostedService(sp => sp.GetRequiredService<SchemaInferenceService>());
        }

        return services;
    }

    /// <summary>
    /// Adds schema evolution analysis services to the service collection.
    /// Must be called after AddSurgewaveSchemaRegistry.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="config">Optional evolution config (uses defaults if null).</param>
    /// <param name="llmCompleteFunc">Optional LLM completion delegate for AI-enhanced explanations.
    /// Signature: (systemPrompt, userMessage, ct) => Task&lt;string&gt;. Pass null for rule-based only.</param>
    public static IServiceCollection AddSurgewaveSchemaEvolution(
        this IServiceCollection services,
        SchemaEvolutionConfig? config = null,
        Func<string, string, CancellationToken, Task<string>>? llmCompleteFunc = null)
    {
        config ??= new SchemaEvolutionConfig();

        services.AddSingleton(config);
        services.AddSingleton<SchemaEvolutionAnalyzer>();
        services.AddSingleton<SchemaMigrationCodeGenerator>();
        services.AddSingleton(_ => new SchemaEvolutionLlmEnhancer(llmCompleteFunc));
        services.AddSingleton<SchemaEvolutionService>();

        if (config.Enabled)
        {
            services.AddHostedService(sp => sp.GetRequiredService<SchemaEvolutionService>());
        }

        return services;
    }

    /// <summary>
    /// Adds zero-downtime schema migration services to the service collection.
    /// Must be called after AddSurgewaveSchemaRegistry.
    /// </summary>
    public static IServiceCollection AddSurgewaveSchemaMigration(
        this IServiceCollection services,
        SchemaMigrationConfig? config = null)
    {
        config ??= new SchemaMigrationConfig();

        services.AddSingleton(config);
        services.AddSingleton<SchemaMigrator>();
        services.AddSingleton(new SchemaMigrationCache(config.MaxCachedMigrators));
        services.AddSingleton<SchemaMigrationInterceptor>();

        return services;
    }

    /// <summary>
    /// Adds schema linking services to the service collection.
    /// Must be called after AddSurgewaveSchemaRegistry.
    /// </summary>
    public static IServiceCollection AddSurgewaveSchemaLinking(
        this IServiceCollection services,
        SchemaLinkingConfig? config = null,
        string? localClusterId = null,
        string? statePath = null)
    {
        config ??= new SchemaLinkingConfig();

        services.AddSingleton(config);
        services.AddSingleton<SchemaLinkingService>(sp =>
        {
            var store = sp.GetRequiredService<SchemaStore>();
            var logger = sp.GetRequiredService<ILogger<SchemaLinkingService>>();
            return new SchemaLinkingService(config, store, logger, localClusterId, statePath);
        });

        if (config.Enabled)
        {
            services.AddHostedService(sp => sp.GetRequiredService<SchemaLinkingService>());
        }

        return services;
    }

    /// <summary>
    /// Maps Schema Registry REST API endpoints.
    /// </summary>
    /// <param name="app">The endpoint route builder.</param>
    /// <param name="mapBowireWorkbench">
    /// Whether to map the Bowire workbench at <c>/bowire</c>. Standalone
    /// hosts (Schema.Registry.App) keep the default; hosts that already map
    /// their own workbench on the same Kestrel (the broker does in
    /// Program.cs) MUST pass <c>false</c> — a second MapBowire on the same
    /// path throws AmbiguousMatchException on every /bowire request.
    /// </param>
    public static IEndpointRouteBuilder MapSurgewaveSchemaRegistry(this IEndpointRouteBuilder app, bool mapBowireWorkbench = true)
    {
        app.MapOpenApi();
        if (mapBowireWorkbench)
        {
            app.MapBowire("/bowire", options =>
            {
                options.Title = "Surgewave Schema Registry";
                options.Description = "Interactive browser for the Confluent-compatible Schema Registry REST API";
            });
        }

        var group = app.MapGroup("")
            .WithTags("Schema Registry");

        // Root - get schema types
        group.MapGet("/", GetSchemaTypes)
            .WithName("GetSchemaTypes")
            .WithSummary("Get supported schema types")
            .Produces<string[]>();

        // Subjects
        group.MapGet("/subjects", GetSubjects)
            .WithName("GetSubjects")
            .WithSummary("Get all subjects")
            .Produces<IReadOnlyList<string>>();

        group.MapGet("/subjects/{subject}/versions", GetVersions)
            .WithName("GetVersions")
            .WithSummary("Get all versions for a subject")
            .Produces<IReadOnlyList<int>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/subjects/{subject}/versions", RegisterSchema)
            .WithName("RegisterSchema")
            .WithSummary("Register a new schema under a subject")
            .Produces<RegisterSchemaResponse>(StatusCodes.Status200OK)
            .ProducesValidationProblem()
            .ProducesProblem(StatusCodes.Status409Conflict);

        group.MapGet("/subjects/{subject}/versions/{version}", GetSchemaByVersion)
            .WithName("GetSchemaByVersion")
            .WithSummary("Get schema by subject and version")
            .Produces<SchemaResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/subjects/{subject}/versions/latest", GetLatestSchema)
            .WithName("GetLatestSchema")
            .WithSummary("Get latest schema for a subject")
            .Produces<SchemaResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapDelete("/subjects/{subject}", DeleteSubject)
            .WithName("DeleteSubject")
            .WithSummary("Delete a subject and all its versions")
            .Produces<IReadOnlyList<int>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapDelete("/subjects/{subject}/versions/{version}", DeleteVersion)
            .WithName("DeleteVersion")
            .WithSummary("Delete a specific version of a subject")
            .Produces<int>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/subjects/{subject}", LookupSchema)
            .WithName("LookupSchema")
            .WithSummary("Check if schema is already registered under subject")
            .Produces<SchemaResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Schemas by ID
        group.MapGet("/schemas/ids/{id:int}", GetSchemaById)
            .WithName("GetSchemaById")
            .WithSummary("Get schema by its global ID")
            .Produces<GetSchemaByIdResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/schemas/ids/{id:int}/versions", GetSchemaVersions)
            .WithName("GetSchemaVersions")
            .WithSummary("Get all subject-versions for a schema ID")
            .Produces<IReadOnlyList<SubjectVersion>>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/schemas/types", GetSupportedTypes)
            .WithName("GetSupportedTypes")
            .WithSummary("Get supported schema types")
            .Produces<string[]>();

        // Compatibility
        group.MapPost("/compatibility/subjects/{subject}/versions/{version}", CheckCompatibility)
            .WithName("CheckCompatibility")
            .WithSummary("Test schema compatibility against a specific version")
            .Produces<CompatibilityCheckResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/compatibility/subjects/{subject}/versions/latest", CheckCompatibilityLatest)
            .WithName("CheckCompatibilityLatest")
            .WithSummary("Test schema compatibility against latest version")
            .Produces<CompatibilityCheckResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Config
        group.MapGet("/config", GetGlobalConfig)
            .WithName("GetGlobalConfig")
            .WithSummary("Get global compatibility config")
            .Produces<ConfigResponse>();

        group.MapPut("/config", SetGlobalConfig)
            .WithName("SetGlobalConfig")
            .WithSummary("Set global compatibility config")
            .Produces<ConfigResponse>();

        group.MapGet("/config/{subject}", GetSubjectConfig)
            .WithName("GetSubjectConfig")
            .WithSummary("Get subject-level compatibility config")
            .Produces<ConfigResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPut("/config/{subject}", SetSubjectConfig)
            .WithName("SetSubjectConfig")
            .WithSummary("Set subject-level compatibility config")
            .Produces<ConfigResponse>();

        group.MapDelete("/config/{subject}", DeleteSubjectConfig)
            .WithName("DeleteSubjectConfig")
            .WithSummary("Delete subject-level compatibility config")
            .Produces<ConfigResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        // Mode
        group.MapGet("/mode", GetGlobalMode)
            .WithName("GetGlobalMode")
            .WithSummary("Get global mode")
            .Produces<ModeResponse>();

        // Schema Inference endpoints
        group.MapGet("/schemas/infer/{topic}", InferSchemaForTopic)
            .WithName("InferSchemaForTopic")
            .WithSummary("Infer JSON Schema from topic messages")
            .WithTags("Schema Inference")
            .Produces<InferredSchemaResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/schemas/infer/{topic}/register", InferAndRegisterSchema)
            .WithName("InferAndRegisterSchema")
            .WithSummary("Infer schema and register it in the registry")
            .WithTags("Schema Inference")
            .Produces<RegisterSchemaResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/schemas/inferred", ListInferredSchemas)
            .WithName("ListInferredSchemas")
            .WithSummary("List all auto-inferred schemas")
            .WithTags("Schema Inference")
            .Produces<IReadOnlyList<InferredSchemaSummary>>();

        // Schema Evolution endpoints
        group.MapGet("/api/schema-evolution/changes", GetEvolutionChanges)
            .WithName("GetEvolutionChanges")
            .WithSummary("List all detected schema changes")
            .WithTags("Schema Evolution")
            .Produces<IReadOnlyList<SchemaChange>>();

        group.MapGet("/api/schema-evolution/changes/{subject}", GetEvolutionChangesForSubject)
            .WithName("GetEvolutionChangesForSubject")
            .WithSummary("Get schema changes for a specific subject")
            .WithTags("Schema Evolution")
            .Produces<IReadOnlyList<SchemaChange>>();

        group.MapGet("/api/schema-evolution/report/{subject}/{version:int}", GetEvolutionReport)
            .WithName("GetEvolutionReport")
            .WithSummary("Get impact report for a schema version")
            .WithTags("Schema Evolution")
            .Produces<SchemaImpactReport>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/api/schema-evolution/code/{subject}/{version:int}", GetEvolutionCode)
            .WithName("GetEvolutionCode")
            .WithSummary("Get generated migration code for a schema version")
            .WithTags("Schema Evolution")
            .Produces<SchemaEvolutionCodeResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/api/schema-evolution/analyze", AnalyzeSchemaEvolution)
            .WithName("AnalyzeSchemaEvolution")
            .WithSummary("Manually analyze two schemas for evolution")
            .WithTags("Schema Evolution")
            .Produces<SchemaEvolutionAnalyzeResult>();

        group.MapPost("/api/schema-evolution/generate-model", GenerateModelFromSchema)
            .WithName("GenerateModelFromSchema")
            .WithSummary("Generate a C# model class from a JSON schema")
            .WithTags("Schema Evolution")
            .Produces<SchemaEvolutionCodeResponse>();

        // Schema Migration endpoints (zero-downtime)
        group.MapPost("/api/schema-migration/migrate", MigrateMessage)
            .WithName("MigrateMessage")
            .WithSummary("Migrate a JSON message between schema versions")
            .WithTags("Schema Migration")
            .Produces<SchemaMigrationResponse>()
            .ProducesValidationProblem();

        group.MapGet("/api/schema-migration/path/{subject}/{from:int}/{to:int}", GetMigrationPath)
            .WithName("GetMigrationPath")
            .WithSummary("Get the migration path between two schema versions")
            .WithTags("Schema Migration")
            .Produces<MigrationPath>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapPost("/api/schema-migration/test", TestMigration)
            .WithName("TestMigration")
            .WithSummary("Test migration with sample data and schemas")
            .WithTags("Schema Migration")
            .Produces<SchemaMigrationTestResponse>()
            .ProducesValidationProblem();

        group.MapGet("/api/schema-migration/config", GetMigrationConfig)
            .WithName("GetMigrationConfig")
            .WithSummary("Get current schema migration configuration")
            .WithTags("Schema Migration")
            .Produces<SchemaMigrationConfig>();

        group.MapPut("/api/schema-migration/config", UpdateMigrationConfig)
            .WithName("UpdateMigrationConfig")
            .WithSummary("Update schema migration configuration")
            .WithTags("Schema Migration")
            .Produces<SchemaMigrationConfig>();

        group.MapGet("/api/schema-migration/cache/stats", GetMigrationCacheStats)
            .WithName("GetMigrationCacheStats")
            .WithSummary("Get schema migration cache statistics")
            .WithTags("Schema Migration")
            .Produces<SchemaMigrationCacheStats>();

        return app;
    }

    private static string[] GetSchemaTypes(ISchemaTypeHandlerRegistry registry) => registry.GetSupportedTypes().ToArray();

    private static IReadOnlyList<string> GetSubjects(SchemaStore store, bool? deleted)
    {
        return store.GetSubjects(deleted ?? false);
    }

    private static IResult GetVersions(string subject, SchemaStore store, bool? deleted)
    {
        var versions = store.GetVersions(subject, deleted ?? false);
        if (versions.Count == 0 && store.GetLatestSchema(subject) == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40401, Message = $"Subject '{subject}' not found" });
        }
        return Results.Ok(versions);
    }

    private static IResult RegisterSchema(
        string subject,
        RegisterSchemaRequest request,
        SchemaStore store,
        CompatibilityChecker checker)
    {
        if (string.IsNullOrEmpty(request.Schema))
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42201, Message = "Schema cannot be empty" });
        }

        var schemaType = ParseSchemaType(request.SchemaType);

        // Validate schema
        var (isValid, error) = checker.ValidateSchema(request.Schema, schemaType);
        if (!isValid)
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42201, Message = $"Invalid schema: {error}" });
        }

        // Check compatibility
        var compatibility = store.GetCompatibility(subject);
        var existingSchemas = store.GetSchemasForCompatibilityCheck(subject, compatibility);

        if (existingSchemas.Count > 0)
        {
            var compatibilityResult = checker.CheckCompatibility(request.Schema, schemaType, existingSchemas, compatibility);
            if (!compatibilityResult.IsCompatible)
            {
                return Results.Conflict(new ErrorResponse
                {
                    ErrorCode = 409,
                    Message = string.Join("; ", compatibilityResult.Messages ?? ["Schema is not compatible"])
                });
            }
        }

        var schema = store.RegisterSchema(subject, request.Schema, schemaType, request.References?.Select(r => new SchemaReference(r.Name, r.Subject, r.Version)).ToList());

        return Results.Ok(new RegisterSchemaResponse { Id = schema.Id });
    }

    private static IResult GetSchemaByVersion(string subject, string version, SchemaStore store)
    {
        Schema? schema;

        if (version.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            schema = store.GetLatestSchema(subject);
        }
        else if (int.TryParse(version, out var versionNum))
        {
            schema = store.GetSchema(subject, versionNum);
        }
        else
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42202, Message = "Invalid version" });
        }

        if (schema == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40402, Message = "Version not found" });
        }

        return Results.Ok(new SchemaResponse
        {
            Subject = schema.Subject,
            Id = schema.Id,
            Version = schema.Version,
            SchemaType = schema.SchemaType.ToString().ToUpperInvariant(),
            Schema = schema.SchemaString,
            References = schema.References?.Select(r => new SchemaReferenceResponse
            {
                Name = r.Name,
                Subject = r.Subject,
                Version = r.Version
            }).ToList()
        });
    }

    private static IResult GetLatestSchema(string subject, SchemaStore store)
    {
        return GetSchemaByVersion(subject, "latest", store);
    }

    private static IResult DeleteSubject(string subject, SchemaStore store, bool? permanent)
    {
        var deletedVersions = store.DeleteSubject(subject, permanent ?? false);
        if (deletedVersions.Count == 0)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40401, Message = $"Subject '{subject}' not found" });
        }
        return Results.Ok(deletedVersions);
    }

    private static IResult DeleteVersion(string subject, string version, SchemaStore store, bool? permanent)
    {
        if (!int.TryParse(version, out var versionNum))
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42202, Message = "Invalid version" });
        }

        var deleted = store.DeleteVersion(subject, versionNum, permanent ?? false);
        if (deleted == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40402, Message = "Version not found" });
        }
        return Results.Ok(deleted);
    }

    private static IResult LookupSchema(string subject, RegisterSchemaRequest request, SchemaStore store)
    {
        if (string.IsNullOrEmpty(request.Schema))
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42201, Message = "Schema cannot be empty" });
        }

        var schemaType = ParseSchemaType(request.SchemaType);
        var schemaId = store.LookupSchemaId(subject, request.Schema, schemaType);

        if (schemaId == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40403, Message = "Schema not found" });
        }

        var schema = store.GetSchemaById(schemaId.Value);
        if (schema == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40403, Message = "Schema not found" });
        }

        // Find the version under this subject
        var versions = store.GetVersions(subject);
        foreach (var v in versions)
        {
            var s = store.GetSchema(subject, v);
            if (s?.Id == schemaId)
            {
                return Results.Ok(new SchemaResponse
                {
                    Subject = subject,
                    Id = s.Id,
                    Version = s.Version,
                    SchemaType = s.SchemaType.ToString().ToUpperInvariant(),
                    Schema = s.SchemaString
                });
            }
        }

        return Results.NotFound(new ErrorResponse { ErrorCode = 40403, Message = "Schema not found" });
    }

    private static IResult GetSchemaById(int id, SchemaStore store)
    {
        var schema = store.GetSchemaById(id);
        if (schema == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40403, Message = "Schema not found" });
        }

        return Results.Ok(new GetSchemaByIdResponse
        {
            Schema = schema.SchemaString,
            SchemaType = schema.SchemaType.ToString().ToUpperInvariant(),
            References = schema.References?.Select(r => new SchemaReferenceResponse
            {
                Name = r.Name,
                Subject = r.Subject,
                Version = r.Version
            }).ToList()
        });
    }

    private static IResult GetSchemaVersions(int id, SchemaStore store)
    {
        var schema = store.GetSchemaById(id);
        if (schema == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40403, Message = "Schema not found" });
        }

        // Find all subject-versions for this schema
        var subjectVersions = new List<SubjectVersion>();
        foreach (var subject in store.GetSubjects())
        {
            foreach (var version in store.GetVersions(subject))
            {
                var s = store.GetSchema(subject, version);
                if (s?.Id == id)
                {
                    subjectVersions.Add(new SubjectVersion { Subject = subject, Version = version });
                }
            }
        }

        return Results.Ok(subjectVersions);
    }

    private static string[] GetSupportedTypes(ISchemaTypeHandlerRegistry registry) => registry.GetSupportedTypes().ToArray();

    private static IResult CheckCompatibility(
        string subject,
        string version,
        RegisterSchemaRequest request,
        SchemaStore store,
        CompatibilityChecker checker)
    {
        if (string.IsNullOrEmpty(request.Schema))
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42201, Message = "Schema cannot be empty" });
        }

        var schemaType = ParseSchemaType(request.SchemaType);
        var compatibility = store.GetCompatibility(subject);

        Schema? existingSchema;
        if (version.Equals("latest", StringComparison.OrdinalIgnoreCase))
        {
            existingSchema = store.GetLatestSchema(subject);
        }
        else if (int.TryParse(version, out var versionNum))
        {
            existingSchema = store.GetSchema(subject, versionNum);
        }
        else
        {
            return Results.BadRequest(new ErrorResponse { ErrorCode = 42202, Message = "Invalid version" });
        }

        if (existingSchema == null)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40402, Message = "Version not found" });
        }

        var result = checker.CheckCompatibility(request.Schema, schemaType, [existingSchema], compatibility);

        return Results.Ok(new CompatibilityCheckResponse
        {
            IsCompatible = result.IsCompatible,
            Messages = result.Messages
        });
    }

    private static IResult CheckCompatibilityLatest(
        string subject,
        RegisterSchemaRequest request,
        SchemaStore store,
        CompatibilityChecker checker)
    {
        return CheckCompatibility(subject, "latest", request, store, checker);
    }

    private static ConfigResponse GetGlobalConfig(SchemaStore store)
    {
        return new ConfigResponse { CompatibilityLevel = store.GlobalCompatibility.ToString().ToUpperInvariant() };
    }

    private static ConfigResponse SetGlobalConfig(ConfigRequest request, SchemaStore store)
    {
        var compatibility = ParseCompatibilityMode(request.Compatibility);
        store.GlobalCompatibility = compatibility;
        return new ConfigResponse { CompatibilityLevel = compatibility.ToString().ToUpperInvariant() };
    }

    private static IResult GetSubjectConfig(string subject, SchemaStore store)
    {
        if (store.GetLatestSchema(subject) == null && store.GetVersions(subject).Count == 0)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40401, Message = $"Subject '{subject}' not found" });
        }

        var compatibility = store.GetCompatibility(subject);
        return Results.Ok(new ConfigResponse { CompatibilityLevel = compatibility.ToString().ToUpperInvariant() });
    }

    private static ConfigResponse SetSubjectConfig(string subject, ConfigRequest request, SchemaStore store)
    {
        var compatibility = ParseCompatibilityMode(request.Compatibility);
        store.SetCompatibility(subject, compatibility);
        return new ConfigResponse { CompatibilityLevel = compatibility.ToString().ToUpperInvariant() };
    }

    private static IResult DeleteSubjectConfig(string subject, SchemaStore store)
    {
        if (store.GetLatestSchema(subject) == null && store.GetVersions(subject).Count == 0)
        {
            return Results.NotFound(new ErrorResponse { ErrorCode = 40401, Message = $"Subject '{subject}' not found" });
        }

        // Reset to global default
        store.SetCompatibility(subject, store.GlobalCompatibility);
        return Results.Ok(new ConfigResponse { CompatibilityLevel = store.GlobalCompatibility.ToString().ToUpperInvariant() });
    }

    private static ModeResponse GetGlobalMode() => new() { Mode = "READWRITE" };

    private static SchemaType ParseSchemaType(string? schemaType)
    {
        return schemaType?.ToUpperInvariant() switch
        {
            "JSON" => SchemaType.Json,
            "PROTOBUF" => SchemaType.Protobuf,
            "FLATBUFFERS" => SchemaType.FlatBuffers,
            _ => SchemaType.Avro // Default to Avro
        };
    }

    private static CompatibilityMode ParseCompatibilityMode(string? compatibility)
    {
        return compatibility?.ToUpperInvariant() switch
        {
            "NONE" => CompatibilityMode.None,
            "BACKWARD" => CompatibilityMode.Backward,
            "BACKWARD_TRANSITIVE" => CompatibilityMode.BackwardTransitive,
            "FORWARD" => CompatibilityMode.Forward,
            "FORWARD_TRANSITIVE" => CompatibilityMode.ForwardTransitive,
            "FULL" => CompatibilityMode.Full,
            "FULL_TRANSITIVE" => CompatibilityMode.FullTransitive,
            _ => CompatibilityMode.Backward
        };
    }

    // ========== Schema Inference Endpoints ==========

    private static async Task<IResult> InferSchemaForTopic(
        string topic,
        SchemaInferenceService inferenceService,
        int? sample)
    {
        var response = await inferenceService.InferSchemaForTopicAsync(topic, sample);
        if (response is null)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40401,
                Message = $"Topic '{topic}' not found or contains no valid JSON messages"
            });
        }
        return Results.Ok(response);
    }

    private static async Task<IResult> InferAndRegisterSchema(
        string topic,
        SchemaInferenceService inferenceService)
    {
        var schema = await inferenceService.RegisterInferredSchemaAsync(topic);
        if (schema is null)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40401,
                Message = $"Topic '{topic}' not found or contains no valid JSON messages"
            });
        }
        return Results.Ok(new RegisterSchemaResponse { Id = schema.Id });
    }

    private static IReadOnlyList<InferredSchemaSummary> ListInferredSchemas(
        SchemaInferenceService inferenceService)
    {
        return inferenceService.GetInferredSchemas();
    }

    // ========== Schema Evolution Endpoints ==========

    private static IReadOnlyList<SchemaChange> GetEvolutionChanges(
        SchemaEvolutionService evolutionService)
    {
        return evolutionService.GetAllChanges();
    }

    private static IReadOnlyList<SchemaChange> GetEvolutionChangesForSubject(
        string subject,
        SchemaEvolutionService evolutionService)
    {
        return evolutionService.GetChangesForSubject(subject);
    }

    private static IResult GetEvolutionReport(
        string subject,
        int version,
        SchemaEvolutionService evolutionService)
    {
        var report = evolutionService.GetReport(subject, version);
        if (report is null)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40401,
                Message = $"No evolution report found for '{subject}' version {version}"
            });
        }
        return Results.Ok(report);
    }

    private static IResult GetEvolutionCode(
        string subject,
        int version,
        SchemaEvolutionService evolutionService)
    {
        var code = evolutionService.GetMigrationCode(subject, version);
        if (code is null)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40401,
                Message = $"No migration code found for '{subject}' version {version}"
            });
        }
        return Results.Ok(new SchemaEvolutionCodeResponse { Code = code });
    }

    private static IResult AnalyzeSchemaEvolution(
        SchemaEvolutionAnalyzeRequest request,
        SchemaEvolutionService evolutionService)
    {
        if (string.IsNullOrEmpty(request.OldSchema) || string.IsNullOrEmpty(request.NewSchema))
        {
            return Results.BadRequest(new ErrorResponse
            {
                ErrorCode = 42201,
                Message = "Both oldSchema and newSchema are required"
            });
        }

        var result = evolutionService.AnalyzeManually(request.OldSchema, request.NewSchema, request.Subject ?? "manual");
        return Results.Ok(result);
    }

    private static IResult GenerateModelFromSchema(
        SchemaEvolutionGenerateModelRequest request,
        SchemaMigrationCodeGenerator codeGen,
        SchemaEvolutionConfig config)
    {
        if (string.IsNullOrEmpty(request.Schema))
        {
            return Results.BadRequest(new ErrorResponse
            {
                ErrorCode = 42201,
                Message = "Schema is required"
            });
        }

        var className = request.ClassName ?? "GeneratedModel";
        var namespaceName = request.Namespace ?? config.DefaultNamespace;
        var code = codeGen.GenerateModelClass(request.Schema, className, namespaceName);

        return Results.Ok(new SchemaEvolutionCodeResponse { Code = code });
    }

    // ========== Schema Migration Endpoints (Zero-Downtime) ==========

    private static IResult MigrateMessage(
        SchemaMigrationMigrateRequest request,
        SchemaMigrator migrator,
        SchemaMigrationConfig config,
        SchemaStore store)
    {
        if (string.IsNullOrEmpty(request.Message) || string.IsNullOrEmpty(request.Subject))
        {
            return Results.BadRequest(new ErrorResponse
            {
                ErrorCode = 42201,
                Message = "Subject and message are required"
            });
        }

        var fromSchema = store.GetSchema(request.Subject, request.FromVersion);
        var toSchema = store.GetSchema(request.Subject, request.ToVersion);

        if (fromSchema is null)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40402,
                Message = $"Source schema version {request.FromVersion} not found for '{request.Subject}'"
            });
        }

        if (toSchema is null)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40402,
                Message = $"Target schema version {request.ToVersion} not found for '{request.Subject}'"
            });
        }

        try
        {
            var messageBytes = System.Text.Encoding.UTF8.GetBytes(request.Message);
            var migratedBytes = migrator.Migrate(messageBytes, fromSchema.SchemaString, toSchema.SchemaString, config);
            var migratedJson = System.Text.Encoding.UTF8.GetString(migratedBytes);

            return Results.Ok(new SchemaMigrationResponse
            {
                OriginalMessage = request.Message,
                MigratedMessage = migratedJson,
                FromVersion = request.FromVersion,
                ToVersion = request.ToVersion,
                Subject = request.Subject
            });
        }
        catch (SchemaMigrationException ex)
        {
            return Results.BadRequest(new ErrorResponse
            {
                ErrorCode = 42210,
                Message = $"Migration failed: {ex.Message}"
            });
        }
    }

    private static IResult GetMigrationPath(
        string subject,
        int from,
        int to,
        SchemaMigrator migrator,
        SchemaStore store)
    {
        var versions = store.GetVersions(subject);
        if (versions.Count == 0)
        {
            return Results.NotFound(new ErrorResponse
            {
                ErrorCode = 40401,
                Message = $"Subject '{subject}' not found"
            });
        }

        // Build version-to-schema map
        var schemasByVersion = new Dictionary<int, string>();
        foreach (var v in versions)
        {
            var schema = store.GetSchema(subject, v);
            if (schema is not null)
            {
                schemasByVersion[v] = schema.SchemaString;
            }
        }

        var path = migrator.GetMigrationPath(subject, from, to, schemasByVersion);
        return Results.Ok(path);
    }

    private static IResult TestMigration(
        SchemaMigrationTestRequest request,
        SchemaMigrator migrator,
        SchemaMigrationConfig defaultConfig)
    {
        if (string.IsNullOrEmpty(request.Message) ||
            string.IsNullOrEmpty(request.FromSchema) ||
            string.IsNullOrEmpty(request.ToSchema))
        {
            return Results.BadRequest(new ErrorResponse
            {
                ErrorCode = 42201,
                Message = "Message, fromSchema, and toSchema are required"
            });
        }

        try
        {
            var messageBytes = System.Text.Encoding.UTF8.GetBytes(request.Message);
            var migratedBytes = migrator.Migrate(messageBytes, request.FromSchema, request.ToSchema, defaultConfig);
            var migratedJson = System.Text.Encoding.UTF8.GetString(migratedBytes);

            return Results.Ok(new SchemaMigrationTestResponse
            {
                OriginalMessage = request.Message,
                MigratedMessage = migratedJson,
                Success = true
            });
        }
        catch (SchemaMigrationException ex)
        {
            return Results.Ok(new SchemaMigrationTestResponse
            {
                OriginalMessage = request.Message,
                MigratedMessage = null,
                Success = false,
                Error = ex.Message
            });
        }
    }

    private static SchemaMigrationConfig GetMigrationConfig(SchemaMigrationConfig config)
    {
        return config;
    }

    private static SchemaMigrationConfig UpdateMigrationConfig(
        SchemaMigrationConfig newConfig,
        SchemaMigrationConfig currentConfig)
    {
        currentConfig.Enabled = newConfig.Enabled;
        currentConfig.AutoMigrateOnRead = newConfig.AutoMigrateOnRead;
        currentConfig.AutoMigrateOnWrite = newConfig.AutoMigrateOnWrite;
        currentConfig.MissingFieldStrategy = newConfig.MissingFieldStrategy;
        currentConfig.ExtraFieldStrategy = newConfig.ExtraFieldStrategy;
        currentConfig.TypeMismatchStrategy = newConfig.TypeMismatchStrategy;
        currentConfig.MaxCachedMigrators = newConfig.MaxCachedMigrators;
        return currentConfig;
    }

    private static SchemaMigrationCacheStats GetMigrationCacheStats(SchemaMigrationCache cache)
    {
        return cache.GetStats();
    }
}

/// <summary>
/// Response containing generated C# code.
/// </summary>
public sealed class SchemaEvolutionCodeResponse
{
    /// <summary>Generated C# code string.</summary>
    public required string Code { get; init; }
}

/// <summary>
/// Request to manually analyze two schemas for evolution.
/// </summary>
public sealed class SchemaEvolutionAnalyzeRequest
{
    /// <summary>The old schema JSON string.</summary>
    public string? OldSchema { get; set; }

    /// <summary>The new schema JSON string.</summary>
    public string? NewSchema { get; set; }

    /// <summary>Optional subject name (defaults to "manual").</summary>
    public string? Subject { get; set; }
}

/// <summary>
/// Request to generate a C# model class from a JSON Schema.
/// </summary>
public sealed class SchemaEvolutionGenerateModelRequest
{
    /// <summary>The JSON Schema string.</summary>
    public string? Schema { get; set; }

    /// <summary>The desired C# class name.</summary>
    public string? ClassName { get; set; }

    /// <summary>The desired C# namespace.</summary>
    public string? Namespace { get; set; }
}

/// <summary>
/// Request to migrate a message between schema versions.
/// </summary>
public sealed class SchemaMigrationMigrateRequest
{
    /// <summary>The schema subject name.</summary>
    public string? Subject { get; set; }

    /// <summary>The JSON message to migrate.</summary>
    public string? Message { get; set; }

    /// <summary>The source schema version.</summary>
    public int FromVersion { get; set; }

    /// <summary>The target schema version.</summary>
    public int ToVersion { get; set; }
}

/// <summary>
/// Response from a schema migration operation.
/// </summary>
public sealed class SchemaMigrationResponse
{
    /// <summary>The original JSON message.</summary>
    public required string OriginalMessage { get; init; }

    /// <summary>The migrated JSON message.</summary>
    public required string MigratedMessage { get; init; }

    /// <summary>Source schema version.</summary>
    public int FromVersion { get; init; }

    /// <summary>Target schema version.</summary>
    public int ToVersion { get; init; }

    /// <summary>The schema subject.</summary>
    public required string Subject { get; init; }
}

/// <summary>
/// Request to test migration with inline schemas.
/// </summary>
public sealed class SchemaMigrationTestRequest
{
    /// <summary>The JSON message to migrate.</summary>
    public string? Message { get; set; }

    /// <summary>The source JSON Schema.</summary>
    public string? FromSchema { get; set; }

    /// <summary>The target JSON Schema.</summary>
    public string? ToSchema { get; set; }
}

/// <summary>
/// Response from a schema migration test.
/// </summary>
public sealed class SchemaMigrationTestResponse
{
    /// <summary>The original JSON message.</summary>
    public required string OriginalMessage { get; init; }

    /// <summary>The migrated JSON message, or null on failure.</summary>
    public string? MigratedMessage { get; init; }

    /// <summary>Whether the migration succeeded.</summary>
    public bool Success { get; init; }

    /// <summary>Error message if migration failed.</summary>
    public string? Error { get; init; }
}
