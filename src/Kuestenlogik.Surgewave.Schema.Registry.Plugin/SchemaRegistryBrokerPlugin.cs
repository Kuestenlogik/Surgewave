using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Handlers;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry.Plugin;

/// <summary>
/// Standalone <see cref="IBrokerPlugin"/> for the Schema Registry — installable
/// via <c>surgewave plugin install schema-registry.swpkg</c>. No compile-time dependency
/// on the Broker project. Reads configuration directly from <see cref="IConfiguration"/>.
/// </summary>
public sealed class SchemaRegistryBrokerPlugin : IBrokerPlugin
{
    public string FeatureId => "Surgewave.SchemaRegistry";
    public string DisplayName => "Schema Registry";

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue("Surgewave:SchemaRegistry:Enabled", true);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        var externalUrl = configuration.GetValue<string>("Surgewave:SchemaRegistry:ExternalUrl");

        // External mode: proxy to remote registry
        if (!string.IsNullOrEmpty(externalUrl))
        {
            var url = externalUrl.TrimEnd('/');
            services.AddHttpClient("SchemaRegistryProxy", c => c.BaseAddress = new Uri(url + "/"));
            services.AddSingleton<ISchemaStore>(sp =>
            {
                var factory = sp.GetRequiredService<IHttpClientFactory>();
                return new RemoteSchemaStore(factory.CreateClient("SchemaRegistryProxy"));
            });

            services.AddSurgewaveSchemaInference(new SchemaInferenceConfig
            {
                Enabled = configuration.GetValue("Surgewave:SchemaRegistry:Inference:Enabled", true),
                SampleSize = configuration.GetValue("Surgewave:SchemaRegistry:Inference:SampleSize", 100),
                RefreshIntervalSeconds = configuration.GetValue("Surgewave:SchemaRegistry:Inference:RefreshIntervalSeconds", 60),
                AutoRegister = configuration.GetValue("Surgewave:SchemaRegistry:Inference:AutoRegister", true)
            });
            return;
        }

        // Embedded mode
        var dataPath = configuration.GetValue<string>("Surgewave:SchemaRegistry:DataPath")
            ?? Path.Combine(configuration.GetValue<string>("Surgewave:DataDirectory") ?? "./data", "schemas");
        var compat = configuration.GetValue<string>("Surgewave:SchemaRegistry:DefaultCompatibility") ?? "Backward";
        _ = Enum.TryParse<CompatibilityMode>(compat, ignoreCase: true, out var compatMode);

        services.AddSurgewaveSchemaRegistry(new SchemaRegistryConfig
        {
            DataPath = dataPath,
            DefaultCompatibility = compatMode
        });

        services.AddAvroSchemaHandler();
        services.AddJsonSchemaHandler();
        services.AddProtobufSchemaHandler();
        services.AddFlatBuffersSchemaHandler();

        services.AddSurgewaveSchemaInference(new SchemaInferenceConfig
        {
            Enabled = configuration.GetValue("Surgewave:SchemaRegistry:Inference:Enabled", true),
            SampleSize = configuration.GetValue("Surgewave:SchemaRegistry:Inference:SampleSize", 100),
            RefreshIntervalSeconds = configuration.GetValue("Surgewave:SchemaRegistry:Inference:RefreshIntervalSeconds", 60),
            AutoRegister = configuration.GetValue("Surgewave:SchemaRegistry:Inference:AutoRegister", true)
        });

        services.AddSurgewaveSchemaEvolution(new SchemaEvolutionConfig
        {
            Enabled = configuration.GetValue("Surgewave:SchemaEvolution:Enabled", false),
            CheckIntervalSeconds = configuration.GetValue("Surgewave:SchemaEvolution:CheckIntervalSeconds", 60),
            AutoGenerateCode = configuration.GetValue("Surgewave:SchemaEvolution:AutoGenerateCode", true),
            NotifyAssistant = configuration.GetValue("Surgewave:SchemaEvolution:NotifyAssistant", true),
            DefaultNamespace = configuration.GetValue<string>("Surgewave:SchemaEvolution:DefaultNamespace") ?? "Surgewave.Models"
        });

        services.AddSurgewaveSchemaMigration(new SchemaMigrationConfig
        {
            Enabled = configuration.GetValue("Surgewave:SchemaMigration:Enabled", false),
            AutoMigrateOnRead = configuration.GetValue("Surgewave:SchemaMigration:AutoMigrateOnRead", true),
            AutoMigrateOnWrite = configuration.GetValue("Surgewave:SchemaMigration:AutoMigrateOnWrite", false),
            MaxCachedMigrators = configuration.GetValue("Surgewave:SchemaMigration:MaxCachedMigrators", 100)
        });

        var linkingEnabled = configuration.GetValue("Surgewave:SchemaLinking:Enabled", false);
        if (linkingEnabled)
        {
            var linkingStatePath = Path.Combine(
                configuration.GetValue<string>("Surgewave:DataDirectory") ?? "./data", "schema-linking-state.json");
            services.AddSurgewaveSchemaLinking(
                new SchemaLinkingConfig { Enabled = true },
                localClusterId: configuration.GetValue<string>("Surgewave:ClusterId"),
                statePath: linkingStatePath);
        }
    }

    public void Configure(object host, IServiceProvider services)
    {
        var app = (WebApplication)host;
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("Surgewave.SchemaRegistry");

        var externalUrl = app.Configuration.GetValue<string>("Surgewave:SchemaRegistry:ExternalUrl");
        if (!string.IsNullOrEmpty(externalUrl))
        {
            var url = externalUrl.TrimEnd('/');
            var httpClientFactory = services.GetRequiredService<IHttpClientFactory>();
            var httpClient = httpClientFactory.CreateClient("SchemaRegistryProxy");
            httpClient.BaseAddress = new Uri(url + "/");

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

            logger.LogInformation("Schema Registry proxy → {ExternalUrl}", url);
            return;
        }

        app.MapSurgewaveSchemaRegistry();
        logger.LogInformation("Schema Registry enabled at /subjects, /schemas, /config");
    }

    private static bool HasExternalUrl(IConfiguration configuration)
        => !string.IsNullOrEmpty(configuration.GetValue<string>("Surgewave:SchemaRegistry:ExternalUrl"));
}
