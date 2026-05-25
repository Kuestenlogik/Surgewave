using Kuestenlogik.Surgewave.Schema.Registry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Extension methods for adding Surgewave Schema Registry to an application.
/// </summary>
public static class SchemaRegistryExtensions
{
    /// <summary>
    /// Adds Surgewave Schema Registry with default configuration.
    /// Configuration is read from "Surgewave:SchemaRegistry" section.
    /// </summary>
    public static IHostApplicationBuilder AddSurgewaveSchemaRegistry(this IHostApplicationBuilder builder)
    {
        return builder.AddSurgewaveSchemaRegistry("Surgewave:SchemaRegistry");
    }

    /// <summary>
    /// Adds Surgewave Schema Registry with configuration from a named section.
    /// </summary>
    public static IHostApplicationBuilder AddSurgewaveSchemaRegistry(
        this IHostApplicationBuilder builder,
        string configurationSectionName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrEmpty(configurationSectionName);

        var section = builder.Configuration.GetSection(configurationSectionName);
        builder.Services.Configure<SchemaRegistryOptions>(section);
        builder.Services.AddSchemaRegistryCore();

        return builder;
    }

    /// <summary>
    /// Adds Surgewave Schema Registry with configuration action.
    /// </summary>
    public static IHostApplicationBuilder AddSurgewaveSchemaRegistry(
        this IHostApplicationBuilder builder,
        Action<SchemaRegistryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);
        builder.Services.AddSchemaRegistryCore();

        return builder;
    }

    private static IServiceCollection AddSchemaRegistryCore(this IServiceCollection services)
    {
        if (services.Any(s => s.ServiceType == typeof(SchemaStore)))
        {
            return services;
        }

        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<SchemaRegistryOptions>>();
            var logger = sp.GetRequiredService<ILogger<SchemaStore>>();
            return new SchemaStore(logger, options.Value.DataDirectory);
        });

        return services;
    }
}

/// <summary>
/// Schema Registry configuration options.
/// </summary>
public sealed class SchemaRegistryOptions
{
    /// <summary>
    /// Default compatibility mode for schemas.
    /// Options: "None", "Backward", "BackwardTransitive", "Forward", "ForwardTransitive", "Full", "FullTransitive".
    /// Default: "Backward".
    /// </summary>
    public string CompatibilityMode { get; set; } = "Backward";

    /// <summary>
    /// Storage backend: "Memory" or "File".
    /// Default: "Memory".
    /// </summary>
    public string Storage { get; set; } = "Memory";

    /// <summary>
    /// Data directory for file-based storage.
    /// </summary>
    public string? DataDirectory { get; set; }
}
